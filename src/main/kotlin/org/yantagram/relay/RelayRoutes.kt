package org.yantagram.relay

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger("org.yantagram.relay.RingStats")
private val osBean = ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean

private const val MAX_VERIFICATION_KEY_LENGTH = 64

/** Constant-time byte-array equality, length-aware. */
private fun constantTimeEquals(a: ByteArray, b: ByteArray): Boolean {
    val ad = MessageDigest.getInstance("SHA-256").digest(a)
    val bd = MessageDigest.getInstance("SHA-256").digest(b)
    return MessageDigest.isEqual(ad, bd) && a.size == b.size
}

/** Application-level heartbeat: seq=0, keyLen=0, empty payload — 10 bytes. */
private val HEARTBEAT_FRAME = Frame.Binary(true, ByteArray(10))

/** Encode `[seq:8 BE][keyLen:2 BE][key bytes][payload]` for delivery to subscribers. */
private fun encodeOutbound(packet: Packet): ByteArray {
    val keyBytes = packet.verificationKey?.toByteArray(Charsets.UTF_8) ?: ByteArray(0)
    val buf = ByteBuffer.allocate(8 + 2 + keyBytes.size + packet.payload.size)
    buf.putLong(packet.seq)
    buf.putShort(keyBytes.size.toShort())
    buf.put(keyBytes)
    buf.put(packet.payload)
    return buf.array()
}

/** Creates a [RingLimitProvider] that dynamically computes the ring limit from JVM heap. */
private fun jvmAwareLimitProvider(config: RelayConfig): RingLimitProvider = RingLimitProvider {
    val runtime = Runtime.getRuntime()
    val maxHeap = runtime.maxMemory()
    val fractionLimit = (maxHeap * config.ringHeapFraction).toLong()
    val hardCap = config.ringHardCapBytes
    if (hardCap != null) minOf(fractionLimit, hardCap) else fractionLimit
}

fun Application.relayModule(
    config: RelayConfig,
    ring: PacketRing = PacketRing(jvmAwareLimitProvider(config)),
) {
    install(WebSockets) {
        maxFrameSize = config.maxFrameBytes
        pingPeriodMillis = 30_000
        timeoutMillis = 60_000
    }

    openApiModule()

    // Periodically log ring memory usage.
    CoroutineScope(Dispatchers.Default).launch {
        while (true) {
            delay(30.seconds)
            val runtime = Runtime.getRuntime()
            val jvmUsed = runtime.totalMemory() - runtime.freeMemory()
            val jvmMax = runtime.maxMemory()
            @Suppress("DEPRECATION")
            val osFree = osBean.freePhysicalMemorySize
            @Suppress("DEPRECATION")
            val osTotal = osBean.totalPhysicalMemorySize

            val effectiveMax = jvmAwareLimitProvider(config).effectiveMaxBytes()

            logger.info(
                "ring: {} / {} MB ({} packets, seq={}) | jvm: {} / {} MB | os: {} / {} MB free",
                ring.currentBytes() / 1_048_576,
                effectiveMax / 1_048_576,
                ring.packetCount(),
                ring.latestSeq(),
                jvmUsed / 1_048_576,
                jvmMax / 1_048_576,
                osFree / 1_048_576,
                osTotal / 1_048_576,
            )
        }
    }

    routing {
        // Publisher endpoint: POST binary payload with X-Publish-Secret header.
        post("/publish") {
            val headerSecret = call.request.headers["X-Publish-Secret"]?.toByteArray(Charsets.UTF_8)
            if (headerSecret == null || !constantTimeEquals(headerSecret, config.publishSecret)) {
                call.respond(HttpStatusCode.Unauthorized, "unauthorized")
                return@post
            }

            val verificationKey = call.request.headers["X-Verification-Key"]
            if (verificationKey != null && verificationKey.length > MAX_VERIFICATION_KEY_LENGTH) {
                call.respond(HttpStatusCode.BadRequest, "verification key exceeds $MAX_VERIFICATION_KEY_LENGTH characters")
                return@post
            }

            val body = call.receive<ByteArray>()
            if (body.isEmpty()) {
                call.respond(HttpStatusCode.BadRequest, "empty payload")
                return@post
            }

            ring.append(body, verificationKey)
            call.respond(HttpStatusCode.NoContent)
        }

        // Subscriber endpoint: optional ?since=<seq> and ?keys=<comma-separated> for filtering.
        webSocket("/subscribe") {
            val since = call.request.queryParameters["since"]?.toLongOrNull() ?: 0L
            val keys = call.request.queryParameters["keys"]
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotEmpty() }
                ?.toSet()

            // Server→client keep-alive: send heartbeat every N seconds.
            if (config.keepAlivePeriodSeconds > 0) {
                launch {
                    while (true) {
                        delay(config.keepAlivePeriodSeconds.seconds)
                        outgoing.send(HEARTBEAT_FRAME)
                    }
                }
            }

            // Client heartbeat watchdog: close if no message received within timeout.
            val lastClientMessage = AtomicLong(System.currentTimeMillis())
            if (config.clientHeartbeatTimeoutSeconds > 0) {
                launch {
                    val checkInterval = (config.clientHeartbeatTimeoutSeconds.seconds / 2)
                    while (true) {
                        delay(checkInterval)
                        val elapsed = System.currentTimeMillis() - lastClientMessage.get()
                        if (elapsed > config.clientHeartbeatTimeoutSeconds * 1000) {
                            close(CloseReason(CloseReason.Codes.GOING_AWAY, "client heartbeat timeout"))
                            return@launch
                        }
                    }
                }
            }

            // Drain incoming frames (client heartbeats) in background.
            launch {
                try {
                    for (frame in incoming) {
                        lastClientMessage.set(System.currentTimeMillis())
                    }
                } catch (_: ClosedReceiveChannelException) {
                    // peer closed
                }
            }

            // Replay + live stream.
            val replay = ring.snapshotSince(since, keys)
            var lastDelivered = since
            for (p in replay) {
                outgoing.send(Frame.Binary(true, encodeOutbound(p)))
                lastDelivered = p.seq
            }

            try {
                ring.stream.collect { p ->
                    if (p.seq > lastDelivered && (keys == null || p.verificationKey in keys)) {
                        outgoing.send(Frame.Binary(true, encodeOutbound(p)))
                        lastDelivered = p.seq
                    }
                }
            } catch (_: ClosedReceiveChannelException) {
                // peer closed
            }
        }
    }
}
