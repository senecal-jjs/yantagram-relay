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
import io.ktor.websocket.Frame
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.security.MessageDigest
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger("org.yantagram.relay.RingStats")

/** Constant-time byte-array equality, length-aware. */
private fun constantTimeEquals(a: ByteArray, b: ByteArray): Boolean {
    val ad = MessageDigest.getInstance("SHA-256").digest(a)
    val bd = MessageDigest.getInstance("SHA-256").digest(b)
    return MessageDigest.isEqual(ad, bd) && a.size == b.size
}

/** Encode `[seq:8 BE][payload]` for delivery to subscribers. */
private fun encodeOutbound(packet: Packet): ByteArray {
    val buf = ByteBuffer.allocate(8 + packet.payload.size)
    buf.putLong(packet.seq)
    buf.put(packet.payload)
    return buf.array()
}

fun Application.relayModule(
    config: RelayConfig,
    ring: PacketRing = PacketRing(config.ringMaxBytes),
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
            logger.info(
                "ring: {} bytes / {} max ({} packets, seq={})",
                ring.currentBytes(),
                config.ringMaxBytes,
                ring.packetCount(),
                ring.latestSeq(),
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

            val body = call.receive<ByteArray>()
            if (body.isEmpty()) {
                call.respond(HttpStatusCode.BadRequest, "empty payload")
                return@post
            }

            ring.append(body)
            call.respond(HttpStatusCode.NoContent)
        }

        // Subscriber endpoint: optional ?since=<seq> for catch-up replay.
        webSocket("/subscribe") {
            val since = call.request.queryParameters["since"]?.toLongOrNull() ?: 0L

            // Subscribe to the live stream BEFORE replay so we don't miss anything.
            // Snapshot first; live collector will skip anything <= the highest replayed seq.
            val replay = ring.snapshotSince(since)
            var lastDelivered = since
            for (p in replay) {
                outgoing.send(Frame.Binary(true, encodeOutbound(p)))
                lastDelivered = p.seq
            }

            try {
                ring.stream.collect { p ->
                    if (p.seq > lastDelivered) {
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
