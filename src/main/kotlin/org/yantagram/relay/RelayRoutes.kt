package org.yantagram.relay

import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readBytes
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.flow.collect
import java.nio.ByteBuffer
import java.security.MessageDigest

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

    routing {
        // Publisher endpoint: first binary frame must be the shared secret.
        webSocket("/publish") {
            try {
                val first = incoming.receive()
                if (first !is Frame.Binary) {
                    close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "binary expected"))
                    return@webSocket
                }
                if (!constantTimeEquals(first.readBytes(), config.publishSecret)) {
                    close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "unauthorized"))
                    return@webSocket
                }

                for (frame in incoming) {
                    if (frame is Frame.Binary) {
                        ring.append(frame.readBytes())
                    }
                    // Silently ignore non-binary frames.
                }
            } catch (_: ClosedReceiveChannelException) {
                // peer closed; nothing to do
            }
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





