package org.yantagram.relay

import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.server.testing.testApplication
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readBytes
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RelayRoutesTest {

    private val secret = "test-secret".toByteArray()

    private fun testConfig(ringBytes: Long = 1024L, maxFrame: Long = 1L * 1024 * 1024) =
        RelayConfig(
            host = "127.0.0.1",
            port = 0,
            publishSecret = secret,
            ringMaxBytes = ringBytes,
            maxFrameBytes = maxFrame,
        )

    private fun decode(bytes: ByteArray): Pair<Long, ByteArray> {
        val buf = ByteBuffer.wrap(bytes)
        val seq = buf.long
        val payload = ByteArray(bytes.size - 8)
        buf.get(payload)
        return seq to payload
    }

    @Test
    fun `publish then live subscribe receives the packet`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val client = createClient { install(ClientWebSockets) }

        val sub = client.webSocketSession("/subscribe")
        delay(150)

        val pub = client.webSocketSession("/publish")
        pub.send(Frame.Binary(true, secret))
        pub.send(Frame.Binary(true, "hello".toByteArray()))

        val (seq, payload) = withTimeout(2000) {
            val frame = sub.incoming.receive() as Frame.Binary
            decode(frame.readBytes())
        }

        assertEquals(1L, seq)
        assertContentEquals("hello".toByteArray(), payload)

        pub.close()
        sub.close()
    }

    @Test
    fun `wrong publish secret is rejected`() = testApplication {
        val cfg = testConfig()
        application { relayModule(cfg, PacketRing(cfg.ringMaxBytes)) }
        val client = createClient { install(ClientWebSockets) }

        val pub = client.webSocketSession("/publish")
        pub.send(Frame.Binary(true, "wrong".toByteArray()))
        val closed = withTimeout(2000) { pub.closeReason.await() }
        assertTrue(closed != null)
    }

    @Test
    fun `subscribe with since replays only newer packets`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val client = createClient { install(ClientWebSockets) }

        runBlocking {
            ring.append("a".toByteArray())
            ring.append("b".toByteArray())
            ring.append("c".toByteArray())
        }

        val sub = client.webSocketSession("/subscribe?since=1")
        val received = mutableListOf<Pair<Long, ByteArray>>()
        withTimeout(2000) {
            repeat(2) {
                val frame = sub.incoming.receive() as Frame.Binary
                received += decode(frame.readBytes())
            }
        }
        sub.close()

        assertEquals(listOf(2L, 3L), received.map { it.first })
        assertContentEquals("b".toByteArray(), received[0].second)
        assertContentEquals("c".toByteArray(), received[1].second)
    }

    @Test
    fun `ring evicts oldest past max bytes`() = runBlocking {
        val ring = PacketRing(maxBytes = 4)
        ring.append(byteArrayOf(1, 2))
        ring.append(byteArrayOf(3, 4))
        ring.append(byteArrayOf(5, 6))
        val snap = ring.snapshotSince(0)
        assertEquals(listOf(2L, 3L), snap.map { it.seq })
    }

    @Test
    fun `latest seq tracks appends`() = runBlocking {
        val ring = PacketRing(maxBytes = 1024)
        assertEquals(0L, ring.latestSeq())
        ring.append(byteArrayOf(1))
        ring.append(byteArrayOf(2))
        assertEquals(2L, ring.latestSeq())
    }

    @Test
    fun `snapshot returns empty when nothing newer`() {
        val ring = PacketRing(maxBytes = 1024)
        runBlocking { ring.append(byteArrayOf(1)) }
        assertTrue(ring.snapshotSince(10).isEmpty())
    }
}


