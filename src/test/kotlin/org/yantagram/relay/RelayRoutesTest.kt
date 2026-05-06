package org.yantagram.relay

import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
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
import kotlin.time.Duration.Companion.milliseconds

class RelayRoutesTest {

    private val secret = "test-secret".toByteArray()
    private val secretStr = "test-secret"

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
        val wsClient = createClient { install(ClientWebSockets) }

        val sub = wsClient.webSocketSession("/subscribe")
        delay(150.milliseconds)

        val response = client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            contentType(ContentType.Application.OctetStream)
            setBody("hello".toByteArray())
        }
        assertEquals(HttpStatusCode.NoContent, response.status)

        val (seq, payload) = withTimeout(2000.milliseconds) {
            val frame = sub.incoming.receive() as Frame.Binary
            decode(frame.readBytes())
        }

        assertEquals(1L, seq)
        assertContentEquals("hello".toByteArray(), payload)

        sub.close()
    }

    @Test
    fun `wrong publish secret is rejected`() = testApplication {
        val cfg = testConfig()
        application { relayModule(cfg, PacketRing(cfg.ringMaxBytes)) }

        val response = client.post("/publish") {
            header("X-Publish-Secret", "wrong")
            contentType(ContentType.Application.OctetStream)
            setBody("hello".toByteArray())
        }
        assertEquals(HttpStatusCode.Unauthorized, response.status)
    }

    @Test
    fun `publish without secret header is rejected`() = testApplication {
        val cfg = testConfig()
        application { relayModule(cfg, PacketRing(cfg.ringMaxBytes)) }

        val response = client.post("/publish") {
            contentType(ContentType.Application.OctetStream)
            setBody("hello".toByteArray())
        }
        assertEquals(HttpStatusCode.Unauthorized, response.status)
    }

    @Test
    fun `subscribe with since replays only newer packets`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val wsClient = createClient { install(ClientWebSockets) }

        runBlocking {
            ring.append("a".toByteArray())
            ring.append("b".toByteArray())
            ring.append("c".toByteArray())
        }

        val sub = wsClient.webSocketSession("/subscribe?since=1")
        val received = mutableListOf<Pair<Long, ByteArray>>()
        withTimeout(2000.milliseconds) {
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


