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
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

data class DecodedPacket(val seq: Long, val verificationKey: String?, val payload: ByteArray)

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

    /** Decode `[seq:8 BE][keyLen:2 BE][key bytes][payload]` */
    private fun decode(bytes: ByteArray): DecodedPacket {
        val buf = ByteBuffer.wrap(bytes)
        val seq = buf.long
        val keyLen = buf.short.toInt() and 0xFFFF
        val key = if (keyLen > 0) {
            val keyBytes = ByteArray(keyLen)
            buf.get(keyBytes)
            String(keyBytes, Charsets.UTF_8)
        } else null
        val payload = ByteArray(buf.remaining())
        buf.get(payload)
        return DecodedPacket(seq, key, payload)
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

        val decoded = withTimeout(2000.milliseconds) {
            val frame = sub.incoming.receive() as Frame.Binary
            decode(frame.readBytes())
        }

        assertEquals(1L, decoded.seq)
        assertNull(decoded.verificationKey)
        assertContentEquals("hello".toByteArray(), decoded.payload)

        sub.close()
    }

    @Test
    fun `publish with verification key is delivered to subscriber`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val wsClient = createClient { install(ClientWebSockets) }

        val sub = wsClient.webSocketSession("/subscribe")
        delay(150.milliseconds)

        val response = client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", "user-abc")
            contentType(ContentType.Application.OctetStream)
            setBody("data".toByteArray())
        }
        assertEquals(HttpStatusCode.NoContent, response.status)

        val decoded = withTimeout(2000.milliseconds) {
            val frame = sub.incoming.receive() as Frame.Binary
            decode(frame.readBytes())
        }

        assertEquals(1L, decoded.seq)
        assertEquals("user-abc", decoded.verificationKey)
        assertContentEquals("data".toByteArray(), decoded.payload)

        sub.close()
    }

    @Test
    fun `subscriber with keys filter only receives matching packets`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val wsClient = createClient { install(ClientWebSockets) }

        // Subscribe filtered to "alice" only
        val sub = wsClient.webSocketSession("/subscribe?keys=alice")
        delay(150.milliseconds)

        // Publish packets with different keys
        client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", "bob")
            contentType(ContentType.Application.OctetStream)
            setBody("bob-data".toByteArray())
        }
        client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", "alice")
            contentType(ContentType.Application.OctetStream)
            setBody("alice-data".toByteArray())
        }
        client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            contentType(ContentType.Application.OctetStream)
            setBody("no-key-data".toByteArray())
        }

        val decoded = withTimeout(2000.milliseconds) {
            val frame = sub.incoming.receive() as Frame.Binary
            decode(frame.readBytes())
        }

        assertEquals(2L, decoded.seq, "should receive seq=2 (alice), skipping seq=1 (bob)")
        assertEquals("alice", decoded.verificationKey)
        assertContentEquals("alice-data".toByteArray(), decoded.payload)

        sub.close()
    }

    @Test
    fun `subscriber with multiple keys filter receives all matching`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val wsClient = createClient { install(ClientWebSockets) }

        val sub = wsClient.webSocketSession("/subscribe?keys=alice,bob")
        delay(150.milliseconds)

        client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", "alice")
            contentType(ContentType.Application.OctetStream)
            setBody("a".toByteArray())
        }
        client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", "charlie")
            contentType(ContentType.Application.OctetStream)
            setBody("c".toByteArray())
        }
        client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", "bob")
            contentType(ContentType.Application.OctetStream)
            setBody("b".toByteArray())
        }

        val received = mutableListOf<DecodedPacket>()
        withTimeout(2000.milliseconds) {
            repeat(2) {
                val frame = sub.incoming.receive() as Frame.Binary
                received += decode(frame.readBytes())
            }
        }

        assertEquals(listOf(1L, 3L), received.map { it.seq })
        assertEquals(listOf("alice", "bob"), received.map { it.verificationKey })

        sub.close()
    }

    @Test
    fun `replay with keys filter only returns matching packets`() = testApplication {
        val cfg = testConfig()
        val ring = PacketRing(cfg.ringMaxBytes)
        application { relayModule(cfg, ring) }
        val wsClient = createClient { install(ClientWebSockets) }

        runBlocking {
            ring.append("a".toByteArray(), "alice")
            ring.append("b".toByteArray(), "bob")
            ring.append("c".toByteArray(), "alice")
        }

        val sub = wsClient.webSocketSession("/subscribe?since=0&keys=alice")
        val received = mutableListOf<DecodedPacket>()
        withTimeout(2000.milliseconds) {
            repeat(2) {
                val frame = sub.incoming.receive() as Frame.Binary
                received += decode(frame.readBytes())
            }
        }
        sub.close()

        assertEquals(listOf(1L, 3L), received.map { it.seq })
        assertEquals(listOf("alice", "alice"), received.map { it.verificationKey })
    }

    @Test
    fun `verification key exceeding max length is rejected`() = testApplication {
        val cfg = testConfig()
        application { relayModule(cfg, PacketRing(cfg.ringMaxBytes)) }

        val longKey = "x".repeat(65)
        val response = client.post("/publish") {
            header("X-Publish-Secret", secretStr)
            header("X-Verification-Key", longKey)
            contentType(ContentType.Application.OctetStream)
            setBody("data".toByteArray())
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
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
        val received = mutableListOf<DecodedPacket>()
        withTimeout(2000.milliseconds) {
            repeat(2) {
                val frame = sub.incoming.receive() as Frame.Binary
                received += decode(frame.readBytes())
            }
        }
        sub.close()

        assertEquals(listOf(2L, 3L), received.map { it.seq })
        assertContentEquals("b".toByteArray(), received[0].payload)
        assertContentEquals("c".toByteArray(), received[1].payload)
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
