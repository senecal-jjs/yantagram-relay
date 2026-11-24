package org.yantagram.relay

import fr.acinq.secp256k1.Secp256k1
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.yantagram.relay.bridge.Nostr
import org.yantagram.relay.bridge.NostrEvent
import org.yantagram.relay.bridge.StrfryBridge
import org.yantagram.relay.protocol.Codec
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.SyncCursor
import org.yantagram.relay.protocol.SyncRequest
import org.yantagram.relay.protocol.WireJson
import org.yantagram.relay.store.MetaStore
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BridgeTest {
    private val legacyTag = "ab".repeat(16)

    @Test
    fun `forwarded events are valid NIP-01 events the strfry policy accepts`() {
        val (id, message) = Nostr.eventMessage(legacyTag, "AAEC", "cd".repeat(16), createdAt = 1_800_000_000)
        val event = WireJson.parseToJsonElement(message).jsonArray[1].jsonObject
        val pubkey = event["pubkey"]!!.jsonPrimitive.content
        val serialized = JsonArray(
            listOf(
                kotlinx.serialization.json.JsonPrimitive(0), event["pubkey"]!!, event["created_at"]!!, event["kind"]!!,
                event["tags"]!!, event["content"]!!,
            ),
        ).toString()
        val digest = MessageDigest.getInstance("SHA-256").digest(serialized.toByteArray())
        assertEquals(id, Codec.hex(digest))
        assertEquals(id, event["id"]!!.jsonPrimitive.content)
        assertTrue(Secp256k1.verifySchnorr(Codec.hexToBytes(event["sig"]!!.jsonPrimitive.content), digest, Codec.hexToBytes(pubkey)))

        val tags = event["tags"]!!.jsonArray.associate { t -> t.jsonArray[0].jsonPrimitive.content to t.jsonArray[1].jsonPrimitive.content }
        assertEquals(legacyTag, tags["y"])
        assertEquals("cd".repeat(16), tags["s"])
        assertEquals(1_800_000_000 + Nostr.MAX_TTL_SECONDS, tags["expiration"]!!.toLong())
        assertEquals(Nostr.KIND.toLong(), event["kind"]!!.jsonPrimitive.long)
    }

    @Test
    fun `imports strfry events into legacy streams, once, skipping our own`() = runTest {
        val relay = TestRelay(bridgeEnabled = true)
        val bridge = StrfryBridge(io.ktor.client.HttpClient(), "ws://unused", relay.store, MetaStore(relay.db), relay.service, relay.metrics)
        fun event(text: String) = NostrEvent(
            id = text, createdAt = 1, kind = Nostr.KIND, routingTag = legacyTag, senderMarker = null, expiration = null,
            content = Base64.getEncoder().encodeToString(text.toByteArray()),
        )
        bridge.import(event("from an old client"))
        bridge.import(event("from an old client")) // strfry replays an overlap on reconnect

        // Something a new client published with this legacy tag and the bridge forwarded:
        relay.service.publish(StreamKey().publish(Lane.GROUP, listOf(item("from a new client")), legacyTag), "1.1.1.1")
        relay.store.outboxDone(relay.store.outbox(1).single())
        bridge.import(event("from a new client"))

        val items = relay.service.sync(SyncRequest(listOf(SyncCursor(legacyTag)))).streams.single().items
        assertEquals(listOf("from an old client"), items.map { bodyText(it.body) })
    }

    @Test
    fun `forwards queued items to strfry and removes them once strfry says OK`() = testApplication {
        val received = CopyOnWriteArrayList<JsonObject>()
        install(io.ktor.server.websocket.WebSockets)
        routing {
            webSocket("/strfry") {
                for (frame in incoming) {
                    val message = WireJson.parseToJsonElement((frame as Frame.Text).readText()).jsonArray
                    if (message[0].jsonPrimitive.content != "EVENT") continue
                    val event = message[1].jsonObject
                    received += event
                    val id = event["id"]!!.jsonPrimitive.content
                    // Refuse the first attempt at one event, as a busy strfry would.
                    val accepted = received.size != 1
                    val reason = if (accepted) "" else "rate-limited: slow down"
                    send(Frame.Text("""["OK","$id",$accepted,"$reason"]"""))
                }
            }
        }
        startApplication()
        val relay = TestRelay(bridgeEnabled = true)
        val client = createClient { install(WebSockets) }
        val bridge = StrfryBridge(client, "/strfry", relay.store, MetaStore(relay.db), relay.service, relay.metrics)
        relay.service.publish(StreamKey().publish(Lane.CONTROL, listOf(item("op1"), item("op2")), legacyTag), "1.1.1.1")

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            bridge.start(scope)
            bridge.poke()
            withTimeout(15_000) {
                while (relay.store.outboxSize() > 0) delay(50)
            }
        } finally {
            scope.cancel()
        }
        val contents = received.map { bodyText(it["content"]!!.jsonPrimitive.content) }
        assertEquals(listOf("op1", "op1", "op2"), contents)
        assertTrue(received.all { e -> e["tags"]!!.jsonArray.any { it.jsonArray[1].jsonPrimitive.content == legacyTag } })
    }
}
