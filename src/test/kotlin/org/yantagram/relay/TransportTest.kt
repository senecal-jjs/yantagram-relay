package org.yantagram.relay

import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.Limits
import org.yantagram.relay.protocol.PublishRequest
import org.yantagram.relay.protocol.WireJson
import kotlin.test.Test
import kotlin.test.assertEquals

class TransportTest {
    private fun relayTest(relay: TestRelay = TestRelay(), block: suspend ApplicationTestBuilder.(TestRelay) -> Unit) =
        testApplication {
            application { relay.install(this) }
            block(relay)
        }

    private suspend fun ApplicationTestBuilder.post(path: String, body: String) = client.post(path) {
        contentType(ContentType.Application.Json)
        setBody(body)
    }

    private fun publishJson(request: PublishRequest) = WireJson.encodeToString(PublishRequest.serializer(), request)

    private suspend fun DefaultClientWebSocketSession.next(): JsonObject = withTimeout(5_000) {
        WireJson.parseToJsonElement((incoming.receive() as Frame.Text).readText()).jsonObject
    }

    private val JsonObject.type get() = this["type"]!!.jsonPrimitive.content

    @Test
    fun `HTTP publish and sync`() = relayTest {
        val key = StreamKey()
        val stored = post("/v2/publish", publishJson(key.publish(Lane.CONTROL, listOf(item("a"), item("b")))))
        assertEquals(HttpStatusCode.OK, stored.status)
        assertEquals("""{"tag":"${key.tag}","seqs":[1,2],"head":2}""", stored.bodyAsText())

        val sync = post("/v2/sync", """{"cursors":[{"tag":"${key.tag}","after":1}]}""")
        val stream = WireJson.parseToJsonElement(sync.bodyAsText()).jsonObject["streams"]!!.jsonArray.single().jsonObject
        assertEquals(listOf("b"), stream["items"]!!.jsonArray.map { bodyText(it.jsonObject["body"]!!.jsonPrimitive.content) })
        assertEquals(2, stream["head"]!!.jsonPrimitive.long)
    }

    @Test
    fun `HTTP errors carry a reason code and a fitting status`() = relayTest(TestRelay(limits = Limits(itemsPerWindow = 1))) {
        val key = StreamKey()
        val badSig = key.publish(Lane.CONTROL, listOf(item("a"))).copy(sig = "00".repeat(64))
        assertEquals(HttpStatusCode.BadRequest, post("/v2/publish", publishJson(badSig)).status)
        assertEquals("""{"reason":"bad-sig"}""", post("/v2/publish", publishJson(badSig)).bodyAsText())
        assertEquals(HttpStatusCode.BadRequest, post("/v2/publish", "{nope").status)

        post("/v2/publish", publishJson(key.publish(Lane.CONTROL, listOf(item("a")))))
        val limited = post("/v2/publish", publishJson(key.publish(Lane.CONTROL, listOf(item("b")))))
        assertEquals(HttpStatusCode.TooManyRequests, limited.status)
        assertEquals("""{"reason":"rate-limited"}""", limited.bodyAsText())
    }

    @Test
    fun `WebSocket sync catches up, then streams new items live, in order`() = relayTest { relay ->
        val key = StreamKey()
        val other = StreamKey()
        relay.service.publish(key.publish(Lane.CONTROL, listOf(item("old"))), "1.1.1.1")
        val ws = createClient { install(WebSockets) }

        ws.webSocket("/v2/ws") {
            send(Frame.Text("""{"type":"sync","req_id":7,"live":true,"cursors":[{"tag":"${key.tag}"}]}"""))
            val page = next()
            assertEquals("items", page.type)
            assertEquals(7, page["req_id"]!!.jsonPrimitive.long)
            assertEquals(listOf("old"), page["items"]!!.jsonArray.map { bodyText(it.jsonObject["body"]!!.jsonPrimitive.content) })
            val caughtUp = next()
            assertEquals("caught_up", caughtUp.type)
            assertEquals(1, caughtUp["head"]!!.jsonPrimitive.long)
            assertEquals("sync_done", next().type)

            // Published elsewhere (another connection, HTTP): arrives live.
            relay.service.publish(key.publish(Lane.CONTROL, listOf(item("new1"), item("new2"))), "1.1.1.1")
            relay.service.publish(other.publish(Lane.CONTROL, listOf(item("not subscribed"))), "1.1.1.1")
            val live = next()
            assertEquals("items", live.type)
            assertEquals(null, live["req_id"])
            assertEquals(listOf(2L, 3L), live["items"]!!.jsonArray.map { it.jsonObject["seq"]!!.jsonPrimitive.long })

            // Publishing on the socket answers with stored, then the live copy follows.
            val request = WireJson.encodeToJsonElement(PublishRequest.serializer(), key.publish(Lane.CONTROL, listOf(item("mine"))))
            val withType = JsonObject(request.jsonObject + mapOf("type" to kotlinx.serialization.json.JsonPrimitive("publish"), "req_id" to kotlinx.serialization.json.JsonPrimitive("p1")))
            send(Frame.Text(withType.toString()))
            val stored = next()
            assertEquals("stored", stored.type)
            assertEquals("p1", stored["req_id"]!!.jsonPrimitive.content)
            assertEquals(4, stored["head"]!!.jsonPrimitive.long)
            assertEquals(listOf(4L), next()["items"]!!.jsonArray.map { it.jsonObject["seq"]!!.jsonPrimitive.long })

            send(Frame.Text("""{"type":"bogus","req_id":1}"""))
            val rejected = next()
            assertEquals("rejected", rejected.type)
            assertEquals("bad-type", rejected["reason"]!!.jsonPrimitive.content)

            send(Frame.Text("""{"type":"unsubscribe","req_id":2,"tags":["${key.tag}"]}"""))
            assertEquals("unsubscribed", next().type)
            relay.service.publish(key.publish(Lane.CONTROL, listOf(item("after unsubscribe"))), "1.1.1.1")
            send(Frame.Text("""{"type":"have","req_id":3,"tag":"${key.tag}","ids":[]}"""))
            assertEquals("have", next().type) // nothing live came first
        }
        assertEquals(0, relay.service.bus.subscribedTags)
    }
}
