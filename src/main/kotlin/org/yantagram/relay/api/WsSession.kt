package org.yantagram.relay.api

import io.ktor.websocket.DefaultWebSocketSession
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.serializer
import org.slf4j.LoggerFactory
import org.yantagram.relay.Metrics
import org.yantagram.relay.protocol.CheckpointRequest
import org.yantagram.relay.protocol.Codec
import org.yantagram.relay.protocol.DeleteRequest
import org.yantagram.relay.protocol.FetchRequest
import org.yantagram.relay.protocol.HaveRequest
import org.yantagram.relay.protocol.HeadListener
import org.yantagram.relay.protocol.PublishRequest
import org.yantagram.relay.protocol.RelayService
import org.yantagram.relay.protocol.RetireRequest
import org.yantagram.relay.protocol.SyncRequest
import org.yantagram.relay.protocol.UnsubscribeRequest
import org.yantagram.relay.protocol.WireItem
import org.yantagram.relay.protocol.WireJson
import org.yantagram.relay.store.Rejected

/**
 * One WebSocket connection. Everything it does runs on one coroutine, reading from one
 * queue (client messages and stream-head notifications), so responses and live items go
 * out in order and a stream's items are never interleaved or repeated.
 *
 * Messages to the client:
 *   {"type":"stored","req_id",tag,seqs,head}       publish committed (fsynced)
 *   {"type":"items","req_id"?,tag,items:[...]}     sync pages, and live items (no req_id)
 *   {"type":"caught_up","req_id",tag,head,low}     one per stream in a sync
 *   {"type":"sync_done","req_id"}                  after every stream's caught_up
 *   {"type":"fetched"|"checkpoint"|"have"|"deleted"|"retired"|"unsubscribed","req_id",...}
 *   {"type":"rejected","req_id",reason}
 */
class WsSession(
    private val socket: DefaultWebSocketSession,
    private val service: RelayService,
    private val metrics: Metrics,
    private val ip: String,
) {
    private sealed interface Event {
        class Message(val text: String) : Event
        class Head(val tag: String, val head: Long) : Event
    }

    private val log = LoggerFactory.getLogger(WsSession::class.java)
    private val events = Channel<Event>(Channel.UNLIMITED)
    private val listener = HeadListener { tag, head -> events.trySend(Event.Head(tag, head)) }

    /** Live streams and the last seq sent on each. */
    private val live = HashMap<String, Long>()

    suspend fun run() = coroutineScope {
        val reader = launch {
            try {
                for (frame in socket.incoming) {
                    if (frame is Frame.Text) events.send(Event.Message(frame.readText()))
                }
            } finally {
                events.close()
            }
        }
        try {
            for (event in events) {
                when (event) {
                    is Event.Message -> handle(event.text)
                    is Event.Head -> if ((live[event.tag] ?: continue) < event.head) drain(event.tag, reqId = null)
                }
            }
        } finally {
            live.keys.forEach { service.bus.unsubscribe(it, listener) }
            live.clear()
            reader.cancel()
        }
    }

    private suspend fun handle(text: String) {
        val message = try {
            WireJson.parseToJsonElement(text).jsonObject
        } catch (_: Exception) {
            return send("rejected", null, ErrorBody("bad-json"))
        }
        val type = message["type"]?.jsonPrimitive?.contentOrNull
        val reqId = message["req_id"]
        if (type != null) metrics.request(type, "ws")
        try {
            when (type) {
                "publish" -> send("stored", reqId, service.publish(decode<PublishRequest>(message), ip))
                "sync" -> sync(decode<SyncRequest>(message), reqId)
                "unsubscribe" -> {
                    decode<UnsubscribeRequest>(message).tags.forEach { tag ->
                        if (live.remove(tag) != null) service.bus.unsubscribe(tag, listener)
                    }
                    send("unsubscribed", reqId, Empty)
                }
                "fetch" -> send("fetched", reqId, service.fetch(decode<FetchRequest>(message)))
                "checkpoint" -> send("checkpoint", reqId, service.latestCheckpoint(decode<CheckpointRequest>(message)))
                "have" -> send("have", reqId, service.have(decode<HaveRequest>(message)))
                "delete" -> send("deleted", reqId, service.delete(decode<DeleteRequest>(message)))
                "retire" -> send("retired", reqId, service.retire(decode<RetireRequest>(message)))
                else -> throw Rejected("bad-type")
            }
        } catch (e: Rejected) {
            metrics.rejected(type ?: "unknown", e.reason)
            send("rejected", reqId, ErrorBody(e.reason))
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Exception) {
            // The class only: messages can quote request contents.
            log.error("{} failed: {}", type, e.javaClass.name)
            metrics.rejected(type ?: "unknown", "internal")
            send("rejected", reqId, ErrorBody("internal"))
        }
    }

    private suspend fun sync(request: SyncRequest, reqId: JsonElement?) {
        val ordered = service.orderedCursors(request.cursors)
        if (request.live && live.size + ordered.count { it.first.tag !in live } > MAX_LIVE_STREAMS) {
            throw Rejected("too-many-streams")
        }
        for ((cursor, state) in ordered) {
            // Subscribe before reading, so nothing stored in between is missed; a
            // notification for something already sent is ignored.
            if (request.live && live.put(cursor.tag, cursor.after) == null) service.bus.subscribe(cursor.tag, listener)
            val last = drainFrom(cursor.tag, cursor.after, reqId)
            if (request.live) live[cursor.tag] = last
            send("caught_up", reqId, CaughtUp(cursor.tag, maxOf(state.head, last), state.low))
        }
        send("sync_done", reqId, Empty)
    }

    private suspend fun drain(tag: String, reqId: JsonElement?) {
        val from = live[tag] ?: return
        live[tag] = drainFrom(tag, from, reqId)
    }

    /** Sends every item after [after]; returns the last seq sent (or [after]). */
    private suspend fun drainFrom(tag: String, after: Long, reqId: JsonElement?): Long {
        val tagBytes = Codec.tag(tag)
        var last = after
        while (true) {
            val page = service.store.page(tagBytes, last, RelayService.PAGE_ITEMS, RelayService.PAGE_BYTES)
            if (page.isEmpty()) return last
            send("items", reqId, ItemsBody(tag, page.map(Codec::item)))
            metrics.itemsDelivered.increment(page.size.toDouble())
            last = page.last().seq
        }
    }

    private suspend inline fun <reified T> send(type: String, reqId: JsonElement?, body: T) =
        send(type, reqId, body, serializer<T>())

    private suspend fun <T> send(type: String, reqId: JsonElement?, body: T, serializer: KSerializer<T>) {
        val fields = WireJson.encodeToJsonElement(serializer, body).jsonObject
        val out = buildJsonObject {
            put("type", JsonPrimitive(type))
            if (reqId != null) put("req_id", reqId)
            fields.forEach { (k, v) -> put(k, v) }
        }
        socket.send(Frame.Text(out.toString()))
    }

    private inline fun <reified T> decode(message: JsonObject): T = try {
        WireJson.decodeFromJsonElement(serializer<T>(), message)
    } catch (e: Exception) {
        log.debug("bad request: {}", e.javaClass.simpleName)
        throw Rejected("bad-request")
    }

    @kotlinx.serialization.Serializable
    private data class ErrorBody(val reason: String)

    @kotlinx.serialization.Serializable
    private data class CaughtUp(val tag: String, val head: Long, val low: Long)

    @kotlinx.serialization.Serializable
    private data class ItemsBody(val tag: String, val items: List<WireItem>)

    @kotlinx.serialization.Serializable
    private data object Empty

    companion object {
        const val MAX_LIVE_STREAMS = 10_000
    }
}
