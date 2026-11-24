package org.yantagram.relay.bridge

import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.yantagram.relay.Metrics
import org.yantagram.relay.api.BridgeStatus
import org.yantagram.relay.protocol.Codec
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.RelayService
import org.yantagram.relay.store.ItemStore
import org.yantagram.relay.store.MetaStore
import org.yantagram.relay.store.NewItem
import org.yantagram.relay.store.OutboxEntry
import java.util.Base64
import java.util.HexFormat
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Keeps old clients (strfry only) and new clients (this relay) talking during the
 * migration.
 *
 * - **Import (strfry → here):** one subscription to every kind-2929 event. Each is
 *   stored in the legacy stream named by its routing tag, which new clients sync
 *   alongside their own streams. Events the bridge forwarded itself are skipped.
 * - **Forward (here → strfry):** items published with a `legacy_tag` are queued in
 *   `bridge_outbox` in the same transaction that stores them, then sent to strfry one
 *   at a time, each removed only after strfry answers OK (or refuses it for good).
 */
class StrfryBridge(
    private val client: HttpClient,
    private val url: String,
    private val store: ItemStore,
    private val meta: MetaStore,
    private val service: RelayService,
    private val metrics: Metrics,
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
    private val random: Random = Random.Default,
) : BridgeStatus {
    private val log = LoggerFactory.getLogger(StrfryBridge::class.java)
    private val importing = AtomicBoolean(false)
    private val forwarding = AtomicBoolean(false)
    private val newest = AtomicLong(0)
    private val pending = Channel<Unit>(Channel.CONFLATED)
    private val hex = HexFormat.of()

    override fun state(): String = if (importing.get() && forwarding.get()) "connected" else "reconnecting"

    fun start(scope: CoroutineScope) {
        scope.launch {
            newest.set(meta.get(CURSOR_KEY)?.toLongOrNull() ?: nowSeconds())
            scope.launch { reconnecting("import", importing) { importLoop() } }
            scope.launch { reconnecting("forward", forwarding) { forwardLoop() } }
            while (isActive) {
                delay(1.minutes)
                saveCursor()
            }
        }
    }

    /** Wakes the forwarder (called after a publish queued items). */
    fun poke() {
        pending.trySend(Unit)
    }

    suspend fun saveCursor() {
        try {
            meta.set(CURSOR_KEY, newest.get().toString())
        } catch (e: Exception) {
            log.warn("cursor save failed: {}", e.javaClass.simpleName)
        }
    }

    // ---- Import ----

    private suspend fun DefaultClientWebSocketSession.importLoop() {
        val since = (newest.get() - OVERLAP.inWholeSeconds).coerceAtLeast(0)
        send(Frame.Text(Nostr.req(SUBSCRIPTION_ID, since)))
        for (frame in incoming) {
            if (frame !is Frame.Text) continue
            when (val message = Nostr.parse(frame.readText())) {
                is NostrMessage.Event -> if (message.subscriptionId == SUBSCRIPTION_ID) import(message.event)
                is NostrMessage.Closed -> return
                else -> Unit
            }
        }
    }

    internal suspend fun import(event: NostrEvent) {
        if (event.kind != Nostr.KIND) return
        newest.updateAndGet { maxOf(it, minOf(event.createdAt, nowSeconds())) }
        val tagHex = event.routingTag ?: return
        val body = try {
            Base64.getDecoder().decode(event.content)
        } catch (_: IllegalArgumentException) {
            return metrics.bridgeDropped("bad_content")
        }
        if (body.isEmpty() || body.size > Lane.LEGACY.maxItemBytes) return metrics.bridgeDropped("bad_size")
        val item = NewItem(body = body, marker = event.senderMarker?.let(hex::parseHex))
        val outcome = store.importLegacy(hex.parseHex(tagHex), item) ?: return
        metrics.bridgeImported.increment()
        service.afterCommit(tagHex, Lane.LEGACY, outcome)
    }

    // ---- Forward ----

    private suspend fun DefaultClientWebSocketSession.forwardLoop() {
        while (isActive) {
            val batch = store.outbox(BATCH)
            if (batch.isEmpty()) {
                withTimeoutOrNull(IDLE_POLL) { pending.receive() }
                continue
            }
            for (entry in batch) {
                when (forward(entry)) {
                    Forwarded.DONE -> continue
                    // Retry this entry before any later one, so strfry sees them in order.
                    Forwarded.RETRY -> break
                    Forwarded.RECONNECT -> return
                }
            }
        }
    }

    private enum class Forwarded { DONE, RETRY, RECONNECT }

    /** Sends one entry and waits for strfry's answer. */
    private suspend fun DefaultClientWebSocketSession.forward(entry: OutboxEntry): Forwarded {
        if (entry.body.size > Nostr.MAX_BODY_BYTES) {
            // strfry refuses it; old clients can't receive it (see README, Migration).
            metrics.bridgeDropped("too_large")
            store.outboxDone(entry)
            return Forwarded.DONE
        }
        val (id, message) = Nostr.eventMessage(
            routingTag = Codec.hex(entry.legacyTag),
            content = Base64.getEncoder().encodeToString(entry.body),
            senderMarker = entry.marker?.let(Codec::hex),
            createdAt = nowSeconds(),
        )
        send(Frame.Text(message))
        val ok = withTimeoutOrNull(OK_TIMEOUT) {
            for (frame in incoming) {
                if (frame !is Frame.Text) continue
                val reply = Nostr.parse(frame.readText())
                if (reply is NostrMessage.Ok && reply.eventId == id) return@withTimeoutOrNull reply
            }
            null
        } ?: return Forwarded.RECONNECT
        return when {
            ok.accepted || ok.message.startsWith("duplicate") -> {
                store.outboxDone(entry)
                metrics.bridgeForwarded.increment()
                Forwarded.DONE
            }
            ok.message.startsWith("rate-limited") -> {
                metrics.bridgeDropped("rate_limited_retry")
                delay(RATE_LIMIT_PAUSE)
                Forwarded.RETRY
            }
            else -> {
                // invalid / blocked / pow / error: retrying won't change strfry's mind.
                metrics.bridgeDropped(ok.message.substringBefore(':').take(20).ifEmpty { "refused" })
                store.outboxDone(entry)
                Forwarded.DONE
            }
        }
    }

    // ---- Connection handling ----

    private suspend fun reconnecting(
        name: String,
        connected: AtomicBoolean,
        session: suspend DefaultClientWebSocketSession.() -> Unit,
    ) {
        var attempt = 0
        while (true) {
            try {
                client.webSocket(url) {
                    connected.set(true)
                    attempt = 0
                    log.info("bridge {} connected", name)
                    session()
                }
            } catch (e: CancellationException) {
                connected.set(false)
                throw e
            } catch (e: Exception) {
                if (attempt % 10 == 0) log.warn("bridge {} connection failed (attempt {}): {}", name, attempt + 1, e.javaClass.simpleName)
            }
            if (connected.getAndSet(false)) log.warn("bridge {} disconnected", name)
            metrics.bridgeReconnects.increment()
            delay(backoff(attempt++))
        }
    }

    internal fun backoff(attempt: Int): Duration {
        val ceiling = (1_000L shl attempt.coerceAtMost(10)).coerceAtMost(30_000)
        return (ceiling / 2 + random.nextLong(ceiling / 2 + 1)).milliseconds
    }

    private companion object {
        const val SUBSCRIPTION_ID = "bridge"
        const val CURSOR_KEY = "bridge_cursor"
        const val BATCH = 50
        val OVERLAP = 60.seconds
        val OK_TIMEOUT = 10.seconds
        val IDLE_POLL = 5.seconds
        val RATE_LIMIT_PAUSE = 2.seconds
    }
}
