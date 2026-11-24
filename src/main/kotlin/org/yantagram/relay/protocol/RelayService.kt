package org.yantagram.relay.protocol

import org.slf4j.LoggerFactory
import org.yantagram.relay.Metrics
import org.yantagram.relay.store.ItemStore
import org.yantagram.relay.store.NewItem
import org.yantagram.relay.store.PublishOutcome
import org.yantagram.relay.store.Rejected
import org.yantagram.relay.store.StreamState

/** Called after new items are committed (push matching). Must not throw. */
fun interface NewItemsListener {
    suspend fun onNewItems(tag: String, lane: Lane, items: List<NewItem>)
}

/**
 * The protocol operations, shared by the HTTP and WebSocket transports. Every method
 * validates fully before touching the store and throws [Rejected] with a short code.
 */
class RelayService(
    val store: ItemStore,
    val bus: StreamBus,
    private val limits: Limits,
    private val metrics: Metrics,
    private val bridgeEnabled: Boolean,
    private val onNewItems: NewItemsListener = NewItemsListener { _, _, _ -> },
) {
    private val log = LoggerFactory.getLogger(RelayService::class.java)

    suspend fun publish(request: PublishRequest, ip: String): StoredResponse {
        val lane = Lane.fromWire(request.lane)?.takeIf { it.clientWritable } ?: throw Rejected("bad-lane")
        if (request.items.isEmpty() || request.items.size > MAX_BATCH_ITEMS) throw Rejected("bad-batch")
        val pk = Codec.hexOf(request.pk, WriteAuth.PUBLIC_KEY_BYTES, "bad-pk")
        val sig = Codec.hexOf(request.sig, WriteAuth.SIGNATURE_BYTES, "bad-sig")
        val legacyTag = request.legacyTag?.let {
            if (lane == Lane.BLOB) throw Rejected("bad-legacy-tag")
            Codec.tag(it)
        }

        val items = request.items.map { item ->
            val body = Codec.body(item.body)
            if (body.isEmpty()) throw Rejected("bad-body")
            if (body.size > lane.maxItemBytes) throw Rejected("too-large")
            if (item.checkpoint && lane != Lane.CONTROL) throw Rejected("bad-checkpoint")
            if (item.deleteHash != null && lane != Lane.INBOX) throw Rejected("bad-delete-hash")
            NewItem(
                body = body,
                ref = item.ref?.let { Codec.hexOf(it, REF_BYTES, "bad-ref") },
                checkpoint = item.checkpoint,
                deleteHash = item.deleteHash?.let { Codec.hexOf(it, 32, "bad-delete-hash") },
                marker = item.marker?.let { Codec.hexOf(it, MARKER_BYTES, "bad-marker") },
            )
        }
        if (items.sumOf { it.body.size } > MAX_BATCH_BYTES) throw Rejected("too-large")

        val signed = items.map { SignedItem(it.id, it.checkpoint, it.ref, it.deleteHash) }
        if (!WriteAuth.verify(pk, WriteAuth.publishMessage(lane, signed), sig)) throw Rejected("bad-sig")

        limits.charge(ip, items.size, items.sumOf { it.body.size.toLong() })

        val tag = WriteAuth.tagFor(pk)
        val outcome = store.publish(tag, lane, items, legacyTag.takeIf { bridgeEnabled })
        val tagHex = Codec.hex(tag)
        afterCommit(tagHex, lane, outcome)
        return StoredResponse(tagHex, outcome.seqs, outcome.head)
    }

    /** Notifies live connections and push for newly stored items. */
    suspend fun afterCommit(tagHex: String, lane: Lane, outcome: PublishOutcome) {
        metrics.itemsStored(lane, outcome.added.size)
        if (outcome.added.isEmpty()) return
        bus.notify(tagHex, outcome.head)
        try {
            onNewItems.onNewItems(tagHex, lane, outcome.added.map { it.second })
        } catch (e: Exception) {
            log.warn("new-item listener failed: {}", e.javaClass.simpleName)
        }
    }

    /** Validated sync cursors, ordered so the most important lanes come first. */
    suspend fun orderedCursors(cursors: List<SyncCursor>): List<Pair<SyncCursor, StreamState>> {
        if (cursors.size > MAX_SYNC_STREAMS) throw Rejected("too-many-streams")
        val unique = cursors.associateBy { it.tag }.values.toList()
        unique.forEach { Codec.tag(it.tag); if (it.after < 0) throw Rejected("bad-cursor") }
        val states = store.states(unique.map { Codec.tag(it.tag) })
        return unique.zip(states).sortedBy { (_, state) -> state.lane?.priority ?: Int.MAX_VALUE }
    }

    /** One-shot sync for HTTP: everything after each cursor, up to [maxBytes] in total. */
    suspend fun sync(request: SyncRequest, maxBytes: Long = HTTP_SYNC_BYTES): SyncResponse {
        var budget = maxBytes
        val pages = orderedCursors(request.cursors).map { (cursor, state) ->
            val tag = Codec.tag(cursor.tag)
            val items = ArrayList<WireItem>()
            var after = cursor.after
            var more = false
            while (after < state.head) {
                if (budget <= 0) {
                    more = true
                    break
                }
                val page = store.page(tag, after, PAGE_ITEMS, minOf(budget, PAGE_BYTES))
                if (page.isEmpty()) break
                page.forEach { budget -= it.body.size }
                items += page.map(Codec::item)
                after = page.last().seq
            }
            StreamPage(cursor.tag, items, maxOf(state.head, after), state.low, more)
        }
        return SyncResponse(pages)
    }

    suspend fun fetch(request: FetchRequest): ItemsResponse {
        if (request.refs.size > MAX_REFS) throw Rejected("too-many-refs")
        val refs = request.refs.map { Codec.hexOf(it, REF_BYTES, "bad-ref") }
        return ItemsResponse(store.byRefs(refs).map(Codec::item))
    }

    suspend fun latestCheckpoint(request: CheckpointRequest): CheckpointResponse {
        if (request.tags.size > MAX_SYNC_STREAMS) throw Rejected("too-many-streams")
        val item = store.latestCheckpoint(request.tags.map(Codec::tag))
        return CheckpointResponse(item?.let(Codec::item))
    }

    suspend fun have(request: HaveRequest): HaveResponse {
        if (request.ids.size > MAX_REFS) throw Rejected("too-many-ids")
        val ids = request.ids.map { Codec.hexOf(it, 32, "bad-id") }
        return HaveResponse(store.have(Codec.tag(request.tag), ids).map(Codec::hex))
    }

    suspend fun delete(request: DeleteRequest): DeleteResponse {
        val k = Codec.hexRange(request.k, 16, 64, "bad-key")
        return DeleteResponse(store.deleteWithKey(Codec.tag(request.tag), request.seq, k))
    }

    suspend fun retire(request: RetireRequest): RetireResponse {
        if (request.seqs.isEmpty() || request.seqs.size > MAX_BATCH_ITEMS) throw Rejected("bad-batch")
        val pk = Codec.hexOf(request.pk, WriteAuth.PUBLIC_KEY_BYTES, "bad-pk")
        val sig = Codec.hexOf(request.sig, WriteAuth.SIGNATURE_BYTES, "bad-sig")
        if (!WriteAuth.verify(pk, WriteAuth.retireMessage(request.seqs), sig)) throw Rejected("bad-sig")
        return RetireResponse(store.retireCheckpoints(WriteAuth.tagFor(pk), request.seqs))
    }

    companion object {
        const val MAX_BATCH_ITEMS = 256
        const val MAX_BATCH_BYTES = 4L * 1024 * 1024
        const val MAX_SYNC_STREAMS = 5_000
        const val MAX_REFS = 512
        const val REF_BYTES = 16
        const val MARKER_BYTES = 16
        const val PAGE_ITEMS = 256
        const val PAGE_BYTES = 2L * 1024 * 1024
        const val HTTP_SYNC_BYTES = 8L * 1024 * 1024
    }
}
