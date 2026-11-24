package org.yantagram.relay.store

import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.WriteAuth
import java.security.MessageDigest
import java.sql.Connection
import java.sql.ResultSet
import java.util.HexFormat

/** An item as a client publishes it. [id] is SHA-256 of [body]. */
class NewItem(
    val body: ByteArray,
    val ref: ByteArray? = null,
    val checkpoint: Boolean = false,
    val deleteHash: ByteArray? = null,
    /** The sender marker for the push service (HMAC of the base64 body); never returned to clients. */
    val marker: ByteArray? = null,
) {
    val id: ByteArray = WriteAuth.sha256(body)
}

/** An item as stored. */
class StoredItem(
    val tag: ByteArray,
    val seq: Long,
    val id: ByteArray,
    val body: ByteArray,
    val checkpoint: Boolean,
)

/** A stream's state as a sync reports it. [low] is the oldest retained seq, or head + 1 if none. */
data class StreamState(val lane: Lane?, val head: Long, val low: Long)

/** The outcome of a publish: a seq per published item, and the items that were new. */
class PublishOutcome(val seqs: List<Long>, val head: Long, val added: List<Pair<Long, NewItem>>)

/** A publish the relay refuses. [reason] is a fixed, short code returned to the client. */
class Rejected(val reason: String) : Exception(reason, null, false, false)

class OutboxEntry(val n: Long, val legacyTag: ByteArray, val body: ByteArray, val marker: ByteArray?)

/**
 * Durable, numbered streams. Every write is one SQLite transaction on the writer
 * thread (see [Database]); a method returns only after its commit is on disk.
 */
class ItemStore(private val db: Database, private val clock: () -> Long = System::currentTimeMillis) {

    /**
     * Appends [items] to the stream [tag] in [lane], all or none.
     *
     * Items already in the stream (same id) keep their seq and aren't stored again, so a
     * retried publish never duplicates. New items get consecutive seqs in order. With
     * [legacyTag], new items are also queued for the strfry bridge in the same transaction.
     */
    suspend fun publish(tag: ByteArray, lane: Lane, items: List<NewItem>, legacyTag: ByteArray? = null): PublishOutcome =
        db.write { c -> publishIn(c, tag, lane, items, legacyTag, clock()) }

    private fun publishIn(
        c: Connection,
        tag: ByteArray,
        lane: Lane,
        items: List<NewItem>,
        legacyTag: ByteArray?,
        now: Long,
    ): PublishOutcome {
        val stream = c.prepareStatement("SELECT lane, head, item_count, bytes FROM streams WHERE tag = ?").use { s ->
            s.setBytes(1, tag)
            s.executeQuery().use { if (it.next()) StreamRow(it.getInt(1), it.getLong(2), it.getLong(3), it.getLong(4)) else null }
        }
        if (stream != null && stream.lane != lane.code) throw Rejected("lane-mismatch")

        var head = stream?.head ?: 0
        var count = stream?.itemCount ?: 0
        var bytes = stream?.bytes ?: 0
        val seqs = ArrayList<Long>(items.size)
        val added = ArrayList<Pair<Long, NewItem>>()
        val inBatch = HashMap<String, Long>()

        c.prepareStatement("SELECT seq FROM items WHERE stream = ? AND id = ?").use { find ->
            c.prepareStatement(
                "INSERT INTO items(stream, seq, id, body, ref, checkpoint, delete_hash, marker, stored_at, expires_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ).use { insert ->
                for (item in items) {
                    val key = HEX.formatHex(item.id)
                    val existing = inBatch[key] ?: run {
                        find.setBytes(1, tag)
                        find.setBytes(2, item.id)
                        find.executeQuery().use { if (it.next()) it.getLong(1) else null }
                    }
                    if (existing != null) {
                        seqs += existing
                        continue
                    }
                    count += 1
                    bytes += item.body.size
                    if (count > lane.maxStreamItems || bytes > lane.maxStreamBytes) throw Rejected("stream-full")
                    val seq = ++head
                    val retention = if (item.checkpoint) Lane.CHECKPOINT_RETENTION else lane.retention
                    insert.setBytes(1, tag)
                    insert.setLong(2, seq)
                    insert.setBytes(3, item.id)
                    insert.setBytes(4, item.body)
                    insert.setBytes(5, item.ref)
                    insert.setInt(6, if (item.checkpoint) 1 else 0)
                    insert.setBytes(7, item.deleteHash)
                    insert.setBytes(8, item.marker)
                    insert.setLong(9, now)
                    insert.setLong(10, now + retention.inWholeMilliseconds)
                    insert.executeUpdate()
                    inBatch[key] = seq
                    seqs += seq
                    added += seq to item
                }
            }
        }

        if (added.isNotEmpty()) {
            c.prepareStatement(
                "INSERT INTO streams(tag, lane, head, item_count, bytes, last_write) VALUES (?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT(tag) DO UPDATE SET head = excluded.head, item_count = excluded.item_count, " +
                    "bytes = excluded.bytes, last_write = excluded.last_write",
            ).use { s ->
                s.setBytes(1, tag)
                s.setInt(2, lane.code)
                s.setLong(3, head)
                s.setLong(4, count)
                s.setLong(5, bytes)
                s.setLong(6, now)
                s.executeUpdate()
            }
            if (legacyTag != null) {
                c.prepareStatement("INSERT INTO bridge_outbox(legacy_tag, body, marker, created_at) VALUES (?, ?, ?, ?)").use { s ->
                    for ((_, item) in added) {
                        s.setBytes(1, legacyTag)
                        s.setBytes(2, item.body)
                        s.setBytes(3, item.marker)
                        s.setLong(4, now)
                        s.addBatch()
                    }
                    s.executeBatch()
                }
            }
        }
        return PublishOutcome(seqs, head, added)
    }

    private class StreamRow(val lane: Int, val head: Long, val itemCount: Long, val bytes: Long)

    /** Head, low mark and lane of each stream. Unknown streams have head 0. */
    suspend fun states(tags: List<ByteArray>): List<StreamState> = db.read { c ->
        c.prepareStatement("SELECT lane, head FROM streams WHERE tag = ?").use { head ->
            c.prepareStatement("SELECT MIN(seq) FROM items WHERE stream = ?").use { low ->
                tags.map { tag ->
                    head.setBytes(1, tag)
                    val row = head.executeQuery().use { if (it.next()) it.getInt(1) to it.getLong(2) else null }
                    if (row == null) {
                        StreamState(null, 0, 1)
                    } else {
                        low.setBytes(1, tag)
                        val min = low.executeQuery().use { if (it.next()) it.getLong(1).takeIf { _ -> !it.wasNull() } else null }
                        StreamState(Lane.fromCode(row.first), row.second, min ?: (row.second + 1))
                    }
                }
            }
        }
    }

    /**
     * Items after [after] in seq order, at most [maxItems], stopping once [maxBytes] is
     * reached (always at least one item, so a large item can't stall a stream).
     */
    suspend fun page(tag: ByteArray, after: Long, maxItems: Int, maxBytes: Long): List<StoredItem> = db.read { c ->
        c.prepareStatement("SELECT seq, id, body, checkpoint FROM items WHERE stream = ? AND seq > ? ORDER BY seq LIMIT ?").use { s ->
            s.setBytes(1, tag)
            s.setLong(2, after)
            s.setInt(3, maxItems)
            s.executeQuery().use { rs ->
                val out = ArrayList<StoredItem>()
                var total = 0L
                while (rs.next()) {
                    val body = rs.getBytes(3)
                    if (out.isNotEmpty() && total + body.size > maxBytes) break
                    total += body.size
                    out += StoredItem(tag, rs.getLong(1), rs.getBytes(2), body, rs.getInt(4) == 1)
                }
                out
            }
        }
    }

    /** Items whose ref is one of [refs]. */
    suspend fun byRefs(refs: List<ByteArray>): List<StoredItem> = db.read { c ->
        refs.chunked(CHUNK).flatMap { chunk ->
            c.prepareStatement(
                "SELECT stream, seq, id, body, checkpoint FROM items WHERE ref IN (${placeholders(chunk.size)}) ORDER BY stored_at, seq",
            ).use { s ->
                chunk.forEachIndexed { i, ref -> s.setBytes(i + 1, ref) }
                s.executeQuery().use(::readItems)
            }
        }
    }

    /** The newest checkpoint in any of [tags], or null. */
    suspend fun latestCheckpoint(tags: List<ByteArray>): StoredItem? = db.read { c ->
        tags.chunked(CHUNK).mapNotNull { chunk ->
            c.prepareStatement(
                "SELECT stream, seq, id, body, checkpoint, stored_at FROM items " +
                    "WHERE checkpoint = 1 AND stream IN (${placeholders(chunk.size)}) ORDER BY stored_at DESC, seq DESC LIMIT 1",
            ).use { s ->
                chunk.forEachIndexed { i, tag -> s.setBytes(i + 1, tag) }
                s.executeQuery().use { rs ->
                    if (rs.next()) {
                        rs.getLong(6) to StoredItem(rs.getBytes(1), rs.getLong(2), rs.getBytes(3), rs.getBytes(4), true)
                    } else {
                        null
                    }
                }
            }
        }.maxByOrNull { it.first }?.second
    }

    /** Which of [ids] the stream still holds. */
    suspend fun have(tag: ByteArray, ids: List<ByteArray>): List<ByteArray> = db.read { c ->
        ids.chunked(CHUNK).flatMap { chunk ->
            c.prepareStatement("SELECT id FROM items WHERE stream = ? AND id IN (${placeholders(chunk.size)})").use { s ->
                s.setBytes(1, tag)
                chunk.forEachIndexed { i, id -> s.setBytes(i + 2, id) }
                s.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getBytes(1)) } }
            }
        }
    }

    /**
     * Deletes an inbox item if SHA-256([k]) matches the hash the sender put on it. Only the
     * recipient can open the sealed body that holds k. Returns whether it was deleted.
     */
    suspend fun deleteWithKey(tag: ByteArray, seq: Long, k: ByteArray): Boolean = db.write { c ->
        val row = c.prepareStatement("SELECT delete_hash, LENGTH(body) FROM items WHERE stream = ? AND seq = ?").use { s ->
            s.setBytes(1, tag)
            s.setLong(2, seq)
            s.executeQuery().use { if (it.next()) it.getBytes(1) to it.getLong(2) else null }
        } ?: return@write false
        val hash = row.first ?: return@write false
        if (!MessageDigest.isEqual(hash, WriteAuth.sha256(k))) return@write false
        deleteRows(c, mapOf(HEX.formatHex(tag) to listOf(seq to row.second)))
        true
    }

    /** Deletes checkpoint items [seqs] from the stream (the signer holds its write key). */
    suspend fun retireCheckpoints(tag: ByteArray, seqs: List<Long>): Int = db.write { c ->
        val rows = c.prepareStatement(
            "SELECT seq, LENGTH(body) FROM items WHERE stream = ? AND checkpoint = 1 AND seq IN (${placeholders(seqs.size)})",
        ).use { s ->
            s.setBytes(1, tag)
            seqs.forEachIndexed { i, seq -> s.setLong(i + 2, seq) }
            s.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1) to rs.getLong(2)) } }
        }
        if (rows.isNotEmpty()) deleteRows(c, mapOf(HEX.formatHex(tag) to rows))
        rows.size
    }

    /**
     * Deletes expired items, in batches, keeping each stream's counters right. Stream rows
     * stay (their head must never go back) until they've been empty and idle for
     * [streamIdleMillis]. Returns the number of items deleted.
     */
    suspend fun sweep(streamIdleMillis: Long, batch: Int = 5_000): Int {
        var total = 0
        while (true) {
            val now = clock()
            val deleted = db.write { c ->
                val rows = c.prepareStatement("SELECT stream, seq, LENGTH(body) FROM items WHERE expires_at <= ? LIMIT ?").use { s ->
                    s.setLong(1, now)
                    s.setInt(2, batch)
                    s.executeQuery().use { rs ->
                        val byStream = HashMap<String, MutableList<Pair<Long, Long>>>()
                        while (rs.next()) {
                            byStream.getOrPut(HEX.formatHex(rs.getBytes(1))) { ArrayList() } += rs.getLong(2) to rs.getLong(3)
                        }
                        byStream
                    }
                }
                deleteRows(c, rows)
                rows.values.sumOf { it.size }
            }
            total += deleted
            if (deleted < batch) break
        }
        val now = clock()
        db.write { c ->
            c.prepareStatement("DELETE FROM streams WHERE item_count = 0 AND last_write < ?").use { s ->
                s.setLong(1, now - streamIdleMillis)
                s.executeUpdate()
            }
            c.prepareStatement("DELETE FROM bridge_forwarded WHERE at < ?").use { s ->
                s.setLong(1, now - FORWARDED_TTL_MILLIS)
                s.executeUpdate()
            }
        }
        return total
    }

    private fun deleteRows(c: Connection, rows: Map<String, List<Pair<Long, Long>>>) {
        c.prepareStatement("DELETE FROM items WHERE stream = ? AND seq = ?").use { delete ->
            c.prepareStatement("UPDATE streams SET item_count = item_count - ?, bytes = bytes - ? WHERE tag = ?").use { update ->
                for ((hex, seqs) in rows) {
                    val tag = HEX.parseHex(hex)
                    for ((seq, _) in seqs) {
                        delete.setBytes(1, tag)
                        delete.setLong(2, seq)
                        delete.addBatch()
                    }
                    delete.executeBatch()
                    update.setLong(1, seqs.size.toLong())
                    update.setLong(2, seqs.sumOf { it.second })
                    update.setBytes(3, tag)
                    update.executeUpdate()
                }
            }
        }
    }

    // ---- Migration bridge ----

    /**
     * Stores an event imported from strfry in the legacy stream [tag], unless the bridge
     * forwarded it there itself. Returns the new item's seq, or null if it was already
     * stored or is one of ours.
     */
    suspend fun importLegacy(tag: ByteArray, item: NewItem): PublishOutcome? = db.write { c ->
        val ours = c.prepareStatement("SELECT 1 FROM bridge_forwarded WHERE id = ?").use { s ->
            s.setBytes(1, item.id)
            s.executeQuery().use { it.next() }
        }
        if (ours) return@write null
        publishIn(c, tag, Lane.LEGACY, listOf(item), legacyTag = null, now = clock()).takeIf { it.added.isNotEmpty() }
    }

    suspend fun outbox(limit: Int): List<OutboxEntry> = db.read { c ->
        c.prepareStatement("SELECT n, legacy_tag, body, marker FROM bridge_outbox ORDER BY n LIMIT ?").use { s ->
            s.setInt(1, limit)
            s.executeQuery().use { rs ->
                buildList { while (rs.next()) add(OutboxEntry(rs.getLong(1), rs.getBytes(2), rs.getBytes(3), rs.getBytes(4))) }
            }
        }
    }

    /** Removes a forwarded (or permanently refused) entry and remembers its body id. */
    suspend fun outboxDone(entry: OutboxEntry) = db.write { c ->
        c.prepareStatement("DELETE FROM bridge_outbox WHERE n = ?").use { s ->
            s.setLong(1, entry.n)
            s.executeUpdate()
        }
        c.prepareStatement("INSERT OR REPLACE INTO bridge_forwarded(id, at) VALUES (?, ?)").use { s ->
            s.setBytes(1, WriteAuth.sha256(entry.body))
            s.setLong(2, clock())
            s.executeUpdate()
        }
    }

    suspend fun outboxSize(): Long = db.read { c ->
        c.createStatement().use { s -> s.executeQuery("SELECT COUNT(*) FROM bridge_outbox").use { it.next(); it.getLong(1) } }
    }

    private fun readItems(rs: ResultSet): List<StoredItem> = buildList {
        while (rs.next()) add(StoredItem(rs.getBytes(1), rs.getLong(2), rs.getBytes(3), rs.getBytes(4), rs.getInt(5) == 1))
    }

    private fun placeholders(n: Int) = List(n) { "?" }.joinToString(",")

    companion object {
        private val HEX = HexFormat.of()
        private const val CHUNK = 500
        /** Longer than strfry keeps anything, so an import can't outlive its forwarded record. */
        private const val FORWARDED_TTL_MILLIS = 8L * 24 * 3600 * 1000
    }
}
