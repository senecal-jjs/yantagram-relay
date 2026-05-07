package org.yantagram.relay

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.sum
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.upsert
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.milliseconds

private val storeLogger = LoggerFactory.getLogger("org.yantagram.relay.PacketStore")

/** Exposed table definition for persisted packets. */
object PacketsTable : Table("packets") {
    val seq = long("seq")
    val verificationKey = text("verification_key").nullable()
    val payload = binary("payload")
    val createdAt = long("created_at")
    val payloadSize = integer("payload_size")
    override val primaryKey = PrimaryKey(seq)
}

/** Exposed table for metadata (nextSeq tracking). */
object MetadataTable : Table("metadata") {
    val key = text("key")
    val value = text("value")
    override val primaryKey = PrimaryKey(key)
}

/**
 * SQLite-backed packet store with async batch writes.
 *
 * Packets are queued in a channel and flushed to disk periodically.
 * The DB retains a longer history than the in-memory ring, allowing
 * subscribers to replay further back after a server restart.
 */
class PacketStore(
    dbPath: String,
    private val flushIntervalMs: Long = 500,
    private val maxHistoryBytes: Long = 0,
) {
    private val db: Database = if (dbPath == ":memory:") {
        // For in-memory DBs, use a single connection to avoid losing tables across connections
        Database.connect("jdbc:sqlite::memory:", driver = "org.sqlite.JDBC")
    } else {
        Database.connect("jdbc:sqlite:$dbPath", driver = "org.sqlite.JDBC")
    }
    private val writeChannel = Channel<Packet>(Channel.UNLIMITED)
    private var flushJob: Job? = null

    /** Initialize schema. */
    fun init() {
        transaction(db) {
            SchemaUtils.create(PacketsTable, MetadataTable)
        }
        storeLogger.info("PacketStore initialized")
    }

    /** Start the background flush coroutine. Call once after init(). */
    fun startFlusher(scope: CoroutineScope) {
        flushJob = scope.launch(Dispatchers.IO) {
            while (isActive) {
                delay(flushIntervalMs.milliseconds)
                flush()
            }
        }
    }

    /** Queue a packet for async persistence. */
    fun enqueue(packet: Packet) {
        writeChannel.trySend(packet)
    }

    /** Flush all queued packets to SQLite in a single transaction. */
    private fun flush() {
        val batch = mutableListOf<Packet>()
        while (true) {
            val p = writeChannel.tryReceive().getOrNull() ?: break
            batch.add(p)
        }
        if (batch.isEmpty()) return

        transaction(db) {
            PacketsTable.batchInsert(batch, ignore = true) { p ->
                this[PacketsTable.seq] = p.seq
                this[PacketsTable.verificationKey] = p.verificationKey
                this[PacketsTable.payload] = p.payload
                this[PacketsTable.createdAt] = System.currentTimeMillis()
                this[PacketsTable.payloadSize] = p.payload.size
            }
            // Update nextSeq to max in batch + 1
            val maxSeq = batch.maxOf { it.seq }
            MetadataTable.upsert {
                it[key] = "nextSeq"
                it[value] = (maxSeq + 1).toString()
            }
        }

        // Prune old history if configured
        if (maxHistoryBytes > 0) {
            pruneHistory()
        }

        storeLogger.debug("Flushed {} packets to DB", batch.size)
    }

    /** Remove oldest packets when total DB payload exceeds maxHistoryBytes. */
    private fun pruneHistory() {
        transaction(db) {
            val totalSizeExpr = PacketsTable.payloadSize.sum()
            val total = PacketsTable.select(totalSizeExpr)
                .map { it[totalSizeExpr] ?: 0 }
                .firstOrNull()?.toLong() ?: 0L

            if (total > maxHistoryBytes) {
                val excess = total - maxHistoryBytes
                var freed = 0L
                val toDelete = mutableListOf<Long>()
                PacketsTable.selectAll().orderBy(PacketsTable.seq, SortOrder.ASC).forEach { row ->
                    if (freed >= excess) return@forEach
                    freed += row[PacketsTable.payloadSize]
                    toDelete.add(row[PacketsTable.seq])
                }
                if (toDelete.isNotEmpty()) {
                    PacketsTable.deleteWhere { seq inList toDelete }
                    storeLogger.debug("Pruned {} packets from DB history", toDelete.size)
                }
            }
        }
    }

    /** Load the persisted nextSeq value (for recovery on startup). */
    fun loadNextSeq(): Long = transaction(db) {
        MetadataTable.selectAll().where { MetadataTable.key eq "nextSeq" }
            .map { it[MetadataTable.value].toLongOrNull() }
            .firstOrNull() ?: 1L
    }

    /** Load packets with seq > [since] from DB, ordered by seq. */
    fun loadSince(since: Long, limit: Int = Int.MAX_VALUE): List<Packet> = transaction(db) {
        PacketsTable.selectAll()
            .where { PacketsTable.seq greater since }
            .orderBy(PacketsTable.seq, SortOrder.ASC)
            .limit(limit)
            .map { row ->
                Packet(
                    seq = row[PacketsTable.seq],
                    verificationKey = row[PacketsTable.verificationKey],
                    payload = row[PacketsTable.payload],
                )
            }
    }

    /** Flush remaining packets and stop. */
    fun close() {
        flushJob?.cancel()
        flush()
    }
}
