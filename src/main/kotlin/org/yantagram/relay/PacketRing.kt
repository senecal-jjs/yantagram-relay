package org.yantagram.relay

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.ArrayDeque

/** Estimated JVM overhead per Packet: object header + fields + ArrayDeque node reference. */
private const val PER_PACKET_OVERHEAD = 80L

/** Estimate the in-memory byte cost of a [Packet], including payload, key, and JVM overhead. */
private fun Packet.estimatedBytes(): Long =
    PER_PACKET_OVERHEAD + payload.size + (verificationKey?.length?.times(2L) ?: 0L)

/**
 * Provides the current effective max bytes the ring is allowed to use.
 * Called on every append to allow dynamic adjustment based on JVM memory.
 */
fun interface RingLimitProvider {
    fun effectiveMaxBytes(): Long
}

/** A relayed binary packet. `seq` is monotonically assigned by the server. */
data class Packet(val seq: Long, val verificationKey: String?, val payload: ByteArray) {
    override fun equals(other: Any?): Boolean =
        other is Packet && other.seq == seq && other.verificationKey == verificationKey && other.payload.contentEquals(payload)

    override fun hashCode(): Int =
        31 * (31 * seq.hashCode() + (verificationKey?.hashCode() ?: 0)) + payload.contentHashCode()
}

/**
 * Bounded in-memory ring of packets evicting oldest entries until the estimated total
 * memory usage is <= the limit returned by [limitProvider].
 *
 * The limit is re-evaluated on every append, allowing dynamic adjustment based on
 * remaining JVM heap memory.
 *
 * Optionally integrates with a [PacketStore] for async persistence — packets are
 * enqueued for batch writing to SQLite after being added to the ring.
 *
 * Live emissions go through [stream] for fan-out to subscribers; replay of unseen
 * packets is served via [snapshotSince].
 */
class PacketRing(
    private val limitProvider: RingLimitProvider,
    private val store: PacketStore? = null,
) {

    /** Convenience constructor for a fixed byte limit (useful in tests). */
    constructor(maxBytes: Long) : this(RingLimitProvider { maxBytes }, null)

    private val lock = Any()
    private val deque: ArrayDeque<Packet> = ArrayDeque()
    private var totalBytes: Long = 0L
    private var nextSeq: Long = 1L

    private val _stream = MutableSharedFlow<Packet>(
        replay = 0,
        extraBufferCapacity = 1024,
    )
    val stream: SharedFlow<Packet> = _stream.asSharedFlow()

    /**
     * Restore state from the [store] on startup.
     * Reloads nextSeq and recent packets into the in-memory ring for immediate replay.
     * Call once before serving traffic.
     */
    fun restore(replayLimit: Int = 10_000) {
        val s = store ?: return
        val restoredSeq = s.loadNextSeq()
        synchronized(lock) {
            nextSeq = restoredSeq
        }
        // Reload recent packets into memory for immediate replay
        val sinceSeq = maxOf(0L, restoredSeq - replayLimit - 1)
        val packets = s.loadSince(sinceSeq, replayLimit)
        synchronized(lock) {
            for (p in packets) {
                deque.addLast(p)
                totalBytes += p.estimatedBytes()
            }
            // Evict if over limit
            val maxBytes = limitProvider.effectiveMaxBytes()
            while (totalBytes > maxBytes && deque.size > 1) {
                val removed = deque.removeFirst()
                totalBytes -= removed.estimatedBytes()
            }
        }
    }

    /** Append a payload, evict oldest as needed, persist, and emit to [stream]. Returns the assigned packet. */
    suspend fun append(payload: ByteArray, verificationKey: String? = null): Packet {
        val packet = synchronized(lock) {
            val p = Packet(nextSeq++, verificationKey, payload)
            deque.addLast(p)
            totalBytes += p.estimatedBytes()
            val maxBytes = limitProvider.effectiveMaxBytes()
            while (totalBytes > maxBytes && deque.size > 1) {
                val removed = deque.removeFirst()
                totalBytes -= removed.estimatedBytes()
            }
            p
        }
        store?.enqueue(packet)
        _stream.emit(packet)
        return packet
    }

    /** Snapshot of currently-buffered packets with seq > [since], optionally filtered by verification keys. */
    fun snapshotSince(since: Long, keys: Set<String>? = null): List<Packet> = synchronized(lock) {
        if (deque.isEmpty()) emptyList()
        else deque.filter { it.seq > since && (keys == null || it.verificationKey in keys) }
    }

    /** Highest seq currently assigned, or 0 if none. */
    fun latestSeq(): Long = synchronized(lock) { nextSeq - 1 }

    /** Current estimated total bytes stored in the ring (payload + key + overhead). */
    fun currentBytes(): Long = synchronized(lock) { totalBytes }

    /** Number of packets currently in the ring. */
    fun packetCount(): Int = synchronized(lock) { deque.size }
}
