package org.yantagram.relay

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.ArrayDeque

/** A relayed binary packet. `seq` is monotonically assigned by the server. */
data class Packet(val seq: Long, val payload: ByteArray) {
    override fun equals(other: Any?): Boolean =
        other is Packet && other.seq == seq && other.payload.contentEquals(payload)

    override fun hashCode(): Int = 31 * seq.hashCode() + payload.contentHashCode()
}

/**
 * Bounded in-memory ring of packets evicting oldest entries until the total payload
 * byte count is <= [maxBytes]. Live emissions go through [stream] for fan-out to
 * subscribers; replay of unseen packets is served via [snapshotSince].
 */
class PacketRing(private val maxBytes: Long) {
    private val lock = Any()
    private val deque: ArrayDeque<Packet> = ArrayDeque()
    private var totalBytes: Long = 0L
    private var nextSeq: Long = 1L

    private val _stream = MutableSharedFlow<Packet>(
        replay = 0,
        extraBufferCapacity = 1024,
    )
    val stream: SharedFlow<Packet> = _stream.asSharedFlow()

    /** Append a payload, evict oldest as needed, and emit to [stream]. Returns the assigned packet. */
    suspend fun append(payload: ByteArray): Packet {
        val packet = synchronized(lock) {
            val p = Packet(nextSeq++, payload)
            deque.addLast(p)
            totalBytes += p.payload.size
            while (totalBytes > maxBytes && deque.size > 1) {
                val removed = deque.removeFirst()
                totalBytes -= removed.payload.size
            }
            p
        }
        _stream.emit(packet)
        return packet
    }

    /** Snapshot of currently-buffered packets with seq > [since], in order. */
    fun snapshotSince(since: Long): List<Packet> = synchronized(lock) {
        if (deque.isEmpty()) emptyList()
        else deque.filter { it.seq > since }
    }

    /** Highest seq currently assigned, or 0 if none. */
    fun latestSeq(): Long = synchronized(lock) { nextSeq - 1 }

    /** Current total payload bytes stored in the ring. */
    fun currentBytes(): Long = synchronized(lock) { totalBytes }

    /** Number of packets currently in the ring. */
    fun packetCount(): Int = synchronized(lock) { deque.size }
}
