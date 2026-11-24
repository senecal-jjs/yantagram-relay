package org.yantagram.relay.protocol

import org.yantagram.relay.store.Rejected
import java.util.concurrent.ConcurrentHashMap

/**
 * Per-IP publish limits, in memory: items per short window, and bytes per UTC day (as
 * the strfry write policy does today). Counters reset on restart, which is fine for
 * abuse limits. Per-stream limits live in the store (see [Lane]).
 */
class Limits(
    private val itemsPerWindow: Int = 400,
    private val windowMillis: Long = 10_000,
    private val bytesPerDay: Long = 200L * 1024 * 1024,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private class Counter(var windowStart: Long, var items: Int, var day: Long, var bytes: Long)

    private val counters = ConcurrentHashMap<String, Counter>()

    /** Charges [items] and [bytes] to [ip], or rejects without charging. */
    fun charge(ip: String, items: Int, bytes: Long) {
        val now = clock()
        val day = now / DAY_MILLIS
        val counter = counters.computeIfAbsent(ip) { Counter(now, 0, day, 0) }
        synchronized(counter) {
            if (now - counter.windowStart >= windowMillis) {
                counter.windowStart = now
                counter.items = 0
            }
            if (counter.day != day) {
                counter.day = day
                counter.bytes = 0
            }
            if (counter.items + items > itemsPerWindow) throw Rejected("rate-limited")
            if (counter.bytes + bytes > bytesPerDay) throw Rejected("daily-limit")
            counter.items += items
            counter.bytes += bytes
        }
    }

    /** Forgets IPs idle for a day; call periodically. */
    fun prune() {
        val now = clock()
        counters.entries.removeIf { (_, c) -> now - c.windowStart > DAY_MILLIS }
    }

    private companion object {
        const val DAY_MILLIS = 86_400_000L
    }
}
