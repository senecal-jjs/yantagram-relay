package org.yantagram.relay

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tracks active WebSocket subscribers by verification key.
 * Used to suppress push notifications when a client is already connected live.
 */
class SubscriberRegistry {
    /** Key → number of active WebSocket subscribers watching that key. */
    private val activeKeys = ConcurrentHashMap<String, AtomicInteger>()

    /** Record that a subscriber connected with the given [keys]. */
    fun onConnect(keys: Set<String>?) {
        keys?.forEach { key ->
            activeKeys.computeIfAbsent(key) { AtomicInteger(0) }.incrementAndGet()
        }
    }

    /** Record that a subscriber disconnected that was watching the given [keys]. */
    fun onDisconnect(keys: Set<String>?) {
        keys?.forEach { key ->
            activeKeys.computeIfPresent(key) { _, count ->
                val remaining = count.decrementAndGet()
                if (remaining <= 0) null else count
            }
        }
    }

    /** Returns true if at least one subscriber is actively watching [key]. */
    fun hasActiveSubscriber(key: String): Boolean =
        (activeKeys[key]?.get() ?: 0) > 0

    /** Returns true if any subscriber is connected (regardless of key). */
    fun hasAnyActiveSubscriber(): Boolean = activeKeys.isNotEmpty()
}

