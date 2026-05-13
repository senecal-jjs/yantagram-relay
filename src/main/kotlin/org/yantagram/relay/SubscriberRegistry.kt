package org.yantagram.relay

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

private val registryLogger = LoggerFactory.getLogger("org.yantagram.relay.SubscriberRegistry")

/**
 * Tracks active WebSocket subscribers by verification key.
 * Used to suppress push notifications when a client is already connected live.
 *
 * All mutations use [ConcurrentHashMap.compute] for atomicity — no race
 * between connect and disconnect.
 */
class SubscriberRegistry {
    /** Key → number of active WebSocket subscribers for that key. */
    private val activeKeys = ConcurrentHashMap<String, Int>()

    /** Record that a subscriber connected with the given VK. */
    fun onConnect(keys: Set<String>?) {
        keys?.forEach { key ->
            activeKeys.compute(key) { _, count ->
                val newCount = (count ?: 0) + 1
                registryLogger.debug("onConnect VK '{}...': count {} → {}", key.take(12), count ?: 0, newCount)
                newCount
            }
        }
    }

    /** Record that a subscriber disconnected with the given VK. */
    fun onDisconnect(keys: Set<String>?) {
        keys?.forEach { key ->
            activeKeys.compute(key) { _, count ->
                val remaining = (count ?: 0) - 1
                registryLogger.debug("onDisconnect VK '{}...': count {} → {}", key.take(12), count ?: 0, remaining)
                if (remaining <= 0) null else remaining
            }
        }
    }

    /** Returns true if at least one subscriber is actively connected for [key]. */
    fun hasActiveSubscriber(key: String): Boolean =
        (activeKeys[key] ?: 0) > 0

    /** Returns true if any subscriber is connected (regardless of key). */
    fun hasAnyActiveSubscriber(): Boolean = activeKeys.isNotEmpty()
}

