package org.yantagram.relay.protocol

import java.util.concurrent.ConcurrentHashMap

/** Something waiting for new items on streams; must not block. */
fun interface HeadListener {
    fun onHead(tag: String, head: Long)
}

/**
 * Tells live connections that a stream has new items. It carries only the new head:
 * each connection reads what it hasn't sent from the store itself, so a missed or
 * merged notification can never leave a gap.
 */
class StreamBus {
    private val listeners = ConcurrentHashMap<String, MutableSet<HeadListener>>()

    fun subscribe(tag: String, listener: HeadListener) {
        listeners.computeIfAbsent(tag) { ConcurrentHashMap.newKeySet() }.add(listener)
    }

    fun unsubscribe(tag: String, listener: HeadListener) {
        listeners.computeIfPresent(tag) { _, set -> set.remove(listener); if (set.isEmpty()) null else set }
    }

    fun notify(tag: String, head: Long) {
        listeners[tag]?.forEach { it.onHead(tag, head) }
    }

    val subscribedTags: Int get() = listeners.size
}
