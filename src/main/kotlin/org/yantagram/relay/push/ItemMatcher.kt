package org.yantagram.relay.push

import org.yantagram.relay.Metrics
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.NewItemsListener
import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.store.NewItem
import java.util.Base64

/**
 * New items on a stream → registered devices → coalesced pushes. Most streams match
 * nothing.
 *
 * A device that sent every one of the new items (its sender marker matches each) gets no
 * push. Others get an alert when an item is a chat message (group and legacy lanes) and
 * they're an iOS device with notifications on, and a silent push otherwise. Control and
 * inbox items always wake silently, so the app syncs ops in the background.
 */
class ItemMatcher(
    private val registry: Registry,
    private val coalescer: Coalescer,
    private val metrics: Metrics,
) : NewItemsListener {
    override suspend fun onNewItems(tag: String, lane: Lane, items: List<NewItem>) {
        if (lane == Lane.BLOB) return // file parts: the app fetches them when it sees the message
        val matches = registry.match(tag)
        if (matches.isEmpty()) return
        metrics.eventsMatched.increment()

        val encoder = Base64.getEncoder()
        // The marker is over the base64 body, as the app computes it for Nostr content.
        val contents = items.map { encoder.encodeToString(it.body) }
        val chat by lazy {
            (lane == Lane.GROUP || lane == Lane.LEGACY) && items.any { PacketKind.isChatMessage(it.body) }
        }

        for (match in matches) {
            val target = match.target
            val selfKey = match.selfKey
            val fromThisDevice = selfKey != null && items.indices.all { i ->
                val marker = items[i].marker
                marker != null && SenderMarker.isFrom(selfKey, contents[i], marker)
            }
            if (fromThisDevice) {
                metrics.selfSkipped(target.platform)
                continue
            }
            val alert = target.platform == Platform.IOS && match.alerts && chat
            coalescer.wake(target, if (alert) PushKind.ALERT else PushKind.SILENT)
        }
    }
}
