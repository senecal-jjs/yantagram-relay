package org.yantagram.relay

import io.github.jav.exposerversdk.ExpoPushMessage
import io.github.jav.exposerversdk.ExpoMessageSound
import io.github.jav.exposerversdk.PushClient
import io.github.jav.exposerversdk.PushClientCustomData
import io.github.jav.exposerversdk.enums.Priority
import io.github.jav.exposerversdk.enums.Status
import io.github.jav.exposerversdk.enums.TicketError
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

private val pushLogger = LoggerFactory.getLogger("org.yantagram.relay.PushNotifier")

/** Minimum interval between pushes to the same VK, in milliseconds. */
private const val DEBOUNCE_MS = 10_000L

/**
 * Sends display push notifications to Expo push tokens for specified recipients.
 *
 * Called from `POST /push/notify`. For each recipient VK:
 * - Skips if the VK has an active WebSocket subscriber (real-time delivery).
 * - Skips if the VK was already notified within the 10s debounce window.
 * - Looks up all registered tokens for the VK and sends a generic display notification.
 *
 * Push payloads contain no message content (E2E encrypted, server can't read it).
 */
class PushNotifier(
    private val tokenStore: PushTokenStore,
    private val subscriberRegistry: SubscriberRegistry,
) {
    private val client = PushClient()

    /** Per-VK debounce: tracks last push-sent timestamp. */
    private val lastNotifiedAt = ConcurrentHashMap<String, Long>()

    /**
     * Notify the given recipient VKs. Applies active-WS skip and per-VK debounce,
     * then sends display notifications via the Expo Push API.
     *
     * This method performs network I/O — call it from a background dispatcher.
     */
    fun notifyRecipients(recipients: List<String>) {
        if (recipients.isEmpty()) return

        val now = System.currentTimeMillis()

        // Filter: skip active WS subscribers and debounced VKs.
        val eligible = recipients.filter { vk ->
            if (subscriberRegistry.hasActiveSubscriber(vk)) {
                pushLogger.debug("VK '{}...' has active subscriber, skipping push", vk.take(12))
                return@filter false
            }
            val last = lastNotifiedAt[vk] ?: 0L
            if (now - last < DEBOUNCE_MS) {
                pushLogger.debug("VK '{}...' debounced, skipping push", vk.take(12))
                return@filter false
            }
            true
        }

        if (eligible.isEmpty()) return

        // Look up tokens for eligible VKs.
        val vkToTokens = tokenStore.getTokensForKeys(eligible)
        if (vkToTokens.isEmpty()) {
            pushLogger.debug("No tokens registered for eligible VKs")
            return
        }

        // Collect all tokens to send to, mark VKs as notified.
        val allTokens = mutableListOf<String>()
        for (vk in eligible) {
            val tokens = vkToTokens[vk] ?: continue
            val validTokens = tokens.filter { PushClientCustomData.isExponentPushToken(it) }
            val invalidTokens = tokens - validTokens.toSet()
            if (invalidTokens.isNotEmpty()) {
                pushLogger.warn("Removing {} invalid push tokens for VK {}...", invalidTokens.size, vk.take(12))
                tokenStore.removeAll(invalidTokens)
            }
            if (validTokens.isNotEmpty()) {
                lastNotifiedAt[vk] = now
                allTokens.addAll(validTokens)
            }
        }

        if (allTokens.isEmpty()) return

        pushLogger.info("Sending display push to {} tokens for {} VKs", allTokens.size, eligible.size)
        val staleTokens = mutableListOf<String>()

        for (batch in allTokens.chunked(100)) {
            try {
                val message = ExpoPushMessage(batch)
                message.title = "New Message"
                message.body = "You have a new message"
                message.sound = ExpoMessageSound()
                message.priority = Priority.NORMAL

                val tickets = client.sendPushNotificationsAsync(listOf(message)).get()
                tickets.forEachIndexed { index, ticket ->
                    if (ticket.status == Status.ERROR && ticket.details?.error == TicketError.DEVICENOTREGISTERED) {
                        if (index < batch.size) staleTokens.add(batch[index])
                    }
                }
            } catch (e: Exception) {
                pushLogger.error("Failed to send push batch: {}", e.message)
            }
        }

        if (staleTokens.isNotEmpty()) {
            tokenStore.removeAll(staleTokens)
        }

        pushLogger.info(
            "Push complete: {} sent, {} stale removed",
            allTokens.size - staleTokens.size,
            staleTokens.size,
        )
    }
}
