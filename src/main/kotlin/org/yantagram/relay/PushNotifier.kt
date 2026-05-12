package org.yantagram.relay

import io.github.jav.exposerversdk.ExpoPushMessage
import io.github.jav.exposerversdk.PushClient
import io.github.jav.exposerversdk.PushClientCustomData
import io.github.jav.exposerversdk.enums.Priority
import io.github.jav.exposerversdk.enums.Status
import io.github.jav.exposerversdk.enums.TicketError
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.hours

private val pushLogger = LoggerFactory.getLogger("org.yantagram.relay.PushNotifier")

/**
 * Sends periodic silent push notifications to all registered Expo tokens
 * to wake client apps for background sync.
 */
class PushNotifier(
    private val tokenStore: PushTokenStore,
    private val intervalHours: Long,
) {
    private val client = PushClient()
    private var schedulerJob: Job? = null

    /** Start the periodic push scheduler. */
    fun start(scope: CoroutineScope) {
        pushLogger.info("Push notifier started (interval={}h)", intervalHours)
        schedulerJob = scope.launch(Dispatchers.IO) {
            while (isActive) {
                sendSyncPush()
                delay(intervalHours.hours)
            }
        }
    }

    /** Send a silent sync push to all registered tokens. */
    fun sendSyncPush() {
        val tokens = tokenStore.getAllTokens()
        if (tokens.isEmpty()) {
            pushLogger.debug("No push tokens registered, skipping sync push")
            return
        }

        val validTokens = tokens.filter { PushClientCustomData.isExponentPushToken(it) }
        val invalidTokens = tokens - validTokens.toSet()
        if (invalidTokens.isNotEmpty()) {
            pushLogger.warn("Removing {} invalid push tokens", invalidTokens.size)
            tokenStore.removeAll(invalidTokens)
        }
        if (validTokens.isEmpty()) return

        pushLogger.info("Sending silent sync push to {} tokens", validTokens.size)
        val batches = validTokens.chunked(100)
        val staleTokens = mutableListOf<String>()

        for (batch in batches) {
            try {
                val message = ExpoPushMessage(batch)
                message.data = mapOf("action" to "sync" as Any)
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

        if (staleTokens.isNotEmpty()) tokenStore.removeAll(staleTokens)
        pushLogger.info(
            "Sync push complete: {} sent, {} stale removed",
            validTokens.size - staleTokens.size,
            staleTokens.size
        )
    }

    fun stop() {
        schedulerJob?.cancel()
    }
}
