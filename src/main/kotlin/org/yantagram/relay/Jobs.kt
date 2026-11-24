package org.yantagram.relay

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.yantagram.relay.protocol.Limits
import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.store.ItemStore
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours

private val log = LoggerFactory.getLogger("org.yantagram.relay.Jobs")

/**
 * Hourly: deletes expired items (each lane's retention; checkpoints a year) and stream
 * rows that have been empty and idle for over a year, so a stream's head can't go back
 * while anyone might still hold a cursor for it.
 */
fun CoroutineScope.startSweep(store: ItemStore, limits: Limits, metrics: Metrics, interval: Duration = 1.hours) = launch {
    while (isActive) {
        runLogged("sweep") {
            val deleted = store.sweep(streamIdleMillis = STREAM_IDLE.inWholeMilliseconds)
            metrics.itemsExpired.increment(deleted.toDouble())
            if (deleted > 0) log.info("swept {} expired items", deleted)
            limits.prune()
        }
        delay(interval)
    }
}

/** Daily: deletes push registrations not refreshed within [ttl]. */
fun CoroutineScope.startRegistrationExpiry(
    registry: Registry,
    ttl: Duration,
    nowMillis: () -> Long = System::currentTimeMillis,
) = launch {
    while (isActive) {
        runLogged("registration expiry") {
            val removed = registry.expire(nowMillis() - ttl.inWholeMilliseconds)
            if (removed > 0) log.info("expired {} registrations", removed)
        }
        delay(1.days)
    }
}

private suspend fun runLogged(name: String, block: suspend () -> Unit) {
    try {
        block()
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        log.warn("{} failed: {}", name, e.javaClass.simpleName)
    }
}

private val STREAM_IDLE = 400.days
