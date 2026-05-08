package org.yantagram.relay

import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers

fun main() {
    val config = RelayConfig.fromEnv()

    val store = PacketStore(
        dbPath = config.dbPath,
        flushIntervalMs = config.dbFlushIntervalMs,
        maxHistoryBytes = config.dbMaxHistoryBytes,
    )
    store.init()

    val ring = PacketRing(
        limitProvider = jvmAwareLimitProvider(config),
        store = store,
    )
    ring.restore()
    store.startFlusher(CoroutineScope(Dispatchers.Default))

    // Push notifications (optional).
    val pushTokenStore: PushTokenStore?
    val pushNotifier: PushNotifier?
    if (config.pushEnabled) {
        pushTokenStore = PushTokenStore(store.db)
        pushTokenStore.init()
        pushNotifier = PushNotifier(
            tokenStore = pushTokenStore,
            intervalHours = config.pushIntervalHours,
        )
        pushNotifier.start(CoroutineScope(Dispatchers.Default))
    } else {
        pushTokenStore = null
        pushNotifier = null
    }

    embeddedServer(
        factory = CIO,
        port = config.port,
        host = config.host,
        module = { relayModule(config, ring, pushTokenStore) },
    ).start(wait = true)
}
