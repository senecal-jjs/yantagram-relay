package org.yantagram.relay

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.http.ContentType
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.Gauge
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.yantagram.relay.api.BridgeStatus
import org.yantagram.relay.api.relayModule
import org.yantagram.relay.bridge.StrfryBridge
import org.yantagram.relay.protocol.Limits
import org.yantagram.relay.protocol.NewItemsListener
import org.yantagram.relay.protocol.RelayService
import org.yantagram.relay.protocol.StreamBus
import org.yantagram.relay.push.ApnsSender
import org.yantagram.relay.push.Coalescer
import org.yantagram.relay.push.FcmSender
import org.yantagram.relay.push.ItemMatcher
import org.yantagram.relay.push.PushDispatcher
import org.yantagram.relay.push.PushSender
import org.yantagram.relay.push.PushyApnsGateway
import org.yantagram.relay.push.ServiceAccountTokenProvider
import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.push.registry.Store
import org.yantagram.relay.store.Database
import org.yantagram.relay.store.ItemStore
import org.yantagram.relay.store.MetaStore
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger("org.yantagram.relay.Main")

fun main() {
    val config = Config.fromEnv()
    val prometheus = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    val metrics = Metrics(prometheus)
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    val db = Database(config.dbPath)
    val store = ItemStore(db)
    val registry = Registry(Store(db), config.maxRegistrations)
    runBlocking { registry.load() }
    log.info("loaded {} push registrations", registry.size)

    val http = HttpClient(CIO) {
        install(WebSockets) { pingIntervalMillis = 20.seconds.inWholeMilliseconds }
        install(HttpTimeout) { connectTimeoutMillis = 10.seconds.inWholeMilliseconds }
    }

    // Push: only the providers that are configured.
    val platformSenders = listOfNotNull<PushSender>(
        config.apns?.let { ApnsSender(PushyApnsGateway.create(it), it.topic) },
        config.fcm?.let { FcmSender(http, it.projectId, ServiceAccountTokenProvider(it)) },
    ).associateBy { it.platform }
    lateinit var coalescer: Coalescer
    val dispatcher = PushDispatcher(platformSenders, registry, metrics, onTokenRemoved = { coalescer.forget(it) })
    coalescer = Coalescer(config.coalesceWindow, dispatcher::enqueue, alertWindow = config.alertCoalesceWindow)
    val matcher = ItemMatcher(registry, coalescer, metrics)
    if (platformSenders.isEmpty()) log.warn("push is off: no APNs or FCM credentials configured")

    var bridge: StrfryBridge? = null
    val limits = Limits()
    val service = RelayService(
        store = store,
        bus = StreamBus(),
        limits = limits,
        metrics = metrics,
        bridgeEnabled = config.strfryUrl != null,
        onNewItems = NewItemsListener { tag, lane, items ->
            if (platformSenders.isNotEmpty()) matcher.onNewItems(tag, lane, items)
            bridge?.poke()
        },
    )
    bridge = config.strfryUrl?.let { StrfryBridge(http, it, store, MetaStore(db), service, metrics) }

    Gauge.builder("push_registrations") { registry.size }.register(prometheus)
    Gauge.builder("relay_live_streams") { service.bus.subscribedTags }.register(prometheus)
    Gauge.builder("bridge_connected") { if (bridge?.state() == "connected") 1 else 0 }.register(prometheus)
    Gauge.builder("bridge_outbox") { runBlocking { store.outboxSize() } }.register(prometheus)

    dispatcher.start(scope)
    coalescer.start(scope)
    bridge?.start(scope)
    scope.startSweep(store, limits, metrics)
    scope.startRegistrationExpiry(registry, config.registrationTtl)

    // Metrics on their own port, never routed publicly (see fly.toml).
    embeddedServer(Netty, port = config.metricsPort) {
        routing {
            get("/metrics") { call.respondText(prometheus.scrape(), ContentType.Text.Plain) }
        }
    }.start(wait = false)

    embeddedServer(Netty, port = config.port) {
        relayModule(service, registry, metrics, bridge ?: BridgeStatus { "off" })
        monitor.subscribe(ApplicationStopped) {
            runBlocking {
                bridge?.saveCursor()
                platformSenders.values.forEach { it.close() }
            }
            dispatcher.close()
            http.close()
            scope.cancel()
            db.close()
        }
    }.start(wait = true)
}
