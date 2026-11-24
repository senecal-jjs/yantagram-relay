package org.yantagram.relay.api

import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.plugins.PayloadTooLargeException
import io.ktor.server.plugins.bodylimit.RequestBodyLimit
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.origin
import io.ktor.server.plugins.ratelimit.RateLimit
import io.ktor.server.request.header
import io.ktor.server.request.receive
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.pingPeriod
import io.ktor.server.websocket.timeout
import io.ktor.server.websocket.webSocket
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import org.yantagram.relay.Metrics
import org.yantagram.relay.protocol.CheckpointRequest
import org.yantagram.relay.protocol.DeleteRequest
import org.yantagram.relay.protocol.ErrorResponse
import org.yantagram.relay.protocol.FetchRequest
import org.yantagram.relay.protocol.HaveRequest
import org.yantagram.relay.protocol.PublishRequest
import org.yantagram.relay.protocol.RelayService
import org.yantagram.relay.protocol.RetireRequest
import org.yantagram.relay.protocol.SyncRequest
import org.yantagram.relay.protocol.WireJson
import org.yantagram.relay.push.REGISTER_RATE_LIMIT
import org.yantagram.relay.push.pushRoutes
import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.store.Rejected
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger("org.yantagram.relay.api")

/** Largest request: a full publish batch (4 MB of bodies) in base64, plus JSON. */
const val MAX_REQUEST_BYTES = 6L * 1024 * 1024

@Serializable
data class Health(val status: String, val bridge: String, val registrations: Int, val liveStreams: Int)

/** What /health reports about the strfry bridge: "off", "connected" or "reconnecting". */
fun interface BridgeStatus {
    fun state(): String
}

/**
 * Plugins and routes. Deliberately no CallLogging: logs never hold IPs, tags, tokens or
 * bodies.
 */
fun Application.relayModule(
    service: RelayService,
    registry: Registry,
    metrics: Metrics,
    bridge: BridgeStatus = BridgeStatus { "off" },
    registrationsPerMinute: Int = 30,
    clock: () -> Long = System::currentTimeMillis,
) {
    install(ContentNegotiation) { json(WireJson) }
    install(WebSockets) {
        pingPeriod = 20.seconds
        timeout = 60.seconds
        maxFrameSize = MAX_REQUEST_BYTES
    }
    install(RateLimit) {
        register(REGISTER_RATE_LIMIT) {
            rateLimiter(limit = registrationsPerMinute, refillPeriod = 1.minutes)
            requestKey { call -> call.clientIp() }
        }
    }
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            when {
                cause is Rejected -> call.respond(statusFor(cause.reason), ErrorResponse(cause.reason))
                cause.causes().any { it is PayloadTooLargeException } ->
                    call.respond(HttpStatusCode.PayloadTooLarge, ErrorResponse("too-large"))
                cause is BadRequestException -> call.respond(HttpStatusCode.BadRequest, ErrorResponse("bad-request"))
                else -> {
                    // The class only: messages can quote request bodies.
                    log.error("request failed: {}", cause.javaClass.name)
                    call.respond(HttpStatusCode.InternalServerError, ErrorResponse("internal"))
                }
            }
        }
    }
    routing {
        relayRoutes(service, metrics)
        pushRoutes(registry, metrics, clock)
        get("/health") {
            call.respond(Health("ok", bridge.state(), registry.size, service.bus.subscribedTags))
        }
    }
}

private fun Route.relayRoutes(service: RelayService, metrics: Metrics) {
    route("/v2") {
        install(RequestBodyLimit) { bodyLimit { MAX_REQUEST_BYTES } }
        post("/publish") {
            metrics.request("publish", "http")
            call.respond(rejecting("publish", metrics) { service.publish(call.receive<PublishRequest>(), call.clientIp()) })
        }
        post("/sync") {
            metrics.request("sync", "http")
            call.respond(rejecting("sync", metrics) { service.sync(call.receive<SyncRequest>()) })
        }
        post("/fetch") {
            metrics.request("fetch", "http")
            call.respond(rejecting("fetch", metrics) { service.fetch(call.receive<FetchRequest>()) })
        }
        post("/checkpoint") {
            metrics.request("checkpoint", "http")
            call.respond(rejecting("checkpoint", metrics) { service.latestCheckpoint(call.receive<CheckpointRequest>()) })
        }
        post("/have") {
            metrics.request("have", "http")
            call.respond(rejecting("have", metrics) { service.have(call.receive<HaveRequest>()) })
        }
        post("/delete") {
            metrics.request("delete", "http")
            call.respond(rejecting("delete", metrics) { service.delete(call.receive<DeleteRequest>()) })
        }
        post("/retire") {
            metrics.request("retire", "http")
            call.respond(rejecting("retire", metrics) { service.retire(call.receive<RetireRequest>()) })
        }
        webSocket("/ws") {
            WsSession(this, service, metrics, call.clientIp()).run()
        }
    }
}

private inline fun <T> rejecting(op: String, metrics: Metrics, block: () -> T): T = try {
    block()
} catch (e: Rejected) {
    metrics.rejected(op, e.reason)
    throw e
}

private fun statusFor(reason: String) = when (reason) {
    "rate-limited", "daily-limit" -> HttpStatusCode.TooManyRequests
    "too-large" -> HttpStatusCode.PayloadTooLarge
    "stream-full" -> HttpStatusCode.Conflict
    else -> HttpStatusCode.BadRequest
}

/** Fly's edge sets Fly-Client-IP; used only for in-memory rate limits, never logged. */
fun ApplicationCall.clientIp(): String = request.header("Fly-Client-IP") ?: request.origin.remoteAddress

private fun Throwable.causes() = generateSequence(this) { it.cause }
