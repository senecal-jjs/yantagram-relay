package org.yantagram.relay.push

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.plugins.bodylimit.RequestBodyLimit
import io.ktor.server.plugins.ratelimit.RateLimitName
import io.ktor.server.plugins.ratelimit.rateLimit
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import org.slf4j.LoggerFactory
import org.yantagram.relay.Metrics
import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.push.registry.RegistrationOptions
import org.yantagram.relay.push.registry.ReplaceResult

val REGISTER_RATE_LIMIT = RateLimitName("register")

/** Room for [MAX_TAGS] tags (~70 KB) and the rest of a registration. */
const val MAX_REGISTER_BODY_BYTES = 128L * 1024

private val log = LoggerFactory.getLogger("org.yantagram.relay.push.routes")

/** Same API as the standalone push service, so the app only changes its push URL. */
fun Route.pushRoutes(
    registry: Registry,
    metrics: Metrics,
    clock: () -> Long = System::currentTimeMillis,
) {
    rateLimit(REGISTER_RATE_LIMIT) {
        route("/push/register") {
            install(RequestBodyLimit) { bodyLimit { MAX_REGISTER_BODY_BYTES } }
            put {
                when (val result = validate(call.receive<RegisterRequest>())) {
                    is Validated.Invalid -> call.respond(HttpStatusCode.BadRequest, result.reason)
                    Validated.TooManyTags -> call.respond(HttpStatusCode.PayloadTooLarge, "too many tags")
                    is Validated.Ok -> when (
                        registry.replace(
                            result.token, result.platform, result.tags, clock(),
                            RegistrationOptions(result.alerts, result.selfKey),
                        )
                    ) {
                        ReplaceResult.OK -> {
                            log.info("registration replaced ({} tags)", result.tags.size)
                            call.respond(HttpStatusCode.NoContent)
                        }
                        ReplaceResult.FULL -> {
                            log.error("ALERT registration cap reached; new registration refused")
                            metrics.registrationCapReached.increment()
                            call.respond(HttpStatusCode.ServiceUnavailable, "registrations full")
                        }
                    }
                }
            }
            // Always 204, so the response never reveals whether a token was registered.
            delete {
                val request = call.receive<UnregisterRequest>()
                if (registry.remove(normalizeToken(request.token))) log.info("registration removed")
                call.respond(HttpStatusCode.NoContent)
            }
        }
    }
}
