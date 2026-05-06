package org.yantagram.relay

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject

/**
 * Builds the OpenAPI 3.1.0 spec for the relay as a [JsonObject].
 */
fun buildOpenApiSpec(): JsonObject = buildJsonObject {
    put("openapi", "3.1.0")

    putJsonObject("info") {
        put("title", "Yantagram Relay")
        put("version", "1.0.0")
        put("description", "Binary packet relay with HTTP publish and WebSocket subscribe endpoints.")
    }

    putJsonObject("components") {
        putJsonObject("securitySchemes") {
            putJsonObject("publishSecret") {
                put("type", "apiKey")
                put("in", "header")
                put("name", "X-Publish-Secret")
                put("description", "Shared secret for publisher authentication.")
            }
        }
    }

    putJsonObject("paths") {
        putJsonObject("/publish") {
            putJsonObject("post") {
                put("summary", "Publish a binary packet")
                put("description", "Appends a binary payload to the packet ring for fan-out to subscribers.")
                put("operationId", "publishPacket")

                putJsonArray("security") {
                    add(buildJsonObject { putJsonArray("publishSecret") {} })
                }

                putJsonObject("requestBody") {
                    put("required", true)
                    putJsonObject("content") {
                        putJsonObject("application/octet-stream") {
                            putJsonObject("schema") {
                                put("type", "string")
                                put("format", "binary")
                            }
                        }
                    }
                }

                putJsonObject("responses") {
                    putJsonObject("204") {
                        put("description", "Packet accepted and queued for delivery.")
                    }
                    putJsonObject("400") {
                        put("description", "Empty payload.")
                        putJsonObject("content") {
                            putJsonObject("text/plain") {
                                putJsonObject("schema") {
                                    put("type", "string")
                                }
                            }
                        }
                    }
                    putJsonObject("401") {
                        put("description", "Missing or invalid X-Publish-Secret header.")
                        putJsonObject("content") {
                            putJsonObject("text/plain") {
                                putJsonObject("schema") {
                                    put("type", "string")
                                }
                            }
                        }
                    }
                }
            }
        }

        putJsonObject("/subscribe") {
            putJsonObject("get") {
                put("summary", "Subscribe to live packet stream (WebSocket)")
                put("operationId", "subscribePackets")
                put(
                    "description",
                    """
                    WebSocket upgrade endpoint. After connection, the server replays buffered packets
                    with seq > `since` (if provided), then streams new packets in real-time.
                    Each message is a binary frame: [seq:8 bytes big-endian][payload].
                    """.trimIndent()
                )

                putJsonArray("parameters") {
                    add(buildJsonObject {
                        put("name", "since")
                        put("in", "query")
                        put("required", false)
                        put("description", "Sequence number for catch-up replay. Only packets with seq > since are delivered.")
                        putJsonObject("schema") {
                            put("type", "integer")
                            put("format", "int64")
                            put("default", JsonPrimitive(0))
                        }
                    })
                }

                putJsonObject("responses") {
                    putJsonObject("101") {
                        put("description", "WebSocket upgrade successful. Binary frames follow.")
                    }
                }
            }
        }
    }
}

/**
 * Installs the GET /openapi.json route that serves the generated spec.
 */
fun Application.openApiModule() {
    val spec = buildOpenApiSpec().toString()

    routing {
        get("/openapi.json") {
            call.respondText(spec, ContentType.Application.Json, HttpStatusCode.OK)
        }
    }
}

