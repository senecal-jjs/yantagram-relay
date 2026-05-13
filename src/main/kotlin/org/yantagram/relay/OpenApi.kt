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

                putJsonArray("parameters") {
                    add(buildJsonObject {
                        put("name", "X-Verification-Key")
                        put("in", "header")
                        put("required", false)
                        put("description", "Optional verification key to tag this packet. Subscribers can filter by key. Max 64 characters.")
                        putJsonObject("schema") {
                            put("type", "string")
                            put("maxLength", JsonPrimitive(64))
                        }
                    })
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

        putJsonObject("/push/register") {
            putJsonObject("post") {
                put("summary", "Register an Expo push token")
                put("operationId", "registerPushToken")
                put("description", "Registers an Expo push token associated with a verification key. Requires X-Publish-Secret and X-Verification-Key headers. Body is the raw Expo push token string. A single VK may have multiple tokens (multiple devices). A token may only be associated with one VK at a time.")

                putJsonArray("security") {
                    add(buildJsonObject { putJsonArray("publishSecret") {} })
                }

                putJsonArray("parameters") {
                    add(buildJsonObject {
                        put("name", "X-Verification-Key")
                        put("in", "header")
                        put("required", true)
                        put("description", "Hex-encoded Ed25519 verification key identifying the user.")
                        putJsonObject("schema") {
                            put("type", "string")
                        }
                    })
                }

                putJsonObject("requestBody") {
                    put("required", true)
                    putJsonObject("content") {
                        putJsonObject("text/plain") {
                            putJsonObject("schema") {
                                put("type", "string")
                                put("description", "Expo push token (e.g. ExponentPushToken[xxx])")
                            }
                        }
                    }
                }

                putJsonObject("responses") {
                    putJsonObject("200") {
                        put("description", "Token registered successfully.")
                    }
                    putJsonObject("400") {
                        put("description", "Empty token or missing X-Verification-Key.")
                    }
                    putJsonObject("401") {
                        put("description", "Missing or invalid X-Publish-Secret header.")
                    }
                }
            }

            putJsonObject("delete") {
                put("summary", "Unregister an Expo push token")
                put("operationId", "unregisterPushToken")
                put("description", "Removes an Expo push token regardless of which verification key it is mapped to. Requires X-Publish-Secret authentication. Body is the raw Expo push token string. Idempotent.")

                putJsonArray("security") {
                    add(buildJsonObject { putJsonArray("publishSecret") {} })
                }

                putJsonObject("requestBody") {
                    put("required", true)
                    putJsonObject("content") {
                        putJsonObject("text/plain") {
                            putJsonObject("schema") {
                                put("type", "string")
                                put("description", "Expo push token to unregister")
                            }
                        }
                    }
                }

                putJsonObject("responses") {
                    putJsonObject("200") {
                        put("description", "Token unregistered successfully.")
                    }
                    putJsonObject("400") {
                        put("description", "Empty token.")
                    }
                    putJsonObject("401") {
                        put("description", "Missing or invalid X-Publish-Secret header.")
                    }
                }
            }
        }

        putJsonObject("/push/notify") {
            putJsonObject("post") {
                put("summary", "Send push notifications to recipients")
                put("operationId", "notifyRecipients")
                put("description", "Requests push notifications be sent to a list of recipient verification keys. For each VK, looks up registered push tokens, skips VKs with active WebSocket subscriptions, applies per-VK debounce (10s), and sends a generic display notification via the Expo Push API. Fire-and-forget from the caller's perspective.")

                putJsonArray("security") {
                    add(buildJsonObject { putJsonArray("publishSecret") {} })
                }

                putJsonObject("requestBody") {
                    put("required", true)
                    putJsonObject("content") {
                        putJsonObject("application/json") {
                            putJsonObject("schema") {
                                put("type", "object")
                                putJsonObject("properties") {
                                    putJsonObject("recipients") {
                                        put("type", "array")
                                        putJsonObject("items") {
                                            put("type", "string")
                                            put("description", "Hex-encoded Ed25519 verification key")
                                        }
                                        put("description", "List of recipient verification keys to notify")
                                    }
                                }
                                putJsonArray("required") {
                                    add(JsonPrimitive("recipients"))
                                }
                            }
                        }
                    }
                }

                putJsonObject("responses") {
                    putJsonObject("200") {
                        put("description", "Notification request accepted (push delivery is best-effort).")
                    }
                    putJsonObject("400") {
                        put("description", "Invalid JSON body.")
                    }
                    putJsonObject("401") {
                        put("description", "Missing or invalid X-Publish-Secret header.")
                    }
                }
            }
        }

        putJsonObject("/poll") {
            putJsonObject("get") {
                put("summary", "Poll queued messages over HTTPS")
                put("operationId", "pollMessages")
                put(
                    "description",
                    "Returns up to 50 queued message frames as a JSON array of base64-encoded strings. " +
                        "Each decoded string has the same binary format as WebSocket frames: " +
                        "[seq:8 bytes BE][keyLen:2 bytes BE][key bytes][payload]. " +
                        "The X-Last-Seq response header contains the highest seq returned (or the since value if empty). " +
                        "Designed for background fetch after a silent push wakes the app."
                )

                putJsonArray("security") {
                    add(buildJsonObject { putJsonArray("publishSecret") {} })
                }

                putJsonArray("parameters") {
                    add(buildJsonObject {
                        put("name", "since")
                        put("in", "query")
                        put("required", false)
                        put("description", "Sequence number for catch-up. Only packets with seq > since are returned.")
                        putJsonObject("schema") {
                            put("type", "integer")
                            put("format", "int64")
                            put("default", JsonPrimitive(0))
                        }
                    })
                    add(buildJsonObject {
                        put("name", "keys")
                        put("in", "query")
                        put("required", false)
                        put("description", "Comma-separated list of verification keys to filter by. If omitted, all packets are returned.")
                        putJsonObject("schema") {
                            put("type", "string")
                        }
                    })
                }

                putJsonObject("responses") {
                    putJsonObject("200") {
                        put("description", "JSON array of base64-encoded message frames.")
                        putJsonObject("headers") {
                            putJsonObject("X-Last-Seq") {
                                put("description", "Highest sequence number in the response, or the since value if no messages.")
                                putJsonObject("schema") {
                                    put("type", "integer")
                                    put("format", "int64")
                                }
                            }
                        }
                        putJsonObject("content") {
                            putJsonObject("application/json") {
                                putJsonObject("schema") {
                                    put("type", "array")
                                    putJsonObject("items") {
                                        put("type", "string")
                                        put("format", "byte")
                                        put("description", "Base64-encoded binary frame")
                                    }
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
                    If `keys` is specified, only packets tagged with a matching verification key are delivered.
                    Each message is a binary frame: [seq:8 bytes big-endian][keyLen:2 bytes big-endian][key bytes][payload].
                    
                    The server sends periodic heartbeat frames (seq=0, empty payload) as keep-alive.
                    Clients should ignore frames with seq=0 and must send periodic binary heartbeat
                    frames (any content) to avoid being disconnected for inactivity.
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
                    add(buildJsonObject {
                        put("name", "keys")
                        put("in", "query")
                        put("required", false)
                        put("description", "Comma-separated list of verification keys to filter by. If omitted, all packets are delivered.")
                        putJsonObject("schema") {
                            put("type", "string")
                        }
                    })
                    add(buildJsonObject {
                        put("name", "vk")
                        put("in", "query")
                        put("required", false)
                        put("description", "The subscriber's own hex-encoded verification key. Used for push notification suppression — while this WebSocket is open, push notifications for this VK are skipped.")
                        putJsonObject("schema") {
                            put("type", "string")
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
