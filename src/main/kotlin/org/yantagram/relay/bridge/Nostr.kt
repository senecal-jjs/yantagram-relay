package org.yantagram.relay.bridge

import fr.acinq.secp256k1.Secp256k1
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.HexFormat

/** The parts of a strfry event the bridge uses. [content] is the base64 packet. */
data class NostrEvent(
    val id: String,
    val createdAt: Long,
    val kind: Int,
    val routingTag: String?,
    val senderMarker: String?,
    val expiration: Long?,
    val content: String,
)

sealed interface NostrMessage {
    data class Event(val subscriptionId: String, val event: NostrEvent) : NostrMessage
    data class Ok(val eventId: String, val accepted: Boolean, val message: String) : NostrMessage
    data class Closed(val subscriptionId: String, val message: String) : NostrMessage
    data object Other : NostrMessage
}

/** NIP-01 messages, as the app and the strfry write policy use them (kind 2929). */
object Nostr {
    const val KIND = 2929
    /** The strfry write policy's limit on an event's lifetime. */
    const val MAX_TTL_SECONDS = 7L * 24 * 3600
    /** The strfry write policy's limit on a decoded packet. */
    const val MAX_BODY_BYTES = 40_960

    private val json = Json { ignoreUnknownKeys = true }
    private val hex = HexFormat.of()
    private val random = SecureRandom()
    private val HEX32 = Regex("^[0-9a-f]{32}$")

    fun req(subscriptionId: String, since: Long): String = buildJsonArray {
        add(JsonPrimitive("REQ"))
        add(JsonPrimitive(subscriptionId))
        add(buildJsonObject {
            putJsonArray("kinds") { add(JsonPrimitive(KIND)) }
            put("since", since)
        })
    }.toString()

    /**
     * A signed EVENT message carrying [content] under [routingTag]. Like the app, each
     * event is signed with a fresh random key, so its pubkey links it to nothing.
     */
    fun eventMessage(routingTag: String, content: String, senderMarker: String?, createdAt: Long): Pair<String, String> {
        val secretKey = ByteArray(32).also(random::nextBytes)
        val pubkey = hex.formatHex(Secp256k1.pubkeyCreate(secretKey).copyOfRange(1, 33))
        val tags = buildJsonArray {
            add(buildJsonArray { add(JsonPrimitive("y")); add(JsonPrimitive(routingTag)) })
            add(buildJsonArray { add(JsonPrimitive("expiration")); add(JsonPrimitive((createdAt + MAX_TTL_SECONDS).toString())) })
            if (senderMarker != null) add(buildJsonArray { add(JsonPrimitive("s")); add(JsonPrimitive(senderMarker)) })
        }
        // NIP-01 canonical form. Our content is base64 and tags are hex and digits, so
        // compact JSON needs no escaping that could differ from the spec.
        val serialized = buildJsonArray {
            add(JsonPrimitive(0))
            add(JsonPrimitive(pubkey))
            add(JsonPrimitive(createdAt))
            add(JsonPrimitive(KIND))
            add(tags)
            add(JsonPrimitive(content))
        }.toString()
        val id = MessageDigest.getInstance("SHA-256").digest(serialized.toByteArray(Charsets.UTF_8))
        val aux = ByteArray(32).also(random::nextBytes)
        val sig = Secp256k1.signSchnorr(id, secretKey, aux)
        val idHex = hex.formatHex(id)
        val event = buildJsonObject {
            put("id", idHex)
            put("pubkey", pubkey)
            put("created_at", createdAt)
            put("kind", KIND)
            put("tags", tags)
            put("content", content)
            put("sig", hex.formatHex(sig))
        }
        return idHex to buildJsonArray { add(JsonPrimitive("EVENT")); add(event) }.toString()
    }

    fun parse(text: String): NostrMessage = try {
        val array = json.parseToJsonElement(text).jsonArray
        when (array.getOrNull(0)?.jsonPrimitive?.contentOrNull) {
            "EVENT" -> parseEvent(array)
            "OK" -> NostrMessage.Ok(
                array[1].jsonPrimitive.content,
                array[2].jsonPrimitive.booleanOrNull ?: false,
                array.getOrNull(3)?.jsonPrimitive?.contentOrNull.orEmpty(),
            )
            "CLOSED" -> NostrMessage.Closed(array[1].jsonPrimitive.content, array.getOrNull(2)?.jsonPrimitive?.contentOrNull.orEmpty())
            else -> NostrMessage.Other
        }
    } catch (_: Exception) {
        NostrMessage.Other
    }

    private fun parseEvent(array: JsonArray): NostrMessage {
        val subscriptionId = array[1].jsonPrimitive.content
        val event = array[2] as? JsonObject ?: return NostrMessage.Other
        val id = event["id"]?.jsonPrimitive?.contentOrNull ?: return NostrMessage.Other
        val createdAt = event["created_at"]?.jsonPrimitive?.longOrNull ?: return NostrMessage.Other
        val kind = event["kind"]?.jsonPrimitive?.intOrNull ?: return NostrMessage.Other
        val tags = (event["tags"] as? JsonArray).orEmpty().mapNotNull { it as? JsonArray }
        fun values(name: String) = tags
            .filter { it.getOrNull(0)?.jsonPrimitive?.contentOrNull == name }
            .mapNotNull { it.getOrNull(1)?.jsonPrimitive?.contentOrNull }
        fun singleHex(name: String) = values(name).singleOrNull()?.takeIf { HEX32.matches(it) }
        return NostrMessage.Event(
            subscriptionId,
            NostrEvent(
                id = id,
                createdAt = createdAt,
                kind = kind,
                routingTag = singleHex("y"),
                senderMarker = singleHex("s"),
                expiration = values("expiration").singleOrNull()?.toLongOrNull(),
                content = event["content"]?.jsonPrimitive?.contentOrNull.orEmpty(),
            ),
        )
    }
}
