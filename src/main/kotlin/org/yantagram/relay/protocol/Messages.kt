package org.yantagram.relay.protocol

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.yantagram.relay.store.Rejected
import org.yantagram.relay.store.StoredItem
import java.util.Base64
import java.util.HexFormat

/*
 * The wire protocol. JSON over HTTPS (POST /v2/<op>) or WebSocket (/v2/ws), the same
 * fields either way; over WebSocket each message also has "type" and a client-chosen
 * "req_id" that the response echoes.
 *
 * Encodings: tags, ids, keys, signatures, refs and hashes are lowercase hex; bodies are
 * standard base64. A tag is 16 bytes, an id 32 (SHA-256 of the body).
 */

val WireJson = Json {
    ignoreUnknownKeys = true
    explicitNulls = false
    encodeDefaults = true
}

// ---- Requests ----

@Serializable
data class PublishItem(
    val body: String,
    val ref: String? = null,
    val checkpoint: Boolean = false,
    @SerialName("delete_hash") val deleteHash: String? = null,
    /** Sender marker for the push service (see SenderMarker). */
    val marker: String? = null,
)

@Serializable
data class PublishRequest(
    val lane: String,
    val pk: String,
    val sig: String,
    val items: List<PublishItem>,
    /** Migration only: the old-version tag to copy these items to on strfry. */
    @SerialName("legacy_tag") val legacyTag: String? = null,
)

@Serializable
data class SyncCursor(val tag: String, val after: Long = 0)

@Serializable
data class SyncRequest(val cursors: List<SyncCursor>, val live: Boolean = false)

@Serializable
data class UnsubscribeRequest(val tags: List<String>)

@Serializable
data class FetchRequest(val refs: List<String>)

@Serializable
data class CheckpointRequest(val tags: List<String>)

@Serializable
data class HaveRequest(val tag: String, val ids: List<String>)

@Serializable
data class DeleteRequest(val tag: String, val seq: Long, val k: String)

@Serializable
data class RetireRequest(val pk: String, val sig: String, val seqs: List<Long>)

// ---- Responses ----

@Serializable
data class WireItem(
    val tag: String,
    val seq: Long,
    val id: String,
    val body: String,
    val checkpoint: Boolean = false,
)

@Serializable
data class StoredResponse(val tag: String, val seqs: List<Long>, val head: Long)

@Serializable
data class StreamPage(
    val tag: String,
    val items: List<WireItem>,
    /** The stream's last seq. A client that has everything up to here has no gap. */
    val head: Long,
    /** The oldest seq still stored (head + 1 if none): anything below it has expired. */
    val low: Long,
    /** HTTP only: the response hit its size limit; sync again from the last item. */
    val more: Boolean = false,
)

@Serializable
data class SyncResponse(val streams: List<StreamPage>)

@Serializable
data class ItemsResponse(val items: List<WireItem>)

@Serializable
data class CheckpointResponse(val item: WireItem? = null)

@Serializable
data class HaveResponse(val present: List<String>)

@Serializable
data class DeleteResponse(val deleted: Boolean)

@Serializable
data class RetireResponse(val retired: Int)

@Serializable
data class ErrorResponse(val reason: String)

// ---- Codec helpers ----

internal object Codec {
    private val hex = HexFormat.of()
    private val b64 = Base64.getEncoder()
    private val b64d = Base64.getDecoder()
    private val HEX = Regex("^[0-9a-f]*$")

    fun hex(bytes: ByteArray): String = hex.formatHex(bytes)
    fun base64(bytes: ByteArray): String = b64.encodeToString(bytes)

    /** Decodes lowercase hex of exactly [bytes] bytes, or rejects with [reason]. */
    fun hexOf(value: String, bytes: Int, reason: String): ByteArray {
        if (value.length != bytes * 2 || !HEX.matches(value)) throw Rejected(reason)
        return hex.parseHex(value)
    }

    fun hexRange(value: String, min: Int, max: Int, reason: String): ByteArray {
        if (value.length % 2 != 0 || value.length < min * 2 || value.length > max * 2 || !HEX.matches(value)) {
            throw Rejected(reason)
        }
        return hex.parseHex(value)
    }

    fun tag(value: String): ByteArray = hexOf(value, WriteAuth.TAG_BYTES, "bad-tag")

    fun body(value: String): ByteArray = try {
        b64d.decode(value)
    } catch (_: IllegalArgumentException) {
        throw Rejected("bad-body")
    }

    fun item(item: StoredItem) = WireItem(hex(item.tag), item.seq, hex(item.id), base64(item.body), item.checkpoint)
}
