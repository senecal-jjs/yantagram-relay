package org.yantagram.relay.protocol

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.Signature
import java.security.spec.X509EncodedKeySpec

/**
 * Write authorization without identity.
 *
 * Each (secret, lane, window) has an Ed25519 key pair that only holders of the secret
 * can derive. A stream's tag is the first 16 bytes of SHA-256 of its public key, so the
 * relay can check that a publisher holds the key for the stream it writes to, and the
 * public key changes every window, so it links nothing across days.
 *
 * Signed messages (all fields raw bytes, integers big-endian):
 *
 *   publish: "yantagram/relay/publish/v1" | u8 lane | for each item:
 *            id(32) | u8 flags | [ref(16) if flags&2] | [delete_hash(32) if flags&4]
 *            flags: 1 checkpoint, 2 has ref, 4 has delete_hash
 *   retire:  "yantagram/relay/retire/v1" | for each seq: u64 seq
 */
object WriteAuth {
    const val TAG_BYTES = 16
    const val PUBLIC_KEY_BYTES = 32
    const val SIGNATURE_BYTES = 64

    private val PUBLISH_DOMAIN = "yantagram/relay/publish/v1".toByteArray(Charsets.US_ASCII)
    private val RETIRE_DOMAIN = "yantagram/relay/retire/v1".toByteArray(Charsets.US_ASCII)

    /** DER prefix of an X.509 SubjectPublicKeyInfo for a raw Ed25519 key. */
    private val ED25519_SPKI_PREFIX = byteArrayOf(
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    )

    fun tagFor(publicKey: ByteArray): ByteArray = sha256(publicKey).copyOf(TAG_BYTES)

    fun publishMessage(lane: Lane, items: List<SignedItem>): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(PUBLISH_DOMAIN)
        out.write(lane.code)
        for (item in items) {
            out.write(item.id)
            var flags = 0
            if (item.checkpoint) flags = flags or 1
            if (item.ref != null) flags = flags or 2
            if (item.deleteHash != null) flags = flags or 4
            out.write(flags)
            item.ref?.let(out::write)
            item.deleteHash?.let(out::write)
        }
        return out.toByteArray()
    }

    fun retireMessage(seqs: List<Long>): ByteArray {
        val buffer = ByteBuffer.allocate(RETIRE_DOMAIN.size + 8 * seqs.size)
        buffer.put(RETIRE_DOMAIN)
        seqs.forEach(buffer::putLong)
        return buffer.array()
    }

    /** Whether [signature] over [message] verifies under the raw Ed25519 [publicKey]. */
    fun verify(publicKey: ByteArray, message: ByteArray, signature: ByteArray): Boolean {
        if (publicKey.size != PUBLIC_KEY_BYTES || signature.size != SIGNATURE_BYTES) return false
        return try {
            val key = KeyFactory.getInstance("Ed25519").generatePublic(X509EncodedKeySpec(ED25519_SPKI_PREFIX + publicKey))
            Signature.getInstance("Ed25519").run {
                initVerify(key)
                update(message)
                verify(signature)
            }
        } catch (_: Exception) {
            false
        }
    }

    fun sha256(bytes: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(bytes)
}

/** The parts of an item a publish signature covers. */
class SignedItem(val id: ByteArray, val checkpoint: Boolean, val ref: ByteArray?, val deleteHash: ByteArray?)
