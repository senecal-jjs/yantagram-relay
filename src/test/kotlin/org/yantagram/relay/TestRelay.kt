package org.yantagram.relay

import io.ktor.server.application.Application
import kotlinx.coroutines.Dispatchers
import org.yantagram.relay.api.BridgeStatus
import org.yantagram.relay.api.relayModule
import org.yantagram.relay.protocol.Codec
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.Limits
import org.yantagram.relay.protocol.NewItemsListener
import org.yantagram.relay.protocol.PublishItem
import org.yantagram.relay.protocol.PublishRequest
import org.yantagram.relay.protocol.RelayService
import org.yantagram.relay.protocol.RetireRequest
import org.yantagram.relay.protocol.SignedItem
import org.yantagram.relay.protocol.StreamBus
import org.yantagram.relay.protocol.WriteAuth
import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.push.registry.Store
import org.yantagram.relay.push.tempDbPath
import org.yantagram.relay.store.Database
import org.yantagram.relay.store.ItemStore
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.Signature
import java.util.Base64

/** A mutable clock for tests, in epoch milliseconds. */
class TestClock(var now: Long = 1_800_000_000_000) {
    fun advance(millis: Long) {
        now += millis
    }
}

/** A relay wired up against a temporary database. */
class TestRelay(
    val path: String = tempDbPath(),
    val clock: TestClock = TestClock(),
    limits: Limits = Limits(itemsPerWindow = 1_000_000, bytesPerDay = Long.MAX_VALUE),
    bridgeEnabled: Boolean = false,
    registry: Registry? = null,
    onNewItems: NewItemsListener = NewItemsListener { _, _, _ -> },
) {
    val db = Database(path)
    val store = ItemStore(db) { clock.now }
    val metrics = Metrics()
    val registry: Registry = registry ?: Registry(Store(db), 200_000, Dispatchers.Unconfined)
    val service = RelayService(store, StreamBus(), limits, metrics, bridgeEnabled, onNewItems)

    fun install(app: Application, registrationsPerMinute: Int = 1_000, bridge: BridgeStatus = BridgeStatus { "off" }) =
        app.relayModule(service, registry, metrics, bridge, registrationsPerMinute) { clock.now }
}

/** A stream's write key, as a client derives it for one (secret, lane, window). */
class StreamKey {
    private val pair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
    private val secret: PrivateKey = pair.private

    /** The raw 32-byte key: the last 32 bytes of its X.509 encoding. */
    val publicKey: ByteArray = pair.public.encoded.takeLast(32).toByteArray()
    val pk: String = Codec.hex(publicKey)
    val tag: String = Codec.hex(WriteAuth.tagFor(publicKey))

    fun sign(message: ByteArray): String = Codec.hex(
        Signature.getInstance("Ed25519").run {
            initSign(secret)
            update(message)
            sign()
        },
    )

    fun publish(lane: Lane, items: List<PublishItem>, legacyTag: String? = null): PublishRequest {
        val signed = items.map {
            val body = Base64.getDecoder().decode(it.body)
            SignedItem(
                WriteAuth.sha256(body),
                it.checkpoint,
                it.ref?.let(Codec::hexToBytes),
                it.deleteHash?.let(Codec::hexToBytes),
            )
        }
        return PublishRequest(lane.wire, pk, sign(WriteAuth.publishMessage(lane, signed)), items, legacyTag)
    }

    fun retire(seqs: List<Long>) = RetireRequest(pk, sign(WriteAuth.retireMessage(seqs)), seqs)
}

fun item(text: String, ref: String? = null, checkpoint: Boolean = false, deleteHash: String? = null, marker: String? = null) =
    PublishItem(Base64.getEncoder().encodeToString(text.toByteArray()), ref, checkpoint, deleteHash, marker)

fun bodyText(base64: String) = String(Base64.getDecoder().decode(base64))

internal fun Codec.hexToBytes(value: String): ByteArray = java.util.HexFormat.of().parseHex(value)
