package org.yantagram.relay.push

import kotlinx.coroutines.test.runTest
import org.yantagram.relay.Metrics
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.push.registry.RegistrationOptions
import org.yantagram.relay.store.NewItem
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals

class ItemMatcherTest {
    /** A minimal MESSAGE packet header (version 1, type 4) with a 1-byte payload. */
    private val chatPacket = byteArrayOf(1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 42)
    private val opPacket = byteArrayOf(1, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 42)
    private val selfKey = ByteArray(32) { 5 }

    private fun marked(body: ByteArray) =
        NewItem(body, marker = SenderMarker.compute(selfKey, Base64.getEncoder().encodeToString(body)))

    private suspend fun wakes(lane: Lane, items: List<NewItem>): List<Wake> {
        val registry = newRegistry()
        registry.replace(apnsToken(1), Platform.IOS, setOf(tag(1)), 0, RegistrationOptions(alerts = true))
        registry.replace(apnsToken(2), Platform.IOS, setOf(tag(1)), 0, RegistrationOptions(alerts = true, selfKey = selfKey))
        registry.replace(fcmToken(1), Platform.ANDROID, setOf(tag(1)), 0)
        val sent = mutableListOf<Wake>()
        ItemMatcher(registry, Coalescer(kotlin.time.Duration.ZERO, { sent += it }), Metrics()).onNewItems(tag(1), lane, items)
        return sent.sortedBy { it.target.token }
    }

    @Test
    fun `chat on the group lane alerts iOS, wakes Android, and skips the sender`() = runTest {
        val sent = wakes(Lane.GROUP, listOf(marked(chatPacket)))
        assertEquals(
            listOf(apnsToken(1) to PushKind.ALERT, fcmToken(1) to PushKind.SILENT),
            sent.map { it.target.token to it.kind }.sortedBy { it.first },
        )
    }

    @Test
    fun `control items always wake silently`() = runTest {
        val sent = wakes(Lane.CONTROL, listOf(NewItem(chatPacket), NewItem(opPacket)))
        assertEquals(3, sent.size)
        assertEquals(setOf(PushKind.SILENT), sent.map { it.kind }.toSet())
    }

    @Test
    fun `a batch only partly from this device still wakes it`() = runTest {
        val sent = wakes(Lane.CONTROL, listOf(marked(opPacket), NewItem(opPacket + 1)))
        assertEquals(3, sent.size)
    }

    @Test
    fun `file parts wake nobody`() = runTest {
        assertEquals(emptyList(), wakes(Lane.BLOB, listOf(NewItem(opPacket))))
    }
}
