package org.yantagram.relay

import kotlinx.coroutines.test.runTest
import org.yantagram.relay.protocol.CheckpointRequest
import org.yantagram.relay.protocol.Codec
import org.yantagram.relay.protocol.DeleteRequest
import org.yantagram.relay.protocol.FetchRequest
import org.yantagram.relay.protocol.HaveRequest
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.protocol.Limits
import org.yantagram.relay.protocol.SyncCursor
import org.yantagram.relay.protocol.SyncRequest
import org.yantagram.relay.protocol.WriteAuth
import org.yantagram.relay.store.Database
import org.yantagram.relay.store.ItemStore
import org.yantagram.relay.store.Rejected
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.days

class RelayServiceTest {
    private val ip = "1.1.1.1"

    private fun rejects(reason: String, block: suspend () -> Unit) = runTest {
        assertEquals(reason, assertFailsWith<Rejected> { block() }.reason)
    }

    @Test
    fun `publish numbers items consecutively under the key's tag`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        val first = relay.service.publish(key.publish(Lane.CONTROL, listOf(item("a"), item("b"))), ip)
        val second = relay.service.publish(key.publish(Lane.CONTROL, listOf(item("c"))), ip)

        assertEquals(key.tag, first.tag)
        assertEquals(listOf(1L, 2L), first.seqs)
        assertEquals(listOf(3L), second.seqs)
        assertEquals(3, second.head)
    }

    @Test
    fun `republishing is idempotent, within and across batches`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        relay.service.publish(key.publish(Lane.CONTROL, listOf(item("a"), item("b"))), ip)
        val again = relay.service.publish(key.publish(Lane.CONTROL, listOf(item("b"), item("c"), item("c"), item("a"))), ip)

        assertEquals(listOf(2L, 3L, 3L, 1L), again.seqs)
        assertEquals(3, again.head)
        val sync = relay.service.sync(SyncRequest(listOf(SyncCursor(key.tag))))
        assertEquals(listOf("a", "b", "c"), sync.streams.single().items.map { bodyText(it.body) })
    }

    @Test
    fun `a batch is stored whole or not at all`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        val tooBig = item("x".repeat(Lane.GROUP.maxItemBytes + 1))
        assertEquals("too-large", assertFailsWith<Rejected> {
            relay.service.publish(key.publish(Lane.GROUP, listOf(item("ok"), tooBig)), ip)
        }.reason)
        assertEquals(0, relay.service.sync(SyncRequest(listOf(SyncCursor(key.tag)))).streams.single().head)
    }

    @Test
    fun `a stream that fills up refuses the whole next batch`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        repeat(Lane.CONTROL.maxStreamItems / 250) { batch ->
            relay.service.publish(key.publish(Lane.CONTROL, (0 until 250).map { item("$batch-$it") }), ip)
        }
        assertEquals("stream-full", assertFailsWith<Rejected> {
            relay.service.publish(key.publish(Lane.CONTROL, listOf(item("one more"))), ip)
        }.reason)
        // Republishing what's already there still answers with its seqs.
        assertEquals(listOf(1L), relay.service.publish(key.publish(Lane.CONTROL, listOf(item("0-0"))), ip).seqs)
    }

    @Test
    fun `the signature covers ids and flags`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        val signed = key.publish(Lane.CONTROL, listOf(item("op")))
        val flipped = signed.copy(items = listOf(signed.items.single().copy(checkpoint = true)))
        val swapped = signed.copy(items = listOf(item("other")))
        val otherKey = signed.copy(pk = StreamKey().pk)
        for (request in listOf(flipped, swapped, otherKey)) {
            assertEquals("bad-sig", assertFailsWith<Rejected> { relay.service.publish(request, ip) }.reason)
        }
        assertEquals("bad-sig", assertFailsWith<Rejected> { relay.service.publish(signed.copy(lane = "group"), ip) }.reason)
    }

    @Test
    fun `a stream keeps the lane it was created in`() = rejects("lane-mismatch") {
        val relay = TestRelay()
        val key = StreamKey()
        relay.service.publish(key.publish(Lane.CONTROL, listOf(item("a"))), ip)
        relay.service.publish(key.publish(Lane.GROUP, listOf(item("b"))), ip)
    }

    @Test
    fun `checkpoints only on control, delete hashes only on inbox, no publishing to legacy`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        val hash = "00".repeat(32)
        val cases = mapOf(
            "bad-checkpoint" to key.publish(Lane.GROUP, listOf(item("a", checkpoint = true))),
            "bad-delete-hash" to key.publish(Lane.CONTROL, listOf(item("a", deleteHash = hash))),
            "bad-lane" to key.publish(Lane.LEGACY, listOf(item("a"))),
            "bad-ref" to key.publish(Lane.CONTROL, listOf(item("a", ref = "ab"))),
        )
        for ((reason, request) in cases) {
            assertEquals(reason, assertFailsWith<Rejected> { relay.service.publish(request, ip) }.reason)
        }
    }

    @Test
    fun `per-IP limits`() = runTest {
        val relay = TestRelay(limits = Limits(itemsPerWindow = 3, bytesPerDay = 1_000))
        val key = StreamKey()
        relay.service.publish(key.publish(Lane.GROUP, listOf(item("a"), item("b"))), ip)
        assertEquals("rate-limited", assertFailsWith<Rejected> {
            relay.service.publish(key.publish(Lane.GROUP, listOf(item("c"), item("d"))), ip)
        }.reason)
        relay.service.publish(key.publish(Lane.GROUP, listOf(item("c"), item("d"))), "2.2.2.2")
        assertEquals("daily-limit", assertFailsWith<Rejected> {
            relay.service.publish(key.publish(Lane.GROUP, listOf(item("x".repeat(1_001)))), "3.3.3.3")
        }.reason)
    }

    @Test
    fun `sync answers inbox, then control, then group, from each cursor`() = runTest {
        val relay = TestRelay()
        val group = StreamKey()
        val control = StreamKey()
        val inbox = StreamKey()
        relay.service.publish(group.publish(Lane.GROUP, listOf(item("g1"), item("g2"))), ip)
        relay.service.publish(control.publish(Lane.CONTROL, listOf(item("c1"), item("c2"), item("c3"))), ip)
        relay.service.publish(inbox.publish(Lane.INBOX, listOf(item("i1"))), ip)
        val unknown = StreamKey().tag

        val sync = relay.service.sync(
            SyncRequest(listOf(SyncCursor(group.tag), SyncCursor(unknown), SyncCursor(control.tag, after = 1), SyncCursor(inbox.tag))),
        )
        assertEquals(listOf(inbox.tag, control.tag, group.tag, unknown), sync.streams.map { it.tag })
        assertEquals(listOf("c2", "c3"), sync.streams[1].items.map { bodyText(it.body) })
        assertEquals(listOf(2L, 3L), sync.streams[1].items.map { it.seq })
        assertEquals(3, sync.streams[1].head)
        assertEquals(1, sync.streams[1].low)
        assertEquals(0, sync.streams[3].head)
        assertEquals(1, sync.streams[3].low)
    }

    @Test
    fun `an HTTP sync over its size limit says there is more`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        relay.service.publish(key.publish(Lane.CONTROL, (1..5).map { item("x".repeat(1_000) + it) }), ip)
        val first = relay.service.sync(SyncRequest(listOf(SyncCursor(key.tag))), maxBytes = 2_500).streams.single()
        assertTrue(first.more)
        assertEquals(listOf(1L, 2L, 3L), first.items.map { it.seq })
        val rest = relay.service.sync(SyncRequest(listOf(SyncCursor(key.tag, first.items.last().seq)))).streams.single()
        assertFalse(rest.more)
        assertEquals(listOf(4L, 5L), rest.items.map { it.seq })
    }

    @Test
    fun `fetch by ref finds an op in any stream`() = runTest {
        val relay = TestRelay()
        val a = StreamKey()
        val b = StreamKey()
        relay.service.publish(a.publish(Lane.CONTROL, listOf(item("op1", ref = "01".repeat(16)))), ip)
        relay.service.publish(b.publish(Lane.CONTROL, listOf(item("op2", ref = "02".repeat(16)), item("op3"))), ip)

        val found = relay.service.fetch(FetchRequest(listOf("02".repeat(16), "01".repeat(16), "03".repeat(16)))).items
        assertEquals(setOf("op1" to a.tag, "op2" to b.tag), found.map { bodyText(it.body) to it.tag }.toSet())
    }

    @Test
    fun `the latest checkpoint across a set of streams`() = runTest {
        val relay = TestRelay()
        val day1 = StreamKey()
        val day2 = StreamKey()
        relay.service.publish(day1.publish(Lane.CONTROL, listOf(item("cp-old", checkpoint = true), item("op"))), ip)
        relay.clock.advance(1.days.inWholeMilliseconds)
        relay.service.publish(day2.publish(Lane.CONTROL, listOf(item("cp-new", checkpoint = true))), ip)

        val latest = relay.service.latestCheckpoint(CheckpointRequest(listOf(day1.tag, day2.tag))).item!!
        assertEquals("cp-new", bodyText(latest.body))
        assertTrue(latest.checkpoint)
        assertEquals("cp-old", bodyText(relay.service.latestCheckpoint(CheckpointRequest(listOf(day1.tag))).item!!.body))
        assertNull(relay.service.latestCheckpoint(CheckpointRequest(listOf(StreamKey().tag))).item)
    }

    @Test
    fun `have reports which ids are still stored`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        relay.service.publish(key.publish(Lane.CONTROL, listOf(item("kept"))), ip)
        val kept = Codec.hex(WriteAuth.sha256("kept".toByteArray()))
        val missing = Codec.hex(WriteAuth.sha256("lost".toByteArray()))
        assertEquals(listOf(kept), relay.service.have(HaveRequest(key.tag, listOf(kept, missing))).present)
    }

    @Test
    fun `an inbox item is deleted only with its key`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        val k = ByteArray(32) { 9 }
        relay.service.publish(key.publish(Lane.INBOX, listOf(item("sealed", deleteHash = Codec.hex(WriteAuth.sha256(k))))), ip)

        assertFalse(relay.service.delete(DeleteRequest(key.tag, 1, "08".repeat(32))).deleted)
        assertFalse(relay.service.delete(DeleteRequest(key.tag, 2, Codec.hex(k))).deleted)
        assertTrue(relay.service.delete(DeleteRequest(key.tag, 1, Codec.hex(k))).deleted)
        val page = relay.service.sync(SyncRequest(listOf(SyncCursor(key.tag)))).streams.single()
        assertEquals(emptyList(), page.items)
        assertEquals(1, page.head)
        assertEquals(2, page.low)
    }

    @Test
    fun `writers can retire their stream's checkpoints, and nothing else`() = runTest {
        val relay = TestRelay()
        val key = StreamKey()
        relay.service.publish(key.publish(Lane.CONTROL, listOf(item("op"), item("cp", checkpoint = true))), ip)

        assertEquals("bad-sig", assertFailsWith<Rejected> {
            relay.service.retire(StreamKey().retire(listOf(2)).copy(pk = key.pk))
        }.reason)
        assertEquals(1, relay.service.retire(key.retire(listOf(1, 2))).retired)
        val items = relay.service.sync(SyncRequest(listOf(SyncCursor(key.tag)))).streams.single().items
        assertEquals(listOf("op"), items.map { bodyText(it.body) })
    }

    @Test
    fun `expiry follows each lane, keeps checkpoints a year, and never reuses a seq`() = runTest {
        val relay = TestRelay()
        val group = StreamKey()
        val control = StreamKey()
        relay.service.publish(group.publish(Lane.GROUP, listOf(item("msg"))), ip)
        relay.service.publish(control.publish(Lane.CONTROL, listOf(item("op"), item("cp", checkpoint = true))), ip)

        relay.clock.advance(31.days.inWholeMilliseconds)
        assertEquals(1, relay.store.sweep(streamIdleMillis = 400.days.inWholeMilliseconds))
        val afterGroup = relay.service.sync(SyncRequest(listOf(SyncCursor(group.tag)))).streams.single()
        assertEquals(1, afterGroup.head)
        assertEquals(2, afterGroup.low)

        relay.clock.advance(150.days.inWholeMilliseconds)
        assertEquals(1, relay.store.sweep(streamIdleMillis = 400.days.inWholeMilliseconds))
        val controlPage = relay.service.sync(SyncRequest(listOf(SyncCursor(control.tag)))).streams.single()
        assertEquals(listOf("cp"), controlPage.items.map { bodyText(it.body) })
        assertEquals(2, controlPage.low)

        assertEquals(listOf(2L), relay.service.publish(group.publish(Lane.GROUP, listOf(item("later"))), ip).seqs)

        relay.clock.advance(200.days.inWholeMilliseconds)
        relay.store.sweep(streamIdleMillis = 400.days.inWholeMilliseconds)
        assertNull(relay.service.latestCheckpoint(CheckpointRequest(listOf(control.tag))).item)
        assertEquals(2, relay.service.sync(SyncRequest(listOf(SyncCursor(control.tag)))).streams.single().head)
    }

    @Test
    fun `stored items survive a restart`() = runTest {
        val first = TestRelay()
        val key = StreamKey()
        first.service.publish(key.publish(Lane.CONTROL, listOf(item("a"), item("b"))), ip)
        first.db.close()

        val reopened = ItemStore(Database(first.path))
        assertEquals(2, reopened.states(listOf(Codec.hexToBytes(key.tag))).single().head)
    }

    @Test
    fun `legacy tags queue items for the bridge only when it runs`() = runTest {
        val legacy = "ab".repeat(16)
        val off = TestRelay(bridgeEnabled = false)
        off.service.publish(StreamKey().publish(Lane.GROUP, listOf(item("a")), legacyTag = legacy), ip)
        assertEquals(0, off.store.outboxSize())

        val on = TestRelay(bridgeEnabled = true)
        val key = StreamKey()
        on.service.publish(key.publish(Lane.GROUP, listOf(item("a"), item("b")), legacyTag = legacy), ip)
        on.service.publish(key.publish(Lane.GROUP, listOf(item("a")), legacyTag = legacy), ip) // a retry
        assertEquals(listOf("a", "b"), on.store.outbox(10).map { String(it.body) })
        assertEquals(legacy, Codec.hex(on.store.outbox(10).first().legacyTag))
    }

    @Test
    fun `large-file parts can carry a legacy tag for the bridge`() = runTest {
        val relay = TestRelay(bridgeEnabled = true)
        val legacy = "cd".repeat(16)
        relay.service.publish(StreamKey().publish(Lane.BLOB, listOf(item("part1"), item("part2")), legacy), ip)
        assertEquals(listOf("part1", "part2"), relay.store.outbox(10).map { String(it.body) })
    }
}
