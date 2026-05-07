package org.yantagram.relay

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PacketStoreTest {

    private val tempFiles = mutableListOf<File>()

    private fun createStore(maxHistory: Long = 0): PacketStore {
        val tempFile = File.createTempFile("packetstore-test-", ".db")
        tempFile.deleteOnExit()
        tempFiles.add(tempFile)
        return PacketStore(
            dbPath = tempFile.absolutePath,
            flushIntervalMs = 50,
            maxHistoryBytes = maxHistory,
        ).also { it.init() }
    }

    @AfterTest
    fun cleanup() {
        tempFiles.forEach { it.delete() }
        tempFiles.clear()
    }

    @Test
    fun `persists packets and recovers nextSeq`() {
        val store = createStore()
        store.startFlusher(CoroutineScope(Dispatchers.Default))

        store.enqueue(Packet(1L, null, "a".toByteArray()))
        store.enqueue(Packet(2L, "key1", "b".toByteArray()))

        runBlocking { delay(200) } // wait for flush

        assertEquals(3L, store.loadNextSeq())
        val loaded = store.loadSince(0)
        assertEquals(2, loaded.size)
        assertEquals(1L, loaded[0].seq)
        assertEquals(null, loaded[0].verificationKey)
        assertContentEquals("a".toByteArray(), loaded[0].payload)
        assertEquals(2L, loaded[1].seq)
        assertEquals("key1", loaded[1].verificationKey)
        assertContentEquals("b".toByteArray(), loaded[1].payload)

        store.close()
    }

    @Test
    fun `loadSince filters correctly`() {
        val store = createStore()
        store.startFlusher(CoroutineScope(Dispatchers.Default))

        store.enqueue(Packet(1L, null, "a".toByteArray()))
        store.enqueue(Packet(2L, null, "b".toByteArray()))
        store.enqueue(Packet(3L, null, "c".toByteArray()))

        runBlocking { delay(200) }

        val loaded = store.loadSince(1)
        assertEquals(listOf(2L, 3L), loaded.map { it.seq })

        store.close()
    }

    @Test
    fun `loadSince with limit`() {
        val store = createStore()
        store.startFlusher(CoroutineScope(Dispatchers.Default))

        store.enqueue(Packet(1L, null, "a".toByteArray()))
        store.enqueue(Packet(2L, null, "b".toByteArray()))
        store.enqueue(Packet(3L, null, "c".toByteArray()))

        runBlocking { delay(200) }

        val loaded = store.loadSince(0, limit = 2)
        assertEquals(listOf(1L, 2L), loaded.map { it.seq })

        store.close()
    }

    @Test
    fun `prunes history when over limit`() {
        // maxHistoryBytes = 2 means only 2 bytes of payload allowed total
        val store = createStore(maxHistory = 2)
        store.startFlusher(CoroutineScope(Dispatchers.Default))

        store.enqueue(Packet(1L, null, byteArrayOf(1)))
        store.enqueue(Packet(2L, null, byteArrayOf(2)))
        store.enqueue(Packet(3L, null, byteArrayOf(3)))

        runBlocking { delay(200) }

        val loaded = store.loadSince(0)
        assertTrue(loaded.size <= 2, "should have pruned to fit maxHistoryBytes, got ${loaded.size}")

        store.close()
    }

    @Test
    fun `ring restore recovers seq from store`() {
        val store = createStore()
        store.startFlusher(CoroutineScope(Dispatchers.Default))

        store.enqueue(Packet(1L, null, "a".toByteArray()))
        store.enqueue(Packet(2L, "key", "b".toByteArray()))
        store.enqueue(Packet(3L, null, "c".toByteArray()))

        runBlocking { delay(200) }

        // Create a new ring that restores from the store
        val ring = PacketRing(
            limitProvider = RingLimitProvider { 10_000L },
            store = store,
        )
        ring.restore()

        // nextSeq should be 4 (one past the last persisted)
        assertEquals(3L, ring.latestSeq())
        assertEquals(3, ring.packetCount())

        // New appends should continue from seq=4
        runBlocking { ring.append("d".toByteArray()) }
        assertEquals(4L, ring.latestSeq())

        store.close()
    }
}
