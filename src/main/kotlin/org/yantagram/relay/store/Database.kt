package org.yantagram.relay.store

import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.sqlite.SQLiteConfig
import java.io.File
import java.sql.Connection
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors

/**
 * One SQLite file in WAL mode.
 *
 * - **Writes** go through one connection on one thread ([write]), so they are
 *   serialized without lock contention, and each [write] block is one transaction.
 *   `synchronous=FULL` makes a commit durable (fsynced) before it returns: the relay
 *   only answers `stored` after that.
 * - **Reads** use a small pool of connections ([read]); WAL lets them run alongside
 *   the writer and see the last committed state.
 */
class Database(path: String, readers: Int = 4) : AutoCloseable {
    private val url = "jdbc:sqlite:$path"
    private val writer: Connection
    @Volatile private var writerThread: Thread? = null
    private val writeThread = Executors.newSingleThreadExecutor { r ->
        Thread(r, "db-writer").apply { isDaemon = true }.also { writerThread = it }
    }
    private val writeDispatcher: ExecutorCoroutineDispatcher = writeThread.asCoroutineDispatcher()
    private val readPool = ArrayBlockingQueue<Connection>(readers)
    private val readDispatcher = Executors.newFixedThreadPool(readers) { r ->
        Thread(r, "db-reader").apply { isDaemon = true }
    }.asCoroutineDispatcher()

    init {
        if (path != ":memory:") File(path).absoluteFile.parentFile?.mkdirs()
        writer = open(readOnly = false)
        writer.autoCommit = true
        Schema.migrate(writer)
        writer.autoCommit = false
        repeat(readers) { readPool.add(open(readOnly = true)) }
    }

    private fun open(readOnly: Boolean): Connection {
        val config = SQLiteConfig().apply {
            setJournalMode(SQLiteConfig.JournalMode.WAL)
            setSynchronous(SQLiteConfig.SynchronousMode.FULL)
            setBusyTimeout(10_000)
            enforceForeignKeys(true)
            if (readOnly) setReadOnly(true)
        }
        return config.createConnection(url)
    }

    /** Runs [block] as one transaction on the writer thread; rolls back if it throws. */
    suspend fun <T> write(block: (Connection) -> T): T = withContext(writeDispatcher) { writeBlocking(block) }

    /** Blocking form of [write], for startup and tests. Must not be called on the writer thread. */
    fun <T> writeBlocking(block: (Connection) -> T): T {
        // Compare the thread itself: coroutine debug mode renames threads.
        if (Thread.currentThread() !== writerThread) {
            return writeThread.submit<T> { writeBlocking(block) }.get()
        }
        try {
            val result = block(writer)
            writer.commit()
            return result
        } catch (e: Throwable) {
            runCatching { writer.rollback() }
            throw e
        }
    }

    /** Runs [block] on a pooled read connection. */
    suspend fun <T> read(block: (Connection) -> T): T = withContext(readDispatcher) {
        val connection = readPool.take()
        try {
            block(connection)
        } finally {
            readPool.put(connection)
        }
    }

    override fun close() {
        writeThread.submit { runCatching { writer.close() } }.get()
        writeThread.shutdown()
        writeDispatcher.close()
        readPool.forEach { runCatching { it.close() } }
        readDispatcher.close()
    }
}

internal object Schema {
    private const val VERSION = 1

    fun migrate(connection: Connection) {
        val current = connection.createStatement().use { s ->
            s.executeQuery("PRAGMA user_version").use { if (it.next()) it.getInt(1) else 0 }
        }
        if (current >= VERSION) return
        connection.createStatement().use { s ->
            for (sql in V1) s.execute(sql)
            s.execute("PRAGMA user_version = $VERSION")
        }
    }

    private val V1 = listOf(
        // One row per stream (tag). `head` is the last sequence number handed out and is
        // never reused, even after the stream's items expire.
        """CREATE TABLE streams (
            tag BLOB PRIMARY KEY,
            lane INTEGER NOT NULL,
            head INTEGER NOT NULL,
            item_count INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            last_write INTEGER NOT NULL
        ) WITHOUT ROWID""",
        """CREATE TABLE items (
            stream BLOB NOT NULL,
            seq INTEGER NOT NULL,
            id BLOB NOT NULL,
            body BLOB NOT NULL,
            ref BLOB,
            checkpoint INTEGER NOT NULL DEFAULT 0,
            delete_hash BLOB,
            marker BLOB,
            stored_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            PRIMARY KEY (stream, seq)
        )""",
        "CREATE UNIQUE INDEX items_stream_id ON items(stream, id)",
        "CREATE INDEX items_ref ON items(ref) WHERE ref IS NOT NULL",
        "CREATE INDEX items_expiry ON items(expires_at)",
        "CREATE INDEX items_checkpoint ON items(stream, stored_at) WHERE checkpoint = 1",
        // Items to copy to strfry for old clients (migration bridge), written in the same
        // transaction as the item so a crash can't lose one.
        """CREATE TABLE bridge_outbox (
            n INTEGER PRIMARY KEY AUTOINCREMENT,
            legacy_tag BLOB NOT NULL,
            body BLOB NOT NULL,
            marker BLOB,
            created_at INTEGER NOT NULL
        )""",
        // Body ids the bridge forwarded, so it doesn't import its own events back.
        """CREATE TABLE bridge_forwarded (
            id BLOB PRIMARY KEY,
            at INTEGER NOT NULL
        ) WITHOUT ROWID""",
        """CREATE TABLE registrations (
            token TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            tags TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            alerts INTEGER NOT NULL DEFAULT 0,
            self_key TEXT
        ) WITHOUT ROWID""",
        """CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID""",
    )
}

/** Small key-value settings (bridge cursor). */
class MetaStore(private val db: Database) {
    suspend fun get(key: String): String? = db.read { c ->
        c.prepareStatement("SELECT value FROM meta WHERE key = ?").use { s ->
            s.setString(1, key)
            s.executeQuery().use { if (it.next()) it.getString(1) else null }
        }
    }

    suspend fun set(key: String, value: String) = db.write { c ->
        c.prepareStatement("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .use { s ->
                s.setString(1, key)
                s.setString(2, value)
                s.executeUpdate()
            }
    }
}
