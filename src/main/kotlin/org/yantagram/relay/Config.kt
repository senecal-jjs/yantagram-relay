package org.yantagram.relay

/** Runtime configuration for the relay. Plain data class so tests can construct it freely. */
data class RelayConfig(
    val host: String,
    val port: Int,
    val publishSecret: ByteArray,
    /** Fraction of max JVM heap the ring is allowed to consume (0.0–1.0). */
    val ringHeapFraction: Double,
    /** Optional hard cap on ring size in bytes. If set, effective limit = min(fraction-based, hardCap). */
    val ringHardCapBytes: Long?,
    val maxFrameBytes: Long,
    /** Interval in seconds for server→client keep-alive on /subscribe. 0 to disable. */
    val keepAlivePeriodSeconds: Long = 15,
    /** Max seconds to wait for a client heartbeat before closing. 0 to disable. */
    val clientHeartbeatTimeoutSeconds: Long = 60,
    /** Path to SQLite database file. Use ":memory:" for in-memory only (no persistence). */
    val dbPath: String = "yantagram-relay.db",
    /** Async batch flush interval in milliseconds. */
    val dbFlushIntervalMs: Long = 500,
    /** Max DB history retention in bytes. 0 = unlimited. */
    val dbMaxHistoryBytes: Long = 0,
    /** Enable Expo push notifications. */
    val pushEnabled: Boolean = false,
    /** Expo access token for enhanced rate limits (optional). */
    val expoAccessToken: String? = null,
    /** Interval in hours between silent sync pushes. */
    val pushIntervalHours: Long = 1,
) {
    companion object {
        fun fromEnv(): RelayConfig {
            val secret = System.getenv("PUBLISH_SECRET")
            require(!secret.isNullOrEmpty()) { "PUBLISH_SECRET environment variable is required" }
            return RelayConfig(
                host = System.getenv("HOST")?.takeIf { it.isNotBlank() } ?: "0.0.0.0",
                port = System.getenv("PORT")?.toIntOrNull() ?: 8080,
                publishSecret = secret.toByteArray(Charsets.UTF_8),
                ringHeapFraction = System.getenv("RING_HEAP_FRACTION")?.toDoubleOrNull() ?: 0.5,
                ringHardCapBytes = System.getenv("RING_MAX_BYTES")?.toLongOrNull(),
                maxFrameBytes = System.getenv("MAX_FRAME_BYTES")?.toLongOrNull() ?: (1L * 1024 * 1024),
                keepAlivePeriodSeconds = System.getenv("KEEP_ALIVE_PERIOD_SECONDS")?.toLongOrNull() ?: 15,
                clientHeartbeatTimeoutSeconds = System.getenv("CLIENT_HEARTBEAT_TIMEOUT_SECONDS")?.toLongOrNull() ?: 60,
                dbPath = System.getenv("DB_PATH") ?: "yantagram-relay.db",
                dbFlushIntervalMs = System.getenv("DB_FLUSH_INTERVAL_MS")?.toLongOrNull() ?: 500,
                dbMaxHistoryBytes = System.getenv("DB_MAX_HISTORY_BYTES")?.toLongOrNull() ?: 0,
                pushEnabled = System.getenv("PUSH_ENABLED")?.toBooleanStrictOrNull() ?: false,
                expoAccessToken = System.getenv("EXPO_ACCESS_TOKEN"),
                pushIntervalHours = System.getenv("PUSH_INTERVAL_HOURS")?.toLongOrNull() ?: 24,
            )
        }
    }
}
