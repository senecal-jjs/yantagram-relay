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
            )
        }
    }
}
