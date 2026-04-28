package org.yantagram.relay

/** Runtime configuration for the relay. Plain data class so tests can construct it freely. */
data class RelayConfig(
    val host: String,
    val port: Int,
    val publishSecret: ByteArray,
    val ringMaxBytes: Long,
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
                ringMaxBytes = System.getenv("RING_MAX_BYTES")?.toLongOrNull() ?: (64L * 1024 * 1024),
                maxFrameBytes = System.getenv("MAX_FRAME_BYTES")?.toLongOrNull() ?: (1L * 1024 * 1024),
            )
        }
    }
}


