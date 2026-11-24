package org.yantagram.relay.push

enum class ApnsEnvironment { PRODUCTION, SANDBOX }

data class ApnsConfig(
    /** Contents of the `.p8` signing key (PEM). */
    val keyP8: String,
    val keyId: String,
    val teamId: String,
    val topic: String,
    val environment: ApnsEnvironment,
)

data class FcmConfig(
    val projectId: String,
    val clientEmail: String,
    /** PKCS#8 PEM private key of the service account. */
    val privateKey: String,
)
