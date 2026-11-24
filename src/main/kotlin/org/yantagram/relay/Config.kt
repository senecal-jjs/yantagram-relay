package org.yantagram.relay

import org.yantagram.relay.push.ApnsConfig
import org.yantagram.relay.push.ApnsEnvironment
import org.yantagram.relay.push.FcmConfig
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds

data class Config(
    val port: Int = 8080,
    /** Prometheus metrics; never exposed publicly (see fly.toml). */
    val metricsPort: Int = 9091,
    val dbPath: String,
    /** Push is off when neither provider is configured (local runs, tests). */
    val apns: ApnsConfig?,
    val fcm: FcmConfig?,
    /** strfry's WebSocket URL; the migration bridge runs only when set. */
    val strfryUrl: String?,
    val coalesceWindow: Duration = 30.seconds,
    val alertCoalesceWindow: Duration = 10.seconds,
    val registrationTtl: Duration = 14.days,
    val maxRegistrations: Int = 200_000,
) {
    companion object {
        /**
         * Reads the configuration from environment variables. Fails fast, listing every
         * missing or malformed variable at once. Never includes secret values in errors.
         */
        fun fromEnv(env: Map<String, String> = System.getenv()): Config {
            val errors = mutableListOf<String>()

            fun optional(name: String): String? = env[name]?.takeIf { it.isNotBlank() }

            fun required(name: String): String = optional(name) ?: "".also { errors += "$name is required" }

            fun int(name: String, default: Int): Int {
                val raw = optional(name) ?: return default
                return raw.toIntOrNull()?.takeIf { it > 0 } ?: default.also { errors += "$name must be a positive integer" }
            }

            val apns = if (optional("APNS_KEY_P8") != null) {
                val apnsEnv = when (val raw = optional("APNS_ENV")?.lowercase() ?: "sandbox") {
                    "production" -> ApnsEnvironment.PRODUCTION
                    "sandbox" -> ApnsEnvironment.SANDBOX
                    else -> ApnsEnvironment.SANDBOX.also { errors += "APNS_ENV must be production or sandbox, not $raw" }
                }
                ApnsConfig(
                    keyP8 = pemKey("APNS_KEY_P8", required("APNS_KEY_P8"), errors),
                    keyId = required("APNS_KEY_ID"),
                    teamId = required("APNS_TEAM_ID"),
                    topic = required("APNS_TOPIC"),
                    environment = apnsEnv,
                )
            } else {
                null
            }
            val fcm = if (optional("FIREBASE_PRIVATE_KEY") != null) {
                FcmConfig(
                    projectId = required("FCM_PROJECT_ID"),
                    clientEmail = required("FCM_CLIENT_EMAIL"),
                    privateKey = pemKey("FIREBASE_PRIVATE_KEY", required("FIREBASE_PRIVATE_KEY"), errors),
                )
            } else {
                null
            }

            val config = Config(
                port = int("PORT", 8080),
                metricsPort = int("METRICS_PORT", 9091),
                dbPath = required("DB_PATH"),
                apns = apns,
                fcm = fcm,
                strfryUrl = optional("STRFRY_URL"),
                coalesceWindow = int("COALESCE_SECONDS", 30).seconds,
                alertCoalesceWindow = int("ALERT_COALESCE_SECONDS", 10).seconds,
                registrationTtl = int("REGISTRATION_TTL_DAYS", 14).days,
                maxRegistrations = int("MAX_REGISTRATIONS", 200_000),
            )
            require(errors.isEmpty()) { "Invalid configuration:\n  " + errors.joinToString("\n  ") }
            return config
        }

        private val PEM = Regex("-----BEGIN PRIVATE KEY-----(.*?)-----END PRIVATE KEY-----", RegexOption.DOT_MATCHES_ALL)
        private val BASE64 = Regex("^[A-Za-z0-9+/=]+$")

        /**
         * Rebuilds a PKCS#8 PEM key from however a secret store mangled it: literal `\n`s,
         * newlines turned into spaces, or only the base64 body. Errors never include the value.
         */
        internal fun pemKey(name: String, raw: String, errors: MutableList<String>): String {
            if (raw.isEmpty()) return raw
            val unescaped = raw.replace("\\n", "\n").replace("\\r", "")
            val body = (PEM.find(unescaped)?.groupValues?.get(1) ?: unescaped).filterNot { it.isWhitespace() }
            if (body.isEmpty() || !BASE64.matches(body) || "BEGIN" in unescaped && PEM.find(unescaped) == null) {
                errors += "$name must be a PKCS#8 PEM private key (-----BEGIN PRIVATE KEY----- ...)"
                return raw
            }
            return "-----BEGIN PRIVATE KEY-----\n" + body.chunked(64).joinToString("\n") + "\n-----END PRIVATE KEY-----\n"
        }
    }
}
