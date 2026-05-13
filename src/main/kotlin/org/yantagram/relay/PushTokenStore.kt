package org.yantagram.relay

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.upsert
import org.slf4j.LoggerFactory

private val tokenLogger = LoggerFactory.getLogger("org.yantagram.relay.PushTokenStore")

/** Exposed table for Expo push tokens, keyed by token with a VK association. */
object PushTokensTable : Table("push_tokens") {
    val token = text("token")
    val verificationKey = text("verification_key").index("idx_push_tokens_vk")
    val registeredAt = long("registered_at")
    override val primaryKey = PrimaryKey(token)
}

/**
 * SQLite-backed store for Expo push notification tokens.
 * Each token is associated with exactly one verification key (hex Ed25519 public key).
 * A single VK may have multiple tokens (multiple devices).
 * Shares the same database as [PacketStore].
 */
class PushTokenStore(private val db: Database) {

    /** Create the push_tokens table if it doesn't exist. */
    fun init() {
        transaction(db) {
            SchemaUtils.create(PushTokensTable)
        }
        tokenLogger.info("PushTokenStore initialized")
    }

    /**
     * Register (or re-register) a push token for a verification key.
     * If the token already exists under a different VK, it is reassigned.
     */
    fun register(verificationKey: String, token: String) {
        transaction(db) {
            PushTokensTable.upsert {
                it[PushTokensTable.token] = token
                it[PushTokensTable.verificationKey] = verificationKey
                it[registeredAt] = System.currentTimeMillis()
            }
        }
        tokenLogger.debug("Registered push token: {}... for VK: {}...", token.take(20), verificationKey.take(12))
    }

    /** Unregister a push token regardless of which VK it belongs to. */
    fun unregister(token: String) {
        transaction(db) {
            PushTokensTable.deleteWhere { PushTokensTable.token eq token }
        }
        tokenLogger.debug("Unregistered push token: {}...", token.take(20))
    }

    /** Remove multiple stale tokens (e.g. DeviceNotRegistered). */
    fun removeAll(tokens: List<String>) {
        if (tokens.isEmpty()) return
        transaction(db) {
            PushTokensTable.deleteWhere { PushTokensTable.token inList tokens }
        }
        tokenLogger.info("Removed {} stale push tokens", tokens.size)
    }

    /**
     * Look up all registered tokens for the given verification keys.
     * Returns a map of VK → list of tokens.
     */
    fun getTokensForKeys(vks: List<String>): Map<String, List<String>> {
        if (vks.isEmpty()) return emptyMap()
        return transaction(db) {
            PushTokensTable.selectAll()
                .where { PushTokensTable.verificationKey inList vks }
                .groupBy { it[PushTokensTable.verificationKey] }
                .mapValues { (_, rows) -> rows.map { it[PushTokensTable.token] } }
        }
    }

    /** Get all registered tokens (flat list). */
    fun getAllTokens(): List<String> = transaction(db) {
        PushTokensTable.selectAll().map { it[PushTokensTable.token] }
    }

    /** Count of registered tokens. */
    fun count(): Long = transaction(db) {
        PushTokensTable.selectAll().count()
    }
}


