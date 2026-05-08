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

/** Exposed table for Expo push tokens. */
object PushTokensTable : Table("push_tokens") {
    val token = text("token")
    val createdAt = long("created_at")
    override val primaryKey = PrimaryKey(token)
}

/**
 * SQLite-backed store for Expo push notification tokens.
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

    /** Register (or re-register) a push token. */
    fun register(token: String) {
        transaction(db) {
            PushTokensTable.upsert {
                it[PushTokensTable.token] = token
                it[createdAt] = System.currentTimeMillis()
            }
        }
        tokenLogger.debug("Registered push token: {}...", token.take(20))
    }

    /** Unregister a push token. */
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

    /** Get all registered tokens. */
    fun getAllTokens(): List<String> = transaction(db) {
        PushTokensTable.selectAll().map { it[PushTokensTable.token] }
    }

    /** Count of registered tokens. */
    fun count(): Long = transaction(db) {
        PushTokensTable.selectAll().count()
    }
}


