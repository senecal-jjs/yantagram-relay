package org.yantagram.relay.push.registry

import org.yantagram.relay.push.Platform
import org.yantagram.relay.store.Database

/** A stored registration: exactly what onboarding discloses. */
data class StoredRegistration(
    val token: String,
    val platform: Platform,
    val tags: Set<String>,
    val updatedAt: Long,
    /** Whether the user has notifications on: chat messages may be sent as alerts. */
    val alerts: Boolean = false,
    /** The device's sender marker key K, hex; null if it sent none. */
    val selfKey: String? = null,
)

/**
 * Push registrations in the relay's database. Blocking: call from Dispatchers.IO. The
 * Registry is the only writer, and serializes its writes.
 */
class Store(private val db: Database) {
    fun loadAll(): List<StoredRegistration> = db.writeBlocking { c ->
        c.createStatement().use { s ->
            s.executeQuery("SELECT token, platform, tags, updated_at, alerts, self_key FROM registrations").use { rs ->
                buildList {
                    while (rs.next()) {
                        val platform = Platform.fromWire(rs.getString(2)) ?: continue
                        add(
                            StoredRegistration(
                                token = rs.getString(1),
                                platform = platform,
                                tags = rs.getString(3).split(' ').filterTo(mutableSetOf()) { it.isNotEmpty() },
                                updatedAt = rs.getLong(4),
                                alerts = rs.getInt(5) == 1,
                                selfKey = rs.getString(6),
                            ),
                        )
                    }
                }
            }
        }
    }

    fun upsert(registration: StoredRegistration) {
        db.writeBlocking { c ->
            c.prepareStatement(
                "INSERT INTO registrations(token, platform, tags, updated_at, alerts, self_key) VALUES (?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT(token) DO UPDATE SET platform = excluded.platform, tags = excluded.tags, " +
                    "updated_at = excluded.updated_at, alerts = excluded.alerts, self_key = excluded.self_key",
            ).use { s ->
                s.setString(1, registration.token)
                s.setString(2, registration.platform.wire)
                s.setString(3, registration.tags.joinToString(" "))
                s.setLong(4, registration.updatedAt)
                s.setInt(5, if (registration.alerts) 1 else 0)
                s.setString(6, registration.selfKey)
                s.executeUpdate()
            }
        }
    }

    fun delete(token: String) = deleteAll(listOf(token))

    fun deleteAll(tokens: Collection<String>) {
        if (tokens.isEmpty()) return
        db.writeBlocking { c ->
            c.prepareStatement("DELETE FROM registrations WHERE token = ?").use { s ->
                for (token in tokens) {
                    s.setString(1, token)
                    s.addBatch()
                }
                s.executeBatch()
            }
        }
    }
}
