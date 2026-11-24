package org.yantagram.relay.push

import org.yantagram.relay.push.registry.Registry
import org.yantagram.relay.push.registry.Store
import org.yantagram.relay.store.Database
import kotlinx.coroutines.Dispatchers
import java.nio.file.Files

fun tempDbPath(): String = Files.createTempDirectory("push-test").resolve("push.db").toString()

fun newRegistry(path: String = tempDbPath(), max: Int = 200_000) =
    Registry(Store(Database(path)), max, Dispatchers.Unconfined)

fun tag(n: Int): String = n.toString(16).padStart(32, '0')

fun apnsToken(n: Int): String = n.toString(16).padStart(64, 'a')

fun fcmToken(n: Int): String = ("fcm$n:" + "A".repeat(150))
