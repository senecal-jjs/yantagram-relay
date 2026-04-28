package org.yantagram.relay

import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer

fun main() {
    val config = RelayConfig.fromEnv()

    embeddedServer(
        factory = CIO,
        port = config.port,
        host = config.host,
        module = { relayModule(config) },
    ).start(wait = true)
}


