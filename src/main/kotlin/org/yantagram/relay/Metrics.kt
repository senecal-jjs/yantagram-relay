package org.yantagram.relay

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.yantagram.relay.protocol.Lane
import org.yantagram.relay.push.Platform
import org.yantagram.relay.push.PushKind

/**
 * Counts only: no tag, token, id or address ever becomes a label. Labels come from
 * fixed sets (lane, platform, op, short reason codes).
 */
class Metrics(val registry: MeterRegistry = SimpleMeterRegistry()) {
    // Relay
    fun itemsStored(lane: Lane, n: Int) {
        if (n > 0) Counter.builder("relay_items_stored").tag("lane", lane.wire).register(registry).increment(n.toDouble())
    }

    fun request(op: String, transport: String) =
        Counter.builder("relay_requests").tag("op", op).tag("transport", transport).register(registry).increment()

    fun rejected(op: String, reason: String) =
        Counter.builder("relay_rejected").tag("op", op).tag("reason", reason).register(registry).increment()

    val itemsDelivered: Counter = Counter.builder("relay_items_delivered").register(registry)
    val itemsExpired: Counter = Counter.builder("relay_items_expired").register(registry)

    // Bridge
    val bridgeImported: Counter = Counter.builder("bridge_imported").register(registry)
    val bridgeForwarded: Counter = Counter.builder("bridge_forwarded").register(registry)
    fun bridgeDropped(reason: String) = counter("bridge_dropped", "reason", reason).increment()
    val bridgeReconnects: Counter = Counter.builder("bridge_reconnects").register(registry)

    // Push
    val eventsMatched: Counter = Counter.builder("push_events_matched").register(registry)
    val registrationCapReached: Counter = Counter.builder("push_registration_cap_reached").register(registry)

    fun sent(platform: Platform, kind: PushKind) =
        Counter.builder("push_sent").tag("platform", platform.wire).tag("type", kind.label)
            .register(registry).increment()

    fun selfSkipped(platform: Platform) = counter("push_self_skipped", "platform", platform.wire).increment()

    fun dropped(reason: String) = counter("push_dropped", "reason", reason).increment()

    fun failed(platform: Platform, reason: String) =
        Counter.builder("push_failed").tag("platform", platform.wire).tag("reason", reason)
            .register(registry).increment()

    private fun counter(name: String, tag: String, value: String) =
        Counter.builder(name).tag(tag, value).register(registry)
}
