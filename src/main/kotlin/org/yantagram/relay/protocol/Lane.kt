package org.yantagram.relay.protocol

import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

private const val KB = 1024
private const val MB = 1024 * 1024

/**
 * A stream's retention class, fixed when the stream is first written. The client says
 * which lane it is publishing to; the relay never looks inside items to decide.
 *
 * [priority]: lower is answered first in a sync. Inbox first (a new member's history
 * arrives there), then control (ops unblock everything else), then chat.
 */
enum class Lane(
    val wire: String,
    val code: Int,
    val retention: Duration,
    val maxItemBytes: Int,
    /** Per stream (one stream is one tag, so one day of one group). */
    val maxStreamItems: Int,
    val maxStreamBytes: Long,
    val priority: Int,
    /** Clients may publish here. Legacy streams are written by the strfry bridge only. */
    val clientWritable: Boolean = true,
) {
    INBOX("inbox", 2, 60.days, MB, 2_000, 40L * MB, priority = 0),
    CONTROL("control", 1, 180.days, MB, 1_000, 20L * MB, priority = 1),
    GROUP("group", 0, 30.days, 256 * KB, 20_000, 200L * MB, priority = 2),
    LEGACY("legacy", 4, 7.days, 64 * KB, 50_000, 500L * MB, priority = 3, clientWritable = false),
    /** Large-file parts: one stream per file, 10 MB at most (~250 parts of 40 KB). */
    BLOB("blob", 3, 7.days, 64 * KB, 400, 12L * MB, priority = 4);

    companion object {
        /** Checkpoints outlive their lane so a member gone longer than 180 days can rejoin. */
        val CHECKPOINT_RETENTION: Duration = 365.days

        fun fromWire(value: String): Lane? = entries.firstOrNull { it.wire == value }
        fun fromCode(code: Int): Lane? = entries.firstOrNull { it.code == code }
    }
}
