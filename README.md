# Yantagram relay (v2)

A store-and-forward relay built for Yantagram. It replaces strfry (a general Nostr
relay) and the separate push service with one service whose job is **reliable
delivery**, of group control traffic (CGKA ops) above all:

- Items are stored durably and numbered per stream with no gaps. A publish is
  acknowledged only after its transaction is fsynced.
- Clients keep a cursor per stream and ask for everything after it, so they know
  exactly whether they have missed something.
- Control traffic is kept 180 days, and checkpoints a year, so a member who was
  offline for a long time can still rejoin.
- The relay still can't tell who published an item, who it's for, or which group it
  belongs to.

The design and its reasoning are in the app repository, in `relay-server-design.md`.
This README is the contract a client implements, plus how to run the relay.

## Layout

| Path | What it does |
|---|---|
| `Main.kt`, `Config.kt`, `Jobs.kt` | Wiring, environment config, hourly expiry and registration cleanup |
| `store/Database.kt` | SQLite (WAL, `synchronous=FULL`): one writer thread, a pool of readers, schema |
| `store/ItemStore.kt` | Streams and items: atomic publish, paging, refs, checkpoints, deletes, expiry, bridge queue |
| `protocol/` | Lanes, write authorization (Ed25519), wire messages, the operations shared by both transports, per-IP limits, live-update bus |
| `api/` | Ktor module, HTTP routes, WebSocket sessions |
| `push/` | APNs/FCM push, moved from the standalone push service: registry, matcher, coalescer, senders |
| `bridge/` | The strfry migration bridge: imports old clients' events, forwards new clients' items |

## Concepts

**Stream.** An append-only log named by a 16-byte tag. Each item gets the next
sequence number (`seq`) in its stream; numbers are never reused, even after items
expire. One stream is one (secret, lane, UTC day): tags still change daily, so stored
items can't be linked across days.

**Lane.** A stream's retention class, fixed when it's first written:

| Lane | For | Retention | Max item | Per stream |
|---|---|---|---|---|
| `inbox` | sealed acks, a new member's history | 60 days, or until the recipient deletes it | 1 MB | 2,000 items / 40 MB |
| `control` | `GROUP_CONTROL` (ops, delegations, revocations, removals), checkpoints, era handoffs | 180 days; checkpoints 365 | 1 MB | 1,000 / 20 MB |
| `group` | messages, notices, small files | 30 days | 256 KB | 20,000 / 200 MB |
| `blob` | large-file parts (one stream per file) | 7 days | 64 KB | 400 / 12 MB |
| `legacy` | events imported from strfry (bridge only) | 7 days | 64 KB | — |

Syncs answer `inbox` first, then `control`, then `group`.

**Write key.** Each (secret, lane, window) has an Ed25519 key pair only members can
derive. The stream's tag is `SHA-256(pk)[0..16]`, so the relay can check a writer
holds the stream's key without learning anything that links days. Suggested
derivation (client side; the relay only sees `pk`):

```
seed = HMAC-SHA256(HKDF(secret, info = "yantagram/relay/<lane>-write/v1"), u32be(window))
(sk, pk) = Ed25519 key pair from seed
tag = SHA-256(pk)[0..16]
```

**Item id.** `SHA-256(body)`. Publishing an item whose id is already in the stream
returns its existing seq: retries never duplicate.

## Protocol

JSON over `POST https://<host>/v2/<op>` or a WebSocket at `wss://<host>/v2/ws`. Same
fields either way. Over WebSocket each message also has `"type"` (the op name) and an
optional `"req_id"` (any JSON value) that the answer echoes.

Encodings: tags, ids, keys, signatures, refs and hashes are lowercase hex; bodies
are standard base64.

### publish

```json
{ "lane": "control", "pk": "<32-byte hex>", "sig": "<64-byte hex>",
  "items": [ { "body": "<base64>", "ref": "<16-byte hex>?", "checkpoint": false,
               "delete_hash": "<32-byte hex>?", "marker": "<16-byte hex>?" } ],
  "legacy_tag": "<16-byte hex>?" }
→ { "tag": "...", "seqs": [5, 6], "head": 6 }          WebSocket type: "stored"
```

- **All or nothing**, at most 256 items and 4 MB per batch, consecutive seqs in order.
- `sig` is Ed25519 over
  `"yantagram/relay/publish/v1" | u8 lane | per item: id(32) | u8 flags | [ref] | [delete_hash]`,
  where `flags` is 1 checkpoint, 2 has ref, 4 has delete_hash. Lane codes: group 0,
  control 1, inbox 2, blob 3.
- `ref`: an opaque index for fetch-by-reference, e.g.
  `HMAC-SHA256(HKDF(secret, "yantagram/relay/ref/v1"), opHash)[0..16]`.
- `checkpoint` only on `control`; `delete_hash` only on `inbox`.
- `marker`: the push sender marker, `HMAC-SHA256(selfKey, base64(body))[0..16]`, so
  the sending device isn't woken. Not signed, never returned to readers.
- `legacy_tag` (migration only): also copy these items to strfry under this
  old-version tag, for clients that haven't updated.
- The answer comes **after the commit is on disk**. Until then, treat the item as
  unsent and retry; retries are safe.

### sync

```json
{ "cursors": [ { "tag": "...", "after": 0 } ], "live": false }
```

- **HTTP** answers `{ "streams": [ { "tag", "items": [ {tag, seq, id, body, checkpoint} ],
  "head", "low", "more" } ] }`, up to 8 MB. With `more: true`, sync that stream
  again from its last item.
- **WebSocket** sends, per stream in lane order, `items` messages, then
  `{ "type": "caught_up", "tag", "head", "low" }`; then `{ "type": "sync_done" }`.
  With `"live": true`, new items on those streams keep arriving as `items` messages
  (no `req_id`), in order, without gaps or repeats. `{"type":"unsubscribe","tags":[...]}`
  stops them. At most 5,000 streams per sync and 10,000 live per connection.
- `head` is the stream's last seq. A client that has stored everything up to `head`
  has no gap. Seqs below `low` have expired. Gaps between `low` and `head` are items
  deleted on purpose (an inbox delete, a retired checkpoint), not missing ones.
- If `head` is lower than your cursor, the stream was recreated after being idle over
  400 days: start again from 0.
- **Advance a cursor only after the items are saved locally.** Delivery is
  at-least-once; dedupe by id.

### fetch, checkpoint, have

```json
fetch      { "refs": ["<16-byte hex>", ...] }       → { "items": [...] }       "fetched"
checkpoint { "tags": ["...", ...] }                 → { "item": {...} | absent } "checkpoint"
have       { "tag": "...", "ids": ["<32-byte hex>"] } → { "present": [...] }   "have"
```

- `fetch` finds items by `ref` in any stream: use it for an op's missing predecessors.
- `checkpoint` returns the newest checkpoint among the given control streams.
- `have` lets a sender confirm its recent publishes are still stored after a
  reconnect (see *Durability*).

### delete, retire

```json
delete { "tag": "...", "seq": 3, "k": "<16–64 byte hex>" }        → { "deleted": true }  "deleted"
retire { "pk": "...", "sig": "...", "seqs": [2] }                 → { "retired": 1 }    "retired"
```

- `delete` removes an inbox item if `SHA-256(k)` equals its `delete_hash`. Put `k`
  inside the sealed body, so only the recipient can delete.
- `retire` removes superseded **checkpoints** from the stream `pk` writes to.
  `sig` is over `"yantagram/relay/retire/v1" | u64be seq...`.

### Errors

HTTP answers `{ "reason": "<code>" }` with 400 (bad input), 409 (`stream-full`), 413
(`too-large`) or 429 (`rate-limited`, `daily-limit`). WebSocket answers
`{ "type": "rejected", "req_id", "reason" }`. A rejected publish stored nothing.

### Push

`PUT` and `DELETE /push/register` are unchanged from the standalone push service, so
the app only needs a new push URL. Register control and inbox tags as well as group
tags (up to 2,048). Group-lane chat messages alert on iOS (when the device has alerts
on). Everything else wakes the app silently, so it can sync ops in the background.
Wake-ups are coalesced per device (30 s; alerts 10 s).

## Durability

- SQLite in WAL mode with `synchronous=FULL`: `stored` means the commit is on disk.
- Litestream ships the WAL to object storage about every second and restores it on a
  new volume. An item acknowledged in the last second before the volume is lost can
  still be lost, so a client keeps each op in its outbox for 24 hours after `stored`
  and checks it with `have` on its next reconnect, republishing anything missing.
- Expiry runs hourly. Stream rows are kept until they've been empty and idle for 400
  days, so a stream's head never goes back while a cursor might point into it.

## Migration from strfry

Old app versions only use strfry. While they're still around, the bridge
(`STRFRY_URL`) keeps the two in step:

- **strfry → relay:** every kind-2929 event is imported into the `legacy` stream for
  its routing tag. New clients sync their groups' old-version tags as legacy streams,
  next to their own.
- **relay → strfry:** items published with `legacy_tag` are queued in the same
  transaction and forwarded in order. Each is removed only after strfry answers OK.
  If strfry refuses it permanently, it's dropped and counted. The bridge doesn't import
  its own forwarded events back.

**Limits to know about:**

- strfry accepts packets up to 40 KB, so larger items (unfragmented control ops,
  checkpoints) can't reach old clients. They're dropped and counted as
  `bridge_dropped{reason="too_large"}`. Until the sunset, the app should keep sending
  oversized control items to old clients as fragments, through `legacy_tag`.
- After a connection drop, an item may be forwarded twice (with different Nostr
  ids). Clients already dedupe packets.
- **strfry must exempt the bridge from its per-IP limits.** The bridge publishes
  everyone's items from one address, which would trip `yantagram-policy.js`'s
  200 events per 10 s. The updated policy trusts sources in Fly's private network
  (`TRUSTED_SOURCE_PREFIXES`, default `fdaa:`). Deploy that before turning the
  bridge on. Without it the bridge still delivers everything, in order, but
  throttled.

Turn the bridge off (unset `STRFRY_URL`) at the sunset described in the design.

## Running

```bash
./gradlew test
```

```bash
DB_PATH=./relay.db ./gradlew run
```

Push is off unless APNs or FCM credentials are set. The bridge is off unless
`STRFRY_URL` is set.

| Variable | Default | |
|---|---|---|
| `DB_PATH` | (required) | SQLite file |
| `PORT`, `METRICS_PORT` | 8080, 9091 | Metrics are served on their own port, not public |
| `STRFRY_URL` | unset | Turns on the migration bridge |
| `APNS_KEY_P8`, `APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_TOPIC`, `APNS_ENV` | unset | APNs (token auth) |
| `FCM_PROJECT_ID`, `FCM_CLIENT_EMAIL`, `FIREBASE_PRIVATE_KEY` | unset | FCM HTTP v1 |
| `COALESCE_SECONDS`, `ALERT_COALESCE_SECONDS` | 30, 10 | Push coalescing |
| `REGISTRATION_TTL_DAYS`, `MAX_REGISTRATIONS` | 14, 200000 | Push registrations |
| `LITESTREAM_REPLICA_URL`, `LITESTREAM_ACCESS_KEY_ID`, `LITESTREAM_SECRET_ACCESS_KEY` | unset | Replication (Docker image only) |
| `LITESTREAM_ENDPOINT`, `LITESTREAM_FORCE_PATH_STYLE` | unset | For S3-compatible stores other than AWS (Tigris: `https://fly.storage.tigris.dev`) |

## Deploying

This is a **new** Fly app, `yantagram-relay-v2`. The strfry relay keeps the name
`yantagram-relay` until the sunset. Deploys are manual (`workflow_dispatch`) until
launch.

```bash
fly apps create yantagram-relay-v2
fly volumes create relay_v2_data --region iad --size 10
fly secrets set APNS_KEY_ID=… APNS_TEAM_ID=… FCM_PROJECT_ID=… FCM_CLIENT_EMAIL=… \
  APNS_KEY_P8="$(op read …)" FIREBASE_PRIVATE_KEY="$(op read …)" \
  LITESTREAM_REPLICA_URL=s3://<bucket>/relay LITESTREAM_ACCESS_KEY_ID=… LITESTREAM_SECRET_ACCESS_KEY=…
fly deploy
```

## Watching it

Useful metrics: `relay_items_stored{lane}`, `relay_rejected{op,reason}`,
`relay_items_expired`, `relay_live_streams`, `bridge_connected`, `bridge_outbox`
(should stay near 0), `bridge_dropped{reason}`, `push_sent` / `push_failed`. The logs
never contain tags, tokens, IPs or bodies.
