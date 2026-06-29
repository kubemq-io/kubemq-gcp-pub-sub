# Capabilities

What the Google Cloud Pub/Sub connector supports, what it accepts-and-ignores, and what it
rejects. The connector implements the **real Pub/Sub v1
gRPC services** — **38 RPCs** total — so unmodified Google client libraries and `gcloud pubsub`
talk to KubeMQ exactly as they would to Google's local emulator.

Milestone tags: **M1** core drop-in · **M2** advanced delivery · **M3** full parity.

## The 38-RPC Matrix

| Service | Count |
|---------|-------|
| `google.pubsub.v1.Publisher` | 9 |
| `google.pubsub.v1.Subscriber` | 16 |
| `google.pubsub.v1.SchemaService` | 10 |
| `google.iam.v1.IAMPolicy` | 3 (permissive stubs) |
| **Total** | **38** |

### Publisher (9)

| # | RPC | M | Notes |
|---|-----|---|-------|
| 1 | `CreateTopic` | M1 | Validates the name; `kms_key_name` accepted-and-ignored; ingestion configs rejected (`INVALID_ARGUMENT`); retention clamped to the broker ceiling. Backing log `pubsub.{t}`. |
| 2 | `GetTopic` | M1 | Returns the **requested (un-clamped)** retention. |
| 3 | `ListTopics` | M1 | Opaque page token. |
| 4 | `DeleteTopic` | M1 | **Tombstone** — the record is retained so existing subscriptions survive; re-creating the topic reuses the log. |
| 5 | `Publish` | M1 | Batch (≤ 1000); assigns id + publish-time; writes the topic log once, then fans out one queue copy per subscription (applying each sub's filter). M3 adds schema enforce-on-publish. |
| 6 | `ListTopicSubscriptions` | M1 | |
| 7 | `UpdateTopic` | M2 | FieldMask over `labels`, `message_retention_duration`, `schema_settings`. |
| 8 | `ListTopicSnapshots` | M3 | |
| 9 | `DetachSubscription` | M3 | Marks detached, drops backlog + leases; the subscription stays re-fillable by `Seek`. |

### Subscriber (16)

| # | RPC | M | Notes |
|---|-----|---|-------|
| 1 | `CreateSubscription` | M1 | Binds to a topic; queue `pubsub.sub.{s}` created lazily. Export subscriptions (BigQuery/Cloud Storage/Bigtable) and ingestion are rejected. Filter compiled at create-time (**immutable**). |
| 2 | `GetSubscription` | M1 | Returns requested (un-clamped) retention. |
| 3 | `ListSubscriptions` | M1 | |
| 4 | `DeleteSubscription` | M1 | Drops backlog + leases. |
| 5 | `UpdateSubscription` | M2 | FieldMask over ack deadline / retention / DLQ / retry / push / exactly-once / labels. `name` and `filter` are **immutable**. |
| 6 | `Pull` | M1 | `max_messages ≤ 1000`; a detached subscription → `FAILED_PRECONDITION`. |
| 7 | `Acknowledge` | M1 | Decodes `ack_id` → acks the broker sequence. Exactly-once subs return a status for invalid/expired ids. |
| 8 | `ModifyAckDeadline` | M1 | `0` = immediate nack/redeliver; `>0` = extend (10..600 s). |
| 9 | `StreamingPull` | M1 | Bidirectional; per-stream flow control; exactly-once confirmations; periodic server-initiated close every `StreamCloseSeconds`. |
| 10 | `ModifyPushConfig` | M2 | Switch pull ↔ push; an empty config returns to pull. |
| 11 | `Seek` | M3 | To a timestamp or a snapshot; replays the topic log into the subscription queue, bounded by `MaxSeekReplay`. |
| 12 | `CreateSnapshot` | M3 | Cursor snapshot, 7-day default expiry. |
| 13 | `GetSnapshot` | M3 | |
| 14 | `ListSnapshots` | M3 | |
| 15 | `UpdateSnapshot` | M3 | May change `labels` and `expire_time`. |
| 16 | `DeleteSnapshot` | M3 | |

> The 250 ms lease sweeper, retry backoff, dead-letter republish, and ordering cursor are
> connector-internal mechanics behind these RPCs — see
> [../concepts/subscriptions-and-pull.md](../concepts/subscriptions-and-pull.md).

### Key RPC Parameters & Returns

The parameters that matter operationally, with their type, default, and accepted range, plus what
each RPC returns. Every value is enforced exactly as Google does (see
[limits-and-rules.md](limits-and-rules.md)); only the `CONNECTORS_PUB_SUB_*` knobs are tunable.

| RPC | Key parameters (type · default · range) | Returns |
|-----|-----------------------------------------|---------|
| `Publish` | `messages[]` (`PubsubMessage` · — · **1..1000** per batch); each `data`+`attributes` ≤ **10 MiB** total, ≤ **100** attributes (key ≤ 256 B, value ≤ 1024 B), `ordering_key` (string · `""` · ≤ 1024 B); `data` **or** `attributes` must be non-empty | One `message_id` (server-assigned) **per message, in request order**, each with a server `publish_time`. The whole batch is validated first — one bad message rejects all with `INVALID_ARGUMENT` and nothing is enqueued. |
| `Pull` | `max_messages` (int · — · **1..1000**, also bounded by the flow-control ceiling); `return_immediately` (bool · deprecated) | `ReceivedMessage[]`, each with an opaque `ack_id` and a `delivery_attempt`. A detached subscription → `FAILED_PRECONDITION`. |
| `StreamingPull` | `stream_ack_deadline_seconds` (int · — · **10..600**); `max_outstanding_messages` / `max_outstanding_bytes` (int64 · `0` = unlimited, capped by `MaxOutstandingMessages`, default **1000**) | A stream of `StreamingPullResponse` (messages + on exactly-once subs, `AcknowledgeConfirmation` / `ModifyAckDeadlineConfirmation`). Server closes the stream every `StreamCloseSeconds` (default **1800 s**) with `UNAVAILABLE`; clients transparently reconnect. |
| `Acknowledge` | `ack_ids[]` (string · — · opaque, node-local) | Empty on success. On an exactly-once sub a stale/expired id → `FAILED_PRECONDITION` + `ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID)` (not `INVALID_ARGUMENT`). |
| `ModifyAckDeadline` | `ack_ids[]` (string); `ack_deadline_seconds` (int · — · **`0` = immediate nack/redeliver, else 10..600 s**) | Empty on success; exactly-once stale-id behavior as for `Acknowledge`. |
| `CreateTopic` | `name` (resource id · — · **3..255 chars, starts with a letter**, charset `[A-Za-z0-9._~%+-]`, no `goog`, **not** starting `sub.`); `message_retention_duration` (duration · — · **10 min..31 days**, clamped to broker `Store.MaxRetention`); `kms_key_name` accepted-and-ignored; ingestion configs rejected (`INVALID_ARGUMENT`) | The created `Topic`. Idempotent. `GetTopic` later returns the **requested (un-clamped)** retention even though fan-out/seek use the clamped value (gotcha #5). |
| `CreateSubscription` | `topic` (string · — · must exist); `ack_deadline_seconds` (int · `10` · **10..600**); `filter` (CEL-subset · `""` · ≤ 256 chars, **immutable**); `enable_message_ordering` (bool · `false`); `enable_exactly_once_delivery` (bool · `false`); `dead_letter_policy.max_delivery_attempts` (int · `0` = unset · else **5..100**); `push_config` (optional → push mode) | The created `Subscription`. `name` and `filter` are immutable thereafter; export/ingestion subscriptions are rejected (`INVALID_ARGUMENT`). |
| `Seek` | `time` (timestamp) **or** `snapshot` (name) — exactly one | Empty on success. Replays the topic log into the subscription queue, bounded by `MaxSeekReplay` (default **1,000,000** → hit cap = WARN, no silent loss). A pre-window timestamp **clamps** to the earliest retained message (not an error); a snapshot of a detached subscription → `FAILED_PRECONDITION`. |

A subscription `filter` is attributes-only and compiled once at create-time, e.g.:

```
attributes.type = "order" AND attributes.region = "eu" AND NOT attributes:test
```

Exactly-once acks return a per-id `AckResult` rather than fire-and-forget — await it: a success is
durable, `invalid_ack_ids` mean the lease was lost (the message may redeliver),
`temporary_failed_ack_ids` should be retried. See [../concepts/exactly-once.md](../concepts/exactly-once.md).

### SchemaService (10, M3)

`CreateSchema`, `GetSchema` (BASIC/FULL), `ListSchemas`, `ListSchemaRevisions`, `CommitSchema`,
`RollbackSchema`, `DeleteSchemaRevision` (keeps ≥ 1), `DeleteSchema`, `ValidateSchema`,
`ValidateMessage` — over **Avro** (`hamba/avro/v2`) and **Protobuf** (`jhump/protoreflect`)
definitions, each with server-assigned revision ids and full revision history. A definition that
fails to parse → `INVALID_ARGUMENT`; definitions are capped at **300 KB**. See
[../concepts/schema-validation.md](../concepts/schema-validation.md).

### IAMPolicy (3, M2)

`GetIamPolicy` returns an empty `Policy{Version: 3}`; `SetIamPolicy` and `TestIamPermissions`
**echo** the request. These are **permissive stubs** — there is no IAM enforcement (emulator
parity). The connector runs with **no authentication**.

## Feature Support

| Feature | Supported? | Notes |
|---------|------------|-------|
| Basic publish / pull / ack | ● | M1 core drop-in. |
| Batch publish (≤ 1000) | ● | Atomic — the whole batch is validated before anything is enqueued. |
| StreamingPull + flow control | ● | Per-stream `max_outstanding_messages`/`_bytes`; `≤ 0` = unlimited (capped by `MaxOutstandingMessages`). |
| Ordering keys | ● | `enable_message_ordering`; at most one in flight per key, in-order redelivery. |
| Attribute filtering | ● | CEL-**subset**, attributes-only, ≤ 256 chars, immutable, applied at fan-out. |
| Dead-letter topic | ● | `dead_letter_topic` + `max_delivery_attempts` (5..100). |
| Push delivery | ● | Wrapped JSON envelope or `no_wrapper`; optional OIDC Bearer; HTTPS (HTTP only for localhost). |
| Exactly-once delivery | ● **(node-local)** | `enable_exactly_once_delivery`; an `ack_id` is valid only on the node that minted it. |
| Seek to timestamp / snapshot | ● | Replays the topic log; a pre-window timestamp **clamps** to the earliest retained message. |
| Snapshots | ● | 7-day default expiry, swept hourly. |
| Schema validation (Avro / Protobuf) | ● | Enforce-on-publish; whole batch rejected on first non-conforming message. |
| Cross-protocol interop | ● | Pub/Sub publish ⇄ native KubeMQ consume on Events Store `pubsub.{t}`. |
| IAM enforcement | ○ | Permissive stubs only; no enforcement. |

A push subscription's worker POSTs the wrapped JSON envelope (`data` is base64; `no_wrapper`
sends the raw body); a `2xx` response acks, non-`2xx`/timeout retries with backoff:

```json
{
  "message": {
    "data": "<base64>",
    "attributes": { "key": "value" },
    "messageId": "...",
    "publishTime": "...",
    "orderingKey": "..."
  },
  "subscription": "..."
}
```

## Accepted-and-Ignored / Rejected

| Operation / field | Behavior |
|-------------------|----------|
| `kms_key_name` (on `CreateTopic`) | **accepted and ignored** — no KMS in emulator mode. |
| Project segment `projects/{p}/…` | **parsed, validated, then ignored** — single-tenant; resource ids are global across projects. |
| Ingestion sources (topic/sub) | **rejected** (`INVALID_ARGUMENT`) — no KubeMQ analog. |
| Export subscriptions (BigQuery / Cloud Storage / Bigtable) | **rejected** (`INVALID_ARGUMENT`). |
| REST / JSON v1 (grpc-gateway) | **not served** — gRPC only (gotcha #9). |
| Google OAuth2 / JWT / IAM | **not validated** — emulator mode, no auth. |

## The Nine Gotchas (summary)

| # | Gotcha | Where documented |
|---|--------|------------------|
| 1 | **Exactly-once is node-local** — an `ack_id` minted on one node is invalid on another; pin a subscription's StreamingPull to one node (sticky LB) or accept at-least-once across nodes | [../concepts/exactly-once.md](../concepts/exactly-once.md), [../guides/reliability.md](../guides/reliability.md), [migration-from-gcp.md](migration-from-gcp.md) |
| 2 | **Project id parsed but ignored** — resource ids are global across projects | [../getting-started.md](../getting-started.md), [channel-mapping.md](channel-mapping.md) |
| 3 | **Credentials cleared / insecure path when `PUBSUB_EMULATOR_HOST` is set** — the SDK skips Google auth and dials insecure gRPC | [../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md) |
| 4 | **Filtering is attributes-only CEL-subset** — malformed → `INVALID_ARGUMENT` | [../guides/filtering.md](../guides/filtering.md), [limits-and-rules.md](limits-and-rules.md) |
| 5 | **Retention clamped to broker `Store.MaxRetention`** — `GetTopic`/`GetSubscription` echo the requested value; fan-out/seek/dashboard use the clamped value | [limits-and-rules.md](limits-and-rules.md), [../concepts/seek-and-snapshots.md](../concepts/seek-and-snapshots.md) |
| 6 | **`max_delivery_attempts` must be 5..100** (0 = unset) | [../concepts/dead-letter-and-retry.md](../concepts/dead-letter-and-retry.md), [limits-and-rules.md](limits-and-rules.md) |
| 7 | **Topic ids may not start with `sub.`** (reserved broker namespace) | [channel-mapping.md](channel-mapping.md), [limits-and-rules.md](limits-and-rules.md) |
| 8 | **Seek of a pre-window timestamp clamps to earliest retained** (not an error) | [../concepts/seek-and-snapshots.md](../concepts/seek-and-snapshots.md) |
| 9 | **gRPC only — no REST/JSON v1** (grpc-gateway not used) | [../architecture.md](../architecture.md), this page |

## See Also

- [channel-mapping.md](channel-mapping.md) — topic/subscription channel grammar + the reserved tags.
- [limits-and-rules.md](limits-and-rules.md) — the Google-exact limits + the connector config knobs.
- [error-codes.md](error-codes.md) — the gRPC status codes the connector returns.
- [migration-from-gcp.md](migration-from-gcp.md) — the known deviations and what won't work.
