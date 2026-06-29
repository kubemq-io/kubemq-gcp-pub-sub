# Migrating from Google Cloud Pub/Sub

## The Pitch

**Point your existing Google Cloud Pub/Sub application at KubeMQ by setting one environment
variable.** Same Google client library, same code, same Pub/Sub v1 gRPC services. There is no SDK
to adopt, no proto, no data migration — topics live in normal KubeMQ Events Store logs and
subscriptions in normal Queue channels. Rollback is **config-only** (`CONNECTORS_GCP_ENABLE=false`).

But several connector behaviors **deviate from real Google Pub/Sub**. Read the deviations below
before you migrate; most are invisible until a corner case hits production.

## Point the SDK at KubeMQ

Set the standard emulator env var and a project id — no code change. The library clears Google
credentials and uses the insecure emulator path automatically.

```bash
export PUBSUB_EMULATOR_HOST=<kubemq-host>:8085
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
```

| Aspect | Google Pub/Sub | KubeMQ Pub/Sub connector |
|--------|----------------|--------------------------|
| Endpoint | `pubsub.googleapis.com` | `PUBSUB_EMULATOR_HOST=<host>:8085` — **only this changes** |
| Auth | OAuth2 / service-account JWT | **none** (emulator mode); SDK skips auth when the env var is set |
| Transport | TLS gRPC + REST/JSON v1 | **insecure gRPC only** — no TLS, no REST/JSON v1 |
| Project | multi-tenant, enforced | **parsed and ignored** — single-tenant; resource ids global across projects |
| IAM | enforced | permissive stubs (no enforcement) |
| Credentials | required | **cleared** by the SDK when `PUBSUB_EMULATOR_HOST` is set |

Examples use the standard Google env vars only (no bespoke wrapper var) — zero-code-change is the
value prop. See [../getting-started.md](../getting-started.md) and
[../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md).

## The Deviations

| # | Area | Google Pub/Sub | KubeMQ Pub/Sub connector | Gotcha |
|---|------|----------------|--------------------------|--------|
| 1 | **Authentication** | OAuth2/JWT/IAM enforced | **none** (emulator); IAM RPCs are permissive stubs — do not expose to untrusted networks | #3 |
| 2 | **Exactly-once scope** | cluster-wide | **node-local** — an `ack_id` is valid only on the node that minted it; no distributed exactly-once | #1 |
| 3 | **Unary invalid-ack status** | (resolved via SDK) | `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` — matches the real SDK contract, **not** a literal `INVALID_ARGUMENT` | — |
| 4 | **Retention** | per-resource, up to limits | **clamped to broker `Store.MaxRetention`**; `GetTopic`/`GetSubscription` echo the requested value, fan-out/seek/dashboard use the clamped value | #5 |
| 5 | **Export subscriptions** | BigQuery / Cloud Storage / Bigtable | **rejected** (`INVALID_ARGUMENT`) — no KubeMQ analog | — |
| 6 | **KMS & ingestion** | supported | `kms_key_name` **accepted-and-ignored**; ingestion sources **rejected** | — |
| 7 | **Project id** | multi-tenant | **parsed and ignored** — resource ids global across projects | #2 |
| 8 | **Transport** | gRPC + REST/JSON v1 | **gRPC only** — no REST/JSON v1 (grpc-gateway) | #9 |
| 9 | **Filtering** | full CEL | **CEL-subset**, attributes-only, ≤ 256 chars, immutable; malformed → `INVALID_ARGUMENT` | #4 |
| 10 | **Seek pre-window** | error / earliest | a pre-window timestamp **clamps** to the earliest retained message (not an error) | #8 |
| 11 | **`max_delivery_attempts`** | 5..100 | same — 5..100 (0 = unset); values 1..4 rejected | #6 |
| 12 | **Topic id `sub.` prefix** | allowed | **rejected** — `sub.` is the reserved broker subscription namespace | #7 |
| 13 | **Schema deps** | Google validators | Avro via `hamba/avro/v2`; Protobuf via `jhump/protoreflect`; definition ≤ 300 KB | — |

## Before You Migrate

1. **Recreate your resources.** Topics, subscriptions, snapshots, and schemas are **not** migrated
   automatically. Re-create them with your existing IaC / SDK calls against KubeMQ — they are fast
   (in-memory + BoltDB).
2. **Decide on exactly-once (gotcha #1).** Exactly-once works **per node**. On a KubeMQ cluster,
   pin a subscription's StreamingPull traffic to one node (sticky load balancer / session
   affinity), or accept at-least-once across nodes. Single-node deployments are unaffected. See
   [../concepts/exactly-once.md](../concepts/exactly-once.md) and
   [../guides/reliability.md](../guides/reliability.md).
3. **Mind the limits (gotchas #6, #7, #5).** `max_delivery_attempts` must be 5..100; resource ids
   must start with a letter and may not begin `sub.`; retention is clamped to the server's
   `Store.MaxRetention`. All are validated up front with clear `INVALID_ARGUMENT` messages — see
   [limits-and-rules.md](limits-and-rules.md).
4. **Audit your filters (gotcha #4).** Only the attributes-only CEL-subset is supported
   (`attributes:KEY`, `= / != "v"`, `hasPrefix(...)`, `AND/OR/NOT`). Body-payload filtering and
   full CEL functions are not.
5. **Drop REST/JSON v1 assumptions (gotcha #8/#9).** The connector is gRPC only. Any tooling that
   used the Pub/Sub REST API must switch to the gRPC path (the standard SDKs already do).
6. **Drop project/IAM assumptions (gotchas #2, #3).** The project segment is ignored and IAM is a
   stub; do not rely on per-project isolation or IAM-based authorization.

## Interop Bridge

Migration can be incremental: anything published via Pub/Sub is consumable **natively** from the
Events Store channel `pubsub.{topic}`, and a subscription's backlog is a native Queue
`pubsub.sub.{sub}`. Bridge legacy native KubeMQ consumers onto the same messages with no double
write. The `interop/native-events-store` example shows the deterministic
subscribe-before-publish pattern (`startAt = "new"`). See
[../concepts/cross-protocol-interop.md](../concepts/cross-protocol-interop.md) and
[channel-mapping.md](channel-mapping.md).

## Rollback

Rollback is **config-only**:

```bash
export CONNECTORS_GCP_ENABLE=false   # closes the Pub/Sub listener port (8085)
```

There is **no data migration to undo** — topic data lives in ordinary KubeMQ Events Store logs and
subscription backlogs in ordinary Queue channels. Point your app back at
`pubsub.googleapis.com` (unset `PUBSUB_EMULATOR_HOST`) and you are done.

## See Also

- [capabilities.md](capabilities.md) — supported RPCs + the accepted-and-ignored / rejected list + the nine gotchas.
- [limits-and-rules.md](limits-and-rules.md) — the Google-exact limits you must respect.
- [error-codes.md](error-codes.md) — the gRPC status codes, incl. the unary invalid-ack `ErrorInfo`.
- [channel-mapping.md](channel-mapping.md) — the `pubsub.{t}` / `pubsub.sub.{s}` mapping (no rename needed).
