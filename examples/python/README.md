# Python — KubeMQ Google Cloud Pub/Sub examples

Runnable [`google-cloud-pubsub`](https://cloud.google.com/python/docs/reference/pubsub/latest)
examples that drive the KubeMQ embedded **Google Cloud Pub/Sub wire-protocol connector** by setting
the standard `PUBSUB_EMULATOR_HOST` env var. These are native, unmodified Pub/Sub SDK apps — when
`PUBSUB_EMULATOR_HOST` is set the client **clears credentials, skips Google auth, and uses insecure
gRPC**, exactly as it would against Google's local emulator. The only "KubeMQ-specific" thing is the
two env vars. Managed with **uv**.

All **15 variants** ship at full parity across the concept groups (`topics/`, `subscriptions/`,
`delivery/`, `advanced/`, `interop/`). The authoritative conventions — the
`PUBSUB_EMULATOR_HOST` + project-id convention, the 15-variant master table, the per-example README
template, and the GCP gotchas — live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Python 3.10+** (the GCP SDK `google-cloud-pubsub >= 2.39` requires 3.10).
- [`uv`](https://docs.astral.sh/uv/) (do not use `pip`/`poetry` directly).
- A running **KubeMQ broker with the Pub/Sub connector enabled** (`CONNECTORS_GCP_ENABLE=true`,
  gRPC port **8085**), reachable at `PUBSUB_EMULATOR_HOST` (default `localhost:8085`). The
  `interop/native_events_store` variant additionally needs the native KubeMQ gRPC broker (default
  `localhost:50000`).

### Pinned dependencies (`pyproject.toml` + committed `uv.lock`)

| Package | Version | Used by |
|---------|---------|---------|
| `google-cloud-pubsub` | `2.39.0` | all 15 variants (topics + subscriptions + delivery + advanced) |
| `kubemq` | `4.1.5` | `interop/native_events_store` only (native gRPC Events Store peer) |

Exact transitive versions are pinned in the committed `uv.lock` (lockfile policy: pin via lock;
`uv` only; no floating range).

## Setup

```bash
cd examples/python
uv sync
```

## Connection

```bash
export PUBSUB_EMULATOR_HOST="localhost:8085"   # connector default gRPC port; SDK uses the insecure path
export PUBSUB_PROJECT_ID="my-project"          # any id; the project segment is parsed but ignored
# some clients also honour GOOGLE_CLOUD_PROJECT=my-project
# interop/native_events_store only:
export KUBEMQ_GRPC_ADDRESS="localhost:50000"
```

> **Emulator drop-in (the headline).** When `PUBSUB_EMULATOR_HOST` is set, `google-cloud-pubsub`
> auto-detects it, **clears credentials, skips Google auth, and dials insecure gRPC** — no code
> changes. The connector is **no-auth, no-TLS** by design (emulator protocol); DoS guards remain
> active. Do not expose port 8085 to untrusted networks. The project id is **parsed but ignored**
> (single-tenant, like the emulator); resource ids are global across "projects".

## Run a variant

```bash
uv run python -m topics.basic_publish_subscribe
# or, equivalently:
uv run python topics/basic_publish_subscribe/main.py

uv run python -m delivery.ordering_keys
uv run python -m interop.native_events_store
```

Every example prints human-readable progress, asserts its expected outcome with `[OK]` lines, and
**exits non-zero on any failed assertion** — they are runnable proofs, not demos.

## Idiom notes (`google-cloud-pubsub`)

- **Emulator auto-detection is automatic:** with `PUBSUB_EMULATOR_HOST` set,
  `pubsub_v1.PublisherClient()` / `pubsub_v1.SubscriberClient()` skip Google auth and dial insecure
  gRPC — no `EmulatorDetection` flag is needed (unlike C#). The publisher/subscriber are separate
  client classes; topic/subscription admin lives on those same clients.
- **Resource paths are project-qualified strings:** build them with
  `client.topic_path(project, topic)` / `client.subscription_path(project, sub)` even though the
  connector ignores the project segment.
- **Ordering keys must be enabled on the publisher** (`PublisherOptions(enable_message_ordering=True)`)
  AND set per message; messages sharing a key are delivered in publish order with at most one in
  flight per key.
- **Exactly-once** subscriptions return ack/nack *futures* whose result must be awaited to confirm
  success; a failed confirmation surfaces as an `AcknowledgeError`.
- **Channel isolation (parallel-safe):** the canonical channels are shared and global (topic
  `orders` is always `pubsub.orders`; the project is ignored). Examples uuid-suffix their topic/sub
  ids so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not cross-talk,
  and delete what they create on exit (best-effort). See
  [`../../docs/guides/connectivity-and-emulator-mode.md`](../../docs/guides/connectivity-and-emulator-mode.md).
- Lint/format: `uv run ruff format .` + `uv run ruff check --fix .`.

## Variant index

| Group | Variant | Directory |
|-------|---------|-----------|
| `topics/` | Basic Publish & Subscribe | [`topics/basic_publish_subscribe/`](topics/basic_publish_subscribe/) |
| `topics/` | Batch Publish | [`topics/batch_publish/`](topics/batch_publish/) |
| `topics/` | Topic Management | [`topics/topic_management/`](topics/topic_management/) |
| `subscriptions/` | Streaming Pull | [`subscriptions/streaming_pull/`](subscriptions/streaming_pull/) |
| `subscriptions/` | Ack Deadline & Nack | [`subscriptions/ack_deadline_nack/`](subscriptions/ack_deadline_nack/) |
| `delivery/` | Ordering Keys | [`delivery/ordering_keys/`](delivery/ordering_keys/) |
| `delivery/` | Message Filtering | [`delivery/message_filtering/`](delivery/message_filtering/) |
| `delivery/` | Dead-Letter Topic | [`delivery/dead_letter_topic/`](delivery/dead_letter_topic/) |
| `delivery/` | Push Delivery | [`delivery/push_delivery/`](delivery/push_delivery/) |
| `delivery/` | Exactly-Once Delivery | [`delivery/exactly_once/`](delivery/exactly_once/) |
| `advanced/` | Seek to Timestamp | [`advanced/seek_timestamp/`](advanced/seek_timestamp/) |
| `advanced/` | Snapshots | [`advanced/snapshots/`](advanced/snapshots/) |
| `advanced/` | Schema Validation (Avro) | [`advanced/schema_avro/`](advanced/schema_avro/) |
| `advanced/` | Schema Validation (Protobuf) | [`advanced/schema_protobuf/`](advanced/schema_protobuf/) |
| `interop/` | Pub/Sub ⇄ Native Events Store | [`interop/native_events_store/`](interop/native_events_store/) |

> Channel mapping: Pub/Sub topic `t` ↔ KubeMQ Events Store log `pubsub.{t}` (the authoritative,
> replayable, cross-protocol source); subscription `s` ↔ KubeMQ Queue `pubsub.sub.{s}`. A `Publish`
> writes once to the topic log, then fans out one queue copy per subscription, applying each
> subscription's filter. Native consumers additionally see three reserved tags
> (`_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key`).
