# KubeMQ Google Cloud Pub/Sub Examples

Runnable, copy-paste examples that drive the KubeMQ embedded Google Cloud Pub/Sub gRPC
wire-protocol connector using the **standard, unmodified first-party Google Cloud Pub/Sub client
libraries** (no KubeMQ SDK). Every language ships all **15 variants** at full parity — there are no
N/A capability cells (up to 90 programs total).

> **The contract is one environment variable.** Every official Pub/Sub client and `gcloud` honour
> `PUBSUB_EMULATOR_HOST`: when set, the SDK clears credentials, skips Google auth, and uses
> insecure gRPC — exactly as against Google's local emulator. **Zero code changes** are required
> to retarget a Pub/Sub app at KubeMQ. The authoritative conventions (the emulator env contract,
> the 15-variant master table, the per-example README template, run commands, channel isolation)
> live in [`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md). The `interop/native-events-store`
> variant additionally opens the native KubeMQ gRPC broker (`localhost:50000`) to read the topic's
> Events Store log; each per-example README states what is hard-asserted.

## Connection

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default gRPC port; SDK uses the insecure path
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
# some clients/gcloud also accept:
# export GOOGLE_CLOUD_PROJECT=my-project
```

Each example reads `PUBSUB_EMULATOR_HOST` (most SDKs auto-detect it; C#/Ruby/Java need a one-line
opt-in — see the idiom notes). The `delivery/push-delivery` variant additionally binds a tiny local
HTTP(S) endpoint to receive the push envelope; the `interop/native-events-store` variant additionally
opens the native KubeMQ gRPC broker at `localhost:50000`.

> **Security posture.** The connector is **no-auth, no-TLS, insecure gRPC** by design — the
> emulator contract. DoS guards stay active (`MaxRecvMsgSize`, `MaxInflightPerSubscription`,
> `MaxConcurrentPolls`, `MaxSeekReplay`, push backoff). Do **not** expose port 8085 to untrusted
> networks. The connector is **gRPC only** — there is no REST/JSON v1 surface (gotcha #9). The
> project id is parsed but ignored (gotcha #2); resource ids are global across projects.

## Languages & pinned GCP Pub/Sub SDKs

| Language | Official GCP Pub/Sub client | Emulator construction | Manifest | Run prereq |
|----------|-----------------------------|------------------------|----------|------------|
| Go | `cloud.google.com/go/pubsub` | `pubsub.NewClient(ctx, projectID)` (auto-detects env var) | `go/go.mod` | Go 1.25+ |
| Python | `google-cloud-pubsub` (via **uv**) | `pubsub_v1.PublisherClient()` (honours env var) | `python/pyproject.toml` | Python 3.10+, `uv` |
| Java | `com.google.cloud:google-cloud-pubsub` (BOM `libraries-bom`) | channel → emulator host + `NoCredentialsProvider` | `java/pom.xml` | JDK 21+, Maven 3.9+ |
| JS/TS | `@google-cloud/pubsub` | `new PubSub({ projectId })` (auto-detects env var); run via `tsx` | `javascript/package.json` | Node 18+ |
| C# | `Google.Cloud.PubSub.V1` | `…Builder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build()` | `csharp/Directory.Packages.props` | .NET 8 |
| Ruby | `google-cloud-pubsub` | `Google::Cloud::PubSub.new(project_id:, emulator_host:)` | `ruby/Gemfile` | Ruby 3.1+ (3.3.7 via rbenv) |

> The first-party Google client is used in every language — that is the point of the
> emulator-protocol drop-in. There is **no Rust** example (Google ships no first-party GCP Pub/Sub
> client for Rust). Versions are pinned to the latest stable at implementation; bump-and-lock via
> `/check-deps`. Commit `go.sum`, `uv.lock`, `package-lock.json`, and the C# central pins; gitignore
> Ruby `Gemfile.lock`.
>
> Per-language idiom notes (carried in each language's root README): Go auto-detects the emulator
> and uses `topic.EnableMessageOrdering`; Python uses `PublisherOptions(enable_message_ordering=…)`
> under **uv**; Java points a plaintext `ManagedChannel` at the emulator host with
> `NoCredentialsProvider`; JS/TS uses `{ enableMessageOrdering }` + `ackWithResponse()`; C# requires
> `EmulatorDetection.EmulatorOnly`; Ruby passes `emulator_host:` explicitly. The
> `interop/native-events-store` variant pins the native KubeMQ SDK per language (Go
> `kubemq-go/v2@v2.0.3`, Python `kubemq==4.1.5`, Java `kubemq-sdk-Java:3.1.1`, Node `kubemq-js@3.0.1`,
> C# `KubeMQ.SDK.csharp@3.0.1`, Ruby gem `kubemq ~> 1.0`).

## The 15 variants (concept matrix)

Grouped by Pub/Sub concept (`topics/`, `subscriptions/`, `delivery/`, `advanced/`, `interop/`),
NOT KubeMQ patterns. kebab-case dirs for go/javascript/java/csharp; snake_case for python/ruby.

| Group | Variant | Mechanic |
|-------|---------|----------|
| `topics/` | `basic-publish-subscribe` | CreateTopic → CreateSubscription → Publish → Pull → Acknowledge round-trip |
| `topics/` | `batch-publish` | Publish a batch (≤ 1000); message-ids returned in request order; atomic batch validation |
| `topics/` | `topic-management` | List/Get/UpdateTopic; DeleteTopic tombstone; ListTopicSubscriptions |
| `subscriptions/` | `streaming-pull` | StreamingPull with flow control; survives periodic server-initiated reconnect |
| `subscriptions/` | `ack-deadline-nack` | ModifyAckDeadline extend (10..600 s) + 0-nack → redelivery |
| `delivery/` | `ordering-keys` | `enable_message_ordering`; per-key in-order, at-most-one-in-flight |
| `delivery/` | `message-filtering` | CEL-subset attribute filter at create; matching delivered, non-matching suppressed |
| `delivery/` | `dead-letter-topic` | `dead_letter_topic` + `max_delivery_attempts` (5..100); republish + original ack |
| `delivery/` | `push-delivery` | `push_config` wrapped JSON envelope to a local HTTPS/localhost endpoint |
| `delivery/` | `exactly-once` | `enable_exactly_once_delivery`; ack confirmation / unary `ErrorInfo` handling |
| `advanced/` | `seek-timestamp` | Seek to a timestamp; replay re-applies the filter (pre-window clamps to earliest) |
| `advanced/` | `snapshots` | CreateSnapshot → Seek-to-snapshot |
| `advanced/` | `schema-avro` | CreateSchema (Avro) + topic `schema_settings`; enforce-on-publish |
| `advanced/` | `schema-protobuf` | CreateSchema (Protobuf) + enforce-on-publish |
| `interop/` | `native-events-store` | Pub/Sub Publish → consume natively from Events Store `pubsub.{t}` via the **native KubeMQ SDK** |

**15 / 15 in all 6 languages = up to 90 examples.** Full parity, no N/A cells. (See
[`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md) §2 for the full RPC / topic-sub / feature-flag
master table with the canonical connector behaviour referenced.)

## Coverage matrix

Legend: ● = full example required. The `interop/native-events-store` row additionally uses the
native KubeMQ SDK for the Events Store subscribe half.

| Variant \ Lang | Go | Python | Java | JS/TS | C# | Ruby |
|----------------|----|--------|------|-------|----|------|
| topics/basic-publish-subscribe | ● | ● | ● | ● | ● | ● |
| topics/batch-publish | ● | ● | ● | ● | ● | ● |
| topics/topic-management | ● | ● | ● | ● | ● | ● |
| subscriptions/streaming-pull | ● | ● | ● | ● | ● | ● |
| subscriptions/ack-deadline-nack | ● | ● | ● | ● | ● | ● |
| delivery/ordering-keys | ● | ● | ● | ● | ● | ● |
| delivery/message-filtering | ● | ● | ● | ● | ● | ● |
| delivery/dead-letter-topic | ● | ● | ● | ● | ● | ● |
| delivery/push-delivery | ● | ● | ● | ● | ● | ● |
| delivery/exactly-once | ● | ● | ● | ● | ● | ● |
| advanced/seek-timestamp | ● | ● | ● | ● | ● | ● |
| advanced/snapshots | ● | ● | ● | ● | ● | ● |
| advanced/schema-avro | ● | ● | ● | ● | ● | ● |
| advanced/schema-protobuf | ● | ● | ● | ● | ● | ● |
| interop/native-events-store ¹ | ● | ● | ● | ● | ● | ● |

**15 / 15 in all 6 languages = up to 90 examples. No capability N/A cells.**

¹ `interop/native-events-store` is the only variant whose native-KubeMQ side uses a KubeMQ client.
It publishes to a topic via the GCP Pub/Sub SDK, then consumes the same payload natively from the
Events Store channel `pubsub.{t}` via that language's KubeMQ SDK over gRPC (`localhost:50000`),
showing the three reserved tags (`_pubsub_message_id`, `_pubsub_publish_time`,
`_pubsub_ordering_key`). To stay deterministic it opens the native Events Store **subscribe** stream
with the "new only" start policy (`startAt = "new"`) and confirms the stream is open **before** the
Pub/Sub publish — so the published message is always in-window (no startup race). All six languages
use a native gRPC Events Store subscribe; the GCP-SDK half is idiomatic in every language and the
matrix stays at full parity.

## Not in the 15 (future / listed-not-built)

- **Server authentication / TLS** — the connector is no-auth emulator mode by design (gotcha #3);
  there is no auth/TLS variant. The security posture (insecure gRPC; do not expose port 8085) is a
  connectivity callout in
  [`../docs/guides/connectivity-and-emulator-mode.md`](../docs/guides/connectivity-and-emulator-mode.md),
  not a built program.
- **REST/JSON v1** — the connector is **gRPC only** (gotcha #9); no grpc-gateway program is shown.
- **Export subscriptions (BigQuery / Cloud Storage / Bigtable) and ingestion** — rejected by the
  connector; documented as non-goals in
  [`../docs/reference/migration-from-gcp.md`](../docs/reference/migration-from-gcp.md), never
  demonstrated.
- **Multi-node cluster / sticky-LB** — deferred; the node-local exactly-once caveat (gotcha #1)
  appears as a callout in `delivery/exactly-once` and
  [`../docs/concepts/exactly-once.md`](../docs/concepts/exactly-once.md).
