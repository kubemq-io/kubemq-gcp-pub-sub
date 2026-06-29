# Go — KubeMQ Google Cloud Pub/Sub examples

Native **`cloud.google.com/go/pubsub`** example apps that talk to the KubeMQ Pub/Sub connector by
pointing the official Google client at the connector's emulator-protocol gRPC endpoint — no real
GCP project, no separate SDK. Set `PUBSUB_EMULATOR_HOST` and the Google client skips Google auth,
uses an insecure gRPC channel, and just works. Go is the connector's server-test-proven SDK and
the de-facto reference for the other languages.

> Conventions (the 15-variant master table, env vars, README template, the nine Pub/Sub gotchas)
> live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Go 1.25+** (the pinned `cloud.google.com/go/pubsub` requires Go ≥ 1.25).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`), reachable at `PUBSUB_EMULATOR_HOST` (default `localhost:8085`).
- The `interop/native-events-store` example additionally needs the KubeMQ **native gRPC broker**
  at `KUBEMQ_GRPC_ADDRESS` (default `localhost:50000`).

### Pinned GCP Pub/Sub + KubeMQ packages (see `go.mod`)

| Package | Version | Used by |
|---------|---------|---------|
| `cloud.google.com/go/pubsub` | `v1.50.2` | all variants (official GCP Pub/Sub client; latest stable) |
| `github.com/kubemq-io/kubemq-go/v2` | `v2.0.3` | `interop/native-events-store` only (native KubeMQ SDK) |
| `github.com/google/uuid` | `v1.6.0` | per-run resource id suffixes (channel isolation) |

Latest stable is pinned and `go.sum` is committed — no floating range in the lockfile (S9 policy).

## Setup

```bash
cd examples/go
go build ./...   # resolves go.sum and compiles all 15 variants + the shared deps anchor
```

Both `go.mod` and `go.sum` are committed.

## Run a variant

Every example reads the standard Google emulator env vars. **No code changes** are required: when
`PUBSUB_EMULATOR_HOST` is set, the Google client skips auth and uses an insecure gRPC channel
(gotcha #3). The project segment is parsed but ignored by the connector (gotcha #2), so any
`PUBSUB_PROJECT_ID` works.

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085    # connector default port
export PUBSUB_PROJECT_ID=my-project           # any id; project segment is parsed but ignored

go run ./topics/basic-publish-subscribe
go run ./subscriptions/streaming-pull
KUBEMQ_GRPC_ADDRESS=localhost:50000 go run ./interop/native-events-store
```

Each example prints human-readable progress, asserts the expected behavior, exits **0 on success**
and **non-zero on any failed assertion** — they are runnable proofs, not demos.

## Idiom notes (Go / `cloud.google.com/go/pubsub`)

- Construct the client with `pubsub.NewClient(ctx, projectID)`; the emulator path is selected
  automatically from `PUBSUB_EMULATOR_HOST` (no explicit `option.WithoutAuthentication()` /
  endpoint plumbing needed). Resource ids are uuid-suffixed so concurrent runs use distinct
  `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide.
- Ordering is opt-in on **both** ends: set `Topic.EnableMessageOrdering = true` on the publishing
  topic handle **and** create the subscription with `EnableMessageOrdering: true`; then set an
  `OrderingKey` per message (`delivery/ordering-keys`).
- Exactly-once (`delivery/exactly-once`) surfaces ack results via `PublishResult` / the
  `Receive` ack-result API; a `nil` ack error confirms durable ack, a non-nil error must be
  handled (transient → retry, permanent invalid-ack → `FAILED_PRECONDITION` + `ErrorInfo`).
  Exactly-once is **node-local** — pin a subscription's StreamingPull to one node (sticky LB).
- Each operation takes its own `context.Context`; long-lived `Receive` loops are cancelled via
  context to stop StreamingPull cleanly.
- The `interop/native-events-store` half uses the native `kubemq.Client` Events Store
  **subscribe** stream over gRPC with `kubemq.StartFromNewEvents()` (`startAt = "new"`), and
  establishes the subscription **before** the Pub/Sub publish so the published message is in
  window (subscribe-before-publish; no startup race).

## Variant index

Group dirs: `topics/`, `subscriptions/`, `delivery/`, `advanced/`, `interop/`
(kebab-case variant directories).

### topics

| # | Variant | What it shows |
|---|---------|---------------|
| 1 | [`topics/basic-publish-subscribe`](topics/basic-publish-subscribe/) | CreateTopic → CreateSubscription → Publish → Pull → Ack round-trip |
| 2 | [`topics/batch-publish`](topics/batch-publish/) | Publish a batch (≤1000); message ids returned in request order |
| 3 | [`topics/topic-management`](topics/topic-management/) | List/Get/UpdateTopic; DeleteTopic tombstone; ListTopicSubscriptions |

### subscriptions

| # | Variant | What it shows |
|---|---------|---------------|
| 4 | [`subscriptions/streaming-pull`](subscriptions/streaming-pull/) | StreamingPull with flow control + periodic reconnect |
| 5 | [`subscriptions/ack-deadline-nack`](subscriptions/ack-deadline-nack/) | ModifyAckDeadline extend + 0-nack redelivery |

### delivery

| # | Variant | What it shows |
|---|---------|---------------|
| 6 | [`delivery/ordering-keys`](delivery/ordering-keys/) | `enable_message_ordering`; per-key in-order delivery |
| 7 | [`delivery/message-filtering`](delivery/message-filtering/) | CEL-subset attribute filter set at subscription create |
| 8 | [`delivery/dead-letter-topic`](delivery/dead-letter-topic/) | `dead_letter_topic` + `max_delivery_attempts` 5..100 → DLQ |
| 9 | [`delivery/push-delivery`](delivery/push-delivery/) | `push_config` wrapped envelope to a local HTTPS/localhost endpoint |
| 10 | [`delivery/exactly-once`](delivery/exactly-once/) | `enable_exactly_once_delivery`; ack confirmation / `ErrorInfo` handling |

### advanced

| # | Variant | What it shows |
|---|---------|---------------|
| 11 | [`advanced/seek-timestamp`](advanced/seek-timestamp/) | Seek to a timestamp; replay (pre-window clamps, not an error) |
| 12 | [`advanced/snapshots`](advanced/snapshots/) | CreateSnapshot → Seek-to-snapshot |
| 13 | [`advanced/schema-avro`](advanced/schema-avro/) | CreateSchema (Avro) + topic `schema_settings`; enforce-on-publish |
| 14 | [`advanced/schema-protobuf`](advanced/schema-protobuf/) | CreateSchema (Protobuf) + enforce-on-publish |

### interop

| # | Variant | What it shows |
|---|---------|---------------|
| 15 | [`interop/native-events-store`](interop/native-events-store/) | Pub/Sub publish → consume natively from Events Store `pubsub.{t}` via the native KubeMQ SDK (subscribe-before-publish, `startAt = "new"`) |
