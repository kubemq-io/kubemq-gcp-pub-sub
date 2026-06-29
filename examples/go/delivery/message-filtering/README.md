# Go — delivery: Message Filtering

Attributes-only CEL-subset filtering against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription(Filter) + CreateSubscription(none) → Publish(mixed batch) → Pull → assert`.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/go && go run ./delivery/message-filtering
```

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/message-filtering | emulator=localhost:8085 project=my-project
filter: attributes.region = "eu"
CreateTopic: filtered-<uuid8> (channel pubsub.filtered-<uuid8>)
CreateSubscription: sub-filtered-<uuid8> (filter=attributes.region = "eu")
CreateSubscription: sub-all-<uuid8> (no filter)
Publish mixed batch (one Publish per message; region attribute drives the filter):
  publish body="order-1" region=eu  -> id=<message-id>
  publish body="order-2" region=us  -> id=<message-id>
  publish body="order-3" region=eu  -> id=<message-id>
  publish body="order-4" region=ap  -> id=<message-id>
  publish body="order-5" region=eu  -> id=<message-id>
Pull filtered subscription (expects region=eu only):
  filtered received: [order-1 order-3 order-5]
Pull unfiltered subscription (expects everything):
  unfiltered received: [order-1 order-2 order-3 order-4 order-5]
non-matching attribute -> suppressed (2 not enqueued for filtered sub)
Negative path: a malformed filter is rejected at CreateSubscription:
  malformed filter -> InvalidArgument (rpc error: code = InvalidArgument ...)
PASS: attributes-only CEL-subset filter applied at publish fan-out; malformed filter rejected
DeleteSubscription: sub-filtered-<uuid8> ok
DeleteSubscription: sub-all-<uuid8> ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`filtered-<uuid8>` / `sub-filtered-<uuid8>` / `sub-all-<uuid8>`) so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide;
> `<message-id>` values vary per run, and the arrival order within each received list is incidental
> (the assertion compares the sets, not the order). (See `../../../SHARED-CONVENTIONS.md` §1.) Exits 0
> on success, non-zero on any mismatch (e.g. a non-matching message reaching the filtered sub).

## What's Happening

The program creates one topic and two subscriptions: a **filtered** one with the CEL-subset filter
`attributes.region = "eu"`, and an **unfiltered** one. It publishes a five-message batch whose
`region` attribute is `eu`/`us`/`ap`, then drains both subscriptions via a short StreamingPull
`Receive` loop (acking each message). It asserts the filtered subscription receives **only** the
three `region=eu` messages while the unfiltered subscription receives all five, and that exactly two
non-matching messages were suppressed. Finally it exercises the negative path: creating a
subscription with a malformed filter is rejected at create-time with `INVALID_ARGUMENT`. Any mismatch
exits non-zero.

A `Publish` writes once to the topic log `pubsub.filtered-<uuid8>` (the authoritative, replayable,
cross-protocol source) — the log is **unfiltered**. The CEL-subset filter is attributes-only,
compiled once at create-time (immutable), and applied at **publish fan-out**: a non-matching message
is simply never enqueued for the filtered subscription's queue `pubsub.sub.sub-filtered-<uuid8>`
(spec S3.3 / S3.5).

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription (×2 + 1 rejected), Publish (×5), StreamingPull (Receive), Acknowledge, DeleteSubscription, DeleteTopic | `filtered-<uuid8>` / `sub-filtered-<uuid8>`, `sub-all-<uuid8>` | filter | CEL-subset, attributes-only, ≤ 256 chars, immutable after create; malformed → `INVALID_ARGUMENT`. Filter applied at publish fan-out; topic log unfiltered. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/delivery/message_filtering`](../../../python/delivery/message_filtering),
  [`../../../javascript/delivery/message-filtering`](../../../javascript/delivery/message-filtering),
  [`../../../java/delivery/message-filtering`](../../../java/delivery/message-filtering),
  [`../../../csharp/delivery/message-filtering`](../../../csharp/delivery/message-filtering),
  [`../../../ruby/delivery/message_filtering`](../../../ruby/delivery/message_filtering)
- Docs: [`../../../docs/guides/filtering.md`](../../../docs/guides/filtering.md)
- Related: [`../ordering-keys`](../ordering-keys), [`../dead-letter-topic`](../dead-letter-topic).

> **Gotcha — filtering is an attributes-only CEL subset.** Only `attributes:KEY`, `=`/`!=` against a
> string literal, `hasPrefix(...)`, and `AND`/`OR`/`NOT` are supported; it cannot inspect the message
> body. The expression is ≤ 256 chars and **immutable** after create, and a malformed filter is
> rejected at `CreateSubscription` with `INVALID_ARGUMENT`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
