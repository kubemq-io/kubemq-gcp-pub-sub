# Go — interop: Pub/Sub ⇄ Native Events Store

Publish to a topic with the **Google Pub/Sub SDK**, then consume the same payload **natively** from
the topic's Events Store log: `Subscribe(pubsub.{t}, startAt="new") → Publish → receive natively`.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)
- **interop-only:** this is the one variant that also needs the **native KubeMQ gRPC broker** at
  `localhost:50000` and the pinned native KubeMQ SDK `github.com/kubemq-io/kubemq-go/v2 v2.0.3`
  (also in `../../go.mod`). Set `KUBEMQ_GRPC_ADDRESS=localhost:50000`.

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
export KUBEMQ_GRPC_ADDRESS=localhost:50000     # native KubeMQ broker (interop half)
cd examples/go && go run ./interop/native-events-store
```

## Expected Output

```
=== interop: Pub/Sub publish -> native Events Store consume — topic "orders-<uuid8>" / channel "pubsub.orders-<uuid8>" ===
  pubsub : localhost:8085 (PUBSUB_EMULATOR_HOST; insecure, no Google auth)
  grpc   : localhost:50000 (native KubeMQ broker)

CreateTopic -> projects/my-project/topics/orders-<uuid8>
native SubscribeToEventsStore("pubsub.orders-<uuid8>", startAt='new') -> stream open
Publisher.Publish("orders-<uuid8>") -> message_id=<message-id>
  [OK] native consumer received the published message on "pubsub.orders-<uuid8>"
native receive("pubsub.orders-<uuid8>") -> body="from-gcp-pubsub"
    _pubsub_message_id = "<message-id>"
    _pubsub_publish_time = "<publish-time>"
    _pubsub_ordering_key = "shipments"
    region (attribute) = "emea"
  [OK] native side sees the Pub/Sub-published body
  [OK] all three reserved tags are present (_pubsub_message_id/publish_time/ordering_key)
  [OK] _pubsub_message_id matches the Pub/Sub-returned message id
  [OK] _pubsub_ordering_key carries the publish ordering key
  [OK] the message attribute rides along as an ordinary tag
  [OK] exactly one message was consumed (no cross-talk)
  [OK] no subscription errors ([])

PASS — Pub/Sub publish consumed natively on the Events Store topic log.
DeleteTopic("orders-<uuid8>") -> done
```

> Resource ids are uuid-suffixed (`orders-<uuid8>`) so concurrent runs use distinct
> `pubsub.{topic}` channels and do not cross-talk; the `<message-id>` and `<publish-time>` values
> vary per run. `DeleteTopic` runs in a deferred cleanup, so it prints after the `PASS` line. Exit 0
> on success, non-zero on any failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

- The native `kubemq-go/v2` Events Store subscriber attaches to channel `pubsub.orders-<uuid8>`
  **first**, with start policy `StartFromNewEvents()` (only events published *after* the subscription
  is established). `SubscribeToEventsStore` returns only once the gRPC subscribe stream is open, so
  its nil-error return is the **subscribe-before-publish** barrier — the single published message is
  guaranteed in-window and never raced or missed (spec S5.7).
- The official `cloud.google.com/go/pubsub` client then publishes one message (with an ordering key
  and a `region` attribute) to topic `orders-<uuid8>` over `PUBSUB_EMULATOR_HOST` — zero code
  changes; the SDK clears credentials and dials insecure gRPC. `EnableMessageOrdering` is turned on
  so the ordering key is accepted and surfaces as the reserved tag `_pubsub_ordering_key`.
- The native side receives exactly that message on `pubsub.orders-<uuid8>` and asserts the
  cross-protocol invariants: the body matches, the three reserved tags are present, the
  `_pubsub_message_id` equals the SDK-returned message id, the `_pubsub_ordering_key` carries the
  publish ordering key, and the `region` attribute rides along as an ordinary tag.
- A Pub/Sub `Publish` writes **once** to the topic's Events Store log `pubsub.{t}` — the
  authoritative, replayable, cross-protocol source — before fanning out per-subscription queue
  copies. This example reads that topic log directly and natively (no Pub/Sub subscription). The
  connector stamps the three reserved tags (`_pubsub_message_id`, `_pubsub_publish_time`,
  `_pubsub_ordering_key`) on the wire, visible to native consumers and stripped from `attributes`
  for Pub/Sub clients (spec S3.3/S3.4). Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| Pub/Sub `CreateTopic`, `Publish`; native gRPC Events Store `SubscribeToEventsStore` (`startAt="new"`) | `orders` / Events Store `pubsub.orders` | none (interop) | same payload read natively with the three reserved tags present; native gRPC Events Store subscribe via `github.com/kubemq-io/kubemq-go/v2@v2.0.3`; subscribe-before-publish + `startAt = "new"`; message ≤ 10 MiB; ≤ 100 attributes |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/interop/native_events_store`](../../../python/interop/native_events_store),
  [`../../../javascript/interop/native-events-store`](../../../javascript/interop/native-events-store),
  [`../../../java/interop/native-events-store`](../../../java/interop/native-events-store),
  [`../../../csharp/interop/native-events-store`](../../../csharp/interop/native-events-store),
  [`../../../ruby/interop/native_events_store`](../../../ruby/interop/native_events_store)
- Docs: [`../../../docs/concepts/cross-protocol-interop.md`](../../../docs/concepts/cross-protocol-interop.md)
  and [`../../../docs/reference/channel-mapping.md`](../../../docs/reference/channel-mapping.md)

> **Gotcha — the native side subscribes to a stream, not an offset.** A naive "publish then
> subscribe" races: the publish can land before the subscriber attaches and be missed. The canonical
> interop pattern opens the `StartFromNewEvents()` (`startAt = "new"`) subscription and confirms the
> stream is open *before* publishing. Examples needing pre-subscription history use
> `StartFromSequence`/`StartFromTime` instead.

> **Gotcha — project id parsed but ignored.** Resource ids are global across "projects"; topic
> `orders` is always Events Store channel `pubsub.orders`. Run concurrently only with isolated
> (uuid-suffixed) resource names.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
