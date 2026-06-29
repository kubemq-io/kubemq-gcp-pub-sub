# Go — subscriptions: StreamingPull

The high-throughput consume path most client libraries use by default:
`CreateTopic → CreateSubscription → Publish(N) → Receive (StreamingPull) → Acknowledge`.

## Prerequisites

- Go 1.25+ and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `examples/go/go.mod`).
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
cd examples/go && go run ./subscriptions/streaming-pull
```

## Expected Output

```
[kubemq-gcp-pub-sub] subscriptions/streaming-pull | emulator=localhost:8085 project=my-project
CreateTopic        -> orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription -> sub-orders-<uuid8> (channel pubsub.sub.sub-orders-<uuid8>)
Publish            -> 8 messages to the topic log
StreamingPull      -> stream open (Receive loop); acking each via callback
Received           -> 8 messages over the StreamingPull stream
  [OK] received all 8 published messages
  [OK] every published message was delivered exactly once
  [OK] no duplicate deliveries (each acked once)

PASS: StreamingPull round-trip complete — all messages drained and acked.
DeleteSubscription -> ok
DeleteTopic        -> ok
```

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the uuid segments vary per
> run. (See [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md).)

## What's Happening

- A topic and subscription are created with uuid-suffixed ids, then 8 messages are published, each
  tagged with a `seq` attribute; each `Publish` blocks on `result.Get(ctx)` for the connector-assigned id.
- `Subscription.Receive(...)` opens a long-lived **bidirectional StreamingPull** stream; the connector
  pushes messages as they arrive and the callback `m.Ack()`s each one.
- `ReceiveSettings.MaxOutstandingMessages = 8` is the client-side flow-control ceiling on outstanding
  (un-acked) messages (`NumGoroutines = 1` keeps a single deterministic stream). The connector also
  enforces server-side safety nets (`MaxOutstandingMessages` 1000, `MaxInflightPerSubscription` 20000).
- The example blocks until all 8 distinct messages arrive, then cancels the receive context and asserts
  each was delivered exactly once. The connector periodically closes each StreamingPull stream
  (`CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS`, default 1800 s) to force a transparent SDK reconnect —
  no messages are lost across it.
- A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
  cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`; the
  StreamingPull stream drains that subscription's queue. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish ×8, StreamingPull (Receive), Acknowledge | `orders-<uuid8>` → `pubsub.orders-<uuid8>` / `sub-orders-<uuid8>` → `pubsub.sub.sub-orders-<uuid8>` | none | `ReceiveSettings.MaxOutstandingMessages=8`; ack deadline 10 s; server ceilings `MaxOutstandingMessages`=1000, `MaxInflightPerSubscription`=20000; stream auto-closes every 1800 s for reconnect |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/subscriptions/streaming_pull`](../../../python/subscriptions/streaming_pull),
  [`../../../javascript/subscriptions/streaming-pull`](../../../javascript/subscriptions/streaming-pull),
  [`../../../java/subscriptions/streaming-pull`](../../../java/subscriptions/streaming-pull),
  [`../../../csharp/subscriptions/streaming-pull`](../../../csharp/subscriptions/streaming-pull),
  [`../../../ruby/subscriptions/streaming_pull`](../../../ruby/subscriptions/streaming_pull)
- Docs: [`../../../docs/concepts/subscriptions-and-pull.md`](../../../docs/concepts/subscriptions-and-pull.md),
  [`../../../docs/guides/subscribing.md`](../../../docs/guides/subscribing.md)

> **Gotcha — `ack_id`s are node-local.** An `ack_id` minted on one cluster node is invalid on
> another; the lease and receive-count state live on the issuing node. In a cluster, pin a
> subscription's StreamingPull traffic to one node with a sticky load balancer, or accept
> at-least-once across nodes.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
