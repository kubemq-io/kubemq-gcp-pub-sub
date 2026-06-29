# JavaScript — subscriptions: StreamingPull

The high-throughput consume path most client libraries use by default:
`CreateTopic → CreateSubscription → Publish(N) → subscription.on('message') (StreamingPull) → Acknowledge`.

## Prerequisites

- Node.js 18+ (developed against Node 20+) and the official GCP Pub/Sub client
  `@google-cloud/pubsub@5.3.1` (pinned in `examples/javascript/package.json`). Run with `tsx`.
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
cd examples/javascript && npx tsx subscriptions/streaming-pull/index.ts
```

## Expected Output

```
=== subscriptions: StreamingPull with flow control ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none — credentials cleared, insecure gRPC (emulator drop-in)

CreateTopic        -> projects/my-project/topics/orders-<uuid8>
CreateSubscription -> projects/my-project/subscriptions/sub-orders-<uuid8>
Publish            -> 8 messages to the topic log
StreamingPull      -> stream open (message listener); draining via handler
Received           -> 8 distinct messages over the StreamingPull stream (8 total deliveries, 0 redelivery/-ies — at-least-once)
  [OK] received all 8 distinct published messages
  [OK] every published message was delivered at least once

StreamingPull round-trip complete — all messages drained and acked.
```

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the uuid segments vary per
> run. Delivery is **at-least-once**, so the `total deliveries` / `redelivery` counts vary run to run
> (e.g. `10 total deliveries, 2 redeliveries`) — the example tracks distinct messages and passes
> regardless. (See [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md).)

## What's Happening

- A topic and subscription are created with uuid-suffixed ids, then 8 messages are published, each
  tagged with a `seq` attribute.
- `pubsub.subscription(id, { flowControl: { maxMessages: 8 } }).on('message', …)` opens a long-lived
  **bidirectional StreamingPull** stream; the connector pushes messages as they arrive and the
  handler acks each one. `flowControl.maxMessages` is the client-side ceiling on outstanding
  (un-acked) messages; the connector also enforces server-side safety nets
  (`MaxOutstandingMessages` 1000, `MaxInflightPerSubscription` 20000).
- The connector's model is **at-least-once**: the library may re-push a message whose ack is still
  in flight, and the 250 ms sweeper redelivers any lapsed lease — so the same `seq` can arrive more
  than once. The example therefore tracks **distinct** `seq`s and completes once all 8 have been seen
  at least once, acking every delivery. The connector also periodically closes each StreamingPull
  stream (`CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS`, default 1800 s) to force a transparent SDK
  reconnect — no messages are lost across it.
- A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
  cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`; the
  StreamingPull stream drains that subscription's queue. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish ×8, StreamingPull, Acknowledge | `orders-<uuid8>` → `pubsub.orders-<uuid8>` / `sub-orders-<uuid8>` → `pubsub.sub.sub-orders-<uuid8>` | none | `flowControl.maxMessages`=8; ack deadline 10 s; server ceilings `MaxOutstandingMessages`=1000, `MaxInflightPerSubscription`=20000; stream auto-closes every 1800 s for reconnect; at-least-once (redeliveries possible) |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/subscriptions/streaming-pull`](../../../go/subscriptions/streaming-pull),
  [`../../../python/subscriptions/streaming_pull`](../../../python/subscriptions/streaming_pull),
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
