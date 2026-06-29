# Go — subscriptions: Ack Deadline & Nack

The subscription lease lifecycle, driven with unary `Pull` + `ModifyAckDeadline`:
`Pull → ModifyAckDeadline(>0) extend → ModifyAckDeadline(0) nack → redeliver → Acknowledge`.

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
cd examples/go && go run ./subscriptions/ack-deadline-nack
```

## Expected Output

```
[kubemq-gcp-pub-sub] subscriptions/ack-deadline-nack | emulator=localhost:8085 project=my-project
CreateTopic        -> orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription -> sub-orders-<uuid8> (ack deadline 10 s, channel pubsub.sub.sub-orders-<uuid8>)
Publish            -> 1 message to the topic log
  [OK] first Pull returns the message
Pull #1            -> body="lease-me" ack_id=<ack-id>...
ModifyAckDeadline  -> +30s (lease extended, still owned)
ModifyAckDeadline  -> 0 (nack: redeliver now)
  [OK] the nacked message is redelivered on a later Pull
  [OK] redelivered body matches the original
Pull #2            -> body="lease-me" ack_id=<ack-id>...
Acknowledge        -> message acked
  [OK] subscription is empty after the ack (no further redelivery)

PASS: ack-deadline / nack lifecycle complete — nack redelivered, ack drained.
DeleteSubscription -> ok
DeleteTopic        -> ok
```

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<ack-id>` value varies
> per run. (See [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md).)

## What's Happening

- One message is published and pulled with a unary `Pull`; it is now **leased** under the
  subscription's ack deadline (10 s) and carries an opaque `ack_id`. The high-level
  `pubsub.NewClient` handles create/publish/cleanup; the unary lease RPCs (`Pull` /
  `ModifyAckDeadline` / `Acknowledge`) are not on the high-level `Subscription` type, so they go
  through the official low-level `cloud.google.com/go/pubsub/apiv1` `SubscriberClient` (given the
  same insecure/no-auth emulator options, since `PUBSUB_EMULATOR_HOST` only auto-wires the
  high-level constructor).
- `ModifyAckDeadline(ack_id, 30)` **extends** the lease — the deadline is pushed out (clamped to the
  valid 10..600 s window) so a slow consumer keeps ownership instead of letting the sweeper
  redeliver.
- `ModifyAckDeadline(ack_id, 0)` is an immediate **nack**: the message redelivers right away rather
  than waiting out the lease. The connector's 250 ms sweeper also redelivers any lease that simply
  expires un-acked.
- A second `Pull` returns the same body, proving the nack redelivered it; `Acknowledge` then drains
  it for good, and a final `Pull` confirms the subscription is empty.
- A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
  cross-protocol source), then fans out one queue copy into the per-subscription queue
  `pubsub.sub.{s}`; ack/nack act on the broker sequence held under the subscription-owned lease. Any
  failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, Pull ×3, ModifyAckDeadline (30, then 0), Acknowledge | `orders-<uuid8>` → `pubsub.orders-<uuid8>` / `sub-orders-<uuid8>` → `pubsub.sub.sub-orders-<uuid8>` | none | ack deadline 0 (nack) or 10..600 s; default 10 s; 250 ms sweeper redelivers expired/nacked leases |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/subscriptions/ack_deadline_nack`](../../../python/subscriptions/ack_deadline_nack),
  [`../../../javascript/subscriptions/ack-deadline-nack`](../../../javascript/subscriptions/ack-deadline-nack),
  [`../../../java/subscriptions/ack-deadline-nack`](../../../java/subscriptions/ack-deadline-nack),
  [`../../../csharp/subscriptions/ack-deadline-nack`](../../../csharp/subscriptions/ack-deadline-nack),
  [`../../../ruby/subscriptions/ack_deadline_nack`](../../../ruby/subscriptions/ack_deadline_nack)
- Docs: [`../../../docs/concepts/subscriptions-and-pull.md`](../../../docs/concepts/subscriptions-and-pull.md),
  [`../../../docs/guides/subscribing.md`](../../../docs/guides/subscribing.md)

> **Gotcha — `ack_id`s are node-local.** An `ack_id` minted on one cluster node is invalid on
> another; the lease, receive-count, and exactly-once state all live on the issuing node. In a
> cluster, pin a subscription's StreamingPull traffic to one node with a sticky load balancer, or
> accept at-least-once across nodes.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
