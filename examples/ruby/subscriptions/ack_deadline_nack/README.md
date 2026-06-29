# Ruby — subscriptions: ack-deadline + nack redelivery

The ack-deadline lease lifecycle against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish → Pull → ModifyAckDeadline(extend) → ModifyAckDeadline(0)=nack → Pull(redelivered) → Acknowledge`.

## Prerequisites

- Ruby **3.3.7** (rbenv; floor is 3.1) and the official GCP Pub/Sub client
  `google-cloud-pubsub` (pinned `~> 3.3` in `examples/ruby/Gemfile`).
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
eval "$(rbenv init - zsh)"   # rbenv 3.3.7
cd examples/ruby
bundle install
bundle exec ruby subscriptions/ack_deadline_nack/main.rb
```

## Expected Output

```
[PUBSUB] emulator_host=localhost:8085 project=my-project (no-auth / insecure gRPC)
CreateTopic         -> redeliver-<uuid8>
CreateSubscription  -> sub-redeliver-<uuid8> (ack_deadline=10s)
Publish             -> message_id=<message-id>
Pull #1             -> data="process-me" receive_count=1
ModifyAckDeadline   -> 20s (lease extended; not yet acked)
Nack (deadline=0)   -> released for redelivery
Pull #2             -> data="process-me" receive_count=2 (redelivered)
Acknowledge         -> ok (removed from sub-redeliver-<uuid8>)
PASS: nack redelivered the same message_id with receive_count 1 -> 2; ack removed it
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and never collide; the `<uuid8>` suffix and
> `<message-id>` vary per run. (See `../../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

A subscription is created with a **10-second ack deadline**. After one `Publish`, the program
pulls the message (`receive_count=1`) and then calls `ModifyAckDeadline(20)` to **extend** the
lease — the connector resets the lease to expire 20 s out, the way a slow consumer buys more
processing time without losing ownership. Instead of acking, the program then calls
`ModifyAckDeadline(0)`, which is an **immediate NACK**: the connector's 250 ms sweeper expires the
released lease and re-enqueues the message. The next pull returns the **same** message
(`message_id` unchanged) under a **fresh** `ack_id` whose embedded receive count has incremented
`1 → 2`. A real `Acknowledge` then removes it. Any failed assertion exits non-zero.

A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`; the
ack-deadline lease, nack, and redelivery all operate on that per-subscription queue. The `ack_id`
is an opaque `base64(JSON{subscription, channel, node id, txn id, seq, receive count, lease id,
deadline})`; this example decodes only the receive count (`r`) for display.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `ModifyAckDeadline` (extend + 0), `Acknowledge`, `DeleteSubscription`, `DeleteTopic` | `redeliver-<uuid8>` / `sub-redeliver-<uuid8>` | none | ack deadline `0` (nack) or `10..600` s; 250 ms sweeper expires overdue leases and redelivers; `ModifyAckDeadline(0)` = immediate nack |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/subscriptions/ack-deadline-nack`](../../../go/subscriptions/ack-deadline-nack),
  [`../../../python/subscriptions/ack_deadline_nack`](../../../python/subscriptions/ack_deadline_nack),
  [`../../../java/subscriptions/ack-deadline-nack`](../../../java/subscriptions/ack-deadline-nack),
  [`../../../javascript/subscriptions/ack-deadline-nack`](../../../javascript/subscriptions/ack-deadline-nack),
  [`../../../csharp/subscriptions/ack-deadline-nack`](../../../csharp/subscriptions/ack-deadline-nack)
- Docs: [`../../../../docs/guides/subscribing.md`](../../../../docs/guides/subscribing.md)
  (and the concept doc [`../../../../docs/concepts/subscriptions-and-pull.md`](../../../../docs/concepts/subscriptions-and-pull.md))

> **Gotcha — ack/lease tracking is node-local.** The ack-deadline lease (and the `ack_id` that
> encodes its node id) is owned by the node that delivered the message; in a cluster, pin a
> subscription's pull traffic to one node (sticky LB) so the nack/redelivery and the eventual
> acknowledge land on the same node.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
