# C# — subscriptions: Ack deadline + nack

Lease semantics on a unary Pull subscription:
`Publish → Pull (lease) → ModifyAckDeadline(>0) extend → ModifyAckDeadline(0) nack → re-Pull (redelivered) → Acknowledge`.

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client `Google.Cloud.PubSub.V1` **3.36.0**
  (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set (C# requires explicit
  `EmulatorDetection.EmulatorOnly` on the client builder — see the gotcha below):
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/csharp && dotnet run --project subscriptions/ack-deadline-nack/AckDeadlineNack.csproj
```

## Expected Output

```
[*] Created topic 'orders-<uuid8>' → channel pubsub.orders-<uuid8>
[*] Created subscription 'sub-orders-<uuid8>' → channel pubsub.sub.sub-orders-<uuid8> (ackDeadline=10s)
[x] Published 'retry-me'
[v] First pull: 'retry-me' messageId=<message-id> (leased, not acked)
[*] ModifyAckDeadline(30) → extended lease; message stays invisible
[v] Immediate pull during extended lease → 0 messages (still leased)
[*] ModifyAckDeadline(0) → immediate nack (redeliver, bypassing backoff)
[v] Second pull: 'retry-me' messageId=<message-id> (redelivered)
[*] Acknowledged the redelivered message
[ok] Ack-deadline extend + 0-nack redelivery verified
[*] Cleaned up subscription 'sub-orders-<uuid8>' and topic 'orders-<uuid8>'
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.orders-<uuid8>` / `pubsub.sub.sub-orders-<uuid8>` channels and do not collide; the
> `<uuid8>` suffix and the `<message-id>` vary per run (the redelivered copy carries the **same**
> `<message-id>` as the first delivery — that is the assertion). Exits `0` on success, non-zero on
> any failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program publishes one message (`retry-me`) to a uuid-suffixed topic/subscription created with
`ackDeadlineSeconds=10` (the connector minimum; valid values are `0` or `10..600`). It then:

1. **Pulls** the message — the broker places it under a subscription-owned **ack-deadline lease**
   and returns an opaque `ack_id`; the program deliberately does **not** acknowledge it.
2. **Extends** the lease with `ModifyAckDeadline(30)`. While leased the message is invisible, so an
   immediate-return probe pull returns `0` messages.
3. **Nacks** with `ModifyAckDeadline(0)` — the connector redelivers the message at once, bypassing
   the retry backoff (an explicit `0` is a nack, not an extension).
4. **Re-pulls** and gets the *same* message back (verified by the server-assigned `messageId`), then
   **Acknowledges** it. `DeliveryAttempt` stays `0` here because it is only populated when the
   subscription has a `dead_letter_topic` policy (Google's contract — see `delivery/dead-letter-topic`).

A Publish writes once to the topic log `pubsub.{t}` then fans out one queue copy per subscription
`pubsub.sub.{s}`. A 250 ms sweeper expires overdue leases, applies retry backoff, and redelivers
(or dead-letters once the receive count exceeds the policy). Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `ModifyAckDeadline` (extend + 0-nack), `Acknowledge` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | ack deadline `0` (nack/redeliver immediately) or `10..600`s (extend); 250 ms sweeper expires overdue leases; `DeliveryAttempt` only set with a `dead_letter_topic` (`max_delivery_attempts` 5..100) |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/subscriptions/ack-deadline-nack`](../../../go/subscriptions/ack-deadline-nack),
  [`../../../python/subscriptions/ack_deadline_nack`](../../../python/subscriptions/ack_deadline_nack),
  [`../../../java/subscriptions/ack-deadline-nack`](../../../java/subscriptions/ack-deadline-nack),
  [`../../../javascript/subscriptions/ack-deadline-nack`](../../../javascript/subscriptions/ack-deadline-nack),
  [`../../../ruby/subscriptions/ack_deadline_nack`](../../../ruby/subscriptions/ack_deadline_nack)
- Docs: [`../../../../docs/concepts/subscriptions-and-pull.md`](../../../../docs/concepts/subscriptions-and-pull.md)

> The project id is parsed and validated but **ignored** (single-tenant, like the emulator):
> resource ids are global across "projects", which is exactly why this example uuid-suffixes the
> topic/subscription names so parallel runs occupy private `pubsub.{t}` / `pubsub.sub.{s}` channels.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
