# C# — delivery: Dead-Letter Topic

A source subscription with a dead-letter policy moves a poison message to a separate topic once its
receive count exceeds `max_delivery_attempts`:
`CreateTopic ×2 → CreateSubscription(dead_letter_topic, max_delivery_attempts=5) → Publish(poison) → Pull-without-ack (let leases expire) → dead-letter subscription Receive`.

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client **`Google.Cloud.PubSub.V1` 3.36.0**
  (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set. The .NET client additionally needs
  explicit `EmulatorDetection.EmulatorOnly` on the builder (idiom trap):
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/csharp
dotnet run --project delivery/dead-letter-topic/DeadLetterTopic.csproj
```

> **Runs for ~55-65 s.** Dead-lettering is driven by natural ack-deadline expiry; with the minimum
> 10 s ack deadline and `max_delivery_attempts=5` the message dead-letters after ~5 expiry cycles.

## Expected Output

```
[*] Created source topic 'orders-<uuid8>' + dead-letter topic 'dead-letters-<uuid8>'
[*] Created source subscription 'sub-worker-<uuid8>' (dead_letter_topic='dead-letters-<uuid8>', max_delivery_attempts=5)
[*] Created dead-letter subscription 'sub-dlq-<uuid8>'
[x] Published 'poison' to the source topic
[v] source delivery #1 (DeliveryAttempt=1) @ 0s — NOT acking, letting the lease expire
[v] source delivery #2 (DeliveryAttempt=2) @ 11s — NOT acking, letting the lease expire
[v] source delivery #3 (DeliveryAttempt=3) @ 22s — NOT acking, letting the lease expire
[v] source delivery #4 (DeliveryAttempt=4) @ 33s — NOT acking, letting the lease expire
[v] source delivery #5 (DeliveryAttempt=5) @ 44s — NOT acking, letting the lease expire
[v] dead-letter subscription received 'poison' after 5 source deliveries (~56s)
[ok] poison message dead-lettered to 'dead-letters-<uuid8>' once receive count exceeded max_delivery_attempts=5
[*] Cleaned up subscriptions + topics 'orders-<uuid8>' / 'dead-letters-<uuid8>'
```

The exact `@ Ns` timestamps and the delivery count vary slightly per run (the lease sweeper runs on
a 250 ms cadence), but the message always dead-letters after the receive count exceeds
`max_delivery_attempts=5`. Exit code is `0` on success and non-zero if the message is not
dead-lettered within the 90 s budget.

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `dead-letters-<uuid8>` /
> `sub-worker-<uuid8>` / `sub-dlq-<uuid8>`), so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide; the `<uuid8>` suffix varies per run.
> (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a source topic, a dead-letter topic, a source subscription with a
`DeadLetterPolicy { DeadLetterTopic, MaxDeliveryAttempts = 5 }` (plus a tight `RetryPolicy` so
re-armed leases redeliver immediately), and a subscription on the dead-letter topic. It publishes a
single `poison` message and then **pulls the source subscription without ever acking it**.

Dead-lettering on this connector is driven by **natural ack-deadline expiry**, not by an explicit
nack: every time the unacked lease expires, the connector's 250 ms lease sweeper raises the
message's receive count and redelivers it. Once the receive count **exceeds**
`max_delivery_attempts`, the sweeper **republishes the message to the dead-letter topic and acks the
original** off the source subscription. An explicit `ModifyAckDeadline(0)` nack would redeliver but
would **not** raise the count toward the dead-letter threshold (Google semantics), so the example
relies on natural expiry — that is why it lets the lease lapse on each cycle. The program then
consumes the message from the dead-letter subscription and asserts its body is `poison`.

This is connector-level fan-out to the DLQ: the source topic log `pubsub.orders-<uuid8>` fans a
queue copy to `pubsub.sub.sub-worker-<uuid8>`; on exhaustion the sweeper publishes to the
dead-letter topic log `pubsub.dead-letters-<uuid8>`, which fans out to its own subscription
`pubsub.sub.sub-dlq-<uuid8>`.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic (×2) → CreateSubscription (×2, one with DeadLetterPolicy) → Publish → Pull-without-ack (drive natural expiry) → DLT Pull + Acknowledge | source `orders-<uuid8>` → `sub-worker-<uuid8>`; DLT `dead-letters-<uuid8>` → `sub-dlq-<uuid8>` | dead-letter (`dead_letter_topic` + `max_delivery_attempts`) | `max_delivery_attempts` must be 5..100 (0 = unset); ack deadline 10 s (10..600); receive count must **exceed** the budget to dead-letter |

## Related Examples

- Same variant in the other 5 languages:
  [go](../../../go/delivery/dead-letter-topic/),
  [python](../../../python/delivery/dead_letter_topic/),
  [java](../../../java/delivery/dead-letter-topic/),
  [javascript](../../../javascript/delivery/dead-letter-topic/),
  [ruby](../../../ruby/delivery/dead_letter_topic/)
- Docs: [`../../../../docs/concepts/dead-letter-and-retry.md`](../../../../docs/concepts/dead-letter-and-retry.md)

> **Gotcha — `max_delivery_attempts` must be 5..100.** A value of `0` means *unset* (the message
> redelivers indefinitely). Any non-zero value must be in `5..100`; outside that range
> `CreateSubscription` is rejected with `INVALID_ARGUMENT`. Dead-lettering is triggered by the
> receive count rising via natural ack-deadline expiry — an explicit nack alone does not move the
> message to the DLQ.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
