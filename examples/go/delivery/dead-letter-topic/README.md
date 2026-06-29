# Go — delivery: Dead-Letter Topic

Connector-level dead-lettering against the KubeMQ Pub/Sub connector:
`CreateSubscription(DeadLetterPolicy) → Publish → repeated Pull-without-ack (lease expiry) → message moves to the DLQ → Pull + Ack`.

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
cd examples/go && go run ./delivery/dead-letter-topic
```

This variant intentionally waits for natural ack-deadline expiry cycles; with the 10 s minimum ack
deadline and `max_delivery_attempts = 5` it dead-letters in **≈50-65 s** (the loop is bounded at 90 s).

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/dead-letter-topic | emulator=localhost:8085 project=my-project
CreateTopic: work-<uuid8> (source) + work-dlq-<uuid8> (dead-letter)
CreateSubscription: sub-work-<uuid8> (dead_letter_topic=work-dlq-<uuid8>, max_delivery_attempts=5)
CreateSubscription: sub-dlq-<uuid8> (dead-letter consumer)
Publish: messageId=<message-id> body="poison" (to source topic)
Drive: pull the source WITHOUT acking (let leases expire) and poll the dead-letter sub:
  source delivery #1 (DeliveryAttempt=1) @ 0s — NOT acking, letting the lease expire
  source delivery #2 (DeliveryAttempt=2) @ 11s — NOT acking, letting the lease expire
  source delivery #3 (DeliveryAttempt=3) @ 22s — NOT acking, letting the lease expire
  source delivery #4 (DeliveryAttempt=4) @ 33s — NOT acking, letting the lease expire
  source delivery #5 (DeliveryAttempt=5) @ 44s — NOT acking, letting the lease expire
Dead-letter: received "poison" after 5 source deliveries (~55s)
PASS: poison dead-lettered to "work-dlq-<uuid8>" once receive count exceeded max_delivery_attempts=5
cleanup: subscriptions + topics deleted
```

> Resource ids are uuid-suffixed (`work-<uuid8>` / `work-dlq-<uuid8>` / `sub-work-<uuid8>` /
> `sub-dlq-<uuid8>`) so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do
> not collide; `<message-id>`, the exact delivery count, and the elapsed times vary per run. (See
> `../../../SHARED-CONVENTIONS.md` §1.) Exits 0 on success, non-zero if the message is not
> dead-lettered within the bound.

## What's Happening

The program creates a source topic, a dead-letter topic, a source subscription whose
`DeadLetterPolicy` points at the dead-letter topic with `MaxDeliveryAttempts = 5`, and a consumer
subscription on the dead-letter topic. A tight `RetryPolicy` (10-50 ms) makes each swept lease
re-arm and redeliver almost immediately. It publishes one `poison` message, then runs a drive loop
that repeatedly pulls the source **without acking** (using the low-level `apiv1.SubscriberClient`, so
no automatic lease extension) and polls the dead-letter subscription. Each natural ack-deadline
expiry raises the receive count; once it exceeds `max_delivery_attempts`, the connector republishes
to the dead-letter topic and acks the original off the source. The program then pulls and acks the
message off the dead-letter subscription and asserts the body is `poison`. Any failure exits
non-zero.

Dead-lettering is **connector-level fan-out driven by natural lease expiry**, not an explicit nack
(spec S3.4 / S3.5): the 250 ms lease sweeper raises the receive count on every expiry of the unacked
lease on `pubsub.sub.sub-work-<uuid8>`; once the count exceeds `max_delivery_attempts` the sweeper
republishes the topic-log message from `pubsub.work-<uuid8>` to `pubsub.work-dlq-<uuid8>` and acks
the original. (`max_delivery_attempts` must be 5..100; 0 = unset.)

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic (×2), CreateSubscription (×2), Publish, Pull (×N, no ack on source), Acknowledge (DLQ), DeleteSubscription (×2), DeleteTopic (×2) | `work-<uuid8>` + `work-dlq-<uuid8>` / `sub-work-<uuid8>`, `sub-dlq-<uuid8>` | dead-letter | `max_delivery_attempts` 5..100 (0 = unset); driven by ack-deadline expiry (250 ms sweeper), not by nack; ack deadline 10 s minimum. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/delivery/dead_letter_topic`](../../../python/delivery/dead_letter_topic),
  [`../../../javascript/delivery/dead-letter-topic`](../../../javascript/delivery/dead-letter-topic),
  [`../../../java/delivery/dead-letter-topic`](../../../java/delivery/dead-letter-topic),
  [`../../../csharp/delivery/dead-letter-topic`](../../../csharp/delivery/dead-letter-topic),
  [`../../../ruby/delivery/dead_letter_topic`](../../../ruby/delivery/dead_letter_topic)
- Docs: [`../../../docs/concepts/dead-letter-and-retry.md`](../../../docs/concepts/dead-letter-and-retry.md)
- Related: [`../message-filtering`](../message-filtering), [`../push-delivery`](../push-delivery).

> **Gotcha — dead-lettering is driven by lease expiry, and `max_delivery_attempts` must be 5..100.**
> Every expiry of the unacked lease raises the receive count via the 250 ms sweeper; an explicit
> `ModifyAckDeadline(0)` nack redelivers but does **not** raise the count toward the threshold. A
> value below 5 (or 0) is treated as unset — no dead-lettering.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
