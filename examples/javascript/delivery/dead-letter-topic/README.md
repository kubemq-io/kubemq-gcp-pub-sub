# JavaScript — delivery: Dead-letter topic

Redelivery exhaustion redrives to a dead-letter topic against the KubeMQ Pub/Sub connector:
`CreateTopic(work)+CreateTopic(dlq) → CreateSubscription(deadLetterPolicy) → Publish → Pull-and-hold (let leases expire) → DLQ republish + original ack`.

## Prerequisites

- **Node.js 18+** (developed against Node 20+) and the official GCP Pub/Sub client
  `@google-cloud/pubsub` **5.3.1** (pinned in `examples/javascript/package.json`); run with `tsx`.
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
cd examples/javascript && npx tsx delivery/dead-letter-topic/index.ts
```

> This example takes **~60s**: dead-lettering is driven by repeated ack-deadline **expiry** (the
> connector ack-deadline floor is 10s), so it deliberately holds each lease unacked for one full
> ~10s cycle, six times, until the receive count exceeds `max_delivery_attempts`.

## Expected Output

```
=== Pub/Sub dead-letter topic — work 'work-<uuid8>' -> dlq 'work-dlq-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  max_delivery_attempts : 5
  note     : dead-lettering is driven by ack-deadline EXPIRY, not nack —
             each expiry waits ~10s, so this runs ~60s.

CreateTopic(work)      -> projects/my-project/topics/work-<uuid8>
CreateTopic(dlq)       -> projects/my-project/topics/work-dlq-<uuid8>
CreateSubscription(dlq)-> sub-dlq-<uuid8>
CreateSubscription(work)-> sub-work-<uuid8>  (dead_letter_topic=work-dlq-<uuid8>, max_delivery_attempts=5)

Publish -> 'poison-pill'

Driving redelivery (Pull then hold unacked; each expiry raises the receive count):
  pull 1: leased (delivery_attempt=1) -> hold unacked, let the lease expire
  pull 2: leased (delivery_attempt=2) -> hold unacked, let the lease expire
  pull 3: leased (delivery_attempt=3) -> hold unacked, let the lease expire
  pull 4: leased (delivery_attempt=4) -> hold unacked, let the lease expire
  pull 5: leased (delivery_attempt=5) -> hold unacked, let the lease expire

  [OK] poison message landed on the dead-letter topic
  DLQ message body='poison-pill'
  [OK] DLQ holds the original body
  [OK] the source subscription delivered the message before dead-lettering
  [OK] work subscription drained (original acked off the source)

PASS — dead-letter topic: receive count > max_delivery_attempts redrives to the DLQ.
cleanup: DeleteSubscription(sub-work-<uuid8>) ok
cleanup: DeleteSubscription(sub-dlq-<uuid8>) ok
cleanup: DeleteTopic(work) ok
cleanup: DeleteTopic(dlq) ok
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.work-<uuid8>` / `pubsub.sub.sub-work-<uuid8>` channels and do not collide; the `<uuid8>`
> suffix varies per run. The number of `pull N` lines may vary slightly depending on sweep timing,
> but the message always dead-letters once the receive count **exceeds** `max_delivery_attempts`
> (=5). Exits `0` on success, non-zero on any failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a `work` topic and a `work-dlq` topic, a subscription on the DLQ topic (so the
redriven message can be observed), and a `work` subscription whose `deadLetterPolicy` names the DLQ
topic with `maxDeliveryAttempts: 5` and a tight `retryPolicy` (10–50 ms backoff). It publishes one
poison message, then **drives redelivery via natural ack-deadline expiry**: it Pulls the message
(taking a lease) but deliberately never acks it, sleeps one full ack-deadline cycle so the connector's
250 ms lease sweeper expires the lease and raises the broker receive count, and repeats. After each
cycle it probes the DLQ subscription.

This mirrors Google's exact semantics: **only an ack-deadline expiry raises the receive count — an
explicit nack does not.** Once the count *exceeds* `max_delivery_attempts`, the sweeper republishes
the message to the dead-letter topic (connector-level fan-out via `pubsub.{dlt}`) and acks the
original off the source subscription. The program asserts the poison body landed on the DLQ unchanged
and that the source subscription is now drained.

A Publish writes once to the topic log `pubsub.{t}` then fans out one queue copy per subscription
`pubsub.sub.{s}`; the dead-letter decision lives entirely in the lease sweeper (`deadletter.go`).
`max_delivery_attempts` must be **5..100** (0 = unset). Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic` (work + dlq), `CreateSubscription` (`deadLetterPolicy`, `retryPolicy`), `Publish`, `Pull` (hold), `Acknowledge` (on DLQ) | work/dlq topics `pubsub.{t}` / subs `pubsub.sub.{s}` | dead-letter | `max_delivery_attempts` 5..100; ack deadline floor 10s; 250 ms sweeper raises receive count on **expiry only** (nack does not); on exceed → republish to DLQ + ack original |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/dead-letter-topic`](../../../go/delivery/dead-letter-topic),
  [`../../../python/delivery/dead_letter_topic`](../../../python/delivery/dead_letter_topic),
  [`../../../java/delivery/dead-letter-topic`](../../../java/delivery/dead-letter-topic),
  [`../../../csharp/delivery/dead-letter-topic`](../../../csharp/delivery/dead-letter-topic),
  [`../../../ruby/delivery/dead_letter_topic`](../../../ruby/delivery/dead_letter_topic)
- Docs: [`../../../../docs/concepts/dead-letter-and-retry.md`](../../../../docs/concepts/dead-letter-and-retry.md)

> **Gotcha — `max_delivery_attempts` must be 5..100.** A value of `0` means "unset" (no
> dead-lettering); anything outside `5..100` is rejected. The receive count is raised by ack-deadline
> **expiry**, not by an explicit nack — so a poison message is dead-lettered only after enough leases
> lapse, which is why this example holds leases unacked rather than nacking.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
