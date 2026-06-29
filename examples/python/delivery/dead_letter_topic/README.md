# Python — delivery: Dead-Letter Topic

Poison-message safety valve: `CreateTopic(work)+CreateTopic(dlq) → CreateSubscription(dead_letter_policy, max_delivery_attempts=5) → Publish → Pull-and-hold (never ack) → DLQ receipt`, asserting that once the receive count exceeds `max_delivery_attempts` the connector redrives the message to the dead-letter topic.

## Prerequisites

- Python 3.10+ and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `examples/python/pyproject.toml` + `uv.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)
- This variant runs for **~60s** — dead-lettering is driven by ack-deadline **expiry** (the connector
  ack-deadline floor is 10s), so five expiry cycles take roughly a minute.

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/python && uv run python -m delivery.dead_letter_topic
# or, equivalently: uv run python delivery/dead_letter_topic/main.py
```

## Expected Output

```
=== Pub/Sub dead-letter topic — work 'work-<uuid8>' -> dlq 'dead-letters-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  max_delivery_attempts : 5
  note     : dead-lettering is driven by ack-deadline EXPIRY, not nack —
             each expiry waits ~10s, so this runs ~60s.

CreateTopic(work)      -> projects/my-project/topics/work-<uuid8>
CreateTopic(dlq)       -> projects/my-project/topics/dead-letters-<uuid8>
CreateSubscription(dlq)-> projects/my-project/subscriptions/sub-dlq-<uuid8>
CreateSubscription(work)-> projects/my-project/subscriptions/sub-work-<uuid8>  (dead_letter_topic=dead-letters-<uuid8>, max_delivery_attempts=5)

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

PASS — dead-letter topic: receive count over max_delivery_attempts redrives to DLQ.
cleanup: DeleteSubscription(work) ok
cleanup: DeleteSubscription(dlq) ok
cleanup: DeleteTopic(work) ok
cleanup: DeleteTopic(dlq) ok
```

> Resource ids are uuid8-suffixed (`work-<uuid8>` / `dead-letters-<uuid8>` / `sub-work-<uuid8>` /
> `sub-dlq-<uuid8>`) so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never
> collide. The exact `delivery_attempt` count at which the move happens (6 here, i.e. the first
> delivery whose count *exceeds* 5) can vary slightly with timing; the invariant is that the message
> dead-letters and the source subscription drains.

## What's Happening

- Two topics are created — `work` and `dead-letters` — plus a subscription on each. The `work`
  subscription carries a `DeadLetterPolicy(dead_letter_topic, max_delivery_attempts=5)` and a tight
  `RetryPolicy` (10ms..50ms) so a swept lease re-arms and redelivers quickly.
- A `poison-pill` is published, then **pulled and held unacked** in a loop. Each ack-deadline
  **expiry** raises the broker receive count — an explicit nack would *not* (Google semantics: only
  expiry raises the count, and the dead-letter decision lives only in the lease sweeper).
- When the receive count **exceeds** `max_delivery_attempts`, the connector republishes the message
  to the dead-letter topic and **acks the original** off the source subscription, which then drains.
- Connector behavior (spec S3.5): dead-lettering is connector-level fan-out — the message is
  republished to the dead-letter topic's log `pubsub.{dlt}` (then to its subscriptions) via the same
  publish path a normal `Publish` uses; `max_delivery_attempts` must be **5..100**. Any failed
  assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic ×2, CreateSubscription ×2 (DeadLetterPolicy + RetryPolicy), Publish, Pull, ModifyAckDeadline/expiry, Acknowledge | work `work-<uuid8>` / sub `sub-work-<uuid8>` → DLQ topic `dead-letters-<uuid8>` / sub `sub-dlq-<uuid8>` | dead-letter | `max_delivery_attempts` must be 5..100; ack-deadline floor 10s; redelivery via the 250ms lease sweeper |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/dead-letter-topic`](../../../go/delivery/dead-letter-topic),
  [`../../../javascript/delivery/dead-letter-topic`](../../../javascript/delivery/dead-letter-topic),
  [`../../../java/delivery/dead-letter-topic`](../../../java/delivery/dead-letter-topic),
  [`../../../csharp/delivery/dead-letter-topic`](../../../csharp/delivery/dead-letter-topic),
  [`../../../ruby/delivery/dead_letter_topic`](../../../ruby/delivery/dead_letter_topic)
- Docs: [`../../../../docs/concepts/dead-letter-and-retry.md`](../../../../docs/concepts/dead-letter-and-retry.md)

> **Gotcha — dead-lettering is driven by expiry, not nack, and `max_delivery_attempts` must be
> 5..100.** Only an ack-deadline expiry raises the receive count toward the threshold; an explicit
> nack (`ModifyAckDeadline(0)`) redelivers immediately without advancing the count. A value of `0`
> means *unset* (redeliver forever); any non-zero value outside 5..100 is rejected with
> `INVALID_ARGUMENT`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
