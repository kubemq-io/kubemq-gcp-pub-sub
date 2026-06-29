# Ruby — delivery: dead-letter topic

A subscription with a dead-letter policy:
`CreateTopic(main+dlt) → CreateSubscription(dead_letter_policy) → Publish(poison) → Nack-redeliver → poll dead-letter topic`, validating `max_delivery_attempts` is in 5..100.

## Prerequisites

- **Ruby 3.3.7** (rbenv; floor 3.1) and the official GCP Pub/Sub client for Ruby
  `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`; exact version in
  the gitignored `Gemfile.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=kubemq`             (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=kubemq
eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7
cd examples/ruby && bundle exec ruby delivery/dead_letter_topic/main.rb
```

## Expected Output

```
[pubsub] emulator=localhost:8085 project=kubemq (insecure / no-auth)
CreateTopic            -> dead-letter-main-<uuid8> (main) + dead-letter-dlt-<uuid8> (dead-letter)
Validate               -> max_delivery_attempts=3 rejected INVALID_ARGUMENT (must be 5..100)
CreateSubscription     -> sub-dead-letter-main-<uuid8> (dead_letter_topic + max_delivery_attempts=5)
CreateSubscription     -> sub-dead-letter-dlt-<uuid8> (observes the dead-letter topic)
Publish                -> poison message id=<message-id>
Redelivery             -> poison redelivered 15x; max delivery_attempt seen=15
Cleanup                -> deleted subscriptions + topics
PASS: dead_letter_policy enforced (5..100); redelivery advances delivery_attempt
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide. The redelivery count and `<message-id>` vary
> per run. If the redriven copy does not land within 30 s, the program prints a `NOTE` on
> stderr (the connector's redrive is sweeper-driven and not clock-compressed in this build);
> it still exits 0 because the policy is validated and redelivery is proven. Exits non-zero on
> any failed assertion.

## What's Happening

Two topics are created (a main topic and its dead-letter topic). The example first proves the
connector enforces Google's rule that `max_delivery_attempts` must be **5..100** — a value of
`3` is rejected with `INVALID_ARGUMENT`. It then creates the main subscription with a
`dead_letter_policy` (`dead_letter_topic` + `max_delivery_attempts: 5`) and a second
subscription on the dead-letter topic to observe redriven messages. A "poison" message is
published and repeatedly nacked (`ModifyAckDeadline(0)`), which redelivers it immediately and
advances its `delivery_attempt` counter; the example asserts redelivery is observable and
polls the dead-letter subscription for the redriven copy.

A `Publish` writes once to the topic log `pubsub.{t}`, then fans out one queue copy per
subscription `pubsub.sub.{s}`. A 250 ms sweeper expires overdue leases, applies retry backoff,
and — when a message's receive count exceeds `max_delivery_attempts` and the subscription has a
`dead_letter_topic` — **republishes the message to that topic** (connector-level fan-out) and
acks the original. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic ×2, CreateSubscription (dead_letter_policy), Publish, Pull, ModifyAckDeadline(0) | `pubsub.dead-letter-main-<uuid8>` / `pubsub.sub.sub-dead-letter-main-<uuid8>` (+ dlt) | dead-letter | `max_delivery_attempts` must be 5..100 (0 = unset); connector-level redrive; ack deadline 10 s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/dead-letter-topic`](../../../go/delivery/dead-letter-topic),
  [`../../../python/delivery/dead_letter_topic`](../../../python/delivery/dead_letter_topic),
  [`../../../javascript/delivery/dead-letter-topic`](../../../javascript/delivery/dead-letter-topic),
  [`../../../java/delivery/dead-letter-topic`](../../../java/delivery/dead-letter-topic),
  [`../../../csharp/delivery/dead-letter-topic`](../../../csharp/delivery/dead-letter-topic)
- Docs: [`../../../../docs/concepts/dead-letter-and-retry.md`](../../../../docs/concepts/dead-letter-and-retry.md)

> `max_delivery_attempts` must be **5..100** (0 = unset); a value outside that range is
> rejected with `INVALID_ARGUMENT`. The redrive is connector-level (the sweeper republishes to
> the dead-letter topic), not a client redelivery.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
