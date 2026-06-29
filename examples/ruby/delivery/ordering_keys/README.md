# Ruby — delivery: ordering keys

Per-key in-order delivery against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription(enable_message_ordering) → Publish(ordering_key) → Pull → Acknowledge`, asserting each key arrives in publish order.

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
cd examples/ruby && bundle exec ruby delivery/ordering_keys/main.rb
```

## Expected Output

```
[pubsub] emulator=localhost:8085 project=kubemq (insecure / no-auth)
CreateTopic            -> ordering-keys-<uuid8>
CreateSubscription     -> sub-ordering-keys-<uuid8> (enable_message_ordering=true)
Publish                -> 5 on "tenant-A" + 5 on "tenant-B" (interleaved)
Pull+Ack (tenant-A)   -> ["A-step-0", "A-step-1", "A-step-2", "A-step-3", "A-step-4"]
Pull+Ack (tenant-B)   -> ["B-step-0", "B-step-1", "B-step-2", "B-step-3", "B-step-4"]
Assert                 -> each key delivered in publish order (0 ordering violations)
Cleanup                -> deleted subscription + topic
PASS: per-key in-order delivery; keys independent; zero ordering violations
```

> Resource ids are uuid-suffixed (`ordering-keys-<uuid8>` / `sub-ordering-keys-<uuid8>`), so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide.
> Exits non-zero on any failed assertion.

## What's Happening

The subscription is created with `enable_message_ordering: true`, and the publisher calls
`enable_message_ordering!` before publishing. Ten messages are published — five on
`tenant-A` and five on `tenant-B`, interleaved — each tagged with an `ordering_key`. The
example pulls and acks them, bucketing by `Message#ordering_key`, then asserts each key's
messages arrived in publish order (`A-step-0..4`, `B-step-0..4`) with zero violations.

A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`. With
ordering enabled the connector delivers messages sharing an `ordering_key` in publish order
with **at most one in flight per key** — the head of a key blocks until it is acked, and
redelivery is in order; a round-robin cursor spreads delivery across contended keys. The
ordering key rides the wire as the reserved tag `_pubsub_ordering_key`. Any failed assertion
exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, Pull, Acknowledge | `pubsub.ordering-keys-<uuid8>` / `pubsub.sub.sub-ordering-keys-<uuid8>` | ordering (`enable_message_ordering`) | ordering key ≤ 1024 B; one-in-flight-per-key; ack deadline 30 s (range 10..600 s) |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/ordering-keys`](../../../go/delivery/ordering-keys),
  [`../../../python/delivery/ordering_keys`](../../../python/delivery/ordering_keys),
  [`../../../javascript/delivery/ordering-keys`](../../../javascript/delivery/ordering-keys),
  [`../../../java/delivery/ordering-keys`](../../../java/delivery/ordering-keys),
  [`../../../csharp/delivery/ordering-keys`](../../../csharp/delivery/ordering-keys)
- Docs: [`../../../../docs/concepts/ordering-keys.md`](../../../../docs/concepts/ordering-keys.md)

> Ordering is enforced per key, not across keys: `tenant-A` and `tenant-B` are independent.
> A keyless message is delivered unordered.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
