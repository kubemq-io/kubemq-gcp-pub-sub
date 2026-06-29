# Ruby — delivery: exactly-once

Exactly-once delivery acknowledgements:
`CreateTopic → CreateSubscription(enable_exactly_once_delivery) → Publish → Pull → Acknowledge(SUCCESS) → re-ack stale id (FAILED_PRECONDITION)`.

## Prerequisites

- **Ruby 3.3.7** (rbenv; floor 3.1) and the official GCP Pub/Sub client for Ruby
  `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`; exact version in
  the gitignored `Gemfile.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**). Run against a **single** connector
  instance — exactly-once is node-local (see the gotcha below).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=kubemq`             (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=kubemq
eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7
cd examples/ruby && bundle exec ruby delivery/exactly_once/main.rb
```

## Expected Output

```
[pubsub] emulator=localhost:8085 project=kubemq (insecure / no-auth)
CreateTopic            -> exactly-once-<uuid8>
CreateSubscription     -> sub-exactly-once-<uuid8> (enable_exactly_once_delivery=true)
Publish                -> id=<message-id>
Pull                   -> data="exactly-once-payload" (1 message)
Acknowledge            -> confirmation status=SUCCESS
Re-ack (stale id)      -> rejected FAILED_PRECONDITION (ErrorInfo: PERMANENT_FAILURE_INVALID_ACK_ID) [Google::Cloud::FailedPreconditionError]
Cleanup                -> deleted subscription + topic
PASS: exactly-once ack confirmed SUCCESS; stale ack_id rejected FAILED_PRECONDITION
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide. `<message-id>` and the `ack_id` vary per run.
> Exits non-zero on any failed assertion.

## What's Happening

The subscription is created with `enable_exactly_once_delivery: true`, and the example
confirms the flag is set on the returned resource. After publishing and pulling one message,
it acknowledges with a result block: on an exactly-once subscription the client surfaces the
ack confirmation as an `AcknowledgeResult` whose status is `SUCCESS`. It then re-acks the
**same, now-acked** `ack_id` — a stale id — and asserts the connector rejects it.

A `Publish` writes once to the topic log `pubsub.{t}`, then fans out one queue copy per
subscription `pubsub.sub.{s}`. With exactly-once, a successful unary `Acknowledge` confirms the
ack, while an unparseable/expired/unknown `ack_id` returns a `FAILED_PRECONDITION` status
carrying `ErrorInfo{reason: PERMANENT_FAILURE_INVALID_ACK_ID}` — the real Google SDK contract,
which the Ruby client surfaces as `Google::Cloud::FailedPreconditionError` (not a naive
`INVALID_ARGUMENT`). Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription (enable_exactly_once_delivery), Publish, Pull, Acknowledge | `pubsub.exactly-once-<uuid8>` / `pubsub.sub.sub-exactly-once-<uuid8>` | exactly-once | success → ack confirmation; stale `ack_id` → `FAILED_PRECONDITION` + `ErrorInfo`; node-local; ack deadline 10 s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/exactly-once`](../../../go/delivery/exactly-once),
  [`../../../python/delivery/exactly_once`](../../../python/delivery/exactly_once),
  [`../../../javascript/delivery/exactly-once`](../../../javascript/delivery/exactly-once),
  [`../../../java/delivery/exactly-once`](../../../java/delivery/exactly-once),
  [`../../../csharp/delivery/exactly-once`](../../../csharp/delivery/exactly-once)
- Docs: [`../../../../docs/concepts/exactly-once.md`](../../../../docs/concepts/exactly-once.md)

> Exactly-once is **node-local**: an `ack_id` minted on one node is invalid on another (the
> token carries the minting node's id). In a cluster, pin a subscription's StreamingPull/ack
> traffic to one node (sticky LB), or accept at-least-once across nodes.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
