# JavaScript — delivery: Exactly-once

Exactly-once acknowledgement against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription(enableExactlyOnceDelivery) → Publish → StreamingPull → ackWithResponse (await AcknowledgeConfirmation)` — plus the unary invalid-ack contract (`FAILED_PRECONDITION` + `ErrorInfo`).

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
cd examples/javascript && npx tsx delivery/exactly-once/index.ts
```

## Expected Output

```
=== Pub/Sub exactly-once — topic 'eos-<uuid8>' / sub 'sub-eos-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)

CreateTopic            -> projects/my-project/topics/eos-<uuid8>
CreateSubscription     -> sub-eos-<uuid8>  (enableExactlyOnceDelivery=true)

Publish -> 'exactly-once-payload'  id=<message-id>

StreamingPull (exactly-once; ackWithResponse awaits the AcknowledgeConfirmation):
  received body='exactly-once-payload' messageId=<message-id> (exactly-once)
  ackWithResponse -> SUCCESS

  [OK] exactly one delivery surfaced to the application
  [OK] ackWithResponse confirmed Success (AcknowledgeConfirmation), got 'SUCCESS'

Negative path: unary Acknowledge with a bogus ack_id (exactly-once contract):
  bogus ack_id -> FAILED_PRECONDITION (gRPC code 9)
  details: one or more ack ids are no longer valid
  [OK] unary invalid-ack returns FAILED_PRECONDITION (not INVALID_ARGUMENT)

PASS — exactly-once: ack confirmation reconciled; unary invalid-ack is FAILED_PRECONDITION.
cleanup: DeleteSubscription ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.eos-<uuid8>` / `pubsub.sub.sub-eos-<uuid8>` channels and do not collide; the `<uuid8>`
> suffix and the `<message-id>` vary per run (the delivered copy carries the **same** `<message-id>`
> as the published one). Exits `0` on success, non-zero on any failed assertion.
> (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a subscription with `enableExactlyOnceDelivery: true`, publishes one message, then
opens a StreamingPull stream. Under exactly-once, acking is no longer fire-and-forget:
`message.ackWithResponse()` returns a promise that resolves to `AckResponses.Success` only once the
connector has **durably confirmed** the ack (the `AcknowledgeConfirmation` carried back on the stream),
and the client de-duplicates redeliveries so each message surfaces to the application exactly once.
The program asserts exactly one delivery and a `Success` ack response.

It then exercises the **unary invalid-ack contract**: a unary `Acknowledge` with a bogus `ack_id`
returns gRPC `FAILED_PRECONDITION` (status code **9**) — carrying an
`ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID)` — and *not* a literal `INVALID_ARGUMENT`. This
matches the real Google SDK contract.

A Publish writes once to the topic log `pubsub.{t}` then fans out one queue copy per subscription
`pubsub.sub.{s}`. Exactly-once is **node-local**: an `ack_id` is `base64(JSON{… node id …})`, valid
only on the connector node that minted it. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription` (`enableExactlyOnceDelivery`), `Publish`, `StreamingPull` (ackWithResponse), unary `Acknowledge` (invalid-ack) | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | exactly-once | StreamingPull returns `AcknowledgeConfirmation` / `ModifyAckDeadlineConfirmation` (expired → `invalid_ack_ids`, transient → `temporary_failed_ack_ids`); unary invalid-ack → `FAILED_PRECONDITION` + `ErrorInfo`; **node-local** boundary |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/exactly-once`](../../../go/delivery/exactly-once),
  [`../../../python/delivery/exactly_once`](../../../python/delivery/exactly_once),
  [`../../../java/delivery/exactly-once`](../../../java/delivery/exactly-once),
  [`../../../csharp/delivery/exactly-once`](../../../csharp/delivery/exactly-once),
  [`../../../ruby/delivery/exactly_once`](../../../ruby/delivery/exactly_once)
- Docs: [`../../../../docs/concepts/exactly-once.md`](../../../../docs/concepts/exactly-once.md)
  and [`../../../../docs/guides/reliability.md`](../../../../docs/guides/reliability.md)

> **Gotcha — exactly-once is node-local.** An `ack_id` minted on one connector node is invalid on
> another (the node id is baked into the opaque ack id). In a multi-node cluster, pin a subscription's
> StreamingPull traffic to one node (sticky load balancing) or accept at-least-once across nodes.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
