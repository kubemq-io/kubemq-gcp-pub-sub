# Go — delivery: Exactly-Once

Exactly-once delivery against the KubeMQ Pub/Sub connector:
`CreateSubscription(EnableExactlyOnceDelivery) → Publish → StreamingPull → AckWithResult → assert AcknowledgeConfirmation`.

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
cd examples/go && go run ./delivery/exactly-once
```

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/exactly-once | emulator=localhost:8085 project=my-project
CreateTopic: eos-<uuid8> (channel pubsub.eos-<uuid8>)
CreateSubscription: sub-eos-<uuid8> (EnableExactlyOnceDelivery=true)
Publish: messageId=<message-id> body="exactly-once payload"
StreamingPull (Receive): ack with confirmation via AckWithResult()
Receive: messageId=<message-id> body="exactly-once payload" ackConfirmation=Success
PASS: exactly-once ack confirmed by the broker (AcknowledgeStatusSuccess)
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`eos-<uuid8>` / `sub-eos-<uuid8>`) so concurrent runs use distinct
> `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; `<message-id>` varies per run. (See
> `../../../SHARED-CONVENTIONS.md` §1.) Exits 0 on success, non-zero if the ack is not confirmed.

## What's Happening

The program creates a topic and a subscription with `EnableExactlyOnceDelivery = true`, publishes one
message, then opens a StreamingPull `Receive` loop. Instead of the fire-and-forget `Message.Ack()`,
it calls `Message.AckWithResult()` and **blocks** on `AckResult.Get(ctx)`, which returns the broker's
`AcknowledgeConfirmation`. It asserts the body and message id round-trip and that the confirmation is
`AcknowledgeStatusSuccess` — proving the broker durably recorded the ack, so the message will not be
redelivered. Any failure exits non-zero.

With `enable_exactly_once_delivery`, StreamingPull returns an `AcknowledgeConfirmation` (and a
`ModifyAckDeadlineConfirmation`) for each ack/modify so the client can confirm the broker recorded it;
expired ids surface as `invalid_ack_ids` and transient failures as `temporary_failed_ack_ids` (spec
S3.5). A plain `Ack()` gives no confirmation, so exactly-once code MUST use `AckWithResult`. The
message flows through the topic log `pubsub.eos-<uuid8>` and the per-subscription queue
`pubsub.sub.sub-eos-<uuid8>`.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, StreamingPull (Receive), Acknowledge (with confirmation), DeleteSubscription, DeleteTopic | `eos-<uuid8>` / `sub-eos-<uuid8>` | exactly-once (`EnableExactlyOnceDelivery`) | StreamingPull returns `AcknowledgeConfirmation`; a unary invalid-ack returns `FAILED_PRECONDITION` + `ErrorInfo`. **Node-local**: an `ack_id` minted on one node is invalid on another. Ack deadline 10 s. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/delivery/exactly_once`](../../../python/delivery/exactly_once),
  [`../../../javascript/delivery/exactly-once`](../../../javascript/delivery/exactly-once),
  [`../../../java/delivery/exactly-once`](../../../java/delivery/exactly-once),
  [`../../../csharp/delivery/exactly-once`](../../../csharp/delivery/exactly-once),
  [`../../../ruby/delivery/exactly_once`](../../../ruby/delivery/exactly_once)
- Docs: [`../../../docs/concepts/exactly-once.md`](../../../docs/concepts/exactly-once.md)
- Related: [`../ordering-keys`](../ordering-keys), [`../push-delivery`](../push-delivery).

> **Gotcha — exactly-once is node-local.** An `ack_id` is valid only on the node that minted it; an
> ack_id from one node is invalid on another (a unary invalid-ack returns `FAILED_PRECONDITION` +
> `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)`, not a literal `INVALID_ARGUMENT`). In a cluster, pin
> a subscription's StreamingPull to one node (sticky LB), or accept at-least-once across nodes. Always
> use `AckWithResult` (not `Ack`) so the broker's confirmation is observed.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
