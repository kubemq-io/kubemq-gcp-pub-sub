# C# — delivery: Exactly-Once Delivery

An `enable_exactly_once_delivery` subscription strengthens the ack contract — a durable ack is
confirmed and a stale/duplicate ack is rejected:
`CreateTopic → CreateSubscription(enable_exactly_once_delivery) → Publish → Pull → Acknowledge (durable) → Acknowledge(same ack_id) → FAILED_PRECONDITION + ErrorInfo`.

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
dotnet run --project delivery/exactly-once/ExactlyOnce.csproj
```

## Expected Output

```
[*] Created topic 'orders-<uuid8>'
[*] Created exactly-once subscription 'sub-orders-<uuid8>' (enable_exactly_once_delivery=true)
[x] Published 'order-42'
[v] Pulled 'order-42'
[v] Acknowledged → durable (exactly-once ack confirmed)
[v] Stale ack rejected: status=FailedPrecondition, ErrorInfo.reason=PERMANENT_FAILURE_INVALID_ACK_ID, domain=pubsub.googleapis.com
[ok] exactly-once: durable ack confirmed; stale ack rejected with PERMANENT_FAILURE_INVALID_ACK_ID
[*] Cleaned up subscription 'sub-orders-<uuid8>' + topic 'orders-<uuid8>'
```

The negative path is explicit: replaying an already-used `ack_id` is **rejected** with
`FAILED_PRECONDITION` carrying a `google.rpc.ErrorInfo` whose reason is
`PERMANENT_FAILURE_INVALID_ACK_ID`. Exit code is `0` on success and non-zero if the durable ack
fails or the stale ack is unexpectedly accepted.

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`), so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<uuid8>` suffix varies
> per run. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a subscription with `EnableExactlyOnceDelivery = true`, publishes one message,
pulls it, and acks it. With exactly-once the unary `Acknowledge` returning successfully means the
ack is **durable** — the message will not be redelivered. The program then **replays the same
`ack_id`**: because that lease is already consumed, the connector rejects the stale ack with a
unary `FAILED_PRECONDITION` carrying a `google.rpc.ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID,
domain: pubsub.googleapis.com)`. This matches the real Google SDK contract (it is **not** a literal
`INVALID_ARGUMENT`); client libraries map this `ErrorInfo` to a per-message permanent ack failure.
The example reads the `ErrorInfo` from the gRPC rich-status trailer and asserts both the status code
and the reason. (Over StreamingPull the same outcome surfaces as `invalid_ack_ids` /
`temporary_failed_ack_ids` on the `AcknowledgeConfirmation`.)

A `Publish` writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy to `pubsub.sub.sub-orders-<uuid8>`; the lease,
receive count, and the decoded `ack_id` transaction all live on the node that issued the
StreamingPull / Pull.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic → CreateSubscription → Publish → Pull → Acknowledge (durable) → Acknowledge (stale → rejected) | topic `orders-<uuid8>` → sub `sub-orders-<uuid8>` (`pubsub.sub.sub-orders-<uuid8>`) | exactly-once (`enable_exactly_once_delivery`) | stale unary ack → `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` (not `INVALID_ARGUMENT`); ack deadline 10 s (10..600) |

## Related Examples

- Same variant in the other 5 languages:
  [go](../../../go/delivery/exactly-once/),
  [python](../../../python/delivery/exactly_once/),
  [java](../../../java/delivery/exactly-once/),
  [javascript](../../../javascript/delivery/exactly-once/),
  [ruby](../../../ruby/delivery/exactly_once/)
- Docs: [`../../../../docs/concepts/exactly-once.md`](../../../../docs/concepts/exactly-once.md)

> **Gotcha — exactly-once is node-local.** An `ack_id` is valid only on the cluster node that minted
> it; the lease and its decoded broker transaction live on that node. In a cluster you MUST pin a
> subscription's StreamingPull traffic to a single node (sticky load balancer / session affinity),
> or an ack issued against the "wrong" node fails with `PERMANENT_FAILURE_INVALID_ACK_ID` and the
> message redelivers — degrading to at-least-once.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
