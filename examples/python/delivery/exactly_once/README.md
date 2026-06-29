# Python — delivery: Exactly-Once Delivery

Confirmed acks: `CreateTopic → CreateSubscription(enable_exactly_once_delivery) → Publish`, then (1) StreamingPull and **await the ack-result future** to confirm a durable ack, and (2) demonstrate that re-acking a stale `ack_id` over the unary API returns `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)`.

## Prerequisites

- Python 3.10+ and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `examples/python/pyproject.toml` + `uv.lock`).
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
cd examples/python && uv run python -m delivery.exactly_once
# or, equivalently: uv run python delivery/exactly_once/main.py
```

## Expected Output

```
=== Pub/Sub exactly-once — topic 'orders-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)

CreateTopic            -> projects/my-project/topics/orders-<uuid8>
CreateSubscription     -> projects/my-project/subscriptions/sub-eo-stream-<uuid8>  (enable_exactly_once_delivery=True)
CreateSubscription     -> projects/my-project/subscriptions/sub-eo-unary-<uuid8>  (enable_exactly_once_delivery=True)

Part 1 — StreamingPull: await the ack-result future to confirm a DURABLE ack
  Publish -> 'exactly-once-streaming'
  ack confirmed durable (ack_with_response().result() returned)
  [OK] StreamingPull delivered the message exactly once
  [OK] ack confirmation succeeded (durable, no AcknowledgeError)

Part 2 — unary Acknowledge on a stale ack_id -> FAILED_PRECONDITION + ErrorInfo
  Publish -> 'exactly-once-unary'
  [OK] unary Pull delivered the message
  first Acknowledge -> ok (durable)
  second Acknowledge (stale id) -> FAILED_PRECONDITION
    ErrorInfo.reason = PERMANENT_FAILURE_INVALID_ACK_ID
  [OK] stale unary ack returns FAILED_PRECONDITION
  [OK] carries ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID), not INVALID_ARGUMENT

PASS — exactly-once: confirmed StreamingPull ack + unary stale-ack ErrorInfo.
cleanup: DeleteSubscription(stream) ok
cleanup: DeleteSubscription(unary) ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid8-suffixed (`orders-<uuid8>` / `sub-eo-stream-<uuid8>` / `sub-eo-unary-<uuid8>`)
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> underlying `<ack-id>` / `<message-id>` values vary per run. Part 2 prints the negative result
> explicitly (`stale ack_id → FAILED_PRECONDITION`).

## What's Happening

- Two subscriptions are created with `enable_exactly_once_delivery=True`.
- **Part 1 (StreamingPull, confirmed ack):** a message is delivered via `subscriber.subscribe(...)`;
  in the callback, `message.ack_with_response()` returns a **future** whose `.result()` blocks until
  the ack is **confirmed durable** by the connector. With exactly-once you must observe the
  confirmation — a plain fire-and-forget ack is not enough; a permanent failure would raise
  `AcknowledgeError`.
- **Part 2 (unary stale-ack contract):** a second message is pulled and acked over the unary API.
  Re-acking the now-consumed `ack_id` returns `FAILED_PRECONDITION` carrying
  `ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID)` — this is the real Google SDK contract (the
  library maps the `ErrorInfo` reason to a permanent ack failure), **not** a literal
  `INVALID_ARGUMENT`.
- Connector behavior (spec S3.5): a `Publish` writes once to the topic log `pubsub.{t}` and fans out
  to `pubsub.sub.{s}`. With exactly-once, StreamingPull returns `AcknowledgeConfirmation` /
  `ModifyAckDeadlineConfirmation` messages (durable ids in `ack_ids`, expired in `invalid_ack_ids`,
  transient in `temporary_failed_ack_ids`); the unary API returns `FAILED_PRECONDITION` + `ErrorInfo`
  on a stale id. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription ×2, Publish, StreamingPull + Acknowledge (confirmed), Pull + Acknowledge (unary stale) | topic `orders-<uuid8>` / subs `sub-eo-stream-<uuid8>` + `sub-eo-unary-<uuid8>` | exactly-once | confirmed acks via ack-result future; unary stale ack → `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/exactly-once`](../../../go/delivery/exactly-once),
  [`../../../javascript/delivery/exactly-once`](../../../javascript/delivery/exactly-once),
  [`../../../java/delivery/exactly-once`](../../../java/delivery/exactly-once),
  [`../../../csharp/delivery/exactly-once`](../../../csharp/delivery/exactly-once),
  [`../../../ruby/delivery/exactly_once`](../../../ruby/delivery/exactly_once)
- Docs: [`../../../../docs/concepts/exactly-once.md`](../../../../docs/concepts/exactly-once.md)

> **Gotcha — exactly-once is node-local.** An `ack_id` is valid only on the cluster node that minted
> it. In a cluster you MUST pin a subscription's StreamingPull traffic to a single node (sticky load
> balancer / session affinity), or it degrades to at-least-once across nodes. This single-instance
> example does not hit the boundary, but production deployments must.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
