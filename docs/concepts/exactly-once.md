# Exactly-Once Delivery

## Concept

By default Pub/Sub is **at-least-once**: a message may be redelivered (after a nack, a lease
expiry, or a network blip), so consumers must be idempotent. A subscription created with
`enable_exactly_once_delivery` strengthens this: once a message is **successfully acknowledged**,
the connector guarantees it will not be redelivered, and the ack itself is **confirmed** so the
client knows the ack took effect.

> **The headline caveat — exactly-once is node-local.** An `ack_id` is valid only on the cluster
> node that minted it. In a cluster you MUST pin a subscription's StreamingPull traffic to a
> single node (sticky load balancer / session affinity), or accept at-least-once across nodes.
> This is the single most important fact about exactly-once on this connector.

## How It Works

Exactly-once changes the **ack contract** so the client can distinguish a durable ack from a
failed one:

- **StreamingPull** returns `AcknowledgeConfirmation` and `ModifyAckDeadlineConfirmation` messages
  on the stream. Each lists:
  - `ack_ids` — acks/modifications that succeeded.
  - `invalid_ack_ids` — ids that **expired** (the lease elapsed or the id was already used); the
    message may have been redelivered, so do not treat these as durable.
  - `temporary_failed_ack_ids` — a **transient** failure; the client should retry the ack.
- **Unary `Acknowledge` / `ModifyAckDeadline`** on an invalid id returns `FAILED_PRECONDITION`
  with an `ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID)`. This matches the real Google SDK
  contract — it is **not** a literal `INVALID_ARGUMENT`. Client libraries map this `ErrorInfo` to
  a per-message permanent ack failure.

```
StreamingPull:  Acknowledge(ack_ids) ──▶ AcknowledgeConfirmation {
                                            ack_ids:                 [durably acked]
                                            invalid_ack_ids:         [expired — may redeliver]
                                            temporary_failed_ack_ids:[retry the ack]
                                          }

unary Acknowledge(stale ack_id) ──▶ FAILED_PRECONDITION
                                     + ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID)
```

## Handling Ack Results In Each Language

Client libraries surface the confirmation differently — the per-language idiom is the key thing to
get right:

- The ack call returns a **future / promise / awaitable ack result** rather than fire-and-forget.
- Await it: success means the ack is durable; a permanent failure (`PERMANENT_FAILURE_INVALID_ACK_ID`)
  means the lease was lost and the message may redeliver; a transient failure should be retried.
- Do not assume an ack "stuck" just because the call returned — with exactly-once you must observe
  the confirmation.

## Why Node-Local

The lease, receive count, and the `ack_id`'s decoded broker transaction all live on the node that
issued the StreamingPull. A different node has no record of that lease, so an `ack_id` presented
there is rejected. Exactly-once therefore holds **within one node's view of a subscription**; it
cannot span nodes without sticky routing.

> **Gotcha — sticky load balancer required in clusters.** Route all StreamingPull and ack traffic
> for a given subscription to the same node (e.g. session affinity keyed on the subscription or
> client). Without it, an ack issued against the "wrong" node fails with
> `PERMANENT_FAILURE_INVALID_ACK_ID` and the message redelivers — degrading to at-least-once. See
> [../guides/reliability.md](../guides/reliability.md) and
> [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md).

## Combining With Ordering

Exactly-once and ordering keys compose: an ordering-enabled, exactly-once subscription delivers
each key in order with confirmed acks. The same node-local boundary applies to both.

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| exactly-once | [go](../../examples/go/delivery/exactly-once/) | [python](../../examples/python/delivery/exactly_once/) | [java](../../examples/java/delivery/exactly-once/) | [js](../../examples/javascript/delivery/exactly-once/) | [csharp](../../examples/csharp/delivery/exactly-once/) | [ruby](../../examples/ruby/delivery/exactly_once/) |

## See Also

- [subscriptions-and-pull.md](subscriptions-and-pull.md) — `ack_id`s, leases, the node-local note.
- [ordering-keys.md](ordering-keys.md) — composing exactly-once with ordering.
- [../guides/reliability.md](../guides/reliability.md) — exactly-once, ordering, DLQ, sticky-LB.
- [../reference/error-codes.md](../reference/error-codes.md) — `FAILED_PRECONDITION` + `ErrorInfo`.
