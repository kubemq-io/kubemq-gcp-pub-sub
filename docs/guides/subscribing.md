# Subscribing

This guide covers the consume surface: subscription lifecycle, the `Pull` vs `StreamingPull`
paths, the ack-deadline lease model (ack / nack / extend), flow control, and the periodic
server-initiated reconnect. Every subscription is a native KubeMQ **Queue** channel
`pubsub.sub.{subscription}` (see [../reference/channel-mapping.md](../reference/channel-mapping.md)).

## Subscription Lifecycle

The `Subscriber` surface ships **16 RPCs** (see [../reference/capabilities.md](../reference/capabilities.md)):

- `CreateSubscription` — binds to a topic; the queue is created lazily. A `filter` is compiled at
  create-time and is **immutable** thereafter. Export subscriptions (BigQuery / Cloud Storage /
  Bigtable) and ingestion are **rejected** (`INVALID_ARGUMENT`).
- `GetSubscription` / `ListSubscriptions`.
- `UpdateSubscription` — a `FieldMask` over ack deadline, retention, dead-letter, retry, push,
  exactly-once, and labels. **`name` and `filter` are immutable.**
- `DeleteSubscription` — drops the backlog and any leases.
- `Pull`, `Acknowledge`, `ModifyAckDeadline`, `StreamingPull`, `ModifyPushConfig`, `Seek`, and the
  five snapshot RPCs.

## Pull vs StreamingPull

Both paths read from the subscription's queue channel through a poller and place each delivered
message under an **ack-deadline lease**.

### Unary Pull

`Pull` returns up to `max_messages` (≤ 1000) currently-available messages, each with an `ack_id`.
You ack with `Acknowledge(ack_ids)` or nack/extend with `ModifyAckDeadline`. A `Pull` on a
**detached** subscription returns `FAILED_PRECONDITION`.

### StreamingPull

`StreamingPull` is a bidirectional stream: the server pushes messages as they arrive and the client
sends back `ack_ids`, `modify_deadline` requests, and flow-control settings on the same stream.
This is what the high-level `subscriber.Receive(...)` / `subscription.on('message', ...)` helpers
use. The `subscriptions/streaming-pull` example demonstrates it; the `topics/basic-publish-subscribe`
example uses the simpler high-level receive helper.

> **Leases are subscription-owned, not stream-owned.** An ack on one StreamingPull stream correctly
> resolves a message that was delivered on a **different** stream (cross-stream ack). This matters
> for clients that reconnect or run multiple streams.

## The Ack-Deadline Lease

Every delivered message gets an opaque `ack_id` — a base64-JSON token carrying the subscription,
channel, node id, broker transaction id, sequence, receive count, lease id, and deadline. The
message stays leased (invisible to other consumers) until the deadline:

| Action | Effect |
|--------|--------|
| `Acknowledge(ack_ids)` (or StreamingPull `ack_ids`) | Decodes each id and acks the broker sequence — the message is removed. |
| `ModifyAckDeadline(0)` | **Immediate nack / redeliver** (bypasses retry backoff). |
| `ModifyAckDeadline(>0)` | **Extends** the deadline. Valid range **10..600 s**. |
| Deadline expiry | A 250 ms sweeper expires the lease, applies the retry backoff, and **redelivers** — or dead-letters once the receive count exceeds the policy. |

The default ack deadline is `DefaultAckDeadlineSeconds` (default **10 s**, range 10..600). The
`subscriptions/ack-deadline-nack` example demonstrates both extending a deadline and a `0`-nack
redelivery.

> **Ack deadline is 0 (nack) or 10..600 s.** A value between 1 and 9 is not valid; `0` means nack.
> See [../reference/limits-and-rules.md](../reference/limits-and-rules.md).

## Flow Control

On a `StreamingPull` stream the client sets `max_outstanding_messages` / `max_outstanding_bytes`;
the connector keeps per-stream counters keyed by the `ack_id`s that stream emitted (`≤ 0` = use the
connector's `MaxOutstandingMessages` ceiling, default **1000**). Outstanding count is **released on
ack / nack / expiry, and fully released on stream disconnect** — without waiting for lease expiry.

A separate hard ceiling, `MaxInflightPerSubscription` (default **20,000**), caps the total leased
(un-acked) messages per subscription across all streams. These knobs are in
[../configuration.md](../configuration.md).

## Periodic Reconnect

> A `StreamingPull` stream is closed by the server after `StreamCloseSeconds` (default **1800 s** /
> 30 min) with `UNAVAILABLE`. This is **normal** — client libraries transparently reconnect and
> your receive callback keeps running. It bounds per-stream resource lifetime. Do not treat the
> periodic `UNAVAILABLE` as an error.

## Ack-Deadline Reset on Broker Recovery

On a broker not-ready → ready transition the connector **drops all in-memory leases** (their
downstream transactions are dead) and the poller rebuilds. Any in-flight messages are redelivered
after recovery — design consumers to be **idempotent**. See [reliability.md](reliability.md).

## Exactly-Once Subscriptions

A subscription with `enable_exactly_once_delivery` changes the ack contract: StreamingPull returns
`AcknowledgeConfirmation` / `ModifyAckDeadlineConfirmation` messages (expired/unknown ids in
`invalid_ack_ids`, transient failures in `temporary_failed_ack_ids`), and a **unary**
`Acknowledge`/`ModifyAckDeadline` returns `FAILED_PRECONDITION` with an
`ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` for a bad id. **Exactly-once is node-local.** The
full contract — and the sticky-LB requirement — is in [reliability.md](reliability.md) and
[../concepts/exactly-once.md](../concepts/exactly-once.md).

## Concurrency & Resource Isolation

> Subscription channels are global (`pubsub.sub.{s}`). Use **uuid-suffixed** subscription ids per
> run and best-effort delete them on exit so concurrent runs don't drain one another's messages.

## Error Quick Reference

| Trigger | Result |
|---------|--------|
| `Pull` / `Seek` on a detached subscription | `FAILED_PRECONDITION` |
| Export-subscription / ingestion config on `CreateSubscription` | `INVALID_ARGUMENT` |
| `UpdateSubscription` of `name` or `filter` | rejected (immutable) |
| Exactly-once unary ack of an expired/unknown id | `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` |
| Leased messages exceed `MaxInflightPerSubscription` | new deliveries throttled |

## See Also

- [publishing.md](publishing.md) — topics, batch publish, ordering keys, attributes.
- [filtering.md](filtering.md) — the CEL-subset attribute filter set at `CreateSubscription`.
- [reliability.md](reliability.md) — exactly-once, ordering, dead-letter, at-least-once.
- [../concepts/subscriptions-and-pull.md](../concepts/subscriptions-and-pull.md) — the subscription concept + cross-links to all 6 languages.
