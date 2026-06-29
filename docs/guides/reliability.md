# Reliability

This guide covers the connector's delivery guarantees: **exactly-once delivery** (and its
node-local boundary), **ordering keys**, **dead-letter & retry**, and the underlying
**at-least-once** model. Several behaviors carry node-local caveats — read the callouts.

## At-Least-Once Is the Baseline

Every subscription delivers **at least once**. A delivered message is held under an ack-deadline
lease; if it is not acked before the deadline, a 250 ms sweeper applies the retry backoff and
**redelivers** it (the receive count increments). On a broker not-ready → ready transition the
connector drops all in-memory leases and redelivers any in-flight messages. The consequence:

> **Design consumers to be idempotent.** Without `enable_exactly_once_delivery`, a message may be
> delivered more than once (redelivery on deadline expiry, nack, or broker recovery). Exactly-once
> tightens this — but only within a single node (below).

## Exactly-Once Delivery

A subscription with `enable_exactly_once_delivery` changes the ack contract so the SDK can resolve
each ack result deterministically:

- **StreamingPull** returns an `AcknowledgeConfirmation` / `ModifyAckDeadlineConfirmation`:
  - expired or unknown `ack_id`s appear in **`invalid_ack_ids`**;
  - transient broker failures appear in **`temporary_failed_ack_ids`** (the client retries those).
- **Unary** `Acknowledge` / `ModifyAckDeadline` returns a `FAILED_PRECONDITION` status carrying an
  **`ErrorInfo{reason: PERMANENT_FAILURE_INVALID_ACK_ID}`** for an unparseable / expired / unknown
  id.

> **The unary error is `FAILED_PRECONDITION` + `ErrorInfo`, not `INVALID_ARGUMENT`.** This matches
> the **real Google SDK contract** — the SDK reads the ack result from the `ErrorInfo` reason. A
> naive `INVALID_ARGUMENT` would break that resolution. See
> [../reference/error-codes.md](../reference/error-codes.md).

### Exactly-once is node-local

> **Gotcha #1 — exactly-once is node-local; pin StreamingPull to one node.** An `ack_id` is a token
> that carries the **node id** of the node that minted it. An `ack_id` minted on one node is
> **invalid on another** (the node id won't match). There is no cluster-wide distributed
> exactly-once.
>
> In a KubeMQ **cluster**, you must therefore either:
>
> - **pin** an exactly-once subscription's StreamingPull traffic to a **single node** (a sticky
>   load balancer with session affinity), or
> - **accept at-least-once** across nodes.
>
> Single-node deployments are unaffected. This is the most important caveat in this connector — it
> is repeated in [connectivity-and-emulator-mode.md](connectivity-and-emulator-mode.md),
> [../concepts/exactly-once.md](../concepts/exactly-once.md), and
> [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md).

The `delivery/exactly-once` example creates an exactly-once subscription and demonstrates handling
both the confirmation messages (StreamingPull) and the `ErrorInfo` (unary) for an expired id.

## Ordering Keys

With `enable_message_ordering` on the subscription, messages sharing an `ordering_key` are
delivered in **publish order** with **at most one in flight per key**: the head of a key blocks
until it is acked or redelivered, and **redelivery is in order**. A round-robin cursor spreads
delivery fairly across contended keys; keyless messages are unordered.

> Ordering is opt-in on **both** sides: the publisher sets `ordering_key`, the subscription sets
> `enable_message_ordering`. The ordering key travels as the reserved tag `_pubsub_ordering_key`.
> See [publishing.md](publishing.md#ordering-keys) and
> [../concepts/ordering-keys.md](../concepts/ordering-keys.md).

The burn-in `ordering_keys` worker gates on **zero ordering violations** under load.

## Dead-Letter & Retry

**Retry.** Ack-deadline expiry redelivers with an **exponential backoff** clamped to the
subscription's `[min, max]` (defaults 10 s … 600 s). An explicit nack (`ModifyAckDeadline(0)`)
bypasses the backoff and redelivers immediately.

**Dead-letter.** When a message's receive count **exceeds** `max_delivery_attempts` and the
subscription has a `dead_letter_topic`, the sweeper **republishes** the message to that topic (a
connector-level fan-out through the normal publish path) and **acks the original**.

> **Gotcha #6 — `max_delivery_attempts` must be 5..100** (0 = unset). This is Google's rule and is
> validated up front; a value outside the range is rejected. See
> [../reference/limits-and-rules.md](../reference/limits-and-rules.md).

Push subscriptions share the same retry → dead-letter pipeline (retry on non-2xx/timeout, dead-letter
on exhaustion). See [push-delivery.md](push-delivery.md) and
[../concepts/dead-letter-and-retry.md](../concepts/dead-letter-and-retry.md).

The `delivery/dead-letter-topic` example sets `max_delivery_attempts` and a `dead_letter_topic`,
then nacks a message repeatedly until it is republished to the dead-letter topic and the original
is acked.

## Seek & Snapshots (Recovery / Replay)

`Seek` rewinds a subscription to a **timestamp** or a **snapshot** by replaying the topic log
`pubsub.{t}` — useful for reprocessing. A timestamp **before** the retained window is **clamped to
the earliest retained message — not an error** (gotcha #8). Replay is bounded by `MaxSeekReplay`
(default 1,000,000); hitting the cap stops the replay and logs a `WARN` (no silent loss).
Per-resource **retention is clamped** to the broker's `Store.MaxRetention` ceiling (gotcha #5):
`GetTopic`/`GetSubscription` echo the requested value, but fan-out, seek, and the dashboard use the
clamped value. Full detail is in [../concepts/seek-and-snapshots.md](../concepts/seek-and-snapshots.md).

## Node-Local State Summary (Cluster)

> Topic / subscription / snapshot / schema **records** are synced cluster-wide (BoltDB + NATS), so
> resource **existence** is cluster-wide. But three pieces of delivery **state** are node-local and
> require a **sticky load balancer** in a cluster:
>
> - **exactly-once `ack_id`s** (node id baked into the token);
> - **StreamingPull leases / in-flight tracking and flow-control counters**;
> - **push delivery workers and their in-flight retries**.
>
> Single-node deployments are unaffected. Full multi-node guidance is deferred (no clustering guide
> in v1). See [connectivity-and-emulator-mode.md](connectivity-and-emulator-mode.md) and
> [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md).

## Error Quick Reference

| Trigger | Result |
|---------|--------|
| Ack deadline expires without ack | redelivery; receive count increments |
| `ModifyAckDeadline(0)` | immediate nack / redeliver |
| Receive count exceeds `max_delivery_attempts` (DLQ set) | republished to `dead_letter_topic`, original acked |
| Exactly-once unary ack of an expired/unknown id | `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` |
| Exactly-once StreamingPull ack of an expired id | id appears in `invalid_ack_ids` |
| `max_delivery_attempts` outside 5..100 | rejected (`INVALID_ARGUMENT`) |
| `Seek` to a pre-window timestamp | clamped to earliest retained (not an error) |
| Broker not-ready → ready transition | in-memory leases dropped; in-flight messages redelivered |

## See Also

- [subscribing.md](subscribing.md) — the lease model, ack / nack / extend, flow control.
- [push-delivery.md](push-delivery.md) — push retry → dead-letter.
- [../concepts/exactly-once.md](../concepts/exactly-once.md) — the node-local boundary in depth.
- [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md) — the exactly-once migration decision.
