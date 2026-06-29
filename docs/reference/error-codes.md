# Error Codes

The connector speaks the **real Pub/Sub v1 gRPC services**, so it returns **genuine gRPC status
codes** (`google.rpc.Code`) — standard Google client libraries surface them as the normal typed
exceptions (`InvalidArgument`, `FailedPrecondition`, `NotFound`, …). There is **no REST/JSON v1**
(gRPC only, gotcha #9), so there are no AWS-style XML error envelopes.

The one contract that differs from a naive implementation: the **exactly-once unary invalid-ack**
path returns `FAILED_PRECONDITION` + an `ErrorInfo`, *not* `INVALID_ARGUMENT` — matching the real
Google SDK contract (the SDK resolves the ack result from the `ErrorInfo.reason`).

## gRPC Status Code Table

| gRPC status | Trigger | Resolution / Recovery |
|-------------|---------|-----------------------|
| `INVALID_ARGUMENT` | A validation failure: bad resource id (not 3..255, not letter-led, bad charset, `goog` prefix, topic id starting `sub.`); batch > 1000; message > 10 MiB; > 100 attributes; attr key > 256 B / value > 1024 B / ordering key > 1024 B; empty `data` **and** `attributes`; malformed CEL filter (> 256 chars or syntax error); `max_delivery_attempts` outside 5..100; a schema definition that fails to parse or exceeds 300 KB; a message that fails schema enforce-on-publish; a rejected **ingestion** or **export** (BigQuery/Cloud Storage/Bigtable) subscription. | **Fix the value/expression, then retry.** Bring the resource id, batch size, message size, or attribute count inside the limits in [limits-and-rules.md](limits-and-rules.md); set `max_delivery_attempts` to 5..100; correct the CEL filter syntax / length; make the payload conform to the schema; supply non-empty `data` **or** `attributes`. Do **not** blind-retry — the input is rejected deterministically. Ingestion/export subscriptions are unsupported; use the supported subscription types instead. |
| `FAILED_PRECONDITION` | `Pull` on a **detached** subscription; `CreateSnapshot` of a detached subscription; **and** — on an exactly-once subscription — a **unary** `Acknowledge`/`ModifyAckDeadline` with an unparseable / expired / unknown `ack_id` (carries the `ErrorInfo` below). | **Detached:** re-attach the subscription (or target an attached one) before pulling / snapshotting. **Stale exactly-once `ack_id`:** do **not** re-ack a message you already settled — the id is spent; just **re-pull** to get a fresh lease and `ack_id`. If it recurs across nodes, see the node-local boundary below (enable sticky load balancing). |
| `NOT_FOUND` | Operating on a topic / subscription / snapshot / schema that is not in the registry. | **Create the resource first** (`CreateTopic` / `CreateSubscription` / `CreateSnapshot` / `CreateSchema`), then retry; or correct the id you passed. On a cluster, a `NOT_FOUND` for a record that does exist usually means you hit a different node — enable sticky load balancing (see below). |
| `ALREADY_EXISTS` | Creating a resource id that already exists. | **Treat as success / idempotent** — the resource is already there. Get it instead of creating it, or pick a different id. |
| `UNAVAILABLE` | A StreamingPull stream the server **periodically closes** after `StreamCloseSeconds` (default 1800 s / 30 min) — SDKs transparently reconnect. Also the traffic-gate response when the broker is not ready. | **Let the SDK auto-reconnect** — periodic stream close is normal and your `Subscribe` callback keeps running across it; no action needed. For the traffic gate, **retry with backoff** until the broker is ready, and check the connector is up on `:8085` (host/port reachable, `PUBSUB_EMULATOR_HOST` correct). |

> **Batch atomicity.** A `Publish` validates the *entire* batch before enqueuing anything; the
> first offending message rejects the whole batch with `INVALID_ARGUMENT` and nothing is
> published. See [limits-and-rules.md](limits-and-rules.md).

## Exactly-Once Ack Results

On a subscription with `enable_exactly_once_delivery`, ack failures are reported **differently per
path** (gotcha — see [../concepts/exactly-once.md](../concepts/exactly-once.md)):

### StreamingPull — confirmation messages

The server returns an `AcknowledgeConfirmation` / `ModifyAckDeadlineConfirmation` carrying two id
lists; the SDK retries accordingly:

| List | Meaning | Client action |
|------|---------|---------------|
| `invalid_ack_ids` | expired / unknown / wrong-node ids — permanently unackable | give up on those ids |
| `temporary_failed_ack_ids` | a transient broker failure | **retry** those ids |

### Unary `Acknowledge` / `ModifyAckDeadline` — status + ErrorInfo

```
status: FAILED_PRECONDITION
details: ErrorInfo{ reason: "PERMANENT_FAILURE_INVALID_ACK_ID" }
```

This is the **real Google SDK contract** — the SDK reads the ack result from the `ErrorInfo`
reason, **not** from a literal `INVALID_ARGUMENT`. A connector that returned `INVALID_ARGUMENT`
here would break the SDK's exactly-once bookkeeping.

> **Node-local boundary (gotcha #1).** An `ack_id` is minted with the node id baked in; presenting
> it to a *different* node (after a cluster failover or a non-sticky load balancer) makes it appear
> as an invalid id — `invalid_ack_ids` on StreamingPull, `FAILED_PRECONDITION` + `ErrorInfo` on
> unary. Pin a subscription's StreamingPull to one node, or accept at-least-once across nodes.

## Common Triggers by Scenario

| Scenario | Result | Resolution / Recovery |
|----------|--------|-----------------------|
| `Pull` on an empty subscription | _none — returns with no messages (long-poll/empty)_ | None — expected; keep polling / stay subscribed. |
| `ModifyAckDeadline(0)` | _none — immediate nack/redeliver_ | None — this *is* the nack; the message redelivers. |
| Ack-deadline expires before ack | _none — 250 ms sweeper redelivers; receive count++_ | Ack within the deadline, or extend it (`ModifyAckDeadline`) for slow handlers. |
| Receive count exceeds `max_delivery_attempts` (DLQ set) | _none — republished to `dead_letter_topic`, original acked_ | Subscribe to the `dead_letter_topic` to inspect / replay poison messages. |
| Receive count exceeds `max_delivery_attempts` (no DLQ) | _none — dropped after exhaustion_ | Attach a dead-letter topic if you cannot afford to drop poison messages. |
| `Seek` to a timestamp **before** the retained window | _none — **clamps** to the earliest retained message (gotcha #8)_ | None — expected clamp; widen the retention window if you need to replay further back. |
| `Seek` that would replay more than `MaxSeekReplay` | _none — stops at the cap and logs WARN (no silent loss)_ | None — expected cap; raise `MaxSeekReplay` (operator config) if a larger replay is required. |
| `Publish` to a topic with zero subscriptions | _none — succeeds; written to the topic log, no fan-out_ | None — expected; create a subscription before publishing if you need delivery. |
| Filtered-out message during fan-out | _none — never enqueued (≈ auto-acked)_ | None — expected; relax the subscription filter if the drop was unintended. |
| `kms_key_name` on `CreateTopic` | _none — accepted and ignored_ | None — expected; CMEK is a no-op on the emulator protocol. |
| Bad resource id / oversize batch / oversize message / bad filter / schema mismatch | `INVALID_ARGUMENT` | Fix the offending value against [limits-and-rules.md](limits-and-rules.md) and retry; do not blind-retry. |
| Ingestion source / export subscription | `INVALID_ARGUMENT` | Unsupported — use a supported subscription type (pull / push). |
| `Pull` on a detached subscription | `FAILED_PRECONDITION` | Re-attach the subscription (or pull from an attached one). |
| Snapshot of a detached subscription | `FAILED_PRECONDITION` | Re-attach before `CreateSnapshot`. |
| Exactly-once unary ack with invalid/expired/wrong-node id | `FAILED_PRECONDITION` + `ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID)` | Do not re-ack a settled message — re-pull for a fresh `ack_id`; if cross-node, enable sticky load balancing. |
| Operate on a non-existent topic / sub / snapshot / schema | `NOT_FOUND` | Create the resource first (or fix the id); on a cluster, enable sticky load balancing. |
| Create a resource id that already exists | `ALREADY_EXISTS` | Treat as idempotent success; get the resource or choose a new id. |
| StreamingPull periodic close (every `StreamCloseSeconds`) | `UNAVAILABLE` (SDK reconnects) | None — let the SDK auto-reconnect; the callback keeps running. |
| Broker not ready (traffic gate) | `UNAVAILABLE` | Retry with backoff until ready; check the connector is up on `:8085`. |
| `data` and `attributes` both empty | `INVALID_ARGUMENT` | Supply non-empty `data` **or** at least one attribute, then retry. |

## Recovery Cheat-Sheet

The recovery columns above collapse to five rules:

1. **`UNAVAILABLE`** → transient. Let the SDK **auto-reconnect**; for the traffic gate, **retry with backoff** and confirm the connector is up on `:8085` (host reachable, `PUBSUB_EMULATOR_HOST` correct).
2. **`INVALID_ARGUMENT`** → **fix the input, then retry** (never blind-retry). Most often: `max_delivery_attempts` back into **5..100**, a malformed CEL **filter**, an oversize **batch/message/attribute**, a **schema** mismatch, or empty `data`+`attributes`. See [limits-and-rules.md](limits-and-rules.md).
3. **`FAILED_PRECONDITION`** → for an exactly-once **stale `ack_id`**, **do not re-ack** the already-settled message — **re-pull** for a fresh lease; for a **detached** subscription, **re-attach** before `Pull` / `CreateSnapshot`.
4. **`NOT_FOUND`** → **create the topic / subscription / snapshot / schema first** (or fix the id).
5. **Node-local ack errors on a cluster** (`invalid_ack_ids`, or `FAILED_PRECONDITION` / `NOT_FOUND` for records that exist) → **enable sticky load balancing** (session affinity) so a subscription's StreamingPull stays pinned to one node, or accept at-least-once across nodes.

For step-by-step operational diagnosis (connector reachability, emulator-mode env, periodic reconnect, the sticky-LB caveat, and the traffic gate), see the
[connectivity & emulator-mode guide](../guides/connectivity-and-emulator-mode.md) — the operational/troubleshooting reference for this connector.

## See Also

- [../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md) — operational/troubleshooting reference: connector reachability on `:8085`, reconnect, the sticky-LB caveat, and the traffic gate.
- [../concepts/exactly-once.md](../concepts/exactly-once.md) — confirmation messages + the node-local boundary.
- [limits-and-rules.md](limits-and-rules.md) — every value whose violation yields `INVALID_ARGUMENT`.
- [capabilities.md](capabilities.md) — the operations behind each code.
- [migration-from-gcp.md](migration-from-gcp.md) — the deviations (incl. the unary-ack status).
