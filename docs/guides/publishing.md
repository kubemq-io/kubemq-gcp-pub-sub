# Publishing

This guide covers the publish surface end to end: topic lifecycle, a single publish, batch
publish (≤ 1000 messages), the **atomic batch validation** rule, ordering keys, and message
attributes. Every topic is a native KubeMQ **Events Store** log `pubsub.{topic}` (see
[../reference/channel-mapping.md](../reference/channel-mapping.md)).

## Topic Lifecycle

The `Publisher` surface ships **9 RPCs** (see [../reference/capabilities.md](../reference/capabilities.md)):

- `CreateTopic` — validates the name; `kms_key_name` is accepted-and-ignored; ingestion configs
  are **rejected** (`INVALID_ARGUMENT`); requested retention is clamped to the broker ceiling.
- `GetTopic` — returns the **requested** (un-clamped) retention.
- `ListTopics` — opaque page token.
- `UpdateTopic` — a `FieldMask` over `labels`, `message_retention_duration`, `schema_settings`.
- `DeleteTopic` — a **tombstone**: the record is retained so existing subscriptions survive, and
  re-creating the topic reuses the same log.
- `ListTopicSubscriptions`, `ListTopicSnapshots`, `DetachSubscription`, and `Publish` (below).

> **Gotcha — topic ids may not start with `sub.`.** That prefix is the reserved broker namespace
> for subscription queues (`pubsub.sub.{s}`). Resource ids must be 3..255 chars, start with a
> letter, use the charset `[A-Za-z0-9._~%+-]`, and carry no `goog` prefix. See
> [../reference/limits-and-rules.md](../reference/limits-and-rules.md).

## A Single Publish

`topic.Publish` returns a server-assigned **message id**. The connector writes the message **once**
to the Events Store log `pubsub.{topic}` (the authoritative, cross-protocol, replayable copy and
the source for `Seek`), then fans out one queue copy per subscription, applying each subscription's
filter:

1. `Publish` — the SDK sends a `PubsubMessage { data, attributes, ordering_key }`.
2. The connector assigns an **id** and a **publish-time** and returns the id.
3. The message lands in `pubsub.{topic}` via `Array.SendEventsStore` and is fanned out to each
   subscription's queue `pubsub.sub.{s}` via `Array.SendQueueMessage`.

A filtered-out message is never enqueued for that subscription (it is effectively auto-acked);
detached subscriptions are skipped. The `topics/basic-publish-subscribe` example runs the full
`CreateTopic → CreateSubscription → Publish → Pull → Ack` round-trip.

## Batch Publish

`Publish` accepts a **batch of 1..1000** messages. Server-assigned ids are returned **in request
order**, so a client can correlate each id with its input message. The `topics/batch-publish`
example publishes a batch and prints the ids in order.

### Atomic batch validation

> **The whole batch is validated before anything is enqueued.** If any message fails validation,
> the **entire batch** is rejected with `INVALID_ARGUMENT` and **nothing is published** — there is
> no partial publish.

Per-message validation rules:

| Rule | Limit |
|------|-------|
| Batch size | 1..1000 messages |
| Total message size | ≤ 10 MiB |
| Attributes | ≤ 100 per message |
| Attribute key | ≤ 256 B, no `goog` prefix |
| Attribute value | ≤ 1024 B |
| Ordering key | ≤ 1024 B |
| Body | `data` **or** `attributes` must be non-empty |

If the topic has a **schema** (see [../concepts/schema-validation.md](../concepts/schema-validation.md)),
every message is also validated against it and the whole batch is rejected on the first
non-conforming message. The full limit table is in
[../reference/limits-and-rules.md](../reference/limits-and-rules.md).

## Ordering Keys

Set a per-message `ordering_key` and **enable ordering on the subscription**
(`enable_message_ordering`). Messages sharing an `ordering_key` are then delivered in **publish
order**, with **at most one in flight per key** — the head of a key blocks until it is acked or
redelivered, and redelivery is in order. A round-robin cursor spreads delivery fairly across
contended keys; keyless messages are delivered unordered.

> **Ordering is opt-in on the subscriber side.** Publishing with an `ordering_key` is necessary but
> not sufficient — the **subscription** must set `enable_message_ordering` for ordered delivery. The
> ordering key travels as the reserved tag `_pubsub_ordering_key`.

The `delivery/ordering-keys` example demonstrates per-key in-order delivery; the concept detail is
in [../concepts/ordering-keys.md](../concepts/ordering-keys.md) and the reliability angle in
[reliability.md](reliability.md).

## Message Attributes

A `PubsubMessage`'s `attributes` map (string → string) round-trips as KubeMQ message **tags**. On
top of the user attributes the connector carries **three reserved tags** across the wire:

- `_pubsub_message_id` — the server-assigned id;
- `_pubsub_publish_time` — the publish timestamp;
- `_pubsub_ordering_key` — the ordering key (if any).

> **Reserved tags are visible to native consumers, hidden from Pub/Sub clients.** A native KubeMQ
> consumer of `pubsub.{topic}` sees all three reserved tags plus the user attributes; when the
> connector delivers the message back to a Pub/Sub client, the reserved tags are **stripped** from
> `attributes`. See [../reference/channel-mapping.md](../reference/channel-mapping.md).

Attribute constraints (enforced in the atomic validation above): ≤ 100 attributes; key ≤ 256 B with
no `goog` prefix; value ≤ 1024 B.

## Concurrency & Resource Isolation

> The canonical channels are global: topic `orders` is always `pubsub.orders`. Concurrent runs that
> reuse the **same** topic/subscription names cross-talk on the same channels. Examples generate
> **uuid-suffixed** topic/subscription ids per run (e.g. `orders-<uuid8>`) and best-effort delete
> what they create, so parallel runs never collide. State the constraint in your own publishers
> too.

## Error Quick Reference

| Trigger | Result |
|---------|--------|
| Batch > 1000 messages, or any message > 10 MiB / > 100 attributes / oversize key/value | `INVALID_ARGUMENT` — **whole batch** rejected |
| `data` and `attributes` both empty | `INVALID_ARGUMENT` |
| Topic id starts with `sub.`, bad charset, or `goog` prefix | `INVALID_ARGUMENT` on `CreateTopic` |
| Ingestion config on `CreateTopic` | `INVALID_ARGUMENT` |
| Message fails the topic's schema | `INVALID_ARGUMENT` — **whole batch** rejected on first non-conforming message |

## See Also

- [subscribing.md](subscribing.md) — Pull / StreamingPull, ack deadlines, flow control.
- [filtering.md](filtering.md) — attribute filters applied at publish fan-out.
- [../concepts/topics-and-publish.md](../concepts/topics-and-publish.md) — the publish concept + cross-links to all 6 languages.
- [../reference/limits-and-rules.md](../reference/limits-and-rules.md) — the full limit table.
