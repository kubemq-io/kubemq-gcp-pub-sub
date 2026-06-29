# Ordering Keys

## Concept

By default Pub/Sub delivery is **unordered** — messages can be delivered and redelivered in any
order. An **ordering key** is a per-message string that asks the connector to deliver all messages
sharing that key **in publish order**. Ordering is enabled per subscription with
`enable_message_ordering`; messages with no ordering key remain unordered even on an
ordering-enabled subscription.

## How It Works

1. The publisher sets a per-message `ordering_key` (the connector carries it as the reserved tag
   `_pubsub_ordering_key`). To publish ordered, the client library must enable message ordering on
   the publisher side as well, which serializes publishes per key.
2. The subscription is created with `enable_message_ordering = true`.
3. The connector delivers messages for each key in publish order, with **at most one message in
   flight per ordering key** at a time. The next message for a key is only delivered after the
   current one is acked (or redelivered).
4. **Redelivery stays in order** — a nacked or lease-expired ordered message is redelivered before
   any later message for the same key.
5. A **round-robin cursor** spreads delivery fairly across contended keys, so one busy key does not
   starve the others.

```
publish order on key "cust-7":  m1 ─ m2 ─ m3
                                   │
                                   ▼ (at most one in flight per key)
deliver:  m1 ──ack──▶ m2 ──ack──▶ m3
          (if m1 nacks/expires, m1 redelivers BEFORE m2)

keyless messages: delivered unordered, interleaved freely
```

## Per-Key, Not Global

Ordering is a guarantee **within** an ordering key, not across keys. Two different keys
(`cust-7` and `cust-9`) are independent and interleave freely; only the sequence of messages that
share one key is preserved. This is what lets the connector keep throughput high — distinct keys
run concurrently.

> **Gotcha — one in flight per key throttles a hot key.** Because at most one message per key is
> outstanding at a time, a single ordering key is effectively serialized: throughput for that key
> is bounded by its round-trip ack latency. Spread load across many keys (e.g. per-customer or
> per-entity keys) rather than funneling everything through one key.

> **Gotcha — ordering needs publisher-side enablement too.** In-order delivery only holds if the
> publisher also published in order for that key. Most client libraries require you to enable
> message ordering on the publisher (which serializes per-key publishes and surfaces publish
> errors that would otherwise break the order). Enabling it only on the subscription is not enough.

## Interaction With Other Features

- **Redelivery / DLQ** — an ordered message that exhausts its delivery attempts is dead-lettered
  (see [dead-letter-and-retry.md](dead-letter-and-retry.md)); delivery for that key then resumes
  with the next message.
- **Seek** — replay after a seek re-applies the same in-order-per-key delivery on the replayed
  messages (see [seek-and-snapshots.md](seek-and-snapshots.md)).

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| ordering-keys | [go](../../examples/go/delivery/ordering-keys/) | [python](../../examples/python/delivery/ordering_keys/) | [java](../../examples/java/delivery/ordering-keys/) | [js](../../examples/javascript/delivery/ordering-keys/) | [csharp](../../examples/csharp/delivery/ordering-keys/) | [ruby](../../examples/ruby/delivery/ordering_keys/) |

## See Also

- [subscriptions-and-pull.md](subscriptions-and-pull.md) — leases, ack/nack, redelivery.
- [exactly-once.md](exactly-once.md) — combining ordering with exactly-once delivery.
- [../guides/reliability.md](../guides/reliability.md) — ordering, exactly-once, DLQ together.
