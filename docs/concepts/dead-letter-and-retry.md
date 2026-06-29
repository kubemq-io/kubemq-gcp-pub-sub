# Dead-Letter & Retry

## Concept

When a message cannot be processed, the consumer nacks it (or lets the lease expire) and the
connector **redelivers** it with backoff. A message that keeps failing should not loop forever — a
subscription with a **dead-letter topic** moves a message that has been delivered too many times
to a separate topic for inspection, instead of retrying it indefinitely. This is the connector's
poison-message safety valve.

## How It Works

1. A subscription is created with a `dead_letter_topic` and a `max_delivery_attempts` value.
2. Each delivery increments the message's receive count. A nack
   (`ModifyAckDeadline(0)`) or an expired lease triggers redelivery with retry backoff (the 250 ms
   lease sweeper drives this).
3. When the **receive count exceeds `max_delivery_attempts`**, the sweeper **republishes** the
   message to the `dead_letter_topic` and **acks the original** — so it leaves the source
   subscription and lands in the DLQ's own topic log / subscriptions.

```
[ pubsub.sub.orders-worker ]
        │ deliver → nack/expire → redeliver (backoff)   (receive count++)
        │ ... repeat ...
        ▼  receive count > max_delivery_attempts
   republish to dead_letter_topic  +  ack the original
        │
        ▼
[ pubsub.dead-letters ]  ──▶ its own subscriptions (inspect / reprocess)
```

This is **connector-level fan-out to the DLQ**: the dead-letter topic is an ordinary Pub/Sub topic
(`pubsub.{dlt}`), so you attach a subscription to it and consume the failed messages like any
other.

## `max_delivery_attempts` Range

> **Gotcha — `max_delivery_attempts` must be 5..100.** A value of `0` means *unset* (no
> dead-lettering — the message redelivers indefinitely). Any non-zero value **must** be in the
> range **5..100**; outside that range the create is rejected with `INVALID_ARGUMENT`. Google's
> own minimum is 5, and this connector enforces the same floor.

## Retry Backoff

Redelivery is not immediate-and-tight: the lease sweeper applies a retry backoff before
re-leasing a nacked or expired message, so a transiently failing message gets spaced-out retries
rather than a hot loop. Ordering keys keep their in-order guarantee across redeliveries — an
ordered message redelivers before any later message for the same key (see
[ordering-keys.md](ordering-keys.md)).

## Interaction With Other Features

- **Exactly-once** — dead-lettering is still triggered by receive count; with exactly-once, the
  ack of the original (when it moves to the DLQ) is confirmed like any other ack. The node-local
  boundary still applies (see [exactly-once.md](exactly-once.md)).
- **Push delivery** — a push subscription that exhausts its retries dead-letters to the configured
  topic if one is set, otherwise the message is dropped (see
  [../guides/push-delivery.md](../guides/push-delivery.md)).

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| dead-letter-topic | [go](../../examples/go/delivery/dead-letter-topic/) | [python](../../examples/python/delivery/dead_letter_topic/) | [java](../../examples/java/delivery/dead-letter-topic/) | [js](../../examples/javascript/delivery/dead-letter-topic/) | [csharp](../../examples/csharp/delivery/dead-letter-topic/) | [ruby](../../examples/ruby/delivery/dead_letter_topic/) |
| ack-deadline-nack | [go](../../examples/go/subscriptions/ack-deadline-nack/) | [python](../../examples/python/subscriptions/ack_deadline_nack/) | [java](../../examples/java/subscriptions/ack-deadline-nack/) | [js](../../examples/javascript/subscriptions/ack-deadline-nack/) | [csharp](../../examples/csharp/subscriptions/ack-deadline-nack/) | [ruby](../../examples/ruby/subscriptions/ack_deadline_nack/) |

## See Also

- [subscriptions-and-pull.md](subscriptions-and-pull.md) — leases, nack, the 250 ms sweeper.
- [topics-and-publish.md](topics-and-publish.md) — the dead-letter topic is an ordinary topic.
- [../guides/reliability.md](../guides/reliability.md) — DLQ, ordering, exactly-once, at-least-once.
- [../reference/limits-and-rules.md](../reference/limits-and-rules.md) — the `max_delivery_attempts` 5..100 rule.
