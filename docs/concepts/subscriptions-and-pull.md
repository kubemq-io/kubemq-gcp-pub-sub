# Subscriptions & Pull

## Concept

A **subscription** is a named, durable cursor attached to a topic. Every message published to the
topic is fanned out into the subscription's own copy, and consumers pull from that copy, ack what
they process, and let unacked messages redeliver. In this connector a subscription maps onto a
native KubeMQ **Queue** channel:

| Pub/Sub resource | KubeMQ object | Channel |
|------------------|---------------|---------|
| Subscription `projects/{p}/subscriptions/{s}` | Queue | `pubsub.sub.{s}` |

A `Publish` to the topic writes once to the Events Store log `pubsub.{t}` and then fans out one
queue copy per subscription into `pubsub.sub.{s}`, applying that subscription's filter at fan-out.
Each subscription therefore sees an independent, filtered stream of the topic.

## How It Works

1. `CreateSubscription("orders-worker", topic="orders")` registers the subscription and maps it to
   `pubsub.sub.orders-worker`. Optional settings include the ack deadline, a CEL-subset `filter`
   (see [../guides/filtering.md](../guides/filtering.md)), `enable_message_ordering`,
   `enable_exactly_once_delivery`, a `dead_letter_topic`, and `push_config`.
2. `Pull` (unary) or `StreamingPull` (bidirectional) delivers messages, each carrying an opaque
   **`ack_id`** and leased for the ack deadline.
3. `Acknowledge(ack_ids)` acks the underlying broker sequences.
4. `ModifyAckDeadline` extends a lease (`> 0`, clamped to 10..600 s) or **nacks** it immediately
   (`0`) to force redelivery.

```
[ pubsub.orders ] ──fan-out──▶ [ pubsub.sub.orders-worker ] (Queue)
                                         │
                                         ▼
                            Pull / StreamingPull  ──▶ ReceivedMessage(ack_id, leased)
                                         │
                       ┌─────────────────┼──────────────────┐
                       ▼                 ▼                   ▼
                Acknowledge(ack_id)  ModifyAckDeadline(>0)  ModifyAckDeadline(0)=nack
                  (ack the seq)        (extend lease)        (redeliver now)
```

## Pull vs StreamingPull

- **`Pull`** is a unary RPC: ask for up to *N* messages, get what is available now (bounded by the
  flow-control ceiling), ack them, repeat.
- **`StreamingPull`** is a long-lived bidirectional stream: the connector pushes messages as they
  arrive, the client streams back acks and deadline modifications. It is the high-throughput path
  most client libraries use by default.

The connector **periodically closes** each StreamingPull stream (every
`CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS`, default **1800 s**) to force the SDK to reconnect. This
is normal — the client library transparently re-opens the stream and continues; no messages are
lost across the reconnect.

## Leases, Ack & Nack

Every delivered message carries an opaque `ack_id` =
`base64(JSON{subscription, channel, node id, broker txn id, seq, receive count, lease id, deadline})`,
held under an **ack-deadline lease** owned by the subscription (so an ack issued on a different
stream of the same subscription still works):

- `Acknowledge(ack_id)` → the broker acks the message's sequence.
- `ModifyAckDeadline(ack_id, 0)` → immediate **nack**; the message redelivers.
- `ModifyAckDeadline(ack_id, n)` → extend the lease by *n* seconds (clamped to 10..600).
- **Lease expiry** — a 250 ms sweeper expires overdue leases, applies retry backoff, and
  redelivers the message (or dead-letters it once the receive count exceeds the subscription's
  `max_delivery_attempts` — see [dead-letter-and-retry.md](dead-letter-and-retry.md)).

The default ack deadline is `CONNECTORS_PUB_SUB_DEFAULT_ACK_DEADLINE_SECONDS` (**10 s**); valid
deadlines are **0 (nack) or 10..600 s**.

> **Gotcha — `ack_id`s are node-local.** An `ack_id` minted on one cluster node is invalid on
> another. The lease, receive-count, and exactly-once state all live on the issuing node. In a
> cluster, pin a subscription's StreamingPull traffic to one node with a sticky load balancer, or
> accept at-least-once across nodes. See [exactly-once.md](exactly-once.md) and
> [../guides/reliability.md](../guides/reliability.md).

## Flow Control

Two ceilings bound how many un-acked messages a subscription can hold:

- **`CONNECTORS_PUB_SUB_MAX_OUTSTANDING_MESSAGES`** (default **1000**) — per-stream flow-control
  ceiling applied to clients that request unlimited outstanding messages.
- **`CONNECTORS_PUB_SUB_MAX_INFLIGHT_PER_SUBSCRIPTION`** (default **20000**) — a hard cap on the
  number of leased (un-acked) messages per subscription.

Client libraries also enforce their own outstanding-message / outstanding-bytes limits; the
connector ceilings are a server-side safety net. See [../guides/subscribing.md](../guides/subscribing.md).

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| basic-publish-subscribe | [go](../../examples/go/topics/basic-publish-subscribe/) | [python](../../examples/python/topics/basic_publish_subscribe/) | [java](../../examples/java/topics/basic-publish-subscribe/) | [js](../../examples/javascript/topics/basic-publish-subscribe/) | [csharp](../../examples/csharp/topics/basic-publish-subscribe/) | [ruby](../../examples/ruby/topics/basic_publish_subscribe/) |
| streaming-pull | [go](../../examples/go/subscriptions/streaming-pull/) | [python](../../examples/python/subscriptions/streaming_pull/) | [java](../../examples/java/subscriptions/streaming-pull/) | [js](../../examples/javascript/subscriptions/streaming-pull/) | [csharp](../../examples/csharp/subscriptions/streaming-pull/) | [ruby](../../examples/ruby/subscriptions/streaming_pull/) |
| ack-deadline-nack | [go](../../examples/go/subscriptions/ack-deadline-nack/) | [python](../../examples/python/subscriptions/ack_deadline_nack/) | [java](../../examples/java/subscriptions/ack-deadline-nack/) | [js](../../examples/javascript/subscriptions/ack-deadline-nack/) | [csharp](../../examples/csharp/subscriptions/ack-deadline-nack/) | [ruby](../../examples/ruby/subscriptions/ack_deadline_nack/) |

Attribute filtering at subscription create is covered in [../guides/filtering.md](../guides/filtering.md);
push delivery in [../guides/push-delivery.md](../guides/push-delivery.md).

## See Also

- [topics-and-publish.md](topics-and-publish.md) — the send side (`pubsub.{t}`).
- [ordering-keys.md](ordering-keys.md) — per-key in-order delivery.
- [exactly-once.md](exactly-once.md) — the node-local exactly-once boundary.
- [../guides/subscribing.md](../guides/subscribing.md) — pull, streaming pull, ack deadlines, flow control.
