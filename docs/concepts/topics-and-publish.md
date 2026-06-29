# Topics & Publish

## Concept

A **Pub/Sub topic** is the named entry point for messages. A publisher sends one or more messages
to a topic; the connector durably records them and fans a copy out to every subscription attached
to that topic. In this connector a topic maps onto a native KubeMQ **Events Store** log:

| Pub/Sub resource | KubeMQ object | Channel |
|------------------|---------------|---------|
| Topic `projects/{p}/topics/{t}` | Events Store log | `pubsub.{t}` |

The `{p}` project segment is **parsed and validated but ignored** — the connector is single-tenant
like Google's local emulator, so topic ids are global across "projects". The topic log
`pubsub.{t}` is the **authoritative, cross-protocol, replayable source of truth**: a `Publish`
writes there **once** (`Array.SendEventsStore(pubsub.{t})`) and then fans out one queue copy per
subscription. Native KubeMQ gRPC clients can read the same log — see
[cross-protocol-interop.md](cross-protocol-interop.md).

## How It Works

1. `CreateTopic("orders")` registers the topic (registry-authoritative, BoltDB + cluster-synced)
   and maps it to the Events Store log `pubsub.orders`. Topic creation is idempotent.
2. `Publish` validates the **whole batch** before enqueuing anything (see *Publish atomicity*
   below), then writes every accepted message **once** to `pubsub.orders` and fans out one copy
   to each subscription's queue `pubsub.sub.{s}`, applying each subscription's filter at fan-out.
3. Each accepted message is assigned a server `message_id` and a `publish_time`; the publish
   response returns the ids **in request order**.

```
publisher ──Publish(batch)──▶ validate-all (atomic)
                                   │
                                   ▼
                          [ pubsub.orders ]   (Events Store log — written once, replayable)
                                   │ fan-out at publish time (filter applied per sub)
                       ┌───────────┼────────────┐
                       ▼           ▼             ▼
                pubsub.sub.s1  pubsub.sub.s2  pubsub.sub.s3   (one Queue copy per subscription)
```

## Publish Atomicity & Limits

The connector validates the entire batch **before** enqueuing any message — any single failure
rejects the whole batch with `INVALID_ARGUMENT`, so a publish is all-or-nothing:

| Rule | Value |
|------|-------|
| Batch size | 1..1000 messages |
| Message size | ≤ 10 MiB total (`CONNECTORS_PUB_SUB_MAX_MESSAGE_BYTES`, default 10 MiB) |
| Attributes | ≤ 100; key ≤ 256 B (no `goog` prefix); value ≤ 1024 B |
| Ordering key | ≤ 1024 B |
| Body | `data` **or** `attributes` must be non-empty |

A message id is server-assigned and the response preserves request order. See
[../reference/limits-and-rules.md](../reference/limits-and-rules.md) for the full table.

> **Gotcha — topic ids may not start with `sub.`.** Resource ids are 3..255 chars, must start
> with a letter, use the charset `[A-Za-z0-9._~%+-]`, and carry no `goog` prefix. Topic ids
> additionally may **not** begin with `sub.` — that prefix is reserved for the subscription
> channel namespace (`pubsub.sub.{s}`). A violating id is rejected with `INVALID_ARGUMENT`.

> **Gotcha — the project id is parsed but ignored.** Any `PUBSUB_PROJECT_ID` works; resource ids
> are global across projects. Two "different" projects publishing to topic `orders` share the
> same `pubsub.orders` log.

## Reserved Tags

A `PubsubMessage` becomes a KubeMQ message whose body is `data` and whose tags are the message
`attributes` plus **three reserved tags** the connector stamps on every message:

- `_pubsub_message_id` — the server-assigned id
- `_pubsub_publish_time` — the publish timestamp
- `_pubsub_ordering_key` — the ordering key (if any)

Native consumers on `pubsub.{t}` see these tags; they are stripped from `attributes` before a
message is delivered back to a Pub/Sub client. See
[../reference/channel-mapping.md](../reference/channel-mapping.md).

## Topic Management & Retention

`ListTopics`, `GetTopic`, `UpdateTopic`, `ListTopicSubscriptions`, and `DeleteTopic` (tombstone)
round out topic lifecycle. Per-topic message retention is requested in the range **10 min .. 31
days** but is **clamped to the broker's `Store.MaxRetention`** ceiling: `GetTopic` echoes the
*requested* value, while fan-out, seek, and the dashboard use the *clamped* value.

> **Gotcha — retention is clamped to the broker ceiling.** A topic configured for 31-day
> retention on a broker whose `Store.MaxRetention` is shorter retains for the shorter window. The
> `GetTopic` response still shows what you asked for, not what you got.

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| basic-publish-subscribe | [go](../../examples/go/topics/basic-publish-subscribe/) | [python](../../examples/python/topics/basic_publish_subscribe/) | [java](../../examples/java/topics/basic-publish-subscribe/) | [js](../../examples/javascript/topics/basic-publish-subscribe/) | [csharp](../../examples/csharp/topics/basic-publish-subscribe/) | [ruby](../../examples/ruby/topics/basic_publish_subscribe/) |
| batch-publish | [go](../../examples/go/topics/batch-publish/) | [python](../../examples/python/topics/batch_publish/) | [java](../../examples/java/topics/batch-publish/) | [js](../../examples/javascript/topics/batch-publish/) | [csharp](../../examples/csharp/topics/batch-publish/) | [ruby](../../examples/ruby/topics/batch_publish/) |
| topic-management | [go](../../examples/go/topics/topic-management/) | [python](../../examples/python/topics/topic_management/) | [java](../../examples/java/topics/topic-management/) | [js](../../examples/javascript/topics/topic-management/) | [csharp](../../examples/csharp/topics/topic-management/) | [ruby](../../examples/ruby/topics/topic_management/) |

Schema-enforced publishing is covered in [schema-validation.md](schema-validation.md); ordered
publishing in [ordering-keys.md](ordering-keys.md).

## See Also

- [subscriptions-and-pull.md](subscriptions-and-pull.md) — the receive side (`pubsub.sub.{s}`).
- [../guides/publishing.md](../guides/publishing.md) — basic + batch + ordering + attributes.
- [../reference/channel-mapping.md](../reference/channel-mapping.md) — `pubsub.{t}` grammar + reserved-tag table.
- [../reference/limits-and-rules.md](../reference/limits-and-rules.md) — the full limits table.
