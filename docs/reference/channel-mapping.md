# Channel Mapping

Every Pub/Sub resource maps onto a native KubeMQ object on a deterministic channel. This is the
contract that makes cross-protocol interop work: a Pub/Sub `Publish` and a native KubeMQ consume
meet on the same channel.

## Resource → Channel Table

| Pub/Sub resource | KubeMQ object | Channel |
|------------------|---------------|---------|
| Topic `projects/{p}/topics/{t}` | Events Store log | `pubsub.{t}` |
| Subscription `projects/{p}/subscriptions/{s}` | Queue | `pubsub.sub.{s}` |
| Snapshot / Schema | Registry record (BoltDB) | — (no channel) |

The `{p}` **project segment is parsed and validated but ignored** — the connector is
single-tenant, like the emulator, so resource ids are **global across projects** (gotcha #2). Two
clients using different project ids but the same topic id share one `pubsub.{t}` log.

## Topic Grammar

A topic maps to exactly one KubeMQ **Events Store** log:

```
pubsub.{t}
└──┬──┘ └┬┘
   │     └─ the topic id (the bare id you pass to CreateTopic — NOT the projects/.../topics/ path)
   └─ fixed connector prefix ("pubsub.")
```

| Pub/Sub topic | KubeMQ channel |
|---------------|----------------|
| `orders` | `pubsub.orders` |
| `events` | `pubsub.events` |
| `audit-log` | `pubsub.audit-log` |

> **Topic ids may not start with `sub.`** (gotcha #7) — `sub.` is the reserved broker namespace
> for the subscription queues below. A topic id beginning `sub.` is rejected at create
> (`INVALID_ARGUMENT`). See [limits-and-rules.md](limits-and-rules.md) for the full id grammar.

## Subscription Grammar

Each subscription maps to its own KubeMQ **Queue** channel:

```
pubsub.sub.{s}
└────┬────┘ └┬┘
     │       └─ the subscription id (bare, not the projects/.../subscriptions/ path)
     └─ fixed connector prefix + reserved sub-namespace ("pubsub.sub.")
```

| Pub/Sub subscription | KubeMQ channel |
|----------------------|----------------|
| `orders-sub` | `pubsub.sub.orders-sub` |
| `analytics` | `pubsub.sub.analytics` |

## Write-Once-Then-Fan-Out

A `Publish` is **not** copied to every subscription on the wire. The connector:

1. **Writes once** to the topic log `pubsub.{t}` via `Array.SendEventsStore` — the authoritative,
   cross-protocol, replayable copy and the source for `Seek`.
2. **Fans out** one queue copy per subscription via `Array.SendQueueMessage(pubsub.sub.{s})`,
   **applying each subscription's filter** — a filtered-out message is never enqueued (≈
   auto-acked). Detached subscriptions are skipped.

So the topic log holds the complete history; each subscription queue holds the filtered slice that
sub still owes its consumers. See [../concepts/topics-and-publish.md](../concepts/topics-and-publish.md).

## Reserved Tags

A `PubsubMessage` becomes a KubeMQ message whose **body is `data`** and whose **Tags are the
message `attributes`** plus **three reserved tags** carried across the wire:

| Reserved tag | Carries | Set by |
|--------------|---------|--------|
| `_pubsub_message_id` | the server-assigned message id | connector on `Publish` |
| `_pubsub_publish_time` | the publish timestamp | connector on `Publish` |
| `_pubsub_ordering_key` | the ordering key (only if one was set) | connector on `Publish` |

> **Native consumers see these tags; Pub/Sub clients do not.** When the connector delivers a
> message back to a Pub/Sub client it strips the three `_pubsub_*` tags out of `attributes` and
> surfaces them as the native fields (`messageId`, `publishTime`, `orderingKey`). A native
> KubeMQ consumer reading `pubsub.{t}` reads them as ordinary Tags — this is how the
> `interop/native-events-store` example recovers the message id and publish time.

### Attribute ⇄ Tag round-trip

| Pub/Sub field | KubeMQ Tag | Notes |
|---------------|-----------|-------|
| message `attribute {Name}` | Tag `{Name}` | round-trips losslessly; ≤ 100 attrs, key ≤ 256 B (no `goog`), value ≤ 1024 B |
| `data` | message body | ≤ 10 MiB total per message |
| ordering key | `_pubsub_ordering_key` (reserved) | ≤ 1024 B |
| `(server-assigned)` message id | `_pubsub_message_id` (reserved) | |
| `(server-assigned)` publish time | `_pubsub_publish_time` (reserved) | |

Attribute keys must not start with `goog` (Google's reserved prefix); the connector enforces this
at publish (`INVALID_ARGUMENT`). See [limits-and-rules.md](limits-and-rules.md).

## Cross-Protocol Interoperability

Because the topic log is a normal KubeMQ **Events Store** channel, a Pub/Sub `Publish` to topic
`orders` is consumable by a native gRPC/REST Events Store subscriber on channel `pubsub.orders`,
and a subscription's backlog is a native **Queue** channel `pubsub.sub.{s}`. This is the
foundation of the `interop/native-events-store` example and the burn-in `cross_protocol_interop`
worker.

> **Deterministic read.** The canonical interop demo subscribes to the Events Store log with start
> policy `startAt = "new"` **before** issuing the Pub/Sub publish, so the published message is
> guaranteed in-window for the native consumer (no startup race). See
> [../concepts/cross-protocol-interop.md](../concepts/cross-protocol-interop.md).

## Registry Is Authoritative

Topics, subscriptions, snapshots, and schemas live in a per-node BoltDB **registry** (NATS-synced
across a cluster). Only resources created through the Pub/Sub API are visible to the Pub/Sub
surface — a `Pull` from a subscription that was never `CreateSubscription`d returns `NOT_FOUND`.
(A native client can still read the raw `pubsub.{t}` log directly regardless of the registry.)

## See Also

- [../architecture.md](../architecture.md) — the gRPC listener + interceptor chain + fan-out model.
- [../concepts/topics-and-publish.md](../concepts/topics-and-publish.md) — write-once-then-fan-out.
- [../concepts/cross-protocol-interop.md](../concepts/cross-protocol-interop.md) — native consume on `pubsub.{t}`.
- [limits-and-rules.md](limits-and-rules.md) — resource id grammar + attribute limits.
