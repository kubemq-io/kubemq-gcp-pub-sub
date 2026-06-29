# Cross-Protocol Interop

## Concept

A Pub/Sub `Publish` writes the message **once** to the topic's Events Store log `pubsub.{t}` — the
authoritative, replayable source of truth — before fanning out per-subscription queue copies (see
[topics-and-publish.md](topics-and-publish.md)). That topic log is an ordinary native KubeMQ
**Events Store** channel, so a message published with the **Google Pub/Sub SDK** can be consumed
**natively** by a KubeMQ gRPC client on `pubsub.{t}`. A Pub/Sub publish to topic `orders` lands on
Events Store channel `pubsub.orders` and is readable by a native `kubemq` client subscribing to
`pubsub.orders`.

This lets you bridge a Pub/Sub-facing producer to a native KubeMQ consumer (or an analytics tap)
without changing the producer — it still speaks plain Pub/Sub.

## How It Works

```
Google Pub/Sub SDK ──Publish("orders")──▶ Array.SendEventsStore(pubsub.orders)
                                                  │  (written once — replayable log)
                                                  ▼
                              native KubeMQ gRPC client ──SubscribeToEventsStore("pubsub.orders")
                                  sees data + reserved tags:
                                  _pubsub_message_id, _pubsub_publish_time, _pubsub_ordering_key
```

The native consumer attaches via an **Events Store subscribe stream** (not a one-shot read) and
sees the **three reserved tags** the connector stamps on every message — `_pubsub_message_id`,
`_pubsub_publish_time`, `_pubsub_ordering_key` — which are stripped from `attributes` when the
message is delivered to a Pub/Sub client. The message `attributes` ride along as ordinary tags.
See [../reference/channel-mapping.md](../reference/channel-mapping.md).

This is the **only** place in the repo where a KubeMQ SDK appears. The Pub/Sub half of the
`interop/native-events-store` example is idiomatic Pub/Sub in each language; the native half uses
that language's KubeMQ SDK (all six use a **native gRPC Events Store subscribe** at
`localhost:50000`).

## Deterministic Read Start-Position (subscribe-before-publish)

Because the native side subscribes to a **stream** rather than reading a fixed offset, a naive
"publish then subscribe" has a startup race: the publish could land before the subscriber attaches
and be missed. The canonical interop pattern eliminates the race:

1. Open the native Events Store subscription with start policy **`startAt = "new"`** (only events
   published *after* the subscription is established).
2. **Block until the subscribe stream is confirmed open** (SDK ready/ack) **before** calling the
   Pub/Sub `Publish`.
3. Then publish, and await exactly one received event on `pubsub.{t}`.

Every published message is then guaranteed in-window for the native consumer — no missed message,
no duplicate from history.

Per-language "new only" start policy (all enum value `1`):

| Language | Native KubeMQ SDK (pinned) | "new only" start policy |
|----------|----------------------------|--------------------------|
| Go | `github.com/kubemq-io/kubemq-go/v2@v2.0.3` | `client.SubscribeToEventsStore(ctx, "pubsub.orders", "", kubemq.StartFromNewEvents())` |
| Python | `kubemq==4.1.5` | `EventsStoreSubscription(channel="pubsub.orders", events_store_type=EventStoreStartPosition.StartFromNew)` |
| Java | `io.kubemq.sdk:kubemq-sdk-Java:3.1.1` | events-store `StartNewOnly` / `StartFromNewEvents` |
| Node/TS | `kubemq-js@3.0.1` | events-store `StartNewOnly` / `StartFromNewEvents` |
| C# | `KubeMQ.SDK.csharp@3.0.1` | events-store `StartNewOnly` / `StartFromNewEvents` |
| Ruby | gem `kubemq ~> 1.0` (`1.0.0`, Ruby ≥ 3.1) | `EventsStoreSubscription.new(channel: "pubsub.orders", start_position: EventStoreStartPosition::START_NEW_ONLY)` |

> **All six languages use a native gRPC Events Store subscribe — no REST-gateway fallback.** The
> sibling `kubemq-aws` repo's Ruby interop used the KubeMQ REST queue gateway because no native
> Ruby gRPC SDK existed then; the `kubemq-ruby` gem v1.0.0 now provides
> `subscribe_to_events_store`, so Ruby joins the other five with a native subscribe.

An example that needs to consume a message published **before** the subscriber attaches would use
`StartAtSequence` / `StartAtTime` instead; the canonical interop demo uses `new` +
subscribe-first to stay deterministic.

## Direction & Scope

This is a **one-way bridge for the demo**: Pub/Sub publish → native Events Store consume. The
topic log is the shared artifact. The reverse (a native producer writing to `pubsub.{t}` so a
Pub/Sub subscription consumes it) works because the log is the same channel, but the canonical
example demonstrates the Pub/Sub→native direction with the reserved tags visible.

> **Gotcha — the project id is ignored; channels are global.** A Pub/Sub publish to `orders`
> under any project lands on `pubsub.orders`. The native subscriber must use the exact
> `pubsub.{t}` channel name derived from the topic id (with the same uuid suffix if the example
> isolates resources for parallel runs — see the example README's concurrency note).

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| native-events-store | [go](../../examples/go/interop/native-events-store/) | [python](../../examples/python/interop/native_events_store/) | [java](../../examples/java/interop/native-events-store/) | [js](../../examples/javascript/interop/native-events-store/) | [csharp](../../examples/csharp/interop/native-events-store/) | [ruby](../../examples/ruby/interop/native_events_store/) |

> The native (Events Store subscribe) half is the **only** place a KubeMQ SDK appears in this repo;
> the Pub/Sub half is idiomatic in each language. The variant ships in all six languages.

## See Also

- [topics-and-publish.md](topics-and-publish.md) — the topic log `pubsub.{t}` is written once and replayable.
- [seek-and-snapshots.md](seek-and-snapshots.md) — replaying the same log on the Pub/Sub side.
- [../reference/channel-mapping.md](../reference/channel-mapping.md) — `pubsub.{t}` + reserved-tag mapping.
- [../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md) — the two endpoints (`:8085` emulator + `:50000` native gRPC).
