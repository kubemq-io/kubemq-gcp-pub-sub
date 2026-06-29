# JavaScript/TypeScript — interop: SDK ⇄ native Events Store

Publish to a topic with the **official Google Pub/Sub client** (`@google-cloud/pubsub`), then consume the same payload **natively** from the topic's Events Store log:
`CreateTopic → (native) SubscribeToEventsStore("pubsub.orders", startAt=new) → Publish → native receive` — the same payload read off the Events Store log with the three reserved tags present.

## Prerequisites

- **Node.js 18+** (developed against Node 20+; works on current LTS) and the official GCP Pub/Sub
  client `@google-cloud/pubsub` **5.3.1** (pinned in `examples/javascript/package.json`; lockfile
  committed). Run with `tsx`.
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)
- **interop/native-events-store ONLY:** also requires the native **KubeMQ gRPC broker** at
  `localhost:50000` and the pinned native SDK `kubemq-js` **3.0.1** (the native Events Store peer;
  also pinned in `examples/javascript/package.json`). Override the native endpoint with
  `KUBEMQ_GRPC_ADDRESS` (default `localhost:50000`).

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
export KUBEMQ_GRPC_ADDRESS=localhost:50000     # native KubeMQ broker (interop half)
cd examples/javascript && npx tsx interop/native-events-store/index.ts
```

> This is a **single program** that drives both halves in one process: the Google Pub/Sub client
> and the native `kubemq-js` client, so you see the publish and the native receive in one run.

## Expected Output

```
=== interop: Pub/Sub publish -> native Events Store consume — topic 'orders-<uuid8>' / channel 'pubsub.orders-<uuid8>' ===
  pubsub : localhost:8085 (PUBSUB_EMULATOR_HOST; insecure, no Google auth)
  grpc   : localhost:50000 (native KubeMQ broker)

CreateTopic -> projects/my-project/topics/orders-<uuid8> (project 'my-project' parsed but ignored)
Connected native KubeMQ client to localhost:50000
native SubscribeToEventsStore('pubsub.orders-<uuid8>', startAt='new') -> stream open
topic.publishMessage('orders-<uuid8>') -> messageId=<message-id>
  [OK] native consumer received the published message on 'pubsub.orders-<uuid8>'
native receive('pubsub.orders-<uuid8>') (seq 1) -> body="order #4242 — cross-protocol via Pub/Sub"
    _pubsub_message_id = "<message-id>"
    _pubsub_publish_time = "<publish-time>"
    _pubsub_ordering_key = "customer-42"
    region (attribute) = "eu-west-1"
    priority (attribute) = "high"
  [OK] native side sees the Pub/Sub-published body
  [OK] all three reserved tags are present (_pubsub_message_id/publish_time/ordering_key)
  [OK] _pubsub_message_id matches the Pub/Sub-returned message id
  [OK] _pubsub_ordering_key carries the publish ordering key
  [OK] attribute 'region' rides along as an ordinary tag
  [OK] attribute 'priority' rides along as an ordinary tag
DeleteTopic('orders-<uuid8>') -> done

PASS — Pub/Sub publish consumed natively on the Events Store topic log.
```

> Resource ids are uuid-suffixed (`orders-<uuid8>`) so concurrent runs use distinct
> `pubsub.{topic}` channels and do not cross-talk; the native side derives its `pubsub.{t}` channel
> from the same generated id. The `<message-id>` and `<publish-time>` values vary per run.
> Exit 0 on success; any failed assertion (missing event, body mismatch, missing reserved tag)
> exits non-zero.

## What's Happening

The program uuid-suffixes the topic id (`orders-<uuid8>`) and derives the matching Events Store
channel `pubsub.orders-<uuid8>`. The high-level `@google-cloud/pubsub` `PubSub` client
**auto-detects** `PUBSUB_EMULATOR_HOST` (no `apiEndpoint` override needed, unlike C#): it clears
credentials and dials insecure gRPC. It `createTopic`s the topic and enables message ordering on
the publisher (`setPublishOptions({ messageOrdering: true })`) so an ordering key can be sent. It
then connects a native client with `KubeMQClient.create({ address })` — a single call that builds,
validates, and connects to the gRPC broker at `localhost:50000`.

To make publish-then-consume deterministic, it follows the **subscribe-before-publish** pattern:
it opens the native Events Store subscribe stream with `EventStoreStartPosition.StartFromNew`
(the kubemq-js enum mapped to gRPC `StartNewOnly`, value `1` — only events published *after* the
subscription is established) and confirms the stream is active (`subscription.isActive`) **before**
calling `topic.publishMessage`. It then publishes one message (data + an ordering key + two
attributes) and awaits exactly one event natively on `pubsub.orders-<uuid8>`. It asserts the body
matches, the three reserved tags (`_pubsub_message_id`, `_pubsub_publish_time`,
`_pubsub_ordering_key`) are present (the message-id equals the Pub/Sub-returned `messageId`, the
ordering key round-trips), and the Pub/Sub attributes ride along as ordinary tags. On exit it
cancels the native subscription, closes the client, and best-effort deletes the topic.

A Publish writes **once** to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source of truth) before any per-subscription fan-out to `pubsub.sub.{s}`; the native
side attaches to that same log via a native gRPC Events Store **subscribe** stream and sees the
reserved tags that are stripped from `attributes` for Pub/Sub clients. Any failed assertion exits
non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| Pub/Sub `CreateTopic` + `Publish` → native gRPC Events Store `SubscribeToEventsStore` (`startAt="new"`) | `orders-<uuid8>` / Events Store `pubsub.orders-<uuid8>` (no Pub/Sub subscription) | none (interop) | same payload read natively with the three reserved tags present; native gRPC Events Store subscribe via `kubemq-js` 3.0.1; subscribe-before-publish + `startAt = "new"`; resource id 3..255 chars, starts with a letter, no `goog` prefix, topic ids may not start with `sub.` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/interop/native-events-store`](../../../go/interop/native-events-store),
  [`../../../python/interop/native_events_store`](../../../python/interop/native_events_store),
  [`../../../java/interop/native-events-store`](../../../java/interop/native-events-store),
  [`../../../csharp/interop/native-events-store`](../../../csharp/interop/native-events-store),
  [`../../../ruby/interop/native_events_store`](../../../ruby/interop/native_events_store)
- Docs: [`../../../docs/concepts/cross-protocol-interop.md`](../../../docs/concepts/cross-protocol-interop.md)
  and [`../../../docs/reference/channel-mapping.md`](../../../docs/reference/channel-mapping.md)

> **Gotcha — the native side subscribes to a stream, not an offset.** A naive "publish then
> subscribe" races: the publish can land before the subscriber attaches and be missed. The canonical
> interop pattern opens the `startAt = "new"` subscription and confirms the stream is open *before*
> publishing. Examples needing pre-subscription history use `StartAtSequence`/`StartAtTime` instead.

> **Gotcha — the project id is ignored; channels are global.** A Pub/Sub publish to `orders` under
> any project lands on `pubsub.orders`. The native subscriber must use the exact `pubsub.{t}` channel
> derived from the topic id (here uuid-suffixed for parallel-safe runs), or it will not see the
> message. The reserved tags are visible only to native Events Store consumers — they are stripped
> from `attributes` for Pub/Sub clients.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
