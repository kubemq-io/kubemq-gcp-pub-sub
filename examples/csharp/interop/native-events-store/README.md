# C# — interop: SDK ⇄ native Events Store

A Pub/Sub publish bridged to a native KubeMQ consumer:
`CreateTopic → (native) SubscribeToEventsStore("pubsub.orders", startAt=new) → Publish → native receive` — the same payload read off the Events Store log with the three reserved tags present.

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client `Google.Cloud.PubSub.V1` **3.36.0**
  (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)
- **interop/native-events-store ONLY:** also requires the native **KubeMQ gRPC broker** at
  `localhost:50000` and the pinned native SDK `KubeMQ.SDK.csharp` **3.0.1** (the native Events
  Store peer; also pinned in `examples/csharp/Directory.Packages.props`). Override the native
  endpoint with `KUBEMQ_GRPC_ADDRESS` (default `localhost:50000`).

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
export KUBEMQ_GRPC_ADDRESS=localhost:50000   # native gRPC broker (interop half)
cd examples/csharp
dotnet run --project interop/native-events-store/NativeEventsStore.csproj
```

> This is a **single program** that drives both halves in one process: the Google Pub/Sub
> client and the native KubeMQ client, so you see the publish and the native receive in one run.

## Expected Output

```
[*] topic id           : orders-<uuid8>  (Events Store channel 'pubsub.orders-<uuid8>')
[*] PUBSUB_EMULATOR_HOST: localhost:8085
[*] native gRPC broker  : localhost:50000
[*] CreateTopic 'orders-<uuid8>' (project 'my-project' parsed but ignored)
[*] Connected native KubeMQ client to localhost:50000
[*] Events Store subscribe stream open on 'pubsub.orders-<uuid8>' (startAt=new)
[>] Pub/Sub Publish to 'orders-<uuid8>' → messageId=<message-id>
[<] native Events Store received on 'pubsub.orders-<uuid8>' (seq 1) → 'order #4242 — cross-protocol via Pub/Sub'
[v] reserved tag _pubsub_message_id   = <message-id>
[v] reserved tag _pubsub_publish_time = <publish-time>
[v] reserved tag _pubsub_ordering_key = customer-42
[v] attribute region = eu-west-1
[v] attribute priority = high
[*] Cleaned up topic 'orders-<uuid8>'
[PASS] Pub/Sub publish consumed natively on Events Store with reserved tags present
```

> Resource ids are uuid-suffixed (`orders-<uuid8>`), so concurrent runs use distinct
> `pubsub.orders-<uuid8>` channels and do not cross-talk; the native side derives its
> `pubsub.{t}` channel from the same generated id. `<message-id>` / `<publish-time>` vary per run.
> Exit 0 on success; any failed assertion (missing event, body mismatch, missing reserved tag)
> exits non-zero.

## What's Happening

The program uuid-suffixes the topic id (`orders-<uuid8>`) and derives the matching Events Store
channel `pubsub.orders-<uuid8>`. It builds a `PublisherServiceApiClient` with
`EmulatorDetection.EmulatorOnly` (the .NET client does not auto-detect the emulator from the env
var) and `CreateTopic`s the topic. It then connects a native `KubeMQClient` to the gRPC broker at
`localhost:50000`.

To make publish-then-consume deterministic, it follows the **subscribe-before-publish** pattern:
it opens the native Events Store subscribe stream with `EventStoreStartPosition.StartFromNew`
(the C# enum mapped to gRPC `StartNewOnly`, value `1` — only events published *after* the
subscription is established) and confirms the stream is open **before** calling `Publish`. It then
publishes one `PubsubMessage` (data + an ordering key + two attributes) and awaits exactly one
event natively on `pubsub.orders-<uuid8>`. It asserts the body matches, the three reserved tags
(`_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key`) are present (the
message-id equals the Pub/Sub `messageId`, the ordering key round-trips), and the Pub/Sub
attributes ride along as ordinary tags. On exit it best-effort deletes the topic.

A Publish writes **once** to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source of truth) before any per-subscription fan-out; the native side attaches to
that same log via a native gRPC Events Store **subscribe** stream and sees the reserved tags that
are stripped from `attributes` for Pub/Sub clients. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| Pub/Sub `CreateTopic` + `Publish` → native Events Store subscribe on `pubsub.{t}` | `orders-<uuid8>` / Events Store `pubsub.orders-<uuid8>` (no Pub/Sub subscription) | none (interop) | native gRPC Events Store subscribe via `KubeMQ.SDK.csharp` 3.0.1; subscribe-before-publish + `startAt = "new"`; the three reserved tags visible natively; resource id 3..255 chars, starts with a letter, no `goog` prefix, topic ids may not start with `sub.` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/interop/native-events-store`](../../../go/interop/native-events-store),
  [`../../../python/interop/native_events_store`](../../../python/interop/native_events_store),
  [`../../../java/interop/native-events-store`](../../../java/interop/native-events-store),
  [`../../../javascript/interop/native-events-store`](../../../javascript/interop/native-events-store),
  [`../../../ruby/interop/native_events_store`](../../../ruby/interop/native_events_store)
- Docs: [`../../../../docs/concepts/cross-protocol-interop.md`](../../../../docs/concepts/cross-protocol-interop.md),
  [`../../../../docs/reference/channel-mapping.md`](../../../../docs/reference/channel-mapping.md)

> **Gotcha — the project id is ignored; channels are global.** A Pub/Sub publish to `orders`
> under any project lands on `pubsub.orders`. The native subscriber must use the exact
> `pubsub.{t}` channel derived from the topic id (here uuid-suffixed for parallel-safe runs), or
> it will not see the message. The reserved tags are visible only to native Events Store
> consumers — they are stripped from `attributes` for Pub/Sub clients.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
