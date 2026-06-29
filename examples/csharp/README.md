# C# / .NET — KubeMQ GCP Pub/Sub examples

Native **Google Cloud Pub/Sub client for .NET** (`Google.Cloud.PubSub.V1`) examples
that talk to the KubeMQ **Pub/Sub connector** through the standard Google emulator
contract. This is NOT a KubeMQ SDK — every program uses the real Google Pub/Sub
client exactly as it would against Google Cloud Pub/Sub, and simply points it at the
connector by setting `PUBSUB_EMULATOR_HOST` (zero code changes).

Pinned packages (Central Package Management in
[`Directory.Packages.props`](Directory.Packages.props); target `net8.0`):

| Package | Version | Used by |
|---------|---------|---------|
| `Google.Cloud.PubSub.V1` | `3.36.0` | every Pub/Sub variant |
| `KubeMQ.SDK.csharp` | `3.0.1` | `interop/native-events-store` only (native gRPC Events Store peer) |

Conventions (the 15-variant master table, env vars, the per-example README template,
the nine Pub/Sub gotchas, and the GCP client version policy) live in
[`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- .NET SDK **8.0**
- A running KubeMQ broker with the **Pub/Sub connector** enabled
  (`CONNECTORS_GCP_ENABLE=true`, emulator-protocol gRPC port **8085**), reachable
  via `PUBSUB_EMULATOR_HOST` (default `localhost:8085`). The `interop/native-events-store`
  variant also needs the native **KubeMQ gRPC broker** at `localhost:50000`.

## The connection (emulator-protocol drop-in)

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085   # connector default gRPC port
export PUBSUB_PROJECT_ID=my-project          # any id; the project segment is parsed but ignored
```

- **No bespoke wrapper var, no auth, no TLS.** When `PUBSUB_EMULATOR_HOST` is set the
  Google client clears credentials, skips Google auth, and uses insecure gRPC — exactly
  as against Google's local emulator. The standard Google env var IS the contract.
- **C# needs explicit emulator detection (idiom trap).** Unlike Go/Python/Node, the .NET
  client does not auto-detect the emulator from the env var alone; build clients with
  `EmulatorDetection.EmulatorOnly` (or `.EmulatorOrProduction`), e.g.
  `new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build()`.
- **gRPC only** — the connector exposes no REST/JSON v1 surface.
- **Do not expose port 8085 to untrusted networks.** DoS guards stay active, but there is
  no authentication on the emulator listener.

## Build & Run

```bash
dotnet build KubeMQ.GcpPubSub.Examples.sln
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
dotnet run --project topics/basic-publish-subscribe/BasicPublishSubscribe.csproj
```

Each variant is its own console project; run any of them with
`dotnet run --project <group>/<variant>/<Project>.csproj`. Every example prints clear
progress and **exits non-zero on any failed assertion** — they are runnable proofs, not
demos.

## Variant index

| # | Group | Variant | Project dir |
|---|-------|---------|-------------|
| 1 | topics | Basic publish/subscribe | [`topics/basic-publish-subscribe`](topics/basic-publish-subscribe) |
| 2 | topics | Batch publish | [`topics/batch-publish`](topics/batch-publish) |
| 3 | topics | Topic management | [`topics/topic-management`](topics/topic-management) |
| 4 | subscriptions | Streaming pull | [`subscriptions/streaming-pull`](subscriptions/streaming-pull) |
| 5 | subscriptions | Ack deadline + nack | [`subscriptions/ack-deadline-nack`](subscriptions/ack-deadline-nack) |
| 6 | delivery | Ordering keys | [`delivery/ordering-keys`](delivery/ordering-keys) |
| 7 | delivery | Message filtering | [`delivery/message-filtering`](delivery/message-filtering) |
| 8 | delivery | Dead-letter topic | [`delivery/dead-letter-topic`](delivery/dead-letter-topic) |
| 9 | delivery | Push delivery | [`delivery/push-delivery`](delivery/push-delivery) |
| 10 | delivery | Exactly-once | [`delivery/exactly-once`](delivery/exactly-once) |
| 11 | advanced | Seek to timestamp | [`advanced/seek-timestamp`](advanced/seek-timestamp) |
| 12 | advanced | Snapshots | [`advanced/snapshots`](advanced/snapshots) |
| 13 | advanced | Schema (Avro) | [`advanced/schema-avro`](advanced/schema-avro) |
| 14 | advanced | Schema (Protobuf) | [`advanced/schema-protobuf`](advanced/schema-protobuf) |
| 15 | interop | SDK ⇄ native Events Store | [`interop/native-events-store`](interop/native-events-store) |

## Idiom notes (Google.Cloud.PubSub.V1)

- **Emulator detection:** the .NET client requires explicit `EmulatorDetection` on the
  builder (`EmulatorOnly` / `EmulatorOrProduction`); it does not auto-detect from the env
  var the way the Go/Python/Node clients do. Build every `PublisherServiceApiClient` /
  `SubscriberServiceApiClient` (and the higher-level `PublisherClient` / `SubscriberClient`)
  with that option set.
- **Ordering-key enablement:** enable ordering at both ends — set
  `enable_message_ordering` on the subscription at create time, and set
  `EnableMessageOrdering = true` on the `PublisherClient.Settings` before publishing with
  an ordering key. Same-key messages are delivered in publish order, at most one in flight
  per key.
- **Exactly-once ack-result handling:** with `enable_exactly_once_delivery`, ack/nack
  return an awaitable result; await it and treat the outcome as authoritative (expired ack
  ids surface as permanent failures, transient ones may be retried). Exactly-once is
  **node-local** — pin the StreamingPull to one node (sticky load balancing).
- **Resource cleanup / async API:** every operation is `*Async` and is `await`ed; clients
  built from the builders are disposed when done. Examples uuid-suffix topic/subscription
  ids per run and best-effort delete them on exit, so concurrent runs use distinct
  `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide.

## Layout

```
examples/csharp/
├── Directory.Build.props          # net8.0, nullable, implicit usings (shared MSBuild props)
├── Directory.Packages.props       # central package management — pinned versions
├── KubeMQ.GcpPubSub.Examples.sln  # solution: one folder per concept group
├── topics/                        # 3 variants
├── subscriptions/                 # 2 variants
├── delivery/                      # 5 variants
├── advanced/                      # 4 variants
└── interop/                       # 1 variant (native Events Store)
```

Each variant directory holds one console `.csproj` + `Program.cs` + a per-example
`README.md` (the 8-section template in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md)).
Variant directory names are kebab-case.

---

> **Connectivity:** these examples use the emulator-protocol drop-in. See
> `../../docs/guides/connectivity-and-emulator-mode.md` for the env vars, the
> no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
