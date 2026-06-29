# C# — subscriptions: Streaming pull

A long-lived StreamingPull round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish → StreamingPull (flow control) → ack over the stream`.

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client `Google.Cloud.PubSub.V1` **3.36.0**
  (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set (C# requires explicit
  `EmulatorDetection.EmulatorOnly` on the client builder — see the gotcha below):
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/csharp && dotnet run --project subscriptions/streaming-pull/StreamingPull.csproj
```

## Expected Output

```
[*] Created topic 'orders-<uuid8>' → channel pubsub.orders-<uuid8>
[*] Created subscription 'sub-orders-<uuid8>' → channel pubsub.sub.sub-orders-<uuid8>
[x] Published 5 messages (ids in request order)
[*] Opened StreamingPull stream (StreamAckDeadlineSeconds=30, MaxOutstandingMessages=10)
[v] Received 'order #1001' messageId=<message-id> (over stream)
[v] Received 'order #1002' messageId=<message-id> (over stream)
[v] Received 'order #1003' messageId=<message-id> (over stream)
[v] Received 'order #1004' messageId=<message-id> (over stream)
[v] Received 'order #1005' messageId=<message-id> (over stream)
[*] Acked 5 message(s) over the stream
[ok] StreamingPull delivered + acked all 5 messages
[*] Cleaned up subscription 'sub-orders-<uuid8>' and topic 'orders-<uuid8>'
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.orders-<uuid8>` / `pubsub.sub.sub-orders-<uuid8>` channels and do not collide; the
> `<uuid8>` suffix and each `<message-id>` vary per run. Exits `0` on success, non-zero on any
> failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a uuid-suffixed topic and subscription, publishes a 5-message batch, then
opens a **StreamingPull** bidirectional stream. The first request on the stream carries the
subscription name plus per-stream **flow control** (`StreamAckDeadlineSeconds=30`,
`MaxOutstandingMessages=10`); the connector then pushes `ReceivedMessages` down the response half
of the duplex stream. As each batch arrives the program collects the `ack_id`s and writes them
back **on the same stream** to acknowledge — leases are subscription-owned, so a stream-ack
resolves the broker sequence regardless of which stream delivered it. When all five distinct
messages have been seen and acked, the client half-closes the stream (`WriteCompleteAsync`); the
connector also closes idle streams every `StreamCloseSeconds` so SDKs transparently reconnect.

A Publish writes once to the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source), then fans out one queue copy per subscription `pubsub.sub.{s}`; StreamingPull reads from
that per-sub queue through a poller and leases each delivered message under an ack-deadline lease.
Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `StreamingPull` (open + ack) | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | batch ≤ 1000; per-stream flow control `MaxOutstandingMessages` (≤ 0 = unlimited, capped by `MaxInflightPerSubscription`); ack deadline 0 or 10..600s; streams force-closed every `StreamCloseSeconds` (default 1800s) → SDK reconnect |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/subscriptions/streaming-pull`](../../../go/subscriptions/streaming-pull),
  [`../../../python/subscriptions/streaming_pull`](../../../python/subscriptions/streaming_pull),
  [`../../../java/subscriptions/streaming-pull`](../../../java/subscriptions/streaming-pull),
  [`../../../javascript/subscriptions/streaming-pull`](../../../javascript/subscriptions/streaming-pull),
  [`../../../ruby/subscriptions/streaming_pull`](../../../ruby/subscriptions/streaming_pull)
- Docs: [`../../../../docs/concepts/subscriptions-and-pull.md`](../../../../docs/concepts/subscriptions-and-pull.md)

> The project id is parsed and validated but **ignored** (single-tenant, like the emulator):
> resource ids are global across "projects", which is exactly why this example uuid-suffixes the
> topic/subscription names so parallel runs occupy private `pubsub.{t}` / `pubsub.sub.{s}` channels.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
