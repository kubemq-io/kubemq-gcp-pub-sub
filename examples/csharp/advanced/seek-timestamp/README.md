# C# — advanced: Seek to Timestamp

Rewind a subscription to a wall-clock instant and replay the topic log from there:
`CreateTopic → CreateSubscription → Publish → Pull(drain) → Seek(time) → Pull(replay)`,
then a pre-window `Seek(time)` that clamps to the earliest retained message.

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client `Google.Cloud.PubSub.V1` **3.36.0**
  (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/csharp && dotnet run --project advanced/seek-timestamp/SeekTimestamp.csproj
```

## Expected Output

```
[*] Created topic seek-ts-orders-<uuid8>
[*] Created subscription seek-ts-sub-<uuid8>
[x] Published order-1 id=<message-id>
[x] Published order-2 id=<message-id>
[x] Published order-3 id=<message-id>
[x] Published order-4 id=<message-id>
[x] Published order-5 id=<message-id>
[x] Published order-6 id=<message-id>
[ok] Initial drain delivered all 6 messages
[*] Seek(time = boundary before order-3)
[ok] Seek-to-timestamp replayed [order-3, order-4, order-5, order-6] (4 messages at/after the boundary)
[*] Seek(time = 5 years ago) → clamps to earliest retained (not an error)
[ok] Pre-window seek clamped to earliest retained → replayed all 6 messages
[ok] PASS — seek-to-timestamp replay + pre-window clamp verified
[*] Deleted subscription seek-ts-sub-<uuid8>
[*] Deleted topic seek-ts-orders-<uuid8>
```

> Resource ids are uuid-suffixed (`seek-ts-orders-<uuid8>` / `seek-ts-sub-<uuid8>`), so concurrent
> runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the `<message-id>`
> values vary per run. (See `../../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program publishes six messages, capturing a `Timestamp` of the instant just **before** `order-3`,
then drains all six so the subscription cursor sits at the head. It then issues
`Seek(SeekRequest{ Time = boundary })`: the connector resolves the first topic-log sequence at/after
that instant, **purges the subscription queue and drops in-flight leases**, and re-fans the log
forward — so the next pull re-delivers exactly `order-3..order-6`. Finally it seeks to a timestamp
five years in the past; rather than erroring, the connector **clamps to the earliest retained
message**, so all six messages replay. The topic and subscription are deleted in a `finally` block.

A `Seek` reads from the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source) via `Array.RangeEventsStore`, re-applies each subscription's filter, and re-enqueues into
the per-sub queue `pubsub.sub.{s}`, bounded by `MaxSeekReplay`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `Seek` (time), `DeleteSubscription`, `DeleteTopic` | topic `seek-ts-orders-<uuid8>` → `pubsub.{t}`; sub `seek-ts-sub-<uuid8>` → `pubsub.sub.{s}` | seek (timestamp replay) | replay bounded by `MaxSeekReplay` (default 1,000,000 → WARN at cap, no silent loss); retention 10 min..31 days clamped to broker `Store.MaxRetention`; ack deadline 10s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/seek-timestamp`](../../../go/advanced/seek-timestamp),
  [`../../../python/advanced/seek_timestamp`](../../../python/advanced/seek_timestamp),
  [`../../../java/advanced/seek-timestamp`](../../../java/advanced/seek-timestamp),
  [`../../../javascript/advanced/seek-timestamp`](../../../javascript/advanced/seek-timestamp),
  [`../../../ruby/advanced/seek_timestamp`](../../../ruby/advanced/seek_timestamp)
- Docs: [`../../../../docs/concepts/seek-and-snapshots.md`](../../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — seeking before the retained window clamps; it is not an error.** A `Seek` to a
> timestamp older than the earliest retained message does not fail — it clamps to the earliest
> retained message and replays from there. Don't rely on a pre-window seek returning an error to
> detect "too far back". (The C# client also needs explicit `EmulatorDetection.EmulatorOnly` — it
> does not auto-detect the emulator from the env var the way Go/Python/Node do.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
