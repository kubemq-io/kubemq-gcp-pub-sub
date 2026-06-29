# C# — advanced: Snapshots

Capture a subscription cursor as a snapshot, then rewind to it:
`CreateTopic → CreateSubscription → Publish → Pull(drain) → CreateSnapshot → Publish → Pull(drain)
→ Seek(snapshot) → Pull(replay)`, plus the detached-subscription rejection.

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
cd examples/csharp && dotnet run --project advanced/snapshots/Snapshots.csproj
```

## Expected Output

```
[*] Created topic snap-orders-<uuid8>
[*] Created subscription snap-sub-<uuid8>
[x] Published order-1 id=<message-id>
[x] Published order-2 id=<message-id>
[x] Published order-3 id=<message-id>
[ok] Drained first 3 messages (order-1..order-3)
[*] CreateSnapshot snap-cursor-<uuid8> (cursor at head, expires <expire-time>)
[x] Published order-4 id=<message-id>
[x] Published order-5 id=<message-id>
[x] Published order-6 id=<message-id>
[ok] Drained next 3 messages (order-4..order-6)
[*] Seek(snapshot snap-cursor-<uuid8>)
[ok] Seek-to-snapshot replayed [order-3, order-4, order-5, order-6] (from the snapshot cursor onward; pre-snapshot messages excluded)
[*] Detached a subscription (deleted its topic), then attempted CreateSnapshot
[ok] CreateSnapshot on a detached subscription → FAILED_PRECONDITION (expected)
[ok] PASS — snapshot create + seek-to-snapshot + detached-subscription rejection verified
[*] Deleted snapshot snap-cursor-<uuid8>
[*] Deleted subscription snap-sub-<uuid8>
[*] Deleted topic snap-orders-<uuid8>
```

> Resource ids are uuid-suffixed (`snap-orders-<uuid8>` / `snap-sub-<uuid8>` / `snap-cursor-<uuid8>`),
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> `<message-id>` / `<expire-time>` values vary per run. (See
> `../../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program publishes and drains `order-1..order-3`, then calls `CreateSnapshot` to capture the
subscription's cursor (a registry record with a **7-day default expiry**, swept hourly). It publishes
and drains `order-4..order-6`, then `Seek(SeekRequest{ Snapshot })` rewinds the subscription to the
captured cursor. The connector records the cursor as the topic log's **last retained sequence at
capture time** (a *conservative* replay anchor), so the replay re-drives the log forward from the
boundary message `order-3` inclusive — `order-3..order-6` — and never re-delivers the pre-snapshot
`order-1`/`order-2`. Finally it deletes a topic out from under a subscription and shows that
`CreateSnapshot` on the resulting detached subscription fails `FAILED_PRECONDITION`. All resources
are cleaned up in a `finally` block.

A snapshot/seek reads the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source) via `Array.RangeEventsStore`, re-applies the subscription's filter, and re-enqueues into the
per-sub queue `pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `CreateSnapshot`, `Seek` (snapshot), `DeleteSnapshot`, `DeleteSubscription`, `DeleteTopic` | topic `snap-orders-<uuid8>` → `pubsub.{t}`; sub `snap-sub-<uuid8>` → `pubsub.sub.{s}`; snapshot `snap-cursor-<uuid8>` (registry record) | seek (snapshot replay) | snapshot 7-day default expiry, swept hourly; replay bounded by `MaxSeekReplay` (default 1,000,000 → WARN at cap, no silent loss); detached-subscription snapshot → `FAILED_PRECONDITION` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/snapshots`](../../../go/advanced/snapshots),
  [`../../../python/advanced/snapshots`](../../../python/advanced/snapshots),
  [`../../../java/advanced/snapshots`](../../../java/advanced/snapshots),
  [`../../../javascript/advanced/snapshots`](../../../javascript/advanced/snapshots),
  [`../../../ruby/advanced/snapshots`](../../../ruby/advanced/snapshots)
- Docs: [`../../../../docs/concepts/seek-and-snapshots.md`](../../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — you cannot snapshot a detached subscription.** `CreateSnapshot` on a subscription whose
> topic has been deleted/detached returns `FAILED_PRECONDITION` — snapshot before you detach. (The C#
> client also needs explicit `EmulatorDetection.EmulatorOnly`; it does not auto-detect the emulator
> from the env var the way Go/Python/Node do.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
