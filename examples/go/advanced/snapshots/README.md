# Go — advanced: Snapshots

Capture a subscription cursor as a snapshot, then rewind to it:
`CreateTopic → CreateSubscription → Publish → Pull(drain) → CreateSnapshot → Publish → Pull(drain)
→ Seek(snapshot) → Pull(replay)`, plus the detached-subscription rejection.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
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
cd examples/go && go run ./advanced/snapshots
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/snapshots | emulator=localhost:8085 project=my-project
CreateTopic: snapped-<uuid8> (channel pubsub.snapped-<uuid8>)
CreateSubscription: sub-snapped-<uuid8> (channel pubsub.sub.sub-snapped-<uuid8>)
Publish: order-1 -> messageId=<message-id>
Publish: order-2 -> messageId=<message-id>
Publish: order-3 -> messageId=<message-id>
Pull (initial) -> [order-1 order-2 order-3]
[OK] initial drain delivered order-1..order-3
CreateSnapshot: snap-cursor-<uuid8> (cursor captured, expires <expire-time>)
Publish: order-4 -> messageId=<message-id>
Publish: order-5 -> messageId=<message-id>
Publish: order-6 -> messageId=<message-id>
Pull (after snapshot) -> [order-4 order-5 order-6]
[OK] drained order-4..order-6 after the snapshot
Seek(snapshot=snap-cursor-<uuid8>) -> cursor rewound to captured position
Pull (after seek-to-snapshot) -> [order-3 order-4 order-5 order-6]
[OK] seek-to-snapshot replayed order-3..order-6 from the captured cursor (pre-snapshot excluded)
CreateSubscription: sub-detached-<uuid8> (will be detached)
DeleteTopic: snapped-<uuid8> (detaches its subscriptions)
CreateSnapshot(detached sub) -> FAILED_PRECONDITION
[OK] snapshot of a detached subscription rejected FAILED_PRECONDITION
PASS: snapshot captured the cursor, seek-to-snapshot replayed, detached rejected
DeleteSnapshot: ok
DeleteSubscription: ok
```

> Resource ids are uuid-suffixed (`snapped-<uuid8>` / `sub-snapped-<uuid8>` / `snap-cursor-<uuid8>`),
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> `<message-id>` / `<expire-time>` values vary per run. (See
> `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program publishes and drains `order-1..order-3`, then calls `Subscription.CreateSnapshot` to
capture the subscription's cursor (a registry record with a **7-day default expiry**, swept hourly).
It publishes and drains `order-4..order-6`, then `Subscription.SeekToSnapshot` rewinds the
subscription to the captured cursor. The connector records the cursor as the topic log's **last
retained sequence at capture time** (a *conservative* replay anchor), so the replay re-drives the
log forward from the boundary message `order-3` inclusive — `order-3..order-6` — and never
re-delivers the pre-snapshot `order-1`/`order-2`. Finally it deletes a topic out from under a
subscription and shows that `CreateSnapshot` on the resulting detached subscription fails
`FAILED_PRECONDITION`. All resources are cleaned up on exit.

A snapshot/seek reads the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source) via `Array.RangeEventsStore`, re-applies the subscription's filter, and re-enqueues into the
per-sub queue `pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `CreateSnapshot`, `Seek` (snapshot), `DeleteSnapshot`, `DeleteSubscription`, `DeleteTopic` | topic `snapped-<uuid8>` → `pubsub.{t}`; sub `sub-snapped-<uuid8>` → `pubsub.sub.{s}`; snapshot `snap-cursor-<uuid8>` (registry record) | seek (snapshot replay) | snapshot 7-day default expiry, swept hourly; replay bounded by `MaxSeekReplay` (default 1,000,000 → WARN at cap, no silent loss); detached-subscription snapshot → `FAILED_PRECONDITION` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/advanced/snapshots`](../../../python/advanced/snapshots),
  [`../../../javascript/advanced/snapshots`](../../../javascript/advanced/snapshots),
  [`../../../java/advanced/snapshots`](../../../java/advanced/snapshots),
  [`../../../csharp/advanced/snapshots`](../../../csharp/advanced/snapshots),
  [`../../../ruby/advanced/snapshots`](../../../ruby/advanced/snapshots)
- Docs: [`../../../docs/concepts/seek-and-snapshots.md`](../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — you cannot snapshot a detached subscription.** `CreateSnapshot` on a subscription whose
> topic has been deleted/detached returns `FAILED_PRECONDITION` — snapshot before you detach. (The
> high-level `pubsub.Client` auto-detects the emulator from the env var; this example drains with the
> apiv1 `SubscriberClient`, which does not — it is passed the emulator endpoint + insecure
> credentials explicitly.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
