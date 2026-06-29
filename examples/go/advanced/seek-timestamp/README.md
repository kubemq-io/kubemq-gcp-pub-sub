# Go — advanced: Seek to Timestamp

Rewind a subscription to a wall-clock instant and replay the topic log from there:
`CreateTopic → CreateSubscription → Publish → Pull(drain) → Seek(time) → Pull(replay)`,
then a pre-window `Seek(time)` that clamps to the earliest retained message.

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
cd examples/go && go run ./advanced/seek-timestamp
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/seek-timestamp | emulator=localhost:8085 project=my-project
CreateTopic: seekable-<uuid8> (channel pubsub.seekable-<uuid8>)
CreateSubscription: sub-seekable-<uuid8> (channel pubsub.sub.sub-seekable-<uuid8>, retain_acked=true)
Publish: order-1 -> messageId=<message-id>
Publish: order-2 -> messageId=<message-id>
checkpoint timestamp recorded -> <publish-time>
Publish: order-3 -> messageId=<message-id>
Publish: order-4 -> messageId=<message-id>
Publish: order-5 -> messageId=<message-id>
Publish: order-6 -> messageId=<message-id>
Pull (initial) -> [order-1 order-2 order-3 order-4 order-5 order-6]
[OK] initial drain delivered all 6 in order
Seek(time=checkpoint) -> cursor rewound
Pull (after checkpoint seek) -> [order-3 order-4 order-5 order-6]
[OK] seek-to-checkpoint replayed only order-3..order-6 (4 at/after the boundary)
Seek(time=now-365d) -> pre-window (clamps to earliest retained, not an error)
Pull (after pre-window seek) -> [order-1 order-2 order-3 order-4 order-5 order-6]
[OK] pre-window seek clamped to earliest retained (all 6 replayed)
PASS: timestamp seek replayed the in-window tail and clamped a pre-window seek
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`seekable-<uuid8>` / `sub-seekable-<uuid8>`), so concurrent
> runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the `<message-id>`
> and `<publish-time>` values vary per run. (See `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program creates a subscription with `RetainAckedMessages` set so the topic log stays
replayable after ack, then publishes `order-1` and `order-2`, captures a `time.Time` of the instant
**between** `order-2` and `order-3`, and publishes the rest. It drains all six with deterministic
unary `Pull`/`Acknowledge` so the cursor sits at the head. It then calls
`Subscription.SeekToTime(checkpoint)`: the connector resolves the first topic-log sequence at/after
that instant, **purges the subscription queue and drops in-flight leases**, and re-fans the log
forward — so the next pull re-delivers exactly `order-3..order-6`. Finally it seeks to a timestamp
365 days in the past; rather than erroring, the connector **clamps to the earliest retained
message**, so all six messages replay. The topic and subscription are deleted on exit.

A `Seek` reads from the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source) via `Array.RangeEventsStore`, re-applies each subscription's filter, and re-enqueues into
the per-sub queue `pubsub.sub.{s}`, bounded by `MaxSeekReplay`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `Seek` (time), `DeleteSubscription`, `DeleteTopic` | topic `seekable-<uuid8>` → `pubsub.{t}`; sub `sub-seekable-<uuid8>` → `pubsub.sub.{s}` | seek (timestamp replay) | replay bounded by `MaxSeekReplay` (default 1,000,000 → WARN at cap, no silent loss); retention 10 min..31 days clamped to broker `Store.MaxRetention`; ack deadline 10s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/advanced/seek_timestamp`](../../../python/advanced/seek_timestamp),
  [`../../../javascript/advanced/seek-timestamp`](../../../javascript/advanced/seek-timestamp),
  [`../../../java/advanced/seek-timestamp`](../../../java/advanced/seek-timestamp),
  [`../../../csharp/advanced/seek-timestamp`](../../../csharp/advanced/seek-timestamp),
  [`../../../ruby/advanced/seek_timestamp`](../../../ruby/advanced/seek_timestamp)
- Docs: [`../../../docs/concepts/seek-and-snapshots.md`](../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — seeking before the retained window clamps; it is not an error.** A `Seek` to a
> timestamp older than the earliest retained message does not fail — it clamps to the earliest
> retained message and replays from there. Don't rely on a pre-window seek returning an error to
> detect "too far back". (The high-level `pubsub.Client` auto-detects the emulator from the env var;
> this example drains with the apiv1 `SubscriberClient`, which does not — it is passed the emulator
> endpoint + insecure credentials explicitly.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
