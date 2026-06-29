# JavaScript/TypeScript — advanced: Seek to Timestamp

Rewind a subscription to a point in time and replay the topic log: `CreateTopic → CreateSubscription → Publish → Pull/Ack → Seek(timestamp) → Pull (replay) → Seek(epoch) → Pull (clamp)`.

## Prerequisites

- **Node.js 18+** (developed against Node 20+) and the official GCP Pub/Sub client `@google-cloud/pubsub` 5.3.1 (pinned in `examples/javascript/package.json`). Run with `tsx`.
- A running KubeMQ broker with the Pub/Sub connector enabled (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK clears credentials and uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/javascript && npx tsx advanced/seek-timestamp/index.ts
```

## Expected Output

```
Connecting to KubeMQ Pub/Sub connector via PUBSUB_EMULATOR_HOST=localhost:8085
CreateTopic        -> seekable-<uuid8>  (channel pubsub.seekable-<uuid8>)
CreateSubscription -> sub-seekable-<uuid8>  (channel pubsub.sub.sub-seekable-<uuid8>)
Publish            -> 3 messages (ids <message-id>, <message-id>, <message-id>)
Marker timestamp   -> <publish-time> (between msg-1 and msg-2)
Pull (initial) -> 3 distinct [msg-1, msg-2, msg-3]
Seek(timestamp)    -> rewound to <publish-time>
Pull (after seek-to-timestamp) -> 2 distinct [msg-2, msg-3]
Seek(epoch)        -> 1970-01-01T00:00:00.000Z (pre-window: clamps to earliest retained, not an error)
Pull (after seek-to-epoch / clamp) -> 3 distinct [msg-1, msg-2, msg-3]

PASS: seek-to-timestamp replayed from the marker; pre-window seek clamped to earliest retained.
```

> Resource ids are uuid-suffixed (`seekable-<uuid8>` / `sub-seekable-<uuid8>`), so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<message-id>` / `<publish-time>` values vary per run. Delivery is **at-least-once**, so a streaming-pull window may surface a redelivered duplicate — the example collects each window then asserts on the **distinct set**, and prints the distinct, sorted bodies so output stays stable.

## What's Happening

The example publishes three messages and records a marker timestamp between the first and the rest. After draining the subscription queue once (Pull/Ack of all three), it issues `subscription.seek(date)` to that marker. The connector resolves the start sequence from the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol source), purges the per-subscription queue `pubsub.sub.{s}` and drops its leases, then replays the topic log via `Array.RangeEventsStore` re-applying the subscription's filter — so only `msg-2` and `msg-3` come back. A second seek to the Unix epoch (a pre-window timestamp) demonstrates the clamp: instead of erroring, the connector clamps the start to the earliest retained message and replays the full log. Replay is bounded by `MaxSeekReplay` (default 1,000,000; hitting the cap logs a WARN, never silently loses data). Any failed assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, Pull, Acknowledge, Seek | `seekable-<uuid8>` / `sub-seekable-<uuid8>` | none | replay bounded by `MaxSeekReplay` (1,000,000); pre-window timestamp clamps to earliest retained; retention clamped to broker `Store.MaxRetention` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/seek-timestamp`](../../../go/advanced/seek-timestamp), [`../../../python/advanced/seek_timestamp`](../../../python/advanced/seek_timestamp), [`../../../java/advanced/seek-timestamp`](../../../java/advanced/seek-timestamp), [`../../../csharp/advanced/seek-timestamp`](../../../csharp/advanced/seek-timestamp), [`../../../ruby/advanced/seek_timestamp`](../../../ruby/advanced/seek_timestamp)
- Docs: [`../../../docs/concepts/seek-and-snapshots.md`](../../../docs/concepts/seek-and-snapshots.md)
- Related: [`../snapshots/`](../snapshots/) (seek to a named snapshot instead of a timestamp)

> **Seek clamp.** A seek to a pre-window timestamp clamps to the earliest retained message — it is not an error. Replay is bounded by `MaxSeekReplay`; hitting the cap WARNs (no silent loss).

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
