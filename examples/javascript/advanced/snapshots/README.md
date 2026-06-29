# JavaScript/TypeScript — advanced: Snapshots

Capture a subscription cursor, then seek back to it: `CreateTopic → CreateSubscription → Publish → Pull/Ack → CreateSnapshot → Publish → Pull/Ack → Seek(snapshot) → Pull (replay)`.

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
cd examples/javascript && npx tsx advanced/snapshots/index.ts
```

## Expected Output

```
Connecting to KubeMQ Pub/Sub connector via PUBSUB_EMULATOR_HOST=localhost:8085
CreateTopic        -> snapped-<uuid8>  (channel pubsub.snapped-<uuid8>)
CreateSubscription -> sub-snapped-<uuid8>  (channel pubsub.sub.sub-snapped-<uuid8>)
Pull (drain msg-1,msg-2) -> 2 distinct [msg-1, msg-2]
CreateSnapshot     -> snap-<uuid8> (cursor after msg-1,msg-2; 7-day default expiry, swept hourly)
Pull (drain msg-3) -> 1 distinct [msg-3]
Seek(snapshot)     -> rewound subscription to snapshot snap-<uuid8>
Pull (after seek-to-snapshot) -> 2 distinct [msg-2, msg-3]

PASS: snapshot captured the cursor; seek-to-snapshot replayed messages from the captured position (msg-3 redelivered, msg-1 not).
```

> Resource ids are uuid-suffixed (`snapped-<uuid8>` / `sub-snapped-<uuid8>` / `snap-<uuid8>`), so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and snapshot records and do not collide; the `<message-id>` / `<publish-time>` values vary per run. Delivery is at-least-once; the example asserts on the **distinct set** and prints distinct, sorted bodies.

## What's Happening

The example drains two messages (advancing the subscription cursor past them) and lets the acks commit, then calls `subscription.createSnapshot(name)` to capture that cursor as a registry record. It publishes and drains a third message, so the live subscription is now past `msg-3` as well. Calling `subscription.seek(snapshotName)` rewinds the subscription to the captured cursor: the connector resolves the start sequence from the snapshot's cursor, purges the per-subscription queue `pubsub.sub.{s}`, drops its leases, and replays the topic log `pubsub.{t}` from that sequence via `Array.RangeEventsStore`. `msg-3` (published *after* the snapshot) is redelivered, while `msg-1` (published well before the cursor) is not. The cursor sits at the `msg-1`/`msg-2` boundary, and the replay's start sequence is **inclusive of the boundary message**, so `msg-2` is re-included — the example asserts the deterministic invariants (`msg-3` replayed, `msg-1` not) and accepts the boundary `msg-2`. Snapshots have a 7-day default expiry and are swept hourly; snapshotting a *detached* subscription returns `FAILED_PRECONDITION`. Replay is bounded by `MaxSeekReplay`. Any failed assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, Pull, Acknowledge, CreateSnapshot, Seek | `snapped-<uuid8>` / `sub-snapped-<uuid8>` (snapshot `snap-<uuid8>`) | none | snapshot 7-day default expiry (swept hourly); snapshot of a detached sub → `FAILED_PRECONDITION`; replay bounded by `MaxSeekReplay` (1,000,000) |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/snapshots`](../../../go/advanced/snapshots), [`../../../python/advanced/snapshots`](../../../python/advanced/snapshots), [`../../../java/advanced/snapshots`](../../../java/advanced/snapshots), [`../../../csharp/advanced/snapshots`](../../../csharp/advanced/snapshots), [`../../../ruby/advanced/snapshots`](../../../ruby/advanced/snapshots)
- Docs: [`../../../docs/concepts/seek-and-snapshots.md`](../../../docs/concepts/seek-and-snapshots.md)
- Related: [`../seek-timestamp/`](../seek-timestamp/) (seek to a timestamp instead of a named snapshot)

> **Snapshot of a detached subscription.** Snapshots capture a cursor and expire after 7 days (swept hourly). A `CreateSnapshot` against a detached subscription returns `FAILED_PRECONDITION`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
