# Java — advanced: Snapshots

Capture a subscription cursor as a snapshot, then seek the subscription back to it:
`CreateTopic → CreateSubscription → Publish → Pull+Ack → CreateSnapshot → Publish → Pull+Ack → Seek(snapshot) → Pull(replay)`.

## Prerequisites

- **JDK 21+** (the examples target `maven.compiler.release=21`) and Maven 3.9+, plus the
  official GCP Pub/Sub client **`com.google.cloud:google-cloud-pubsub`**, version-managed via
  **`com.google.cloud:libraries-bom` 26.84.0** (pinned in `examples/java/pom.xml`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no Google credentials**, the connector skips Google
  auth and uses insecure gRPC:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/java && mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.advanced.snapshots.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/snapshots | emulator=localhost:8085 project=my-project
CreateTopic        -> snapped-<uuid8> (channel pubsub.snapped-<uuid8>)
CreateSubscription -> sub-snapped-<uuid8> (channel pubsub.sub.sub-snapped-<uuid8>, retain_acked=true)
Publish            -> msg-1 messageId=<message-id>
Publish            -> msg-2 messageId=<message-id>
Pull (drain msg-1,msg-2) -> 2 distinct [msg-1, msg-2]
[OK] first drain delivered msg-1 and msg-2 ([msg-1, msg-2])
CreateSnapshot     -> snap-<uuid8> (cursor after msg-1,msg-2; 7-day expiry, swept hourly)
Publish            -> msg-3 messageId=<message-id>
Pull (drain msg-3) -> 1 distinct [msg-3]
[OK] second drain delivered msg-3 ([msg-3])
Seek(snapshot)     -> rewound subscription to snapshot snap-<uuid8>
Pull (after seek-to-snapshot) -> 2 distinct [msg-2, msg-3]
[OK] seek-to-snapshot replayed msg-3 (published after the captured cursor)
[OK] msg-1 (well before the captured cursor) was NOT replayed
PASS: snapshot captured the cursor; seek-to-snapshot replayed the tail from it
DeleteSnapshot: ok
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`snapped-<uuid8>` / `sub-snapped-<uuid8>` / `snap-<uuid8>`), so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and registry records and
> never collide; the `<message-id>` values vary per run. (See
> `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program creates a subscription with `retain_acked_messages` set, publishes `msg-1` and
`msg-2`, and drains+acks both so the live cursor advances past them. It then calls `CreateSnapshot`
to capture the cursor as a registry record (7-day default expiry, swept hourly). After publishing
and draining `msg-3`, it issues a `Seek` (`SeekRequest.setSnapshot(...)`): the connector **purges
the subscription queue** and replays the topic log `pubsub.{t}` from the captured position via
`Array.RangeEventsStore`. `msg-3` (published after the snapshot) is redelivered and `msg-1` (well
before the cursor) is not; the cursor boundary is inclusive of the last message acked before
capture, so `msg-2` also replays. Delivery is at-least-once, so the drain asserts on the distinct
set. A snapshot of a **detached** subscription would return `FAILED_PRECONDITION`. The snapshot,
subscription, and topic are deleted on exit.

A `Seek` to a snapshot reads from the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source), re-applies the subscription's filter, and re-enqueues into the per-sub
queue `pubsub.sub.{s}`. Any failed assertion throws and the JVM exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull` (StreamingPull), `Acknowledge`, `CreateSnapshot`, `Seek` (snapshot), `DeleteSnapshot`, `DeleteSubscription`, `DeleteTopic` | topic `snapped-<uuid8>` → `pubsub.{t}`; sub `sub-snapped-<uuid8>` → `pubsub.sub.{s}`; snapshot `snap-<uuid8>` | seek (snapshot replay) | snapshot 7-day default expiry, swept hourly; snapshot of a detached sub → `FAILED_PRECONDITION`; replay bounded by `MaxSeekReplay`; ack deadline 10s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/snapshots`](../../../go/advanced/snapshots),
  [`../../../python/advanced/snapshots`](../../../python/advanced/snapshots),
  [`../../../javascript/advanced/snapshots`](../../../javascript/advanced/snapshots),
  [`../../../csharp/advanced/snapshots`](../../../csharp/advanced/snapshots),
  [`../../../ruby/advanced/snapshots`](../../../ruby/advanced/snapshots)
- Docs: [`../../../docs/concepts/seek-and-snapshots.md`](../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — snapshots expire and detached subs can't be snapshotted.** A snapshot captures only a
> cursor (not the messages); it has a 7-day default expiry (swept hourly), and creating a snapshot
> of a subscription whose topic has been deleted (a *detached* subscription) fails with
> `FAILED_PRECONDITION`. (Java note: when `PUBSUB_EMULATOR_HOST` is set the example builds every
> client on a plaintext channel to that host with `NoCredentialsProvider`, so no Google auth is
> attempted.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
