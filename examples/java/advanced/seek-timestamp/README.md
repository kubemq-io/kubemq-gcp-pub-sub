# Java — advanced: Seek to Timestamp

Rewind a subscription to a wall-clock instant and replay the topic log from there:
`CreateTopic → CreateSubscription → Publish → Pull(drain) → Seek(time) → Pull(replay)`,
then a pre-window `Seek(time)` that clamps to the earliest retained message.

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
cd examples/java && mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.advanced.seektimestamp.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/seek-timestamp | emulator=localhost:8085 project=my-project
CreateTopic        -> seekable-<uuid8> (channel pubsub.seekable-<uuid8>)
CreateSubscription -> sub-seekable-<uuid8> (channel pubsub.sub.sub-seekable-<uuid8>, retain_acked=true)
Publish            -> msg-1 messageId=<message-id>
Marker timestamp   -> <publish-time> (between msg-1 and msg-2/msg-3)
Publish            -> msg-2 messageId=<message-id>
Publish            -> msg-3 messageId=<message-id>
Pull (initial)     -> 3 distinct [msg-1, msg-2, msg-3]
[OK] initial drain delivered all 3 distinct messages ([msg-1, msg-2, msg-3])
Seek(time=marker)  -> cursor rewound to <publish-time>
Pull (after marker seek) -> 2 distinct [msg-2, msg-3]
[OK] marker seek replayed msg-2 and msg-3
[OK] msg-1 (published before the marker) was NOT replayed
Seek(time=epoch)   -> pre-window (clamps to earliest retained, not an error)
Pull (after pre-window seek / clamp) -> 3 distinct [msg-1, msg-2, msg-3]
[OK] pre-window seek clamped to earliest retained (all 3 replayed)
PASS: timestamp seek replayed the in-window tail and clamped a pre-window seek
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`seekable-<uuid8>` / `sub-seekable-<uuid8>`), so concurrent
> runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the `<message-id>`
> and `<publish-time>` values vary per run. (See `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program creates a subscription with `retain_acked_messages` set so the topic log stays
replayable after ack, then publishes `msg-1`, captures an `Instant` of the moment **between**
`msg-1` and `msg-2`/`msg-3`, and publishes the rest. It drains all three through a short
`Subscriber` StreamingPull window, acking each, so the cursor sits at the head. It then issues a
`Seek` (`SeekRequest.setTime(marker)`): the connector resolves the first topic-log sequence
at/after that instant, **purges the subscription queue and drops in-flight leases**, and re-fans
the log forward — so the next drain re-delivers exactly `msg-2` and `msg-3` (and not `msg-1`).
Finally it seeks to the Unix epoch (a pre-window timestamp); rather than erroring, the connector
**clamps to the earliest retained message**, so all three messages replay. Delivery is
at-least-once, so each drain asserts on the distinct set. The topic and subscription are deleted
on exit.

A `Seek` reads from the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source) via `Array.RangeEventsStore`, re-applies each subscription's filter, and re-enqueues into
the per-sub queue `pubsub.sub.{s}`, bounded by `MaxSeekReplay`. Any failed assertion throws and
the JVM exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull` (StreamingPull), `Acknowledge`, `Seek` (time), `DeleteSubscription`, `DeleteTopic` | topic `seekable-<uuid8>` → `pubsub.{t}`; sub `sub-seekable-<uuid8>` → `pubsub.sub.{s}` | seek (timestamp replay) | replay bounded by `MaxSeekReplay` (default 1,000,000 → WARN at cap, no silent loss); retention clamped to broker `Store.MaxRetention`; ack deadline 10s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/seek-timestamp`](../../../go/advanced/seek-timestamp),
  [`../../../python/advanced/seek_timestamp`](../../../python/advanced/seek_timestamp),
  [`../../../javascript/advanced/seek-timestamp`](../../../javascript/advanced/seek-timestamp),
  [`../../../csharp/advanced/seek-timestamp`](../../../csharp/advanced/seek-timestamp),
  [`../../../ruby/advanced/seek_timestamp`](../../../ruby/advanced/seek_timestamp)
- Docs: [`../../../docs/concepts/seek-and-snapshots.md`](../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — seeking before the retained window clamps; it is not an error.** A `Seek` to a
> timestamp older than the earliest retained message does not fail — it clamps to the earliest
> retained message and replays from there. Don't rely on a pre-window seek returning an error to
> detect "too far back". (Java note: when `PUBSUB_EMULATOR_HOST` is set the example builds every
> client — `TopicAdminClient`, `SubscriptionAdminClient`, `Publisher`, `Subscriber` — on a
> plaintext channel to that host with `NoCredentialsProvider`, so no Google auth is attempted.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
