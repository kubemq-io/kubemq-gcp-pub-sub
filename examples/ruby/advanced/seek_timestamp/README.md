# Ruby — advanced: Seek to a Timestamp & Replay

Rewind a subscription to a point in time and replay the topic log from there:
`Publish(m1, <checkpoint>, m2, m3) → drain all 3 → Seek(time=<checkpoint>) → replay m2,m3 only`,
then a pre-window seek that **clamps** to the earliest retained message.

## Prerequisites

- **Ruby 3.1+** (built/verified on rbenv **3.3.7**; `eval "$(rbenv init - zsh)"` first — see the
  Toolchain note in [`../../README.md`](../../README.md)).
- The official GCP Pub/Sub client `google-cloud-pubsub` (floor `~> 3.3`, pinned in
  [`../../Gemfile`](../../Gemfile); exact resolved versions in the gitignored `Gemfile.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled (gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the client skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
eval "$(rbenv init - zsh)"        # selects Ruby 3.3.7
cd examples/ruby
bundle exec ruby advanced/seek_timestamp/main.rb
```

## Expected Output

```
=== advanced/seek_timestamp — topic 'seekable-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateTopic -> projects/my-project/topics/seekable-<uuid8>
  [OK] created topic name matches the requested path
CreateSubscription -> projects/my-project/subscriptions/sub-seekable-<uuid8>  (ack_deadline=10s)
Publish -> m1=<message-id> m2=<message-id> m3=<message-id>
Checkpoint -> <publish-time> (between m1 and m2)
  [OK] all three publishes got server ids
Pull (first pass) -> ["m1", "m2", "m3"]
  [OK] first pass delivers m1, m2, m3 then acks them
  [OK] subscription drained after first pass (no redelivery)
Seek(time=checkpoint) -> replaying pubsub.seekable-<uuid8> from the checkpoint
Pull (replay pass) -> ["m2", "m3"]
  [OK] seek replays exactly m2,m3 (at/after the checkpoint); m1 excluded
Seek(time=now-1h) -> pre-window timestamp clamps to earliest retained (not an error)
Pull (clamped replay) -> ["m1", "m2", "m3"]
  [OK] pre-window seek clamps and replays from the earliest retained message

PASS — seek-to-timestamp replay complete.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide; `<message-id>` / `<publish-time>` vary per run.
> Each failed `[OK]` assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program builds one `Google::Cloud::PubSub` client (it honours `PUBSUB_EMULATOR_HOST`, so it
clears credentials and dials insecure gRPC). Control-plane work goes through the admin clients —
`topic_admin.create_topic`, `subscription_admin.create_subscription`, and
`subscription_admin.seek` — while publish/pull use the data-plane `pubsub.publisher(topic)` /
`pubsub.subscriber(sub)`. It publishes `m1`, records a **checkpoint** wall-clock time, then
publishes `m2` and `m3`. After draining and acking all three, it calls `seek(time: checkpoint)`:
only `m2,m3` (at/after the checkpoint) are redelivered. A final `seek` to *one hour ago* — earlier
than any retained message — does **not** fail; it clamps to the earliest retained message and
replays all three.

On the connector side, every topic is backed by a durable, replayable **Events Store log**
`pubsub.{t}`. A `Seek(time)` resolves the first sequence at/after the timestamp, **purges the
subscription queue and drops outstanding leases**, then replays the topic log from that sequence
via `Array.RangeEventsStore` — re-applying the subscription's filter as it refills `pubsub.sub.{s}`
— bounded by `MaxSeekReplay` (default 1,000,000; hitting the cap logs a WARN, never silently
loses). A timestamp before the retained window clamps to the earliest retained message.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Seek` (timestamp), `Pull`, `Acknowledge` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | Seek resolves start seq from the topic log, purges the queue + drops leases, then replays re-applying the filter. A **pre-window timestamp clamps to the earliest retained message (not an error)**. Replay bounded by `MaxSeekReplay` (default 1,000,000 → WARN at cap, no silent loss). Retention is clamped to broker `Store.MaxRetention`. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/seek-timestamp`](../../../go/advanced/seek-timestamp),
  [`../../../python/advanced/seek_timestamp`](../../../python/advanced/seek_timestamp),
  [`../../../java/advanced/seek-timestamp`](../../../java/advanced/seek-timestamp),
  [`../../../javascript/advanced/seek-timestamp`](../../../javascript/advanced/seek-timestamp),
  [`../../../csharp/advanced/seek-timestamp`](../../../csharp/advanced/seek-timestamp)
- Sibling advanced variant: [`../snapshots`](../snapshots) (seek to a saved cursor instead of a time).
- Docs: [`../../../../docs/concepts/seek-and-snapshots.md`](../../../../docs/concepts/seek-and-snapshots.md)

> Gotcha: **seeking before the retained window clamps; it is not an error.** A `Seek` to a
> timestamp older than the earliest retained message silently starts at the oldest available
> message — don't rely on a pre-window seek raising to detect "too far back". (Retention itself is
> clamped to the broker's `Store.MaxRetention`.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
