# Ruby — advanced: Snapshots (CreateSnapshot → Seek-to-snapshot)

Capture a subscription's cursor as a snapshot and rewind to it later without
knowing an exact timestamp:
`CreateSnapshot (empty backlog) → Publish(m1,m2,m3) → drain & ack → Seek(snapshot) → replay all 3`.

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
bundle exec ruby advanced/snapshots/main.rb
```

## Expected Output

```
=== advanced/snapshots — topic 'snapped-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateTopic -> projects/my-project/topics/snapped-<uuid8>
  [OK] created topic name matches the requested path
CreateSubscription -> projects/my-project/subscriptions/sub-snapped-<uuid8>  (ack_deadline=10s)
CreateSnapshot -> projects/my-project/snapshots/snap-<uuid8>  (expire_time=<+7d>)
  [OK] snapshot anchored at the current cursor (default 7-day expiry)
Publish -> m1=<message-id> m2=<message-id> m3=<message-id>
Pull (live) -> ["m1", "m2", "m3"]
  [OK] m1,m2,m3 delivered live then acked
  [OK] subscription drained (no redelivery before seek)
Seek(snapshot) -> rewind to the captured cursor; replay pubsub.snapped-<uuid8>
Pull (replay after seek-to-snapshot) -> ["m1", "m2", "m3"]
  [OK] seek-to-snapshot replays the whole post-snapshot batch (m1,m2,m3) despite prior acks

PASS — snapshot capture & seek-to-snapshot replay complete.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide; `<message-id>` / `<+7d>` vary per run.
> Each failed `[OK]` assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program builds one `Google::Cloud::PubSub` client (it honours `PUBSUB_EMULATOR_HOST`).
Control-plane work uses the admin clients — `topic_admin.create_topic`,
`subscription_admin.create_subscription`, `subscription_admin.create_snapshot`, and
`subscription_admin.seek` — while publish/pull use the data-plane `pubsub.publisher` /
`pubsub.subscriber`. It snapshots the **fresh, empty** subscription, then publishes `m1,m2,m3`,
drains and acks them, and finally calls `seek(snapshot: …)` — which re-delivers the whole batch
even though it was already acked.

On the connector side, `CreateSnapshot` records the topic Events Store log's **current last
retained sequence** as a conservative replay anchor (it captures a cursor, not the wall clock). A
`Seek(snapshot)` purges the subscription queue, drops outstanding leases, and **replays the topic
log `pubsub.{t}` forward from that anchor** into `pubsub.sub.{s}`, re-applying the subscription's
filter. Because the anchor was taken before any of `m1,m2,m3` were published, all three replay.
Snapshots carry a 7-day default expiry and are swept hourly.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `CreateSnapshot`, `Publish`, `Pull`, `Acknowledge`, `Seek` (snapshot), `DeleteSnapshot` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` (snapshot = registry record) | none | Snapshot cursor = the topic log's **current last retained sequence** (conservative anchor); seek-to-snapshot replays the log **forward from that anchor**, so snapshot the empty/drained subscription *before* the messages you want to replay. **7-day default expiry, swept hourly.** A snapshot of a **detached** subscription (its topic deleted) → `FAILED_PRECONDITION`. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/snapshots`](../../../go/advanced/snapshots),
  [`../../../python/advanced/snapshots`](../../../python/advanced/snapshots),
  [`../../../java/advanced/snapshots`](../../../java/advanced/snapshots),
  [`../../../javascript/advanced/snapshots`](../../../javascript/advanced/snapshots),
  [`../../../csharp/advanced/snapshots`](../../../csharp/advanced/snapshots)
- Sibling advanced variant: [`../seek_timestamp`](../seek_timestamp) (seek to a time instead of a saved cursor).
- Docs: [`../../../../docs/concepts/seek-and-snapshots.md`](../../../../docs/concepts/seek-and-snapshots.md)

> Gotcha: **you cannot snapshot a detached subscription.** `CreateSnapshot` on a subscription whose
> topic has been deleted/detached returns `FAILED_PRECONDITION` — snapshot before you detach. Also
> note the cursor is the topic-log anchor, not Google's acked-state model: snapshot the empty
> subscription *first*, then publish the batch you intend to replay.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
