# Python — advanced: Snapshots

Capture a subscription cursor and seek back to it:
`CreateSnapshot → Publish → Pull/Ack → Seek(snapshot) → Pull (replay)`.

## Prerequisites

- Python 3.10+ and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `../../pyproject.toml`, installed via `uv sync`).
- [`uv`](https://docs.astral.sh/uv/) (do not use `pip`/`poetry` directly).
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
cd examples/python
uv run python -m advanced.snapshots
# or, equivalently:
uv run python advanced/snapshots/main.py
```

## Expected Output

```
== advanced/snapshots — topic 'snapped-<uuid8>', sub 'sub-snapped-<uuid8>', snapshot 'snap-<uuid8>' ==
CreateTopic -> projects/my-project/topics/snapped-<uuid8>  (Events Store log pubsub.snapped-<uuid8>)
CreateSubscription -> projects/my-project/subscriptions/sub-snapped-<uuid8>  (Queue pubsub.sub.sub-snapped-<uuid8>)
CreateSnapshot -> projects/my-project/snapshots/snap-<uuid8>  (cursor captured at current position)
Publish -> order-1, order-2, order-3
Pull (initial) -> ['order-1', 'order-2', 'order-3']
[OK] initial drain delivered all 3 orders
Pull (drain check) -> []
[OK] subscription is empty after acking all 3
Seek(snapshot=snap-<uuid8>) -> cursor rewound to captured position
Pull (after seek-to-snapshot) -> ['order-1', 'order-2', 'order-3']
[OK] seek-to-snapshot replayed all 3 orders from the captured cursor
CreateSubscription -> projects/my-project/subscriptions/sub-detached-<uuid8>  (will be detached)
DeleteTopic -> projects/my-project/topics/snapped-<uuid8>  (detaches its subscriptions)
CreateSnapshot(detached sub) -> FAILED_PRECONDITION
[OK] snapshot of a detached subscription rejected FAILED_PRECONDITION

PASS: snapshot captured the cursor, seek-to-snapshot replayed, detached rejected.
```

> Resource ids are uuid-suffixed (`snapped-<uuid8>` / `sub-snapped-<uuid8>` / `snap-<uuid8>`) so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and the snapshot registry
> records do not collide; the `<uuid8>` values vary per run. The example deletes the
> snapshot/subscriptions/topic it created on exit.

## What's Happening

- `create_snapshot` records the subscription's current cursor as a registry record — pinning the
  position **before** the three orders are published.
- The three orders are published, then `pull` + `acknowledge` drains them all and the cursor
  advances to the tail; a follow-up `pull` confirms the subscription is empty.
- `seek(snapshot=...)` rewinds the subscription to the captured cursor; the next `pull` replays all
  three orders from that position — proving the snapshot is a durable, timestamp-free bookmark.
- The negative path creates a second subscription, deletes the topic to **detach** it, and shows
  that `create_snapshot` on a detached subscription is rejected with `FAILED_PRECONDITION`.
- Under the hood, a snapshot captures a cursor over the topic log `pubsub.{t}` (the authoritative,
  replayable, cross-protocol source; 7-day default snapshot expiry, swept hourly). A `Seek` to that
  snapshot purges the per-subscription queue `pubsub.sub.{s}`, drops its leases, and replays the
  topic log via `Array.RangeEventsStore` re-applying the subscription's filter. Any failed
  assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, **CreateSnapshot**, Publish, Pull, Acknowledge, **Seek(snapshot)** | `snapped-<uuid8>` / `sub-snapped-<uuid8>` (snapshot `snap-<uuid8>`) | none | snapshot = registry-record cursor, **7-day default expiry** swept hourly; snapshot of a **detached** subscription → `FAILED_PRECONDITION`; replay bounded by `MaxSeekReplay` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/snapshots`](../../../go/advanced/snapshots),
  [`../../../javascript/advanced/snapshots`](../../../javascript/advanced/snapshots),
  [`../../../java/advanced/snapshots`](../../../java/advanced/snapshots),
  [`../../../csharp/advanced/snapshots`](../../../csharp/advanced/snapshots),
  [`../../../ruby/advanced/snapshots`](../../../ruby/advanced/snapshots)
- [`../seek_timestamp/`](../seek_timestamp/) — seek to a timestamp instead of a saved cursor.
- Docs: [`../../../../docs/concepts/seek-and-snapshots.md`](../../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — you cannot snapshot a detached subscription.** `CreateSnapshot` on a subscription
> whose topic has been deleted/detached returns `FAILED_PRECONDITION`. Snapshot **before** you
> detach.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
