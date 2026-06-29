# Python — advanced: Seek to Timestamp

Rewind a subscription by time and replay the topic log:
`CreateTopic → CreateSubscription → Publish → Pull/Ack → Seek(timestamp) → Pull (replay)`.

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
uv run python -m advanced.seek_timestamp
# or, equivalently:
uv run python advanced/seek_timestamp/main.py
```

## Expected Output

```
== advanced/seek_timestamp — topic 'seekable-<uuid8>', sub 'sub-seekable-<uuid8>' ==
CreateTopic -> projects/my-project/topics/seekable-<uuid8>  (Events Store log pubsub.seekable-<uuid8>)
CreateSubscription -> projects/my-project/subscriptions/sub-seekable-<uuid8>  (Queue pubsub.sub.sub-seekable-<uuid8>, retain_acked=true)
Publish -> msg-1
checkpoint timestamp recorded -> <timestamp>
Publish -> msg-2, msg-3
Pull (initial) -> ['msg-1', 'msg-2', 'msg-3']
[OK] initial drain delivered all 3 in order
Seek(timestamp=checkpoint) -> cursor rewound
Pull (after checkpoint seek) -> ['msg-2', 'msg-3']
[OK] seek-to-checkpoint replayed only msg-2, msg-3
Seek(timestamp=now-365d) -> pre-window
Pull (after pre-window seek) -> ['msg-1', 'msg-2', 'msg-3']
seek to pre-window timestamp -> clamped to earliest retained (not an error)
[OK] pre-window seek clamped to earliest retained (all 3 replayed)

PASS: timestamp seek replayed the in-window tail and clamped a pre-window seek.
```

> Resource ids are uuid-suffixed (`seekable-<uuid8>` / `sub-seekable-<uuid8>`) so concurrent runs
> use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<uuid8>` and
> `<timestamp>` values vary per run. The example deletes the topic/subscription it created on exit.

## What's Happening

- `create_topic` / `create_subscription` set up a topic and a subscription with
  `retain_acked_messages=true` so already-acknowledged messages stay in the replayable log.
- Three messages are published with a **checkpoint timestamp** recorded strictly between `msg-1`
  and `msg-2`.
- The first `pull` drains all three messages and acks them — the cursor advances to the tail.
- `seek(time=checkpoint)` rewinds the subscription to the checkpoint; the next `pull` replays only
  the messages published **at/after** the checkpoint (`msg-2`, `msg-3`).
- `seek(time=now-365d)` is a **pre-window** seek: rather than erroring, the connector **clamps to
  the earliest retained message**, so the next `pull` replays all three again.
- Under the hood a Pub/Sub `Publish` writes once to the topic log `pubsub.{t}` (the authoritative,
  replayable, cross-protocol source). A `Seek` resolves a start sequence from that log, purges the
  per-subscription queue `pubsub.sub.{s}` and drops its leases, then replays the topic log via
  `Array.RangeEventsStore` — re-applying the subscription's filter — bounded by `MaxSeekReplay`.
  Any failed assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, Pull, Acknowledge, **Seek(time)** | `seekable-<uuid8>` / `sub-seekable-<uuid8>` | none | pre-window timestamp **clamps to earliest retained** (not an error); replay bounded by `MaxSeekReplay` (default 1,000,000 → hit cap = WARN, no silent loss); retention clamped to broker `Store.MaxRetention` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/seek-timestamp`](../../../go/advanced/seek-timestamp),
  [`../../../javascript/advanced/seek-timestamp`](../../../javascript/advanced/seek-timestamp),
  [`../../../java/advanced/seek-timestamp`](../../../java/advanced/seek-timestamp),
  [`../../../csharp/advanced/seek-timestamp`](../../../csharp/advanced/seek-timestamp),
  [`../../../ruby/advanced/seek_timestamp`](../../../ruby/advanced/seek_timestamp)
- [`../snapshots/`](../snapshots/) — seek to a saved cursor instead of a timestamp.
- Docs: [`../../../../docs/concepts/seek-and-snapshots.md`](../../../../docs/concepts/seek-and-snapshots.md)

> **Gotcha — a seek of a pre-window timestamp clamps to the earliest retained message** (it is NOT
> an error). Don't rely on a pre-window seek returning an error to detect "too far back"; it
> silently starts at the oldest available message. What is "retained" depends on the broker's
> `Store.MaxRetention` ceiling.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
