# Python — topics: Batch Publish

Client-side batched publish against the connector:
`CreateTopic → CreateSubscription → Publish(N, batched) → Pull(all) → Acknowledge`.

## Prerequisites

- **Python 3.10+** (uv) and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `../../pyproject.toml` + committed `uv.lock`).
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
cd examples/python && uv run python -m topics.batch_publish
# or, equivalently:
cd examples/python && uv run python topics/batch_publish/main.py
```

## Expected Output

```
=== topics/batch_publish — topic 'events-<uuid8>', batch of 25 ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateTopic -> projects/my-project/topics/events-<uuid8>
CreateSubscription -> projects/my-project/subscriptions/sub-events-<uuid8>
Publish -> 25 ids in request order
           first id=<message-id>
           last  id=<message-id>
  [OK] all 25 publishes returned an id
  [OK] every message id is unique
Pull -> received and acked 25 message(s)
  [OK] received all 25 published messages
  [OK] every published body was delivered exactly once
  [OK] publish ids are ordered by request submission order

PASS — batch of 25 published and consumed exactly once.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide; the `<message-id>` values vary per run.
> Each failed `[OK]` assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The official `PublisherClient` batches `publish()` calls automatically. Configuring
`BatchSettings(max_messages=25, max_latency=1.0)` makes the client buffer all 25 messages and flush
them as a **single** publish batch to the connector. Each `publish()` still returns its own future;
resolving those futures in submission order yields the message ids in request order. The program
then drains the subscription with repeated `pull()` + `acknowledge()` until all 25 are consumed,
asserting every body arrived exactly once and that the publish ids follow request submission order.

On the connector side, a `Publish` (batched or not) **validates the whole batch before enqueuing
anything** — batch 1..1000 messages, each ≤ 10 MiB, ≤ 100 attributes; any single violation rejects
the entire batch (`INVALID_ARGUMENT`). The accepted batch is written **once** to the topic log
`pubsub.{t}`, then fanned out one copy per subscription queue `pubsub.sub.{s}`.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish` (batched), `Pull`, `Acknowledge` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | Batch **≤ 1000** messages, atomic (whole batch validated before enqueue); per-message ≤ 10 MiB, ≤ 100 attributes; ids returned in request order. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/batch-publish`](../../../go/topics/batch-publish),
  [`../../../javascript/topics/batch-publish`](../../../javascript/topics/batch-publish),
  [`../../../java/topics/batch-publish`](../../../java/topics/batch-publish),
  [`../../../csharp/topics/batch-publish`](../../../csharp/topics/batch-publish),
  [`../../../ruby/topics/batch_publish`](../../../ruby/topics/batch_publish)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> Gotcha: publish is **atomic per batch** — one oversized or malformed message (e.g. > 10 MiB or
> > 100 attributes) rejects the entire batch with `INVALID_ARGUMENT`; nothing is enqueued. Keep
> batches within the 1..1000 / 10 MiB / 100-attribute limits.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
