# JavaScript — topics: Batch publish

One `Publish` RPC carrying a batch of messages (1..1000), validated atomically, with server ids
returned **in request order**: `CreateTopic → CreateSubscription → Publish(batch) → Pull → Acknowledge`.

## Prerequisites

- **Node 18+** (run via `tsx`) and the official GCP Pub/Sub client `@google-cloud/pubsub` `5.3.1`
  (pinned in `examples/javascript/package.json`).
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
cd examples/javascript && npx tsx topics/batch-publish/index.ts
```

## Expected Output

```
[*] Created topic 'events-<run>' (→ channel pubsub.events-<run>)
[*] Created subscription 'sub-events-<run>' (→ channel pubsub.sub.sub-events-<run>)
[x] Published 5 messages in one Publish RPC
[*]   ids[0] = <message-id>  ↔  'order #1001'
[*]   ids[1] = <message-id>  ↔  'order #1002'
[*]   ids[2] = <message-id>  ↔  'order #1003'
[*]   ids[3] = <message-id>  ↔  'order #1004'
[*]   ids[4] = <message-id>  ↔  'order #1005'
[v] Pulled and acked 5 messages
[ok] Batch publish of 5 messages complete (ids returned in request order)
[*] Cleaned up subscription 'sub-events-<run>'
[*] Cleaned up topic 'events-<run>'
```

> Resource ids are uuid-suffixed (`events-<run>`, `sub-events-<run>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<run>` suffix and the
> per-message `<message-id>` values vary per run. Exit 0 on success, non-zero on any failed
> assertion.

## What's Happening

The program builds the high-level `PubSub` client (which resolves the emulator transport from
`PUBSUB_EMULATOR_HOST` — insecure gRPC, no Google auth) and reuses its options to build the
low-level `v1.PublisherClient` / `v1.SubscriberClient`. It constructs a 5-element list of
`PubsubMessage`s (`order #1001`..`order #1005`, each tagged with a 1-based `seq` attribute) and sends
the whole list in a **single** `publish` call. The connector validates the entire batch before
enqueuing anything (publish atomicity — any one invalid message rejects the whole batch with
`INVALID_ARGUMENT`), so the program asserts it received exactly five ids and that `messageIds[i]`
corresponds to `messages[i]` (ids returned in request order). It then pulls all five back (looping
until the full batch arrives), acks each batch by `ack_id`, and asserts every published body
round-trips with the matching server id from the publish response. The topic and subscription are
best-effort deleted in `finally` blocks. Each batched message is written once to the topic log
`pubsub.{t}` (the authoritative, replayable, cross-protocol source) and then fanned out as one queue
copy per subscription `pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish (batch), Pull, Acknowledge | topic `events-<run>` → `pubsub.events-<run>`; sub `sub-events-<run>` → `pubsub.sub.sub-events-<run>` | none | batch ≤ 1000 messages, validated atomically; per message ≤ 10 MiB total, ≤ 100 attributes; ids returned in request order |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/batch-publish`](../../../go/topics/batch-publish),
  [`../../../python/topics/batch_publish`](../../../python/topics/batch_publish),
  [`../../../java/topics/batch-publish`](../../../java/topics/batch-publish),
  [`../../../csharp/topics/batch-publish`](../../../csharp/topics/batch-publish),
  [`../../../ruby/topics/batch_publish`](../../../ruby/topics/batch_publish)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> Publish is atomic over the whole batch: the connector validates every message (size, attribute
> count, ids) before enqueuing any of them, so a single malformed message rejects the entire batch
> with `INVALID_ARGUMENT` — nothing is partially published.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
