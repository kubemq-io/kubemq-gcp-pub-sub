# Go — topics: Batch Publish

Publish a batch of messages (≤ 1000) and verify the server-assigned ids come back in request order:
`CreateTopic → Publish (N) → Get each id in order`.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
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
cd examples/go && go run ./topics/batch-publish
```

## Expected Output

```
[kubemq-gcp-pub-sub] topics/batch-publish | emulator=localhost:8085 project=my-project
CreateTopic: events-<uuid8> (channel pubsub.events-<uuid8>)
Publish: issuing a batch of 10 messages (limit 1000/batch)
  [0] event-0 -> messageId=<message-id>
  [1] event-1 -> messageId=<message-id>
  [2] event-2 -> messageId=<message-id>
  [3] event-3 -> messageId=<message-id>
  [4] event-4 -> messageId=<message-id>
  [5] event-5 -> messageId=<message-id>
  [6] event-6 -> messageId=<message-id>
  [7] event-7 -> messageId=<message-id>
  [8] event-8 -> messageId=<message-id>
  [9] event-9 -> messageId=<message-id>
Verified: 10 message ids returned in request order, all distinct
PASS: batch publish verified (ids in request order, atomic batch)
DeleteTopic: ok
```

> The topic id is uuid-suffixed (`events-<uuid8>`) so concurrent runs use a distinct `pubsub.{t}`
> channel and do not collide; each `<message-id>` is server-assigned and varies per run. (See
> `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a uuid-suffixed topic, then issues N `Publish` calls in a tight loop — the
official Go client coalesces them into a publish batch under the hood. Each call returns a
`*PublishResult`; the program collects the server-assigned ids by calling `Get` on each result in
the **same slice order** it issued the publishes, and asserts it got N distinct ids aligned 1:1 to
request order. The topic is deleted on exit so re-runs start clean. Any failed assertion exits
non-zero.

The connector validates the **whole batch before enqueuing anything** (publish atomicity, spec
S3.5): the batch must be 1..1000 messages, each ≤ 10 MiB with ≤ 100 attributes; a single invalid
message rejects the entire batch with `INVALID_ARGUMENT`. Each accepted message is written once to
the topic log `pubsub.events-<uuid8>`, the authoritative replayable source the connector fans out
from.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, Publish (batch), DeleteTopic | `events-<uuid8>` / — | none | Batch ≤ 1000 messages; per message ≤ 10 MiB, ≤ 100 attributes; whole batch rejected (`INVALID_ARGUMENT`) on first invalid message; ids returned in request order. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/topics/batch_publish`](../../../python/topics/batch_publish),
  [`../../../javascript/topics/batch-publish`](../../../javascript/topics/batch-publish),
  [`../../../java/topics/batch-publish`](../../../java/topics/batch-publish),
  [`../../../csharp/topics/batch-publish`](../../../csharp/topics/batch-publish),
  [`../../../ruby/topics/batch_publish`](../../../ruby/topics/batch_publish)
- Docs: [`../../../docs/concepts/topics-and-publish.md`](../../../docs/concepts/topics-and-publish.md)
- Next: [`../basic-publish-subscribe`](../basic-publish-subscribe), [`../topic-management`](../topic-management).

> **Gotcha — atomic batch validation.** The connector validates every message in a batch before
> enqueuing any of it; one over-limit or empty message rejects the **whole** batch
> (`INVALID_ARGUMENT`), so no partial publish ever lands.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
