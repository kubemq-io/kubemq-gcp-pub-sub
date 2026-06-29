# Python — topics: Topic Management

The Publisher admin surface against the connector:
`CreateTopic → GetTopic → UpdateTopic(labels) → ListTopics → ListTopicSubscriptions → DeleteTopic (tombstone)`.

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
cd examples/python && uv run python -m topics.topic_management
# or, equivalently:
cd examples/python && uv run python topics/topic_management/main.py
```

## Expected Output

```
=== topics/topic_management — topic 'managed-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateTopic -> projects/my-project/topics/managed-<uuid8>
GetTopic -> projects/my-project/topics/managed-<uuid8>
  [OK] GetTopic returns the created topic path
UpdateTopic labels -> {'env': 'dev', 'team': 'platform'}
  [OK] labels round-trip through UpdateTopic
ListTopics -> <N> topic(s) in project
  [OK] created topic appears in ListTopics
ListTopicSubscriptions -> ['projects/my-project/subscriptions/sub-managed-<uuid8>']
  [OK] attached subscription appears in ListTopicSubscriptions
DeleteTopic -> tombstoned
  [OK] GetTopic after DeleteTopic -> NOT_FOUND (tombstone)
  [OK] topic id starting with 'sub.' rejected -> INVALID_ARGUMENT (gotcha #7)

PASS — topic management surface exercised.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide. `<N>` (the `ListTopics` count) varies with what
> else exists on the connector — the example asserts *membership*, not the count. Each failed
> `[OK]` assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

Topic administration lives on the same `PublisherClient` as `publish()`. The program creates a
topic, reads it back with `GetTopic`, then mutates it with `UpdateTopic` + a `FieldMask` so only the
`labels` field changes. `ListTopics` confirms the topic is registered project-wide; after attaching
a subscription, `ListTopicSubscriptions` reflects the attachment. `DeleteTopic` then **tombstones**
the topic — a subsequent `GetTopic` raises `NOT_FOUND`. Finally it proves the reserved-namespace
rule: a topic id beginning with `sub.` is rejected with `INVALID_ARGUMENT`. Every step asserts its
outcome with `[OK]` lines and exits non-zero on any failure.

On the connector side, each topic maps to the Events Store log `pubsub.{t}` and each subscription to
the queue `pubsub.sub.{s}`. The `sub.` topic-id prefix is reserved precisely because subscription
queues already live under the `pubsub.sub.*` namespace, so allowing a `sub.`-prefixed topic would
collide with that reserved broker namespace.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `GetTopic`, `UpdateTopic`, `ListTopics`, `ListTopicSubscriptions`, `DeleteTopic` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | Resource id 3..255 chars, must start with a letter, charset `[A-Za-z0-9._~%+-]`, no `goog` prefix; **topic ids may not start with `sub.`**; `UpdateTopic` requires a `FieldMask`. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/topic-management`](../../../go/topics/topic-management),
  [`../../../javascript/topics/topic-management`](../../../javascript/topics/topic-management),
  [`../../../java/topics/topic-management`](../../../java/topics/topic-management),
  [`../../../csharp/topics/topic-management`](../../../csharp/topics/topic-management),
  [`../../../ruby/topics/topic_management`](../../../ruby/topics/topic_management)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> Gotcha (#7): **topic ids may not start with `sub.`** — that prefix is the reserved broker
> namespace for subscription queues (`pubsub.sub.{s}`). A `sub.`-prefixed topic id is rejected with
> `INVALID_ARGUMENT`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
