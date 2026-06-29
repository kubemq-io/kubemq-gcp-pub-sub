# JavaScript — topics: Topic management

The topic admin surface against the connector:
`CreateTopic → GetTopic → ListTopics → ListTopicSubscriptions → UpdateTopic(FieldMask) → DeleteTopic (tombstone)`.

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
cd examples/javascript && npx tsx topics/topic-management/index.ts
```

## Expected Output

```
[*] Created topic 'managed-<run>' (→ channel pubsub.managed-<run>)
[v] GetTopic name=managed-<run> labels.env=dev
[*] ListTopics contains 'managed-<run>' → true
[*] Created subscription 'sub-managed-<run>' on the topic
[*] ListTopicSubscriptions → [sub-managed-<run>]
[*] UpdateTopic labels → env=staging, team=orders
[*] CreateTopic 'sub.bad-<run>' rejected → true
[*] DeleteTopic 'managed-<run>' (tombstone — subscriptions survive)
[v] Subscription 'sub-managed-<run>' survived the topic tombstone (topic field='projects/my-project/topics/managed-<run>')
[*] Cleaned up subscription 'sub-managed-<run>'
[ok] Topic management lifecycle complete (create/get/list/update/tombstone)
```

> Resource ids are uuid-suffixed (`managed-<run>`, `sub-managed-<run>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide. `ListTopics` asserts
> *membership* of the freshly created topic (not a count, which varies with what else exists on the
> connector); the `<run>` suffix varies per run. Exit 0 on success, non-zero on any failed assertion.

## What's Happening

Topic administration lives on the `v1.PublisherClient` (subscription reads on the
`v1.SubscriberClient`), both built from the emulator options resolved by the high-level `PubSub`
client (insecure gRPC, no Google auth). The program creates a topic with an initial label, reads it
back with `GetTopic`, confirms it is registered project-wide via `ListTopics`, attaches a
subscription, and verifies the attachment with `ListTopicSubscriptions`. It then mutates the topic
with `UpdateTopic` + a `FieldMask` (`paths: ["labels"]`) so only the `labels` field changes, and
re-reads to confirm the change is durable. Next it proves the reserved-namespace rule: a topic id
beginning with `sub.` is rejected with `INVALID_ARGUMENT`. Finally `DeleteTopic` **tombstones** the
topic — the record is retained so the attached subscription **survives** (detaches rather than being
destroyed), which the program proves by reading the subscription back with `GetSubscription` after
the delete; it then deletes the surviving subscription itself. On the connector side, each topic maps
to the Events Store log `pubsub.{t}` and each subscription to the queue `pubsub.sub.{s}`; the `sub.`
topic-id prefix is reserved precisely because subscription queues already live under the
`pubsub.sub.*` namespace. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, GetTopic, ListTopics, ListTopicSubscriptions, UpdateTopic, DeleteTopic, GetSubscription | topic `managed-<run>` → `pubsub.managed-<run>`; sub `sub-managed-<run>` → `pubsub.sub.sub-managed-<run>` | none | Resource id 3..255 chars, must start with a letter, charset `[A-Za-z0-9._~%+-]`, no `goog` prefix; **topic ids may not start with `sub.`**; `UpdateTopic` requires a `FieldMask`; `DeleteTopic` is a tombstone (subscriptions survive); retention clamped to broker `Store.MaxRetention` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/topic-management`](../../../go/topics/topic-management),
  [`../../../python/topics/topic_management`](../../../python/topics/topic_management),
  [`../../../java/topics/topic-management`](../../../java/topics/topic-management),
  [`../../../csharp/topics/topic-management`](../../../csharp/topics/topic-management),
  [`../../../ruby/topics/topic_management`](../../../ruby/topics/topic_management)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> Gotcha (#7): **topic ids may not start with `sub.`** — that prefix is the reserved broker
> namespace for subscription queues (`pubsub.sub.{s}`). A `sub.`-prefixed topic id is rejected with
> `INVALID_ARGUMENT`. And `DeleteTopic` is a **tombstone**: the record is retained so existing
> subscriptions survive (they detach), and re-creating the topic reuses the same log.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
