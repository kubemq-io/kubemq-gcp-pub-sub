# C# — topics: Topic management

The topic admin surface end to end:
`CreateTopic → GetTopic → ListTopics → ListTopicSubscriptions → UpdateTopic → DeleteTopic`
(a tombstone — existing subscriptions survive).

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client `Google.Cloud.PubSub.V1` `3.36.0`
  (pinned in `examples/csharp/Directory.Packages.props`).
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
cd examples/csharp && dotnet run --project topics/topic-management/TopicManagement.csproj
```

## Expected Output

```
[*] Created topic 'orders-<run>' (→ channel pubsub.orders-<run>)
[v] GetTopic name=orders-<run> labels.env=dev retention=1h
[*] ListTopics contains 'orders-<run>' → True
[*] Created subscription 'sub-orders-<run>' on the topic
[*] ListTopicSubscriptions → [sub-orders-<run>]
[*] UpdateTopic labels → env=staging, team=orders
[*] DeleteTopic 'orders-<run>' (tombstone — subscriptions survive)
[v] Subscription 'sub-orders-<run>' survived the topic tombstone (topic field='projects/my-project/topics/orders-<run>')
[*] Cleaned up subscription 'sub-orders-<run>'
[ok] Topic management lifecycle complete (create/get/list/update/tombstone)
```

> Resource ids are uuid-suffixed (`orders-<run>`, `sub-orders-<run>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<run>` suffix varies
> per run. Exit 0 on success, non-zero on any failed assertion.

## What's Happening

The program builds the publisher/subscriber API clients with `EmulatorDetection.EmulatorOnly`, then
walks the topic admin lifecycle. `CreateTopic` sets an initial `env=dev` label and a 1-hour
requested retention; `GetTopic` echoes the requested (un-clamped) retention and label, which the
program asserts. `ListTopics` (over the project) must contain the new topic. It attaches a
subscription and asserts `ListTopicSubscriptions` reports it. `UpdateTopic` is driven by a
`FieldMask` over `labels` only (leaving retention untouched) — the program asserts the changed
`env=staging` and added `team=orders` labels, then re-reads them with a fresh `GetTopic`. Finally it
calls `DeleteTopic`, which is a **tombstone**: the topic record is retained so the subscription
survives (it detaches rather than disappearing). The program proves this by reading the subscription
back after the topic delete, then cleans the surviving subscription up itself. `GetTopic` returns the
requested retention while fan-out/seek use the value clamped to the broker's `Store.MaxRetention`;
the topic maps to the Events Store log `pubsub.{t}` and each subscription to the queue
`pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, GetTopic, ListTopics, CreateSubscription, ListTopicSubscriptions, UpdateTopic, DeleteTopic, GetSubscription | topic `orders-<run>` → `pubsub.orders-<run>`; sub `sub-orders-<run>` → `pubsub.sub.sub-orders-<run>` | none | `UpdateTopic` FieldMask over `labels` / `message_retention_duration` / `schema_settings`; `GetTopic` echoes requested (un-clamped) retention; `DeleteTopic` is a tombstone (subscriptions survive); topic ids may not start with `sub.` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/topic-management`](../../../go/topics/topic-management),
  [`../../../python/topics/topic_management`](../../../python/topics/topic_management),
  [`../../../java/topics/topic-management`](../../../java/topics/topic-management),
  [`../../../javascript/topics/topic-management`](../../../javascript/topics/topic-management),
  [`../../../ruby/topics/topic_management`](../../../ruby/topics/topic_management)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> Topic ids may **not** start with `sub.` (the reserved broker namespace for subscription queues).
> `DeleteTopic` is a tombstone — the record is retained so existing subscriptions survive and a
> re-created topic reuses the same `pubsub.{t}` log.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
