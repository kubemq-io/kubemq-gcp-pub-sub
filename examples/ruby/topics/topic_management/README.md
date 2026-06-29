# Ruby — topics: Topic management

The topic admin surface against the KubeMQ Pub/Sub connector:
`CreateTopic → ListTopics → GetTopic → UpdateTopic → CreateSubscription → ListTopicSubscriptions → DeleteTopic`.

## Prerequisites

- **Ruby 3.3.7** (rbenv; floor 3.1) and the official Google Cloud Pub/Sub client
  for Ruby `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips
  Google auth and uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7 (rbenv global)
cd examples/ruby && bundle exec ruby topics/topic_management/main.rb
```

## Expected Output

```
=== topics/topic_management — topic 'managed-<run>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

[*] CreateTopic -> projects/my-project/topics/managed-<run>  (channel pubsub.managed-<run>)
[*] ListTopics -> <n> topic(s) in projects/my-project
  [OK] ListTopics includes the topic we created
[*] GetTopic -> projects/my-project/topics/managed-<run>
  [OK] GetTopic returns the requested topic
[*] UpdateTopic -> message_retention_duration=1800s (requested)
[*]                effective value clamped to the broker's Store.MaxRetention
  [OK] UpdateTopic echoes the requested retention (un-clamped)
[*] CreateSubscription -> projects/my-project/subscriptions/sub-managed-<run>  (channel pubsub.sub.sub-managed-<run>)
[*] ListTopicSubscriptions -> 1 subscription(s): ["projects/my-project/subscriptions/sub-managed-<run>"]
  [OK] the subscription is attached to the topic
[*] CreateTopic('sub.…') -> rejected: InvalidArgumentError
  [OK] topic id starting with 'sub.' is rejected (INVALID_ARGUMENT)
[*] DeleteTopic -> tombstoned (existing subscriptions survive)
  [OK] GetTopic after DeleteTopic raises NotFound

PASS — topic management surface exercised.
[*] Cleaned up subscription 'sub-managed-<run>'
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.managed-<run>` / `pubsub.sub.sub-managed-<run>` channels and never
> cross-talk; the `<run>` suffix and the `<n>` ListTopics count vary per run.
> Cleanup is best-effort (the topic is already tombstoned by step 8, so only the
> subscription is removed here). The example exits `0` on success and non-zero on
> any failed assertion — including the negative path, which prints the rejection
> explicitly (`topic id starting with 'sub.' is rejected (INVALID_ARGUMENT)`).

## What's Happening

`topic_admin` is the auto-generated v3.x admin client. `CreateTopic` registers
the topic and maps it to the Events Store log `pubsub.{topic}`. `ListTopics`
pages the project's topics (the project segment is parsed but ignored, so ids are
global) and `GetTopic` fetches the one we created. `UpdateTopic` carries a
**FieldMask** over `message_retention_duration`; `GetTopic`/`UpdateTopic` echo
the requested (un-clamped) value, while the **effective** retention is clamped to
the broker's `Store.MaxRetention` ceiling. After `CreateSubscription` wires a
per-subscription queue `pubsub.sub.{sub}` to the topic log,
`ListTopicSubscriptions` returns that subscription's name. The negative path
proves a topic id beginning with `sub.` is rejected (`INVALID_ARGUMENT`) because
`sub.` is the reserved broker namespace for subscription queues. Finally
`DeleteTopic` is a **tombstone** — the record is retained so existing
subscriptions survive (re-creating the topic reuses the log `pubsub.{topic}`),
yet a subsequent `GetTopic` raises `NotFound`. Any failed assertion exits
non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| ListTopics, GetTopic, UpdateTopic, CreateSubscription, ListTopicSubscriptions, DeleteTopic | `managed` / `sub-managed` | none | topic ids may not start with `sub.`; retention 10 min..31 days, clamped to broker `Store.MaxRetention`; `DeleteTopic` tombstones (subscriptions survive) |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/topic-management`](../../../go/topics/topic-management),
  [`../../../python/topics/topic_management`](../../../python/topics/topic_management),
  [`../../../javascript/topics/topic-management`](../../../javascript/topics/topic-management),
  [`../../../java/topics/topic-management`](../../../java/topics/topic-management),
  [`../../../csharp/topics/topic-management`](../../../csharp/topics/topic-management)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> `DeleteTopic` is a **tombstone**, not a hard delete: the record is retained so
> any subscriptions attached to it keep working, and re-creating the topic reuses
> the same Events Store log. `GetTopic` on a tombstoned topic still returns
> `NotFound`. The requested retention is echoed un-clamped; the value actually
> applied is clamped to the broker's `Store.MaxRetention`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
