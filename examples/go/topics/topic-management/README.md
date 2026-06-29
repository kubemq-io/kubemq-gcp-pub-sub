# Go — topics: Topic Management

The topic administrative surface against the KubeMQ Pub/Sub connector:
`CreateTopic → ListTopics → GetTopic → UpdateTopic → ListTopicSubscriptions → DeleteTopic (tombstone)`.

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
cd examples/go && go run ./topics/topic-management
```

## Expected Output

```
[kubemq-gcp-pub-sub] topics/topic-management | emulator=localhost:8085 project=my-project
CreateTopic: managed-<uuid8> (channel pubsub.managed-<uuid8>)
CreateSubscription: sub-managed-<uuid8> (channel pubsub.sub.sub-managed-<uuid8>)
ListTopics: found managed-<uuid8>
GetTopic: labels=map[stage:demo team:orders] retention=30m0s
UpdateTopic: labels=map[stage:production team:orders]
ListTopicSubscriptions: found sub-managed-<uuid8> (1 total on topic)
DeleteTopic: ok (tombstone — Exists=false)
PASS: topic management verified (list/get/update/list-subs/delete-tombstone)
```

> Resource ids are uuid-suffixed (`managed-<uuid8>` / `sub-managed-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the per-topic subscription
> count reflects only this run's resources. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a uuid-suffixed topic (with labels and a 30-minute retention) plus one
subscription on it, then walks the admin RPCs: `ListTopics` confirms the topic is in the
project-wide list, `GetTopic` (`Config`) reads its labels and retention back, `UpdateTopic` replaces
the label set and asserts the mutation stuck, and `ListTopicSubscriptions` (`topic.Subscriptions`)
confirms the subscription is attached. Finally `DeleteTopic` removes the topic and `Exists` asserts
the delete is a **tombstone** (returns `false`). The leftover subscription is cleaned up on exit.
Any failed assertion exits non-zero.

Each topic maps to the Events Store log `pubsub.managed-<uuid8>` and each subscription to the queue
`pubsub.sub.sub-managed-<uuid8>` (spec S3.3). The requested `RetentionDuration` is **clamped to the
broker's `Store.MaxRetention`**: `GetTopic` returns the requested value, while fan-out, seek, and the
dashboard use the clamped value (spec S3.6).

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, ListTopics, GetTopic, UpdateTopic, CreateSubscription, ListTopicSubscriptions, DeleteTopic | `managed-<uuid8>` / `sub-managed-<uuid8>` | none | Topic ids may not start with `sub.` (reserved); retention 10 min..31 days clamped to broker `Store.MaxRetention`; DeleteTopic is a tombstone. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/topics/topic_management`](../../../python/topics/topic_management),
  [`../../../javascript/topics/topic-management`](../../../javascript/topics/topic-management),
  [`../../../java/topics/topic-management`](../../../java/topics/topic-management),
  [`../../../csharp/topics/topic-management`](../../../csharp/topics/topic-management),
  [`../../../ruby/topics/topic_management`](../../../ruby/topics/topic_management)
- Docs: [`../../../docs/concepts/topics-and-publish.md`](../../../docs/concepts/topics-and-publish.md)
- Next: [`../basic-publish-subscribe`](../basic-publish-subscribe), [`../batch-publish`](../batch-publish).

> **Gotcha — topic ids may not start with `sub.`.** That prefix is the reserved broker namespace for
> subscription queues (`pubsub.sub.{s}`); a topic id beginning with `sub.` is rejected. Retention is
> also clamped to the broker's `Store.MaxRetention`, so `GetTopic` may report a longer retention than
> fan-out/seek actually apply.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
