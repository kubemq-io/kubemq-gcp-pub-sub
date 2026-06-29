# JavaScript — topics: Basic publish/subscribe

The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish → Pull → Acknowledge`.

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
cd examples/javascript && npx tsx topics/basic-publish-subscribe/index.ts
```

## Expected Output

```
[*] Created topic 'orders-<run>' (→ channel pubsub.orders-<run>)
[*] Created subscription 'sub-orders-<run>' (→ channel pubsub.sub.sub-orders-<run>)
[x] Published MessageId=<message-id>
[v] Pulled 'order #1001' MessageId=<message-id>
[*] Acknowledged the message by ack_id
[ok] Basic publish/subscribe round-trip complete
[*] Cleaned up subscription 'sub-orders-<run>'
[*] Cleaned up topic 'orders-<run>'
```

> Resource ids are uuid-suffixed (`orders-<run>`, `sub-orders-<run>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the `<run>` suffix and
> `<message-id>` vary per run. Exit 0 on success, non-zero on any failed assertion.

## What's Happening

The program first constructs the high-level `PubSub` client, which reads `PUBSUB_EMULATOR_HOST`
and resolves the emulator transport (service host/port + **insecure gRPC, no Google auth**); it then
reuses those resolved options to build the low-level `v1.PublisherClient` and `v1.SubscriberClient`,
whose RPCs map 1:1 to the connector surface. It creates a topic and a subscription, publishes one
`PubsubMessage` (`order #1001`) with a `priority=high` attribute, then pulls it back — asserting the
body, the server-assigned `MessageId`, the attribute, and a non-null server `publish_time`. It also
asserts the reserved tag `_pubsub_message_id` did **not** leak into the Pub/Sub `attributes` map. It
acknowledges by `ack_id` and best-effort deletes the subscription then the topic in `finally` blocks.
A `Publish` writes the message once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source), then the connector fans out one queue copy per subscription `pubsub.sub.{s}`,
applying that subscription's filter. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, Pull, Acknowledge | topic `orders-<run>` → `pubsub.orders-<run>`; sub `sub-orders-<run>` → `pubsub.sub.sub-orders-<run>` | none | ack deadline 10s (10..600 valid); message ≤ 10 MiB; ≤ 100 attributes; the three reserved tags (`_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key`) are stripped from `attributes` for Pub/Sub clients |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/basic-publish-subscribe`](../../../go/topics/basic-publish-subscribe),
  [`../../../python/topics/basic_publish_subscribe`](../../../python/topics/basic_publish_subscribe),
  [`../../../java/topics/basic-publish-subscribe`](../../../java/topics/basic-publish-subscribe),
  [`../../../csharp/topics/basic-publish-subscribe`](../../../csharp/topics/basic-publish-subscribe),
  [`../../../ruby/topics/basic_publish_subscribe`](../../../ruby/topics/basic_publish_subscribe)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> The project id is parsed and validated but **ignored** by the connector — resource ids are global
> across "projects", so unique-per-run names are what keep concurrent runs isolated.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
