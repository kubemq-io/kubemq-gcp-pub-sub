# Ruby — topics: Basic publish / subscribe

The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish → Pull → Acknowledge`.

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
cd examples/ruby && bundle exec ruby topics/basic_publish_subscribe/main.rb
```

## Expected Output

```
=== topics/basic_publish_subscribe — topic 'orders-<run>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

[*] CreateTopic -> projects/my-project/topics/orders-<run>  (channel pubsub.orders-<run>)
  [OK] created topic name matches the requested path
[*] CreateSubscription -> projects/my-project/subscriptions/sub-orders-<run>  (channel pubsub.sub.sub-orders-<run>)
[*]                      ack_deadline=10s
  [OK] default ack deadline is the connector floor (10s)
[x] Publish -> message_id=<message-id>
  [OK] publish returned a server-assigned message id
  [OK] pulled exactly 1 message
[v] Pull -> data="hello from google-cloud-pubsub"  message_id=<message-id>
        published_at=<publish-time>  attributes={"source"=>"basic_publish_subscribe"}
  [OK] pulled body equals the published body
  [OK] pulled message_id matches the publish id
  [OK] server stamped a publish_time
  [OK] attributes round-trip; reserved _pubsub_* tags are not exposed
[*] Acknowledge -> 1 ack_id
  [OK] subscription empty after ack (no redelivery)

PASS — basic publish/subscribe round-trip complete.
[*] Cleaned up subscription 'sub-orders-<run>'
[*] Cleaned up topic 'orders-<run>'
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.orders-<run>` / `pubsub.sub.sub-orders-<run>` channels and never
> cross-talk; the `<run>` suffix, `<message-id>`, and `<publish-time>` values
> vary per run. Cleanup is best-effort. The example exits `0` on success and
> non-zero on any failed assertion.

## What's Happening

`Google::Cloud::PubSub.new(project_id:, emulator_host:)` builds a v3.x client
pointed at the connector — the emulator host alone clears credentials and dials
insecure gRPC, so there is no Google auth. The admin plane lives on
`topic_admin` / `subscription_admin`; the data plane on `publisher` /
`subscriber`.

`CreateTopic` registers the topic and maps it to the Events Store log
`pubsub.{topic}`; `CreateSubscription` wires a per-subscription queue
`pubsub.sub.{sub}` (default ack deadline 10s) to that log. `publisher.publish`
sends one message with a user attribute; the connector writes it **once** to the
topic log `pubsub.{topic}` (the authoritative, replayable, cross-protocol source
of truth) and assigns a server `message_id` + `publish_time`, then fans out one
queue copy per subscription. `subscriber.pull(immediate: false)` reads the copy
back from `pubsub.sub.{sub}`; the body, server id, and the lone caller attribute
round-trip, while the three reserved `_pubsub_*` tags are stripped for Pub/Sub
clients (native Events Store consumers still see them). `acknowledge!` removes
the message from the subscription queue, and a follow-up pull confirms the queue
is empty (no redelivery). Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic → CreateSubscription → Publish → Pull → Acknowledge | `orders` / `sub-orders` | none | one message; ack deadline default 10s (10..600s); reserved `_pubsub_*` tags stripped from `attributes` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/basic-publish-subscribe`](../../../go/topics/basic-publish-subscribe),
  [`../../../python/topics/basic_publish_subscribe`](../../../python/topics/basic_publish_subscribe),
  [`../../../javascript/topics/basic-publish-subscribe`](../../../javascript/topics/basic-publish-subscribe),
  [`../../../java/topics/basic-publish-subscribe`](../../../java/topics/basic-publish-subscribe),
  [`../../../csharp/topics/basic-publish-subscribe`](../../../csharp/topics/basic-publish-subscribe)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> The project id is parsed and validated but **ignored** — the connector is
> single-tenant like Google's emulator, so resource ids are global across
> "projects". Use uuid-suffixed names (as here) to keep concurrent runs isolated.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
