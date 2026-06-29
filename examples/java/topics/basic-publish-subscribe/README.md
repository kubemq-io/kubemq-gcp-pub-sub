# Java — topics: Basic Publish & Subscribe

The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish → Pull → Acknowledge`.

## Prerequisites

- **JDK 21+** and Maven 3.9+, with the official GCP Pub/Sub client
  `com.google.cloud:google-cloud-pubsub` managed via `com.google.cloud:libraries-bom` 26.84.0
  (pinned in `../../pom.xml`; the Pub/Sub dependency carries no `<version>`).
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
cd examples/java && mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.topics.basicpublishsubscribe.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] topics/basic-publish-subscribe | emulator=localhost:8085 project=my-project
CreateTopic: orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription: sub-orders-<uuid8> (channel pubsub.sub.sub-orders-<uuid8>)
Publish: messageId=<message-id>
Receive: messageId=<message-id> body="order #4242 — 3x widget, ship express" attr[priority]="express" publishTime=<publish-time>
Acknowledge: ok (acked by ack_id under its lease)
PASS: full Pub/Sub round-trip verified (publish -> pull -> ack)
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; `<message-id>` and
> `<publish-time>` vary per run. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program reads `PUBSUB_EMULATOR_HOST`, opens one plaintext gRPC channel to the connector, and
wires every official client (`TopicAdminClient`, `SubscriptionAdminClient`, `Publisher`,
`Subscriber`) to it with `NoCredentialsProvider` — the standard Google-emulator posture, no Google
auth and no TLS. It creates a uuid-suffixed topic and a subscription on it, publishes one message
(with a `priority` attribute) and blocks on the publish `ApiFuture` for the server-assigned message
id, then opens a StreamingPull `Subscriber`, pulls exactly that message, acks it, and stops. It
asserts the body round-trips byte-for-byte, the attribute survives, and the received message id
matches the publish id, then tears down the subscription and topic so re-runs start clean. Any
failed assertion throws and the JVM exits non-zero.

A `Publish` writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy to the per-subscription queue
`pubsub.sub.sub-orders-<uuid8>`; the message is delivered under an ack-deadline lease and the `Ack`
acks the broker seq (spec S3.3–S3.5).

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish, StreamingPull (Subscriber), Acknowledge, DeleteSubscription, DeleteTopic | `orders-<uuid8>` / `sub-orders-<uuid8>` | none | Ack deadline 10s (valid 0 or 10..600s); message ≤ 10 MiB; ≤ 100 attributes. One message published, pulled, acked. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/basic-publish-subscribe`](../../../go/topics/basic-publish-subscribe),
  [`../../../python/topics/basic_publish_subscribe`](../../../python/topics/basic_publish_subscribe),
  [`../../../javascript/topics/basic-publish-subscribe`](../../../javascript/topics/basic-publish-subscribe),
  [`../../../csharp/topics/basic-publish-subscribe`](../../../csharp/topics/basic-publish-subscribe),
  [`../../../ruby/topics/basic_publish_subscribe`](../../../ruby/topics/basic_publish_subscribe)
- Docs: [`../../../docs/concepts/topics-and-publish.md`](../../../docs/concepts/topics-and-publish.md)
- Next: [`../batch-publish`](../batch-publish), [`../topic-management`](../topic-management).

> **Gotcha — project id parsed but ignored.** The connector validates the `projects/{p}` segment
> but ignores it (single-tenant, like Google's emulator); resource ids are global across
> "projects", which is why concurrent runs MUST use uuid-suffixed topic/sub ids.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
