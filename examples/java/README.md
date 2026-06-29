# Java — KubeMQ Google Cloud Pub/Sub examples

Official **`google-cloud-pubsub`** (Java) examples for the KubeMQ embedded
Google Cloud Pub/Sub emulator-protocol connector. The apps are ordinary Pub/Sub
SDK programs that connect to the connector with **zero code changes** by setting
the standard `PUBSUB_EMULATOR_HOST` environment variable — the SDK then skips
Google authentication and uses an insecure gRPC channel. (The one exception is
`interop/native-events-store`, whose native half uses the KubeMQ Java SDK to read
the topic's Events Store log over native gRPC.)

Conventions (the 15-variant master table, the `PUBSUB_EMULATOR_HOST` +
project-id convention, the per-example README template, and the cross-protocol
channel mapping `pubsub.{topic}` / `pubsub.sub.{sub}`) live in
[`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md). Each variant has its own
README with the literal expected output, a Pub/Sub-specifics table, and a gotcha
callout.

## Prerequisites

- **JDK 21+** (the examples target `maven.compiler.release=21`) and Maven 3.9+.
- The official Google Cloud Pub/Sub client — version-managed by the BOM in
  `pom.xml`:
  - **`com.google.cloud:google-cloud-pubsub`**, managed via
    **`com.google.cloud:libraries-bom` 26.84.0** (pinned in `pom.xml`; the
    Pub/Sub dependency itself carries no `<version>`).
- For `interop/native-events-store` only: **`io.kubemq.sdk:kubemq-sdk-Java`
  3.1.1** (the native gRPC Events Store peer).
- A running KubeMQ Google Cloud Pub/Sub connector reachable at
  `PUBSUB_EMULATOR_HOST` (default `localhost:8085`).
  `interop/native-events-store` also needs the native gRPC broker at
  `KUBEMQ_GRPC_ADDRESS` (default `localhost:50000`).

## Setup

```bash
export PUBSUB_EMULATOR_HOST="localhost:8085"   # connector default port; SDK uses the insecure path
export PUBSUB_PROJECT_ID="kubemq"              # parsed but not enforced by the connector (gotcha #2)
mvn -q compile
```

> Zero client code changes — the official Pub/Sub SDK honours
> `PUBSUB_EMULATOR_HOST`, clears Google credentials, and talks plain gRPC. The
> connector implements the real Pub/Sub v1 gRPC services (gRPC only — there is no
> REST/JSON v1 surface, gotcha #9).

## Run a variant

Each variant is a single runnable `Main` class wired through the Maven exec
plugin:

```bash
mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.topics.basicpublishsubscribe.Main
```

Every example prints human-readable progress, a final `OK:` line on success, and
exits **non-zero on any failed assertion** (a missing pull delivery, an
out-of-order ordering-key receive, or a rejected schema publish fails the run).

### Resource naming (concurrency-safe)

So the examples can run against a connector shared with the other language
example suites, each example creates uniquely-named, ephemeral topics and
subscriptions (e.g. a `java-` prefix and/or a short UUID suffix) and tears them
down on exit. The interop variant uses the spec's canonical topic name `orders`
(Events Store channel `pubsub.orders`).

## Variant index

| Group | Variant | Main class |
|-------|---------|-----------|
| topics | [basic-publish-subscribe](topics/basic-publish-subscribe) | `io.kubemq.examples.gcp.topics.basicpublishsubscribe.Main` |
| topics | [batch-publish](topics/batch-publish) | `io.kubemq.examples.gcp.topics.batchpublish.Main` |
| topics | [topic-management](topics/topic-management) | `io.kubemq.examples.gcp.topics.topicmanagement.Main` |
| subscriptions | [streaming-pull](subscriptions/streaming-pull) | `io.kubemq.examples.gcp.subscriptions.streamingpull.Main` |
| subscriptions | [ack-deadline-nack](subscriptions/ack-deadline-nack) | `io.kubemq.examples.gcp.subscriptions.ackdeadlinenack.Main` |
| delivery | [ordering-keys](delivery/ordering-keys) | `io.kubemq.examples.gcp.delivery.orderingkeys.Main` |
| delivery | [message-filtering](delivery/message-filtering) | `io.kubemq.examples.gcp.delivery.messagefiltering.Main` |
| delivery | [dead-letter-topic](delivery/dead-letter-topic) | `io.kubemq.examples.gcp.delivery.deadlettertopic.Main` |
| delivery | [push-delivery](delivery/push-delivery) | `io.kubemq.examples.gcp.delivery.pushdelivery.Main` |
| delivery | [exactly-once](delivery/exactly-once) | `io.kubemq.examples.gcp.delivery.exactlyonce.Main` |
| advanced | [seek-timestamp](advanced/seek-timestamp) | `io.kubemq.examples.gcp.advanced.seektimestamp.Main` |
| advanced | [snapshots](advanced/snapshots) | `io.kubemq.examples.gcp.advanced.snapshots.Main` |
| advanced | [schema-avro](advanced/schema-avro) | `io.kubemq.examples.gcp.advanced.schemaavro.Main` |
| advanced | [schema-protobuf](advanced/schema-protobuf) | `io.kubemq.examples.gcp.advanced.schemaprotobuf.Main` |
| interop | [native-events-store](interop/native-events-store) | `io.kubemq.examples.gcp.interop.nativeeventsstore.Main` |

## Java idiom notes

- **Emulator detection is automatic.** The `google-cloud-pubsub` client checks
  `PUBSUB_EMULATOR_HOST` on its own: when set it builds `TopicAdminClient`,
  `SubscriptionAdminClient`, `Publisher`, and `Subscriber` against that host over
  a plaintext channel and substitutes `NoCredentialsProvider` — so the examples
  do **not** hand-roll a `TransportChannelProvider` or credentials when the
  variable is present. (Set `PUBSUB_PROJECT_ID` / `GOOGLE_CLOUD_PROJECT` for the
  project id used in resource names.)
- **`TopicName` / `SubscriptionName` / `ProjectTopicName`.** Build fully-qualified
  resource names with the generated name helpers rather than string-formatting
  `projects/{p}/topics/{t}` by hand.
- **Admin clients are `AutoCloseable`.** `TopicAdminClient` and
  `SubscriptionAdminClient` (and `Publisher`/`Subscriber`) must be shut down;
  the examples use try-with-resources or an explicit `shutdown()` +
  `awaitTermination(...)`.
- **Ordering keys.** Per-key in-order delivery requires both
  `Publisher.Builder.setEnableMessageOrdering(true)` on the publisher AND
  `enable_message_ordering=true` on the subscription at create time.
- **Exactly-once ack handling.** With `enable_exactly_once_delivery`, ack/nack
  return an `ApiFuture` (`AckResponse`) you must await to learn whether the ack
  was confirmed; an expired/invalid `ack_id` surfaces as a failed future, not a
  silent success. Note the connector caveat: exactly-once is **node-local** — an
  `ack_id` is valid only on the node that minted it (gotcha #1), so pin a
  subscription's StreamingPull to one node in a cluster.
- **interop native half.** `interop/native-events-store` uses the KubeMQ Java SDK
  (`io.kubemq.sdk:kubemq-sdk-Java`) to open an Events Store **subscribe** stream
  on channel `pubsub.orders` against `KUBEMQ_GRPC_ADDRESS` with the
  `StartNewOnly` start position, established **before** the Pub/Sub publish, so
  the published message is in-window for the native consumer (deterministic, no
  startup race).

---

**Authentication:** the connector runs in **emulator mode — no authentication,
no TLS**, exactly like Google's local Pub/Sub emulator. Setting
`PUBSUB_EMULATOR_HOST` makes every official client library (and `gcloud`) skip
Google auth and use insecure gRPC. The connector's DoS guards
(`MaxRecvMsgSize`, `MaxInflightPerSubscription`, `MaxConcurrentPolls`,
`MaxSeekReplay`, push backoff) remain active. Do not expose port `8085` to
untrusted networks. See
[`../../docs/guides/connectivity-and-emulator-mode.md`](../../docs/guides/connectivity-and-emulator-mode.md).
