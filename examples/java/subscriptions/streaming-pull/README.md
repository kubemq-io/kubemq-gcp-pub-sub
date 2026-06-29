# Java — subscriptions: Streaming pull

A long-lived StreamingPull round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish(N) → Subscriber (StreamingPull, flow control) → Acknowledge`.

## Prerequisites

- JDK 21+ (the examples target `maven.compiler.release=21`) and Maven 3.9+, plus the official
  GCP Pub/Sub client `com.google.cloud:google-cloud-pubsub` (managed via
  `com.google.cloud:libraries-bom` **26.84.0**, pinned in `examples/java/pom.xml`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set (the Java program wires a plaintext
  `ManagedChannel` to that host with `NoCredentialsProvider` — see the Java idiom note in
  `../../README.md`):
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/java && mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.subscriptions.streamingpull.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] subscriptions/streaming-pull | emulator=localhost:8085 project=my-project
CreateTopic        -> orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription -> sub-orders-<uuid8> (channel pubsub.sub.sub-orders-<uuid8>)
Publish            -> 5 messages to the topic log
StreamingPull      -> stream open (Subscriber); acking each via callback
Received           -> 5 messages over the StreamingPull stream
  [OK] received all 5 published messages
  [OK] every published message was delivered exactly once
  [OK] no duplicate deliveries (each acked once)

PASS: StreamingPull round-trip complete — all messages drained and acked.
DeleteSubscription -> ok
DeleteTopic        -> ok
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.orders-<uuid8>` / `pubsub.sub.sub-orders-<uuid8>` channels and do not collide; the
> `<uuid8>` suffix and each `<message-id>` vary per run. Exits `0` on success, non-zero on any
> failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a uuid-suffixed topic and subscription (`ackDeadline=10s`, the connector
minimum), publishes a 5-message batch — blocking on each `publish(...)` future until the connector
acks it — then opens a long-lived **StreamingPull** stream via the high-level `Subscriber`, which is
the Java client's StreamingPull driver. `FlowControlSettings.setMaxOutstandingElementCount` is the
client-side ceiling on outstanding (un-acked) messages; the connector also enforces its own
server-side `MaxOutstandingMessages` / `MaxInflightPerSubscription` safety nets. `setParallelPullCount(1)`
keeps one StreamingPull stream open so the proof is deterministic. As each message arrives the
`MessageReceiver` callback acks it (`consumer.ack()`) under its ack-deadline lease and records the
`seq` attribute; once all five distinct messages are drained the subscriber is stopped cleanly. The
connector also closes idle streams every `StreamCloseSeconds` (default 1800s) so SDKs transparently
reconnect with no loss.

A Publish writes once to the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source), then fans out one queue copy per subscription `pubsub.sub.{s}`; StreamingPull reads from
that per-sub queue through a poller and leases each delivered message under an ack-deadline lease.
Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `StreamingPull` (open + ack via `Subscriber`) | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | batch ≤ 1000; client-side flow control `MaxOutstandingElementCount` (server caps with `MaxInflightPerSubscription`); ack deadline `0` or `10..600`s; streams force-closed every `StreamCloseSeconds` (default 1800s) → SDK reconnect |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/subscriptions/streaming-pull`](../../../go/subscriptions/streaming-pull),
  [`../../../python/subscriptions/streaming_pull`](../../../python/subscriptions/streaming_pull),
  [`../../../javascript/subscriptions/streaming-pull`](../../../javascript/subscriptions/streaming-pull),
  [`../../../csharp/subscriptions/streaming-pull`](../../../csharp/subscriptions/streaming-pull),
  [`../../../ruby/subscriptions/streaming_pull`](../../../ruby/subscriptions/streaming_pull)
- Docs: [`../../../../docs/concepts/subscriptions-and-pull.md`](../../../../docs/concepts/subscriptions-and-pull.md)

> The project id is parsed and validated but **ignored** (single-tenant, like the emulator):
> resource ids are global across "projects", which is exactly why this example uuid-suffixes the
> topic/subscription names so parallel runs occupy private `pubsub.{t}` / `pubsub.sub.{s}` channels.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
