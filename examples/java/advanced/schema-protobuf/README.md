# Java — advanced: Schema (Protobuf)

Attach a Protobuf schema to a topic and enforce it on publish:
`CreateSchema(PROTOCOL_BUFFER) → CreateTopic{schema_settings} → CreateSubscription → Publish(conforming) → Publish(non-conforming → batch rejected INVALID_ARGUMENT)`.

## Prerequisites

- **JDK 21+** (the examples target `maven.compiler.release=21`) and Maven 3.9+, plus the
  official GCP Pub/Sub client **`com.google.cloud:google-cloud-pubsub`**, version-managed via
  **`com.google.cloud:libraries-bom` 26.84.0** (pinned in `examples/java/pom.xml`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no Google credentials**, the connector skips Google
  auth and uses insecure gRPC:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/java && mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.advanced.schemaprotobuf.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/schema-protobuf | emulator=localhost:8085 project=my-project
CreateSchema(Proto) -> proto-schema-<uuid8> (<= 300 KB; >= 1 revision kept)
CreateTopic         -> proto-topic-<uuid8> (schema proto-schema-<uuid8>, encoding JSON; channel pubsub.proto-topic-<uuid8>)
CreateSubscription  -> sub-proto-<uuid8> (channel pubsub.sub.sub-proto-<uuid8>)
Publish (conforming)     -> accepted, id <message-id>
[OK] conforming publish returned a message id
Publish (non-conforming) -> rejected (batch) INVALID_ARGUMENT
[OK] schema-invalid message was rejected by the connector
[OK] rejection mentions INVALID_ARGUMENT/schema (was: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: message 0 does not conform to schema "proto-schema-<uuid8>": protobuf message does not conform to schema (json): message type Order has no known field named bogus: invalid argument)
PASS: Protobuf schema enforced — conforming accepted, non-conforming batch rejected
DeleteSubscription: ok
DeleteTopic: ok
DeleteSchema: ok
```

> Resource ids are uuid-suffixed (`proto-schema-<uuid8>` / `proto-topic-<uuid8>` / `sub-proto-<uuid8>`),
> so concurrent runs use distinct channels and registry records and never collide; the
> `<message-id>` value varies per run. (See `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program registers a Protobuf schema (`message Order { int64 id = 1; string name = 2; }`) via
`SchemaServiceClient.createSchema`, then creates a topic that references it with JSON encoding
(`SchemaSettings`). With a schema bound, the connector enforces it at publish time. The first
publish — JSON that matches the message — is **accepted** and returns a message id. The second
publish includes an unknown field (`"bogus"`) not present in the proto; the connector rejects the
**entire batch** with `INVALID_ARGUMENT` (the message-publish future fails), which the example
asserts on. Protobuf validation is done connector-side via `jhump/protoreflect`; schema
definitions are ≤ 300 KB and at least one revision is always kept. The subscription, topic, and
schema are deleted on exit.

A conforming `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source) and fans out to the per-sub queue `pubsub.sub.{s}`; a non-conforming batch
never reaches the log. Any failed assertion throws and the JVM exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (PROTOCOL_BUFFER), `CreateTopic` (schema_settings), `CreateSubscription`, `Publish`, `DeleteSubscription`, `DeleteTopic`, `DeleteSchema` | topic `proto-topic-<uuid8>` → `pubsub.{t}`; sub `sub-proto-<uuid8>` → `pubsub.sub.{s}`; schema `proto-schema-<uuid8>` | schema (enforce-on-publish) | conforming publish ok; non-conforming → whole batch rejected `INVALID_ARGUMENT`; Protobuf via `jhump/protoreflect`; definition ≤ 300 KB; revisions kept ≥ 1 |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-protobuf`](../../../go/advanced/schema-protobuf),
  [`../../../python/advanced/schema_protobuf`](../../../python/advanced/schema_protobuf),
  [`../../../javascript/advanced/schema-protobuf`](../../../javascript/advanced/schema-protobuf),
  [`../../../csharp/advanced/schema-protobuf`](../../../csharp/advanced/schema-protobuf),
  [`../../../ruby/advanced/schema_protobuf`](../../../ruby/advanced/schema_protobuf)
- Docs: [`../../../docs/concepts/schema-validation.md`](../../../docs/concepts/schema-validation.md)

> **Gotcha — schema enforcement rejects the whole batch.** When a topic references a schema, a
> single non-conforming message fails the **entire** publish batch with `INVALID_ARGUMENT` — no
> partial accept. Validate client-side before batching mixed payloads. (Java note: when
> `PUBSUB_EMULATOR_HOST` is set the example builds the `SchemaServiceClient`, admin clients, and
> `Publisher` on a plaintext channel to that host with `NoCredentialsProvider`, so no Google auth
> is attempted.)

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
