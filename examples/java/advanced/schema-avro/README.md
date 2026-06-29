# Java — advanced: Schema (Avro)

Attach an Avro schema to a topic and enforce it on publish:
`CreateSchema(AVRO) → CreateTopic{schema_settings} → CreateSubscription → Publish(conforming) → Publish(non-conforming → batch rejected INVALID_ARGUMENT)`.

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
cd examples/java && mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.advanced.schemaavro.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/schema-avro | emulator=localhost:8085 project=my-project
CreateSchema(Avro) -> avro-schema-<uuid8> (<= 300 KB; full revision history)
CreateTopic        -> avro-topic-<uuid8> (schema avro-schema-<uuid8>, encoding JSON; channel pubsub.avro-topic-<uuid8>)
CreateSubscription -> sub-avro-<uuid8> (channel pubsub.sub.sub-avro-<uuid8>)
Publish (conforming)     -> accepted, id <message-id>
[OK] conforming publish returned a message id
Publish (non-conforming) -> rejected (batch) INVALID_ARGUMENT
[OK] schema-invalid message was rejected by the connector
[OK] rejection mentions INVALID_ARGUMENT/schema (was: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: message 0 does not conform to schema "avro-schema-<uuid8>": avro message does not conform to schema (json): field "id": expected long: got string: invalid argument)
PASS: Avro schema enforced — conforming accepted, non-conforming batch rejected
DeleteSubscription: ok
DeleteTopic: ok
DeleteSchema: ok
```

> Resource ids are uuid-suffixed (`avro-schema-<uuid8>` / `avro-topic-<uuid8>` / `sub-avro-<uuid8>`),
> so concurrent runs use distinct channels and registry records and never collide; the
> `<message-id>` value varies per run. (See `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program registers an Avro schema (`record Order { long id; string name }`) via
`SchemaServiceClient.createSchema`, then creates a topic that references it with JSON encoding
(`SchemaSettings`). With a schema bound, the connector enforces it at publish time. The first
publish — JSON that matches the record — is **accepted** and returns a message id. The second
publish sends `{"id":"not-a-long", ...}`, where `id` is a string instead of a `long`; the
connector rejects the **entire batch** with `INVALID_ARGUMENT` (the message-publish future fails),
which the example asserts on. Avro validation is done connector-side via `hamba/avro/v2`; schema
definitions are ≤ 300 KB with full revision history. The subscription, topic, and schema are
deleted on exit.

A conforming `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source) and fans out to the per-sub queue `pubsub.sub.{s}`; a non-conforming batch
never reaches the log. Any failed assertion throws and the JVM exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (AVRO), `CreateTopic` (schema_settings), `CreateSubscription`, `Publish`, `DeleteSubscription`, `DeleteTopic`, `DeleteSchema` | topic `avro-topic-<uuid8>` → `pubsub.{t}`; sub `sub-avro-<uuid8>` → `pubsub.sub.{s}`; schema `avro-schema-<uuid8>` | schema (enforce-on-publish) | conforming publish ok; non-conforming → whole batch rejected `INVALID_ARGUMENT`; Avro via `hamba/avro/v2`; definition ≤ 300 KB; full revision history |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-avro`](../../../go/advanced/schema-avro),
  [`../../../python/advanced/schema_avro`](../../../python/advanced/schema_avro),
  [`../../../javascript/advanced/schema-avro`](../../../javascript/advanced/schema-avro),
  [`../../../csharp/advanced/schema-avro`](../../../csharp/advanced/schema-avro),
  [`../../../ruby/advanced/schema_avro`](../../../ruby/advanced/schema_avro)
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
