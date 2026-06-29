# JavaScript/TypeScript — advanced: Schema Validation (Protobuf)

Attach a Protobuf schema to a topic and enforce it on publish: `CreateSchema(Protobuf) → CreateTopic{schemaSettings} → CreateSubscription → Publish(conforming) → Publish(non-conforming → batch rejected INVALID_ARGUMENT)`.

## Prerequisites

- **Node.js 18+** (developed against Node 20+) and the official GCP Pub/Sub client `@google-cloud/pubsub` 5.3.1 (pinned in `examples/javascript/package.json`). Run with `tsx`.
- A running KubeMQ broker with the Pub/Sub connector enabled (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK clears credentials and uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/javascript && npx tsx advanced/schema-protobuf/index.ts
```

## Expected Output

```
Connecting to KubeMQ Pub/Sub connector via PUBSUB_EMULATOR_HOST=localhost:8085
CreateSchema(Proto) -> proto-schema-<uuid8> (<= 300 KB; >= 1 revision kept)
CreateTopic         -> proto-topic-<uuid8>  (schema proto-schema-<uuid8>, encoding JSON; channel pubsub.proto-topic-<uuid8>)
CreateSubscription  -> sub-proto-<uuid8>  (channel pubsub.sub.sub-proto-<uuid8>)
Publish (conforming)     -> accepted, id <message-id>
Publish (non-conforming) -> rejected (batch) INVALID_ARGUMENT

PASS: Protobuf schema enforced — conforming message accepted, non-conforming batch rejected INVALID_ARGUMENT.
```

> Resource ids are uuid-suffixed (`proto-schema-<uuid8>` / `proto-topic-<uuid8>` / `sub-proto-<uuid8>`), so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and schema registry records and do not collide; the `<message-id>` value varies per run.

## What's Happening

`pubsub.createSchema(id, SchemaTypes.ProtocolBuffer, definition)` registers a proto3 message (`Order { int64 id = 1; string name = 2; }`) as a registry record (BoltDB), capped at 300 KB with revision history (at least one revision is always kept). `createTopic({ name, schemaSettings: { schema, encoding: JSON } })` binds that schema to the topic, after which the connector enforces it at publish time: a JSON-encoded message matching the proto message is accepted (a `<message-id>` is returned), while a message carrying a field not declared in the proto is rejected. Validation is **atomic over the whole batch** — every message is validated before anything is enqueued and the *entire* batch is rejected with `INVALID_ARGUMENT` on the first non-conforming message, so nothing reaches the topic log `pubsub.{t}` or fans out to `pubsub.sub.{s}`. Protobuf is handled connector-side via `jhump/protoreflect`. Any failed assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateSchema, CreateTopic(schema_settings), CreateSubscription, Publish | `proto-topic-<uuid8>` / `sub-proto-<uuid8>` (schema `proto-schema-<uuid8>`) | schema | schema definition ≤ 300 KB; enforce-on-publish; whole batch rejected `INVALID_ARGUMENT` on first non-conforming message; batch ≤ 1000; revisions kept ≥ 1 |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-protobuf`](../../../go/advanced/schema-protobuf), [`../../../python/advanced/schema_protobuf`](../../../python/advanced/schema_protobuf), [`../../../java/advanced/schema-protobuf`](../../../java/advanced/schema-protobuf), [`../../../csharp/advanced/schema-protobuf`](../../../csharp/advanced/schema-protobuf), [`../../../ruby/advanced/schema_protobuf`](../../../ruby/advanced/schema_protobuf)
- Docs: [`../../../docs/concepts/schema-validation.md`](../../../docs/concepts/schema-validation.md)
- Related: [`../schema-avro/`](../schema-avro/) (the same enforce-on-publish flow with an Avro schema)

> **Atomic batch rejection.** A schema-bound topic enforces on publish: the whole batch is rejected with `INVALID_ARGUMENT` on the first non-conforming message — nothing in that batch is enqueued.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
