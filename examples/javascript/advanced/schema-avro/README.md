# JavaScript/TypeScript — advanced: Schema Validation (Avro)

Attach an Avro schema to a topic and enforce it on publish: `CreateSchema(Avro) → CreateTopic{schemaSettings} → CreateSubscription → Publish(conforming) → Publish(non-conforming → batch rejected INVALID_ARGUMENT)`.

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
cd examples/javascript && npx tsx advanced/schema-avro/index.ts
```

## Expected Output

```
Connecting to KubeMQ Pub/Sub connector via PUBSUB_EMULATOR_HOST=localhost:8085
CreateSchema(Avro) -> avro-schema-<uuid8> (<= 300 KB; full revision history)
CreateTopic        -> avro-topic-<uuid8>  (schema avro-schema-<uuid8>, encoding JSON; channel pubsub.avro-topic-<uuid8>)
CreateSubscription -> sub-avro-<uuid8>  (channel pubsub.sub.sub-avro-<uuid8>)
Publish (conforming)     -> accepted, id <message-id>
Publish (non-conforming) -> rejected (batch) INVALID_ARGUMENT

PASS: Avro schema enforced — conforming message accepted, non-conforming batch rejected INVALID_ARGUMENT.
```

> Resource ids are uuid-suffixed (`avro-schema-<uuid8>` / `avro-topic-<uuid8>` / `sub-avro-<uuid8>`), so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and schema registry records and do not collide; the `<message-id>` value varies per run.

## What's Happening

`pubsub.createSchema(id, SchemaTypes.Avro, definition)` registers an Avro record (`{ id: long, name: string }`) as a registry record (BoltDB), capped at 300 KB with full revision history. `createTopic({ name, schemaSettings: { schema, encoding: JSON } })` binds that schema to the topic. From then on the connector enforces the schema at publish time: a JSON-encoded message matching the record is accepted (a `<message-id>` is returned), while a message with the wrong field type (`id` sent as a string instead of a `long`) is rejected. Validation is **atomic over the whole batch** — the connector validates every message before enqueuing anything and rejects the *entire* batch with `INVALID_ARGUMENT` on the first non-conforming message, so nothing reaches the topic log `pubsub.{t}` or fans out to `pubsub.sub.{s}`. Avro is handled connector-side via `hamba/avro/v2`. Any failed assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateSchema, CreateTopic(schema_settings), CreateSubscription, Publish | `avro-topic-<uuid8>` / `sub-avro-<uuid8>` (schema `avro-schema-<uuid8>`) | schema | schema definition ≤ 300 KB; enforce-on-publish; whole batch rejected `INVALID_ARGUMENT` on first non-conforming message; batch ≤ 1000; full revision history |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-avro`](../../../go/advanced/schema-avro), [`../../../python/advanced/schema_avro`](../../../python/advanced/schema_avro), [`../../../java/advanced/schema-avro`](../../../java/advanced/schema-avro), [`../../../csharp/advanced/schema-avro`](../../../csharp/advanced/schema-avro), [`../../../ruby/advanced/schema_avro`](../../../ruby/advanced/schema_avro)
- Docs: [`../../../docs/concepts/schema-validation.md`](../../../docs/concepts/schema-validation.md)
- Related: [`../schema-protobuf/`](../schema-protobuf/) (the same enforce-on-publish flow with a Protobuf schema)

> **Atomic batch rejection.** A schema-bound topic enforces on publish: the whole batch is rejected with `INVALID_ARGUMENT` on the first non-conforming message — nothing in that batch is enqueued.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
