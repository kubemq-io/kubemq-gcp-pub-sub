# Ruby — advanced: Schema Validation (Avro)

Enforce-on-publish schema validation with an Avro schema:
`CreateSchema(Avro) → CreateTopic(schema_settings, JSON) → conforming Publish OK → non-conforming Publish rejected INVALID_ARGUMENT`.

## Prerequisites

- **Ruby 3.1+** (built/verified on rbenv **3.3.7**; `eval "$(rbenv init - zsh)"` first — see the
  Toolchain note in [`../../README.md`](../../README.md)).
- The official GCP Pub/Sub client `google-cloud-pubsub` (floor `~> 3.3`, pinned in
  [`../../Gemfile`](../../Gemfile); exact resolved versions in the gitignored `Gemfile.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled (gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the client skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
eval "$(rbenv init - zsh)"        # selects Ruby 3.3.7
cd examples/ruby
bundle exec ruby advanced/schema_avro/main.rb
```

## Expected Output

```
=== advanced/schema_avro — topic 'avro-topic-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateSchema -> projects/my-project/schemas/avro-order-<uuid8>  type=AVRO  revision=<revision-id>
  [OK] schema registered as AVRO
  [OK] schema got a server-assigned revision id (full revision history)
CreateTopic -> projects/my-project/topics/avro-topic-<uuid8>  (schema_settings → avro-order-<uuid8>, encoding=JSON)
  [OK] topic bound to the schema
  [OK] topic encoding is JSON
Publish (conforming) -> message_id=<message-id>
  [OK] conforming JSON publish accepted (matches the Avro schema)
Publish (non-conforming) -> rejected Google::Cloud::InvalidArgumentError (INVALID_ARGUMENT)
  [OK] non-conforming publish → batch rejected INVALID_ARGUMENT (enforce-on-publish)
ValidateMessage (conforming) -> OK
  [OK] ValidateMessage rejects the non-conforming body (INVALID_ARGUMENT)

PASS — Avro schema enforce-on-publish complete.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` channels and schema
> ids and do not collide; `<message-id>` / `<revision-id>` vary per run. Each failed `[OK]`
> assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program builds one `Google::Cloud::PubSub` client (it honours `PUBSUB_EMULATOR_HOST`). It
registers an Avro record schema through the schema admin client `pubsub.schemas.create_schema`,
creates a topic bound to that schema with JSON encoding via `pubsub.topic_admin.create_topic`
(`schema_settings`), then publishes through the data-plane `pubsub.publisher`. A JSON body that
matches the Avro record (`{id: string, quantity: int}`) is accepted; a body whose `quantity` is a
string is rejected. `pubsub.schemas.validate_message` echoes the same conform/reject decision
without publishing.

On the connector side, when a topic references a schema the publish path **enforces** it: it
validates each message's `data` against the schema (the connector validates Avro via its Avro
engine) as part of the atomic batch check, and **rejects the whole batch with `INVALID_ARGUMENT`
on the first non-conforming message** — nothing is enqueued, nothing is written to the topic log
`pubsub.{t}`. Conforming batches are written once to the log and fanned out per subscription.
Schema definitions are capped at 300 KB and carry a full revision history.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (Avro), `CreateTopic` (schema_settings), `Publish`, `ValidateMessage` | topic `pubsub.{t}` (schema = registry record) | schema | Avro validated by the connector's Avro engine; topic `encoding` JSON or BINARY. Enforcement is **atomic per batch**: the whole `Publish` is rejected with `INVALID_ARGUMENT` on the first non-conforming message — nothing is enqueued. Schema definition **≤ 300 KB**; full revision history (`DeleteSchemaRevision` keeps ≥ 1). |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-avro`](../../../go/advanced/schema-avro),
  [`../../../python/advanced/schema_avro`](../../../python/advanced/schema_avro),
  [`../../../java/advanced/schema-avro`](../../../java/advanced/schema-avro),
  [`../../../javascript/advanced/schema-avro`](../../../javascript/advanced/schema-avro),
  [`../../../csharp/advanced/schema-avro`](../../../csharp/advanced/schema-avro)
- Sibling advanced variant: [`../schema_protobuf`](../schema_protobuf) (same enforcement over a Protobuf definition).
- Docs: [`../../../../docs/concepts/schema-validation.md`](../../../../docs/concepts/schema-validation.md)

> Gotcha: **enforcement is all-or-nothing per batch.** Because publish is atomic, one bad message
> rejects the entire `Publish` call and enqueues **nothing**. Validate client-side (or with
> `ValidateMessage`) or publish smaller batches if you want finer-grained failure isolation. Schema
> definitions larger than 300 KB are rejected at `CreateSchema`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
