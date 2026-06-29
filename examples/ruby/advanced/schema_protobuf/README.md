# Ruby — advanced: Schema Validation (Protobuf)

Enforce-on-publish schema validation with a Protobuf schema:
`CreateSchema(Protobuf) → CreateTopic(schema_settings, JSON) → conforming Publish OK → non-conforming Publish rejected INVALID_ARGUMENT`.

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
bundle exec ruby advanced/schema_protobuf/main.rb
```

## Expected Output

```
=== advanced/schema_protobuf — topic 'proto-topic-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateSchema -> projects/my-project/schemas/proto-order-<uuid8>  type=PROTOCOL_BUFFER  revision=<revision-id>
  [OK] schema registered as PROTOCOL_BUFFER
  [OK] schema got a server-assigned revision id (revisions kept ≥ 1)
CreateTopic -> projects/my-project/topics/proto-topic-<uuid8>  (schema_settings → proto-order-<uuid8>, encoding=JSON)
  [OK] topic bound to the schema
  [OK] topic encoding is JSON
Publish (conforming) -> message_id=<message-id>
  [OK] conforming JSON publish accepted (matches the Protobuf schema)
Publish (non-conforming) -> rejected Google::Cloud::InvalidArgumentError (INVALID_ARGUMENT)
  [OK] non-conforming publish → batch rejected INVALID_ARGUMENT (enforce-on-publish)
ValidateMessage (conforming) -> OK
  [OK] ValidateMessage rejects the non-conforming body (INVALID_ARGUMENT)

PASS — Protobuf schema enforce-on-publish complete.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` channels and schema
> ids and do not collide; `<message-id>` / `<revision-id>` vary per run. Each failed `[OK]`
> assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program builds one `Google::Cloud::PubSub` client (it honours `PUBSUB_EMULATOR_HOST`). It
registers a `proto3` message definition through the schema admin client
`pubsub.schemas.create_schema` (the schema type enum on the wire is `PROTOCOL_BUFFER`), creates a
topic bound to that schema with JSON encoding via `pubsub.topic_admin.create_topic`
(`schema_settings`), then publishes through the data-plane `pubsub.publisher`. A JSON body that
maps to the proto message (`{id: string, quantity: int32}`) is accepted; a body whose `quantity`
is a string is rejected. `pubsub.schemas.validate_message` echoes the same conform/reject decision
without publishing.

On the connector side, when a topic references a schema the publish path **enforces** it: it
validates each message's `data` against the schema (the connector validates Protobuf via its
protoreflect engine) as part of the atomic batch check, and **rejects the whole batch with
`INVALID_ARGUMENT` on the first non-conforming message** — nothing is enqueued, nothing is written
to the topic log `pubsub.{t}`. Conforming batches are written once to the log and fanned out per
subscription. Schema definitions are capped at 300 KB and the connector keeps at least one
revision.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (Protobuf), `CreateTopic` (schema_settings), `Publish`, `ValidateMessage` | topic `pubsub.{t}` (schema = registry record) | schema | Protobuf validated by the connector's protoreflect engine; topic `encoding` JSON or BINARY. Enforcement is **atomic per batch**: the whole `Publish` is rejected with `INVALID_ARGUMENT` on the first non-conforming message — nothing is enqueued. Schema definition **≤ 300 KB**; revisions kept **≥ 1** (`DeleteSchemaRevision` cannot remove the last). |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-protobuf`](../../../go/advanced/schema-protobuf),
  [`../../../python/advanced/schema_protobuf`](../../../python/advanced/schema_protobuf),
  [`../../../java/advanced/schema-protobuf`](../../../java/advanced/schema-protobuf),
  [`../../../javascript/advanced/schema-protobuf`](../../../javascript/advanced/schema-protobuf),
  [`../../../csharp/advanced/schema-protobuf`](../../../csharp/advanced/schema-protobuf)
- Sibling advanced variant: [`../schema_avro`](../schema_avro) (same enforcement over an Avro definition).
- Docs: [`../../../../docs/concepts/schema-validation.md`](../../../../docs/concepts/schema-validation.md)

> Gotcha: **enforcement is all-or-nothing per batch.** Because publish is atomic, one bad message
> rejects the entire `Publish` call and enqueues **nothing**. Validate client-side (or with
> `ValidateMessage`) or publish smaller batches if you want finer-grained failure isolation. Schema
> definitions larger than 300 KB are rejected at `CreateSchema`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
