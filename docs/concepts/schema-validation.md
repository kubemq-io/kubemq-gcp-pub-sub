# Schema Validation

## Concept

A **schema** describes the shape every message published to a topic must conform to. When a topic
references a schema, the connector **enforces it on publish** — non-conforming messages are
rejected before they ever reach the topic log. The connector supports the two schema types Google
Pub/Sub supports: **Avro** and **Protobuf**. Schemas are stored as registry records (BoltDB) and
carry a full revision history.

## How It Works

1. `CreateSchema` registers a schema definition (Avro IDL/JSON or a Protobuf `.proto` message
   definition) under a schema id. The definition must be **≤ 300 KB**.
2. A topic is created (or updated) with `schema_settings` referencing that schema (and an
   encoding — `JSON` or `BINARY`).
3. On every `Publish` to that topic the connector validates each message's `data` against the
   schema. Validation is part of the atomic batch check: the **whole batch is rejected**
   (`INVALID_ARGUMENT`) on the **first** non-conforming message — nothing in the batch is
   enqueued.

```
CreateSchema(avro|protobuf, ≤300KB) ──▶ registry record (revisions)
CreateTopic(schema_settings → schema id, encoding) ──▶ topic bound to schema
Publish(batch) ──▶ validate each message against schema
                     │
        first non-conforming message ──▶ reject WHOLE batch (INVALID_ARGUMENT)
        all conforming                ──▶ write once to pubsub.{t}, fan out
```

## Revisions

A schema is **versioned**. The connector keeps a full revision history:

- `CommitSchema` adds a new revision to an existing schema.
- `RollbackSchema` creates a new revision that restores a prior definition.
- `DeleteSchemaRevision` removes a revision but **always keeps at least one** — you cannot delete
  the last remaining revision.
- `ListSchemaRevisions` / `GetSchema` read the history.

Topics can pin a specific revision range, so a producer evolving its schema doesn't break existing
topics until they opt in to the new revision.

## Avro vs Protobuf

| Type | How it is validated | Notes |
|------|---------------------|-------|
| **Avro** | parsed and validated via the connector's Avro engine | schema is an Avro JSON/IDL definition |
| **Protobuf** | parsed and validated via the connector's protoreflect engine | schema is a `proto` message definition |

Both enforce at publish time with identical batch-atomic semantics; the difference is only the
definition language and encoding.

> **Gotcha — enforcement is all-or-nothing per batch.** Because publish is atomic, one bad message
> rejects the entire `Publish` call. Validate client-side or publish smaller batches if you want
> finer-grained failure isolation. A rejected batch enqueues **nothing**.

> **Gotcha — schema definitions are capped at 300 KB.** A `CreateSchema`/`CommitSchema` with a
> definition larger than 300 KB is rejected. Keep schemas focused.

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| schema-avro | [go](../../examples/go/advanced/schema-avro/) | [python](../../examples/python/advanced/schema_avro/) | [java](../../examples/java/advanced/schema-avro/) | [js](../../examples/javascript/advanced/schema-avro/) | [csharp](../../examples/csharp/advanced/schema-avro/) | [ruby](../../examples/ruby/advanced/schema_avro/) |
| schema-protobuf | [go](../../examples/go/advanced/schema-protobuf/) | [python](../../examples/python/advanced/schema_protobuf/) | [java](../../examples/java/advanced/schema-protobuf/) | [js](../../examples/javascript/advanced/schema-protobuf/) | [csharp](../../examples/csharp/advanced/schema-protobuf/) | [ruby](../../examples/ruby/advanced/schema_protobuf/) |

## See Also

- [topics-and-publish.md](topics-and-publish.md) — publish atomicity (schema reuses it).
- [../reference/limits-and-rules.md](../reference/limits-and-rules.md) — the ≤ 300 KB schema limit.
- [../reference/error-codes.md](../reference/error-codes.md) — `INVALID_ARGUMENT` on non-conforming publish.
