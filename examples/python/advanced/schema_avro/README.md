# Python — advanced: Schema Validation (Avro)

Enforce an Avro schema on publish:
`CreateSchema(Avro) → CreateTopic(schema_settings) → Publish(conforming) → Publish(non-conforming → rejected)`.

## Prerequisites

- Python 3.10+ and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `../../pyproject.toml`, installed via `uv sync`).
- [`uv`](https://docs.astral.sh/uv/) (do not use `pip`/`poetry` directly).
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
cd examples/python
uv run python -m advanced.schema_avro
# or, equivalently:
uv run python advanced/schema_avro/main.py
```

## Expected Output

```
== advanced/schema_avro — schema 'avro-schema-<uuid8>', topic 'avro-topic-<uuid8>' ==
CreateSchema(Avro) -> projects/my-project/schemas/avro-schema-<uuid8>  revision=<revision-id>
[OK] schema created with a server-assigned revision id
CreateTopic -> projects/my-project/topics/avro-topic-<uuid8>  (schema_settings -> avro-schema-<uuid8>, encoding=JSON)
CreateSubscription -> projects/my-project/subscriptions/sub-avro-<uuid8>  (Queue pubsub.sub.sub-avro-<uuid8>)
Publish(conforming) -> message_id=<message-id>
[OK] conforming message accepted (message-id returned)
Publish(non-conforming) -> schema-invalid msg → batch rejected INVALID_ARGUMENT
[OK] non-conforming message rejected with INVALID_ARGUMENT
Pull -> ['{"order_id": "A-1", "amount": 12.5}']
[OK] only the conforming message was enqueued (rejected batch enqueues nothing)

PASS: Avro schema enforced on publish — conforming ok, non-conforming rejected.
```

> Resource ids are uuid-suffixed (`avro-schema-<uuid8>` / `avro-topic-<uuid8>` /
> `sub-avro-<uuid8>`) so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and
> the schema registry records do not collide; `<uuid8>`, `<revision-id>`, and `<message-id>` vary
> per run. The example deletes the subscription/topic/schema it created on exit.

## What's Happening

- `create_schema` registers an **Avro** record definition (`Order` with a `string order_id` and a
  `double amount`) under a schema id. The connector returns a **server-assigned revision id** — a
  schema is versioned, with a full revision history (`CommitSchema`/`RollbackSchema`/
  `DeleteSchemaRevision`, always keeping ≥ 1).
- `create_topic` binds the topic to that schema via `schema_settings` with `JSON` encoding, so the
  connector validates every published message against the Avro definition.
- The **conforming** publish (`order_id` string + `amount` double) is accepted and returns a
  message-id; a later `pull` confirms it reached the topic log.
- The **non-conforming** publish (`amount` is a string) is rejected with `INVALID_ARGUMENT` —
  validation is part of the atomic batch check, so the whole `Publish` call is rejected and
  **nothing** is enqueued. The final `pull` returns only the single conforming message.
- Under the hood, a Pub/Sub `Publish` to a schema-bound topic is validated before it writes once to
  the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol source) and fans out one
  queue copy per subscription `pubsub.sub.{s}`. The Avro engine is `hamba/avro/v2`; definitions are
  capped at ≤ 300 KB. Any failed assertion exits the process non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| **CreateSchema(Avro)**, CreateTopic (schema_settings), CreateSubscription, Publish, Pull, Acknowledge | `avro-topic-<uuid8>` / `sub-avro-<uuid8>` (schema `avro-schema-<uuid8>`) | schema | enforce-on-publish; **whole batch rejected `INVALID_ARGUMENT`** on first non-conforming message (nothing enqueued); Avro via `hamba/avro/v2`; definition ≤ 300 KB; full revision history |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-avro`](../../../go/advanced/schema-avro),
  [`../../../javascript/advanced/schema-avro`](../../../javascript/advanced/schema-avro),
  [`../../../java/advanced/schema-avro`](../../../java/advanced/schema-avro),
  [`../../../csharp/advanced/schema-avro`](../../../csharp/advanced/schema-avro),
  [`../../../ruby/advanced/schema_avro`](../../../ruby/advanced/schema_avro)
- [`../schema_protobuf/`](../schema_protobuf/) — the same enforcement with a Protobuf schema.
- Docs: [`../../../../docs/concepts/schema-validation.md`](../../../../docs/concepts/schema-validation.md)

> **Gotcha — schema enforcement is all-or-nothing per batch.** Because publish is atomic, one bad
> message rejects the entire `Publish` call and enqueues **nothing**. Validate client-side or
> publish smaller batches if you want finer-grained failure isolation. Schema definitions are
> capped at **300 KB**.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
