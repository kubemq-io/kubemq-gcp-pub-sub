# Go — advanced: Schema (Avro)

Register an Avro schema, bind a topic to it, and prove enforce-on-publish:
`CreateSchema(AVRO) → CreateTopic(schema_settings) → Publish(conforming) → Publish(bad batch) →
Pull` — the conforming message lands; a batch with one non-conforming message is rejected whole.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
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
cd examples/go && go run ./advanced/schema-avro
```

## Expected Output

```
[kubemq-gcp-pub-sub] advanced/schema-avro | emulator=localhost:8085 project=my-project
CreateSchema(Avro): avro-schema-<uuid8> revision=<revision-id>
[OK] schema created with a server-assigned revision id
CreateTopic: avro-topic-<uuid8> (channel pubsub.avro-topic-<uuid8>, schema_settings -> avro-schema-<uuid8>, encoding=JSON)
CreateSubscription: sub-avro-<uuid8> (channel pubsub.sub.sub-avro-<uuid8>)
Publish(conforming): messageId=<message-id>
[OK] conforming message accepted (message-id returned)
Publish(batch w/ 1 invalid) -> schema-invalid msg → batch rejected INVALID_ARGUMENT
[OK] non-conforming message rejected with INVALID_ARGUMENT
[OK] the conforming sibling in the rejected batch was NOT enqueued (atomic batch)
Pull -> [{"order_id":"A-1","amount":12.5}]
[OK] only the conforming message was enqueued (rejected batch enqueues nothing)
PASS: Avro schema enforced on publish — conforming ok, non-conforming batch rejected
DeleteSubscription: ok
DeleteTopic: ok
DeleteSchema: ok
```

> Resource ids are uuid-suffixed (`avro-schema-<uuid8>` / `avro-topic-<uuid8>` / `sub-avro-<uuid8>`),
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> `<message-id>` / `<revision-id>` values vary per run. (See
> `../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program registers an Avro `Order` schema (a string `order_id` and a double `amount`, ≤ 300 KB)
via `SchemaClient.CreateSchema`, then creates a topic whose `SchemaSettings` reference that schema
with **JSON** encoding — so each published message's `data` is validated as JSON against the Avro
record. A conforming message (`{"order_id":"A-1","amount":12.5}`) is accepted. A two-message batch
whose second message has a string `amount` is then published and `Flush`-ed as one server-side
batch: because publish is **atomic**, the connector rejects the **whole batch** with
`INVALID_ARGUMENT` on the first non-conforming message, and **nothing** in that batch is enqueued —
the conforming sibling's `PublishResult.Get` returns the same error. A subsequent pull therefore
returns exactly the one earlier conforming message. The subscription, topic, and schema are cleaned
up on exit.

The connector parses the Avro definition (via `hamba/avro/v2`) and validates each message during
the atomic batch check before any conforming message is written once to the topic log `pubsub.{t}`
(the authoritative, replayable, cross-protocol source) and fanned out to the per-sub queue
`pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (AVRO), `CreateTopic` (schema_settings, JSON), `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `DeleteSubscription`, `DeleteTopic`, `DeleteSchema` | topic `avro-topic-<uuid8>` → `pubsub.{t}`; sub `sub-avro-<uuid8>` → `pubsub.sub.{s}`; schema `avro-schema-<uuid8>` (registry record) | schema (Avro) | schema definition ≤ 300 KB; enforce-on-publish; batch ≤ 1000, msg ≤ 10 MiB; whole batch rejected `INVALID_ARGUMENT` on first non-conforming message |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/advanced/schema_avro`](../../../python/advanced/schema_avro),
  [`../../../javascript/advanced/schema-avro`](../../../javascript/advanced/schema-avro),
  [`../../../java/advanced/schema-avro`](../../../java/advanced/schema-avro),
  [`../../../csharp/advanced/schema-avro`](../../../csharp/advanced/schema-avro),
  [`../../../ruby/advanced/schema_avro`](../../../ruby/advanced/schema_avro)
- Docs: [`../../../docs/concepts/schema-validation.md`](../../../docs/concepts/schema-validation.md)

> **Gotcha — enforcement is all-or-nothing per batch.** Because publish is atomic, one bad message
> rejects the entire `Publish` call and enqueues nothing — validate client-side or publish smaller
> batches for finer-grained failure isolation. Note also that `pubsub.NewSchemaClient` does **not**
> auto-detect the emulator from `PUBSUB_EMULATOR_HOST` the way `pubsub.NewClient` does, so this
> example points the schema client at the emulator explicitly via `option.WithEndpoint` +
> insecure credentials.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
