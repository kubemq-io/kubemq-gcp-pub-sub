# C# — advanced: Schema (Avro)

Register an Avro schema, bind a topic to it, and prove enforce-on-publish:
`CreateSchema(AVRO) → CreateTopic(schema_settings) → Publish(conforming) → Publish(bad batch) →
Pull` — the conforming message lands; a batch with one non-conforming message is rejected whole.

## Prerequisites

- .NET SDK **8.0** and the official GCP Pub/Sub client `Google.Cloud.PubSub.V1` **3.36.0**
  (pinned in `examples/csharp/Directory.Packages.props`).
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
cd examples/csharp && dotnet run --project advanced/schema-avro/SchemaAvro.csproj
```

## Expected Output

```
[*] CreateSchema avro-order-<uuid8> (AVRO, revision <revision-id>)
[*] Created topic avro-orders-<uuid8> bound to schema (JSON encoding)
[*] Created subscription avro-sub-<uuid8>
[ok] Conforming message accepted id=<message-id>
[*] Publishing a batch with one schema-invalid message (amount is a string)…
[ok] Schema-invalid batch rejected whole → INVALID_ARGUMENT (nothing enqueued)
[ok] Pulled exactly 1 message (the conforming one); the rejected batch enqueued nothing
[ok] PASS — Avro schema enforce-on-publish (accept conforming, reject batch on first non-conforming) verified
[*] Deleted subscription avro-sub-<uuid8>
[*] Deleted topic avro-orders-<uuid8>
[*] Deleted schema avro-order-<uuid8>
```

> Resource ids are uuid-suffixed (`avro-order-<uuid8>` / `avro-orders-<uuid8>` / `avro-sub-<uuid8>`),
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> `<message-id>` / `<revision-id>` values vary per run. (See
> `../../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program registers an Avro `Order` schema (a required string `id` and an int `amount`,
≤ 300 KB), then creates a topic whose `schema_settings` reference that schema with **JSON**
encoding — so each published message's `data` is validated as JSON against the Avro schema. A
conforming message (`{"id":"A-1","amount":100}`) is accepted. A two-message batch whose second
message has a string `amount` is rejected: because publish is **atomic**, the connector rejects the
**whole batch** with `INVALID_ARGUMENT` on the first non-conforming message, and **nothing** in that
batch is enqueued. A subsequent pull therefore returns exactly the one conforming message. The
subscription, topic, and schema are cleaned up in a `finally` block.

The connector parses the Avro definition (via `hamba/avro/v2`) and validates each message during
the atomic batch check before any conforming message is written once to the topic log `pubsub.{t}`
(the authoritative, replayable, cross-protocol source) and fanned out to the per-sub queue
`pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (AVRO), `CreateTopic` (schema_settings, JSON), `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `DeleteSubscription`, `DeleteTopic`, `DeleteSchema` | topic `avro-orders-<uuid8>` → `pubsub.{t}`; sub `avro-sub-<uuid8>` → `pubsub.sub.{s}`; schema `avro-order-<uuid8>` (registry record) | schema (Avro) | schema definition ≤ 300 KB; enforce-on-publish; batch ≤ 1000, msg ≤ 10 MiB; whole batch rejected `INVALID_ARGUMENT` on first non-conforming message |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-avro`](../../../go/advanced/schema-avro),
  [`../../../python/advanced/schema_avro`](../../../python/advanced/schema_avro),
  [`../../../java/advanced/schema-avro`](../../../java/advanced/schema-avro),
  [`../../../javascript/advanced/schema-avro`](../../../javascript/advanced/schema-avro),
  [`../../../ruby/advanced/schema_avro`](../../../ruby/advanced/schema_avro)
- Docs: [`../../../../docs/concepts/schema-validation.md`](../../../../docs/concepts/schema-validation.md)

> **Gotcha — enforcement is all-or-nothing per batch.** Because publish is atomic, one bad message
> rejects the entire `Publish` call and enqueues nothing — validate client-side or publish smaller
> batches for finer-grained failure isolation. The `SchemaServiceClientBuilder` also has **no**
> `EmulatorDetection` setter (unlike the publisher/subscriber builders), so this example points the
> schema client at the emulator explicitly via `Endpoint` + `ChannelCredentials.Insecure`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
