# C# — advanced: Schema (Protobuf)

Register a Protocol Buffers schema, bind a topic to it, and prove enforce-on-publish:
`CreateSchema(PROTOCOL_BUFFER) → CreateTopic(schema_settings) → Publish(conforming) →
Publish(bad batch) → Pull` — the conforming message lands; a batch with one non-conforming message
is rejected whole.

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
cd examples/csharp && dotnet run --project advanced/schema-protobuf/SchemaProtobuf.csproj
```

## Expected Output

```
[*] CreateSchema proto-order-<uuid8> (PROTOCOL_BUFFER, revision <revision-id>)
[*] Created topic proto-orders-<uuid8> bound to schema (JSON encoding)
[*] Created subscription proto-sub-<uuid8>
[ok] Conforming message accepted id=<message-id>
[*] Publishing a batch with one schema-invalid message (amount is a string)…
[ok] Schema-invalid batch rejected whole → INVALID_ARGUMENT (nothing enqueued)
[ok] Pulled exactly 1 message (the conforming one); the rejected batch enqueued nothing
[ok] PASS — Protobuf schema enforce-on-publish (accept conforming, reject batch on first non-conforming) verified
[*] Deleted subscription proto-sub-<uuid8>
[*] Deleted topic proto-orders-<uuid8>
[*] Deleted schema proto-order-<uuid8>
```

> Resource ids are uuid-suffixed (`proto-order-<uuid8>` / `proto-orders-<uuid8>` / `proto-sub-<uuid8>`),
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> `<message-id>` / `<revision-id>` values vary per run. (See
> `../../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program registers a Protocol Buffers `Order` schema (a single self-contained `.proto` with a
string `id` and an `int32 amount`, ≤ 300 KB), then creates a topic whose `schema_settings` reference
that schema with **JSON** encoding — so each published message's `data` is validated as JSON against
the first declared message type. A conforming message (`{"id":"A-1","amount":100}`) is accepted. A
two-message batch whose second message has a string `amount` is rejected: because publish is
**atomic**, the connector rejects the **whole batch** with `INVALID_ARGUMENT` on the first
non-conforming message, and **nothing** in that batch is enqueued — not even the valid message. A
subsequent pull therefore returns exactly the one conforming message. The subscription, topic, and
schema are cleaned up in a `finally` block.

The connector parses the `.proto` definition (via `jhump/protoreflect`) and validates each message
during the atomic batch check before any conforming message is written once to the topic log
`pubsub.{t}` (the authoritative, replayable, cross-protocol source) and fanned out to the per-sub
queue `pubsub.sub.{s}`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateSchema` (PROTOCOL_BUFFER), `CreateTopic` (schema_settings, JSON), `CreateSubscription`, `Publish`, `Pull`, `Acknowledge`, `DeleteSubscription`, `DeleteTopic`, `DeleteSchema` | topic `proto-orders-<uuid8>` → `pubsub.{t}`; sub `proto-sub-<uuid8>` → `pubsub.sub.{s}`; schema `proto-order-<uuid8>` (registry record) | schema (Protobuf) | schema definition ≤ 300 KB; enforce-on-publish; batch ≤ 1000, msg ≤ 10 MiB; whole batch rejected `INVALID_ARGUMENT` on first non-conforming message |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/advanced/schema-protobuf`](../../../go/advanced/schema-protobuf),
  [`../../../python/advanced/schema_protobuf`](../../../python/advanced/schema_protobuf),
  [`../../../java/advanced/schema-protobuf`](../../../java/advanced/schema-protobuf),
  [`../../../javascript/advanced/schema-protobuf`](../../../javascript/advanced/schema-protobuf),
  [`../../../ruby/advanced/schema_protobuf`](../../../ruby/advanced/schema_protobuf)
- Docs: [`../../../../docs/concepts/schema-validation.md`](../../../../docs/concepts/schema-validation.md)

> **Gotcha — enforcement is all-or-nothing per batch.** Because publish is atomic, one bad message
> rejects the entire `Publish` call and enqueues nothing — validate client-side or publish smaller
> batches for finer-grained failure isolation. The `SchemaServiceClientBuilder` also has **no**
> `EmulatorDetection` setter (unlike the publisher/subscriber builders), so this example points the
> schema client at the emulator explicitly via `Endpoint` + `ChannelCredentials.Insecure`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
