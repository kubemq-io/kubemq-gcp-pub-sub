# Troubleshooting & Diagnostics

A consolidated, symptom-first guide to the issues you are most likely to hit pointing a Google
Cloud Pub/Sub client at the KubeMQ connector. Each entry is **Symptom → Cause → Fix**. The
[Diagnostics](#diagnostics) section at the end covers gRPC reflection (`grpcurl`) and per-language
SDK debug logging.

This is a how-to companion to the reference material: the full status-code contract is in
[reference/error-codes.md](reference/error-codes.md), the connector knobs in
[configuration.md](configuration.md), and the connectivity posture in
[guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md). Common
questions are answered in [faq.md](faq.md).

> **First principles.** The connector *is* the emulator — a dedicated **gRPC listener** (default
> port **8085**) inside the KubeMQ server. There is no Pub/Sub emulator to install and no
> docker-compose. It runs in **emulator mode**: no auth, no TLS, insecure gRPC, **gRPC only** (no
> REST/JSON v1). Most issues below trace back to one of these four facts.

## Contents

- [Emulator not detected (per language)](#emulator-not-detected-per-language)
- [Connection failures & periodic `UNAVAILABLE`](#connection-failures--periodic-unavailable)
- [Exactly-once / sticky-LB ack errors](#exactly-once--sticky-lb-ack-errors)
- [Schema & filter rejection (`INVALID_ARGUMENT`)](#schema--filter-rejection-invalid_argument)
- [Dead-letter: "nothing in the DLQ yet"](#dead-letter-nothing-in-the-dlq-yet)
- [Project-id confusion](#project-id-confusion)
- ["gRPC-only / no REST" reminders](#grpc-only--no-rest-reminders)
- [Resource id rejected](#resource-id-rejected)
- [Diagnostics](#diagnostics)

---

## Emulator not detected (per language)

The single most common failure: the client tries to reach **`pubsub.googleapis.com`** with real
Google credentials instead of dialing the connector insecurely. When `PUBSUB_EMULATOR_HOST` is
set, every official client should clear its Google credentials, skip auth, and use insecure gRPC.
Two languages need an explicit flag.

> **Symptom.** Calls hang, time out, or fail with an auth / credentials error (e.g. "could not
> find default credentials", DNS to `pubsub.googleapis.com`, a TLS handshake error, or an
> `UNAUTHENTICATED` status) — even though `PUBSUB_EMULATOR_HOST=localhost:8085` is exported.

### Go, Python, Node/TS — should auto-detect

> **Cause.** `PUBSUB_EMULATOR_HOST` is not exported in the **process** environment (e.g. set in a
> different shell, not inherited by the test runner, or spelled wrong).
>
> **Fix.** Export it where the program actually runs and confirm it is visible to the process:
>
> ```bash
> export PUBSUB_EMULATOR_HOST=localhost:8085
> export PUBSUB_PROJECT_ID=my-project
> ```
>
> | Language | Construction (reads the env var, dials insecurely) |
> |----------|----------------------------------------------------|
> | Go | `pubsub.NewClient(ctx, os.Getenv("PUBSUB_PROJECT_ID"))` |
> | Python | `pubsub_v1.PublisherClient()` / `SubscriberClient()` |
> | Node/TS | `new PubSub({ projectId })` |
>
> No explicit emulator flag is needed for these three.

### C# — needs `EmulatorDetection`

> **Cause.** `Google.Cloud.PubSub.V1` does **not** auto-detect the emulator from the env var. A
> plain `PublisherServiceApiClient.Create()` dials Google production.
>
> **Fix.** Set `EmulatorDetection` on the client builders:
>
> ```csharp
> var publisher = new PublisherServiceApiClientBuilder
> {
>     EmulatorDetection = EmulatorDetection.EmulatorOnly   // or EmulatorOrProduction
> }.Build();
> ```
>
> Set the same on `SubscriberServiceApiClientBuilder`. Use `EmulatorOnly` to *require* the
> emulator (it errors if `PUBSUB_EMULATOR_HOST` is unset), or `EmulatorOrProduction` to fall back
> to production.

### Ruby — needs `emulator_host:` and v3.x

> **Cause.** The Ruby client does not always pick up the emulator from the env var, and the v3.x
> API differs from v2.x.
>
> **Fix.** Pass `emulator_host:` explicitly to the constructor:
>
> ```ruby
> require "google/cloud/pubsub"
> pubsub = Google::Cloud::PubSub.new(
>   project_id:    ENV["PUBSUB_PROJECT_ID"],
>   emulator_host: ENV["PUBSUB_EMULATOR_HOST"],
> )
> ```
>
> Run `eval "$(rbenv init - zsh)"` first (rbenv 3.3.7).

### Java — needs `NoCredentialsProvider` + a plaintext channel

> **Cause.** The Java client will try to attach Application Default Credentials and dial the
> Google endpoint over TLS unless you point it at the emulator host explicitly.
>
> **Fix.** When `PUBSUB_EMULATOR_HOST` is set, build a **plaintext** `ManagedChannel` to the
> emulator host and pass `NoCredentialsProvider.create()` (via a `FixedTransportChannelProvider`)
> to `Publisher` and the admin clients. This is the same wiring Google's own emulator docs use.

See the full per-language construction table in
[guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md#per-language-emulator-detection).

---

## Connection failures & periodic `UNAVAILABLE`

### Cannot connect at all

> **Symptom.** Connection refused / no route to `localhost:8085`.
>
> **Cause.** The connector is not listening — either KubeMQ is not running, the connector is
> disabled, or you are pointing at the wrong host/port.
>
> **Fix.**
> - The connector is **opt-in** (disabled by default). Enable it on port **8085**:
>   ```bash
>   export CONNECTORS_GCP_ENABLE=true
>   ```
> - Confirm the port matches `CONNECTORS_PUB_SUB_PORT` (default `"8085"`). It must be distinct
>   from the enabled gRPC / REST / HTTP / AWS-connector ports.
> - Point `PUBSUB_EMULATOR_HOST` at the **externally reachable** address. The dashboard's
>   suggested value comes from the cosmetic `CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT` hint — it does
>   **not** change how the listener binds, it only tells operators which host to copy.
> - Do **not** use an `https://` URL or a TLS port — the transport is **insecure gRPC** (no TLS).

### `UNAVAILABLE` right after the broker starts

> **Symptom.** A burst of `UNAVAILABLE` status codes shortly after startup or after a broker
> restart, then calls succeed.
>
> **Cause.** The **Traffic-gate** interceptor short-circuits requests with transient `UNAVAILABLE`
> while the broker is **not ready**. On a not-ready → ready transition the connector drops all
> in-memory leases and the poller rebuilds.
>
> **Fix.** This is expected. SDKs retry `UNAVAILABLE` automatically — let them. Any in-flight
> messages are **redelivered** after recovery, so design consumers to be **idempotent**. See
> [guides/reliability.md](guides/reliability.md#at-least-once-is-the-baseline).

### StreamingPull periodically closes (every ~30 min)

> **Symptom.** A long-running `StreamingPull` / `Subscribe` callback sees a periodic `UNAVAILABLE`
> and the stream is torn down and re-established, roughly every 30 minutes.
>
> **Cause.** The server **deliberately** closes each `StreamingPull` stream after
> `CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS` (default **1800 s** / 30 min) with `UNAVAILABLE`, to
> bound per-stream resource lifetime. This matches the real Pub/Sub behaviour.
>
> **Fix.** Do **not** treat this as an error. Client libraries **transparently reconnect** and
> your receive callback keeps running across the reconnect. Leases are subscription-owned (not
> stream-owned), so an ack on the new stream correctly resolves a message delivered on the old one.
> If you want longer streams, raise `CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS` (must be > 0). See
> [guides/subscribing.md](guides/subscribing.md#periodic-reconnect).

---

## Exactly-once / sticky-LB ack errors

> **Symptom.** On a subscription with `enable_exactly_once_delivery`, acks fail unexpectedly:
> - **unary** `Acknowledge` / `ModifyAckDeadline` returns **`FAILED_PRECONDITION`** carrying an
>   `ErrorInfo{ reason: "PERMANENT_FAILURE_INVALID_ACK_ID" }`;
> - **StreamingPull** reports ids in **`invalid_ack_ids`**.
>
> This may appear even for ids you just received, especially in a cluster behind a load balancer.

> **Cause.** Two distinct cases:
>
> 1. **Stale / expired / re-used `ack_id`.** The deadline expired, the message was already acked,
>    or you re-acked a stale id. The unary invalid-ack path returns `FAILED_PRECONDITION` +
>    `ErrorInfo` — **not** `INVALID_ARGUMENT` — because that is the **real Google SDK contract**
>    (the SDK reads the ack result from the `ErrorInfo.reason`). A naive `INVALID_ARGUMENT` here
>    would break the SDK's exactly-once bookkeeping.
> 2. **Wrong-node id (cluster).** An `ack_id` is minted with the **node id baked in**. Presenting
>    it to a *different* node — after a cluster failover or through a **non-sticky** load balancer —
>    makes it appear invalid (gotcha #1). Exactly-once and StreamingPull leases are **node-local**;
>    there is no cluster-wide distributed exactly-once.

> **Fix.**
> - For a stale id: stop re-acking it. On StreamingPull, ids in `temporary_failed_ack_ids` are the
>   ones to **retry**; ids in `invalid_ack_ids` are **permanently** unackable — give up on them.
> - For a cluster: put a **sticky load balancer** (session affinity) in front of the connector so
>   each subscriber's StreamingPull traffic is pinned to one node for the lifetime of its in-flight
>   messages. Or **accept at-least-once** across nodes. **Single-node deployments are unaffected.**
>
> Full detail: [guides/reliability.md](guides/reliability.md#exactly-once-delivery),
> [concepts/exactly-once.md](concepts/exactly-once.md), and
> [reference/error-codes.md](reference/error-codes.md#exactly-once-ack-results).

---

## Schema & filter rejection (`INVALID_ARGUMENT`)

A broad class of validation failures all surface as `INVALID_ARGUMENT`. The connector validates
the **entire batch before enqueuing anything**, so the first offending message rejects the whole
batch and nothing is published (atomic).

### Schema enforce-on-publish

> **Symptom.** Publishing to a schema-bound topic rejects the **whole batch** with
> `INVALID_ARGUMENT`.
>
> **Cause.** When a topic references an Avro or Protobuf schema, publish is **enforced**: the first
> non-conforming message rejects the entire batch. Also `INVALID_ARGUMENT`: a schema definition
> that fails to parse or exceeds **300 KB**.
>
> **Fix.** Make every message in the batch conform to the bound schema. Validate payloads
> client-side before publishing a large batch so one bad message does not reject the rest. See
> [concepts/schema-validation.md](concepts/schema-validation.md).

### Filter rejected at `CreateSubscription`

> **Symptom.** `CreateSubscription` fails with `INVALID_ARGUMENT`; or a `filter` you expected to
> change later is unchanged.
>
> **Cause.** The filter is a hand-written **CEL-subset** (attributes-only). It is rejected if it is
> malformed/unparseable, longer than **256 characters**, or uses anything outside the supported set
> (`attributes:KEY`, `attributes.KEY = "v"` / `!= "v"`, `hasPrefix(attributes.KEY, "p")`,
> `AND` / `OR` / `NOT`, parentheses). There is **no `data`-based filtering** and no numeric
> comparison operators. The filter is compiled once at create-time and is **immutable**
> (`UpdateSubscription` cannot change `filter` or `name`).
>
> **Fix.** Keep the expression within the supported syntax and ≤ 256 chars. To change a filter,
> create a **new** subscription. See [guides/filtering.md](guides/filtering.md).

### Other `INVALID_ARGUMENT` triggers

> Also rejected with `INVALID_ARGUMENT`: a batch > 1000 messages; a message > 10 MiB; > 100
> attributes; attribute key > 256 B (or starting with `goog`); attribute value > 1024 B; ordering
> key > 1024 B; a message with **both** `data` and `attributes` empty; `max_delivery_attempts`
> outside **5..100**; and **export** (BigQuery / Cloud Storage / Bigtable) or **ingestion**
> subscriptions, which are not supported. See
> [reference/limits-and-rules.md](reference/limits-and-rules.md) and
> [reference/error-codes.md](reference/error-codes.md).

---

## Dead-letter: "nothing in the DLQ yet"

> **Symptom.** You nack a message repeatedly (or let it exceed `max_delivery_attempts`) but the
> dead-letter topic stays empty for a while — nothing arrives in the DLQ within the first ~30
> seconds.
>
> **Cause.** Dead-letter redrive is **sweeper / lease-expiry driven**, not clock-compressed. The
> 250 ms sweeper expires overdue leases and applies an **exponential backoff** clamped to the
> subscription's `[min, max]` (defaults 10 s … 600 s). An explicit nack (`ModifyAckDeadline(0)`)
> bumps the receive count fast and redelivers immediately, but the redrive to the dead-letter topic
> still follows the lease-expiry / backoff schedule. So a freshly-nacked message does **not**
> necessarily reach the DLT inside the first 30 s.
>
> **Fix.** This is expected timing, not a bug. Wait for the backoff schedule rather than asserting
> immediately. Once the receive count **exceeds** `max_delivery_attempts` and the subscription has a
> `dead_letter_topic`, the sweeper **republishes** the message to that topic and **acks the
> original**. Remember the constraints:
> - `max_delivery_attempts` must be **5..100** (`0` = unset / no DLQ) — a value < 5 is rejected
>   with `INVALID_ARGUMENT` (gotcha #6).
> - With no `dead_letter_topic`, a message is simply **dropped** after exhaustion.
>
> The `delivery/dead-letter-topic` examples print a graceful NOTE and still exit 0 when the redrive
> has not landed within the window. See
> [concepts/dead-letter-and-retry.md](concepts/dead-letter-and-retry.md) and
> [guides/reliability.md](guides/reliability.md#dead-letter--retry).

---

## Project-id confusion

> **Symptom.** Two programs that use **different** project ids see each other's topics, or a
> subscription you created "in another project" already exists / collides.
>
> **Cause.** The connector validates the `projects/{p}` segment but is **single-tenant** (like the
> emulator). The project segment is **parsed but ignored** (gotcha #2): resource ids are **global
> across projects**. Topic `orders` is always Events Store channel `pubsub.orders` regardless of
> project, and subscription `s` is always `pubsub.sub.{s}`.
>
> **Fix.**
> - Any project id works — pick any value; it does not isolate resources.
> - To avoid cross-talk between concurrent runs, give resources **unique ids** (e.g. uuid-suffixed
>   `orders-<uuid8>` / `sub-orders-<uuid8>`) rather than relying on the project to separate them.
>   The examples do this and best-effort delete on exit.
> - Set both env vars: `PUBSUB_PROJECT_ID` (and, for clients/`gcloud` that read the alias,
>   `GOOGLE_CLOUD_PROJECT`).
>
> See [getting-started.md](getting-started.md) and
> [reference/channel-mapping.md](reference/channel-mapping.md).

---

## "gRPC-only / no REST" reminders

> **Symptom.** A REST/JSON Pub/Sub client, a `curl` against `https://.../v1/projects/...`, or a
> tool that only speaks the Pub/Sub REST API gets connection or protocol errors.
>
> **Cause.** The connector is **gRPC only** (gotcha #9). There is **no REST/JSON v1** surface
> (grpc-gateway is not used), and there are **no AWS-style XML error envelopes** — the connector
> returns genuine gRPC status codes (`google.rpc.Code`).
>
> **Fix.** Use a **gRPC** client library (Go, Python, Java, Node.js, C#, Ruby) or `gcloud pubsub`
> (which uses gRPC against the emulator). For ad-hoc probing use `grpcurl`, not `curl` — see
> [Diagnostics](#diagnostics). See [architecture.md](architecture.md#grpc-only) and
> [reference/capabilities.md](reference/capabilities.md).

---

## Resource id rejected

> **Symptom.** `CreateTopic` / `CreateSubscription` fails with `INVALID_ARGUMENT` on a name you
> consider valid.
>
> **Cause.** Resource ids follow the **Google-exact** rules: **3..255 characters**, must **start
> with a letter**, charset `[A-Za-z0-9._~%+-]`, no `goog` prefix. Additionally, **topic ids may not
> start with `sub.`** — that is the reserved broker namespace for subscription queues (gotcha #7).
>
> **Fix.** Rename the resource to satisfy the rules. If you are uuid-suffixing ids for isolation,
> make sure the **prefix** still starts with a letter and the topic prefix is not `sub.`. See
> [reference/limits-and-rules.md](reference/limits-and-rules.md).

---

## Diagnostics

When an entry above does not resolve the issue, turn on the two diagnostic surfaces: **gRPC
reflection** (to inspect the live RPC surface with `grpcurl`) and **SDK-side debug logging** (to
see the wire calls your client makes).

### gRPC reflection + `grpcurl`

Server reflection lets `grpcurl` discover the connector's services without a local `.proto`. It is
**off by default** and is a **debugging-only** switch.

> **Enable it** by setting the connector flag, then restart so the gRPC server registers
> reflection:
>
> ```bash
> export CONNECTORS_GCP_ENABLE_REFLECTION=true
> ```
>
> (`EnableReflection`, default `false` — see [configuration.md](configuration.md).)

Because the transport is insecure (no TLS), always pass `-plaintext` to `grpcurl`:

```bash
# List the registered services (Publisher, Subscriber, SchemaService, IAMPolicy)
grpcurl -plaintext localhost:8085 list

# List the RPCs on a service
grpcurl -plaintext localhost:8085 list google.pubsub.v1.Publisher

# Describe a request message
grpcurl -plaintext localhost:8085 describe google.pubsub.v1.Topic

# Create a topic (project segment is parsed but ignored)
grpcurl -plaintext -d '{"name":"projects/my-project/topics/orders"}' \
  localhost:8085 google.pubsub.v1.Publisher/CreateTopic

# List topics for a project
grpcurl -plaintext -d '{"project":"projects/my-project"}' \
  localhost:8085 google.pubsub.v1.Publisher/ListTopics
```

> **Note.** `grpcurl` speaks gRPC, so it works against the connector — unlike `curl`, which would
> need the (non-existent) REST surface. The connector implements **38 RPCs** across four services:
> `google.pubsub.v1.Publisher` (9), `Subscriber` (16), `SchemaService` (10), and the permissive
> `google.iam.v1.IAMPolicy` (3 stubs). See [reference/capabilities.md](reference/capabilities.md).

### SDK-side debug logging (per language)

Enable client-library logging to see the gRPC channel activity (connect, retries, the periodic
`StreamingPull` reconnect, status codes). These are the standard mechanisms for each client:

| Language | Enable debug logging |
|----------|----------------------|
| Go | gRPC logs via env vars: `export GRPC_GO_LOG_VERBOSITY_LEVEL=99` and `export GRPC_GO_LOG_SEVERITY_LEVEL=info`. |
| Python | Standard `logging` at `DEBUG`: `logging.basicConfig(level=logging.DEBUG)`; the gRPC core honours `export GRPC_VERBOSITY=DEBUG` (and optionally `GRPC_TRACE=...`). |
| Node/TS | gRPC core tracing: `export GRPC_VERBOSITY=DEBUG` and `export GRPC_TRACE=all`. |
| Java | `java.util.logging` (the gax/grpc layer logs through it); raise the level for `io.grpc` / `com.google.cloud`. The C-core env vars `GRPC_VERBOSITY` / `GRPC_TRACE` also apply. |
| C# / .NET | Pass an `ILoggerFactory` via `GrpcChannelOptions` (or set `Grpc.Net.Client` log levels in your logging config) to capture channel logs. |
| Ruby | Standard `Logger`; the gRPC C-core honours `export GRPC_VERBOSITY=DEBUG` / `GRPC_TRACE=...`. |

> The `GRPC_VERBOSITY` / `GRPC_TRACE` environment variables are honoured by the **C-based gRPC
> core** that Python, Node, Java, Ruby, and C# share; **Go** uses its own `GRPC_GO_LOG_*` variables
> because its gRPC stack is pure Go. Use these to confirm the client is actually dialing
> `localhost:8085` insecurely (and not `pubsub.googleapis.com`).

### What to capture when reporting a problem

- The exact **gRPC status code** and any `ErrorInfo.reason` (see
  [reference/error-codes.md](reference/error-codes.md)).
- Whether `PUBSUB_EMULATOR_HOST` is set in the **process** environment, and the language /
  emulator-detection mode (C# `EmulatorDetection`, Ruby `emulator_host:`, Java
  `NoCredentialsProvider`).
- Single-node vs cluster (and whether a **sticky LB** is in front) for any exactly-once / ack
  issue.
- For DLQ timing, how long you waited relative to the backoff `[min, max]`.

## See Also

- [reference/error-codes.md](reference/error-codes.md) — the full gRPC status-code table,
  exactly-once ack results, and the node-local boundary.
- [faq.md](faq.md) — quick answers to common questions.
- [configuration.md](configuration.md) — the 11 `CONNECTORS_PUB_SUB_*` knobs (incl.
  `CONNECTORS_GCP_ENABLE_REFLECTION` and `CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS`).
- [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md) — the env
  vars, no-auth / insecure posture, gRPC-only, and the sticky-LB caveat.
- [guides/reliability.md](guides/reliability.md) — exactly-once (node-local), retry, dead-letter.
