# FAQ

Short answers to the common "what is this?" and "why isn't this working?" questions. Each answer
links to the deeper doc. For step-by-step symptom → fix recipes, see
[troubleshooting.md](troubleshooting.md); for the gRPC status codes, see
[reference/error-codes.md](reference/error-codes.md).

## What is this?

### Is this the real Google Cloud Pub/Sub or an emulator?

It is **KubeMQ's embedded Google Cloud Pub/Sub wire-protocol connector** — a dedicated gRPC
listener inside the KubeMQ server (default port **8085**, the emulator convention) that implements
the real Pub/Sub v1 gRPC services (`Publisher`, `Subscriber`, `SchemaService`, and permissive
`IAMPolicy` stubs — 38 RPCs). It runs in **emulator mode**: no authentication, no TLS, exactly like
Google's local emulator. It is **not** managed Google Cloud Pub/Sub — there is no Google billing,
no IAM enforcement, no multi-tenant project isolation, and several behaviors deviate from the
managed service. See [architecture.md](architecture.md) and
[reference/migration-from-gcp.md](reference/migration-from-gcp.md) for the deviations.

### Do I need to change my application code?

**No.** Every official Google Cloud Pub/Sub client library and `gcloud` honour the standard
`PUBSUB_EMULATOR_HOST` environment variable. When it is set, the SDK clears Google credentials,
skips auth, and dials insecure gRPC — exactly as it would against Google's local emulator. Point an
unmodified app at KubeMQ with two env vars:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default gRPC port
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
```

That is the whole contract. A few languages need a one-line opt-in (C#, Ruby, Java) — see
[getting-started.md](getting-started.md) and
[guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

### Which languages are supported?

Six, each driving the **official first-party Google Cloud Pub/Sub client** (there is no KubeMQ SDK
to adopt): **Go** (`cloud.google.com/go/pubsub`), **Python** (`google-cloud-pubsub`), **Java**
(`com.google.cloud:google-cloud-pubsub`), **Node/TS** (`@google-cloud/pubsub`), **C# / .NET**
(`Google.Cloud.PubSub.V1`), and **Ruby** (`google-cloud-pubsub`). There is **no Rust** example
because Google ships no first-party GCP Pub/Sub client for Rust. Each language has 15 example
variants (90 programs); see [../examples/README.md](../examples/README.md).

### Do I need to install an emulator or run docker-compose?

No. There is **no Pub/Sub emulator to install, no docker-compose, and no boot-the-server step** —
the connector *is* the emulator. Just ensure a KubeMQ server with the connector enabled
(`CONNECTORS_GCP_ENABLE=true`, the default) is reachable on port 8085, then export the two env
vars above. See [getting-started.md](getting-started.md).

### How do I see which RPCs are supported?

The full **38-RPC capabilities matrix** (Publisher 9 / Subscriber 16 / SchemaService 10 / IAMPolicy
3) plus the accepted-and-ignored / rejected list and the nine gotchas is in
[reference/capabilities.md](reference/capabilities.md).

## Connectivity & security

### Does it support REST / HTTP?

The connector is **gRPC only** — `google.pubsub.v1` over insecure gRPC. There is **no REST/JSON v1
surface** (no grpc-gateway). The standard SDKs already use the gRPC path, so this is invisible to
them; only tooling that hit the Pub/Sub REST API directly must switch. Note that **push delivery**
*is* HTTP: a push subscription's connector worker POSTs each message to your HTTP(S) endpoint — that
is the outbound delivery transport, not an inbound REST API. See
[guides/push-delivery.md](guides/push-delivery.md) and
[reference/connections-endpoint.md](reference/connections-endpoint.md).

### Is authentication or TLS supported?

**No** — the connector is no-auth, no-TLS, insecure gRPC by design (the emulator contract). The SDK
clears its Google credentials when `PUBSUB_EMULATOR_HOST` is set, and the IAM RPCs are permissive
stubs with no enforcement. DoS guards stay active, but you should **not expose port 8085 to
untrusted networks**. See [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

### Why is my project id ignored?

The connector is single-tenant, like the emulator: the `projects/{p}/…` segment is **parsed and
validated but ignored**, so resource ids are global across projects. Use any project id you like.
See [reference/channel-mapping.md](reference/channel-mapping.md).

## "Why isn't this working?"

### Why is my dead-letter topic empty after I nacked a message?

Dead-letter redrive is **lease-expiry / receive-count driven**, not immediate. A message moves to
the dead-letter topic only after its receive count **exceeds `max_delivery_attempts`** (minimum 5),
and the 250 ms lease sweeper applies **retry backoff** between attempts — it is not clock-compressed.
So an example that nacks once and checks the DLQ within a few seconds will find it empty; the
redrive is on a backoff schedule. The examples print a graceful NOTE and still exit 0. See
[concepts/dead-letter-and-retry.md](concepts/dead-letter-and-retry.md) and
[troubleshooting.md](troubleshooting.md).

### Why does re-acking fail with `FAILED_PRECONDITION`?

On a subscription with `enable_exactly_once_delivery`, an `ack_id` is **single-use and lease-bound**.
Re-acking a stale, expired, or already-used id via a **unary** `Acknowledge` / `ModifyAckDeadline`
returns `FAILED_PRECONDITION` carrying `ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID)`. This
is the **real Google SDK contract** — not a literal `INVALID_ARGUMENT` — and the client library maps
it to a permanent per-message ack failure. The same thing happens if the ack lands on a different
cluster node than the one that minted the id (exactly-once is **node-local**). See
[concepts/exactly-once.md](concepts/exactly-once.md) and
[reference/error-codes.md](reference/error-codes.md).

### Why did `CreateSubscription` reject my `max_delivery_attempts`?

A non-zero `max_delivery_attempts` **must be in the range 5..100**; values 1..4 are rejected with
`INVALID_ARGUMENT`. (A value of `0` means *unset* — no dead-lettering.) Google's own minimum is 5
and the connector enforces the same floor. See
[concepts/dead-letter-and-retry.md](concepts/dead-letter-and-retry.md) and
[reference/limits-and-rules.md](reference/limits-and-rules.md).

### My Ruby code errors on `topic.publish` / `sub.pull` — why?

The Ruby examples use **google-cloud-pubsub v3.x**, whose API differs from v2. Topic and
subscription *management* go through the `pubsub.topic_admin` / `pubsub.subscription_admin` GAPIC
clients (e.g. `create_topic(name:)`, `create_subscription(name:, topic:)`); publishing goes through
`pubsub.publisher(name).publish(...)` and consuming through `pubsub.subscriber(name).pull(...)`.
The v2-style `topic.publish` / `subscription.pull` convenience methods are gone. Construct the
client with `Google::Cloud::PubSub.new(project_id:, emulator_host: ENV["PUBSUB_EMULATOR_HOST"])`
(Ruby does not always pick the emulator host up from the env). rbenv 3.3.7; run
`eval "$(rbenv init - zsh)"` first. See
[guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md) and the Ruby
notes in [SHARED-CONVENTIONS.md](../SHARED-CONVENTIONS.md).

### My C# client tries to reach `pubsub.googleapis.com` instead of KubeMQ — why?

C# does **not** auto-detect the emulator from `PUBSUB_EMULATOR_HOST`. Set
`EmulatorDetection = EmulatorDetection.EmulatorOnly` on the client builders
(`PublisherServiceApiClientBuilder` / `SubscriberServiceApiClientBuilder`) so the client uses the
emulator path. See [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

### Why doesn't `Seek` to an old timestamp return an error?

A `Seek` to a timestamp **before** the retained window does not error — it **clamps** to the
earliest retained message. Likewise a seek that would replay more than `MaxSeekReplay` stops at the
cap (and logs a WARN, no silent loss). See
[concepts/seek-and-snapshots.md](concepts/seek-and-snapshots.md).

### Why was my topic id / export subscription / ingestion config rejected?

A few resources are rejected up front with `INVALID_ARGUMENT`: topic ids may **not start with
`sub.`** (reserved broker namespace); **export subscriptions** (BigQuery / Cloud Storage / Bigtable)
and **ingestion sources** have no KubeMQ analog. (`kms_key_name` is accepted and ignored, not
rejected.) See [reference/limits-and-rules.md](reference/limits-and-rules.md) and
[reference/capabilities.md](reference/capabilities.md).

## See Also

- [troubleshooting.md](troubleshooting.md) — symptom → cause → fix recipes.
- [reference/error-codes.md](reference/error-codes.md) — the gRPC status codes and the exactly-once `ErrorInfo`.
- [reference/migration-from-gcp.md](reference/migration-from-gcp.md) — the full deviations table.
- [reference/capabilities.md](reference/capabilities.md) — the 38-RPC matrix and the nine gotchas.
- [getting-started.md](getting-started.md) — the `PUBSUB_EMULATOR_HOST` drop-in walkthrough.
