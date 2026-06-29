# kubemq-gcp-pub-sub

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Languages](https://img.shields.io/badge/languages-6%20(Go%2C%20Python%2C%20Java%2C%20JS%2FTS%2C%20C%23%2C%20Ruby)-informational.svg)](examples/)
[![Examples](https://img.shields.io/badge/examples-90-success.svg)](examples/)
[![Protocol](https://img.shields.io/badge/protocol-Google%20Cloud%20Pub%2FSub%20(emulator)-orange.svg)](docs/)
[![Direct Connect](https://img.shields.io/badge/KubeMQ-direct--connect-9cf.svg)](https://kubemq.io/)

**Point an unmodified Google Cloud Pub/Sub app at KubeMQ by setting one environment variable — `PUBSUB_EMULATOR_HOST`. Zero code changes.**

## Contents

- [The zero-code-change drop-in](#the-zero-code-change-drop-in)
- [Connection](#connection)
- [Channel mapping](#channel-mapping)
- [Repository map](#repository-map)
- [Languages & GCP Pub/Sub SDKs](#languages--gcp-pubsub-sdks)
- [Getting started](#getting-started)
- [Protocol scope](#protocol-scope)
- [License](#license)

KubeMQ ships an embedded **Google Cloud Pub/Sub wire-protocol connector** inside
the KubeMQ server — a dedicated gRPC listener (default port **8085**, the emulator convention)
that implements the real Pub/Sub v1 gRPC services (`Publisher`, `Subscriber`, `SchemaService`,
and permissive `IAMPolicy` stubs — 38 RPCs). Any standard, unmodified Pub/Sub application
connects to it exactly as it would to Google's local emulator: by exporting
`PUBSUB_EMULATOR_HOST`. No code changes, no library swap, no KubeMQ SDK.

This repository is **documentation + runnable examples + a Go burn-in soak harness**. It teaches
developers to drive the KubeMQ Pub/Sub connector from the official first-party Google Cloud
Pub/Sub client libraries (Go `cloud.google.com/go/pubsub`, Python `google-cloud-pubsub`, Java
`google-cloud-pubsub`, Node/TS `@google-cloud/pubsub`, .NET `Google.Cloud.PubSub.V1`, Ruby
`google-cloud-pubsub`). It ships **no installable package** in any language, **no proto / gRPC
bindings**, and **no published client library**.

## The zero-code-change drop-in

Every official Google Cloud Pub/Sub client library and `gcloud` honour the standard
`PUBSUB_EMULATOR_HOST` environment variable. When it is set, the SDK **clears credentials, skips
Google auth, and uses insecure gRPC** — exactly as it would against Google's local emulator.
Pointing an unmodified Pub/Sub application at KubeMQ therefore requires **zero code changes**:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default port; SDK uses the insecure path
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
```

That is the whole contract. This drop-in is the repo's headline value prop and the thing every
example demonstrates.

## Connection

A KubeMQ server with the Pub/Sub connector **enabled** (`CONNECTORS_GCP_ENABLE=true`,
listening on gRPC port **8085**) is assumed to be running and reachable. There is **no emulator
to install, no docker-compose, no boot-the-server step** — the connector *is* the emulator.

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default gRPC port
export PUBSUB_PROJECT_ID=my-project            # any id; project segment parsed but ignored
# some clients/gcloud also accept:
# export GOOGLE_CLOUD_PROJECT=my-project
```

Most SDKs auto-detect the emulator from the env var (Go, Python, Node). A few need a one-line
opt-in: **C#** sets `EmulatorDetection = EmulatorDetection.EmulatorOnly`; **Ruby** passes
`emulator_host:`; **Java** points the channel at the emulator host with plaintext credentials.
The per-language construction snippets live in [`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md) §1.

> **Security posture.** The connector is **no-auth, no-TLS, insecure gRPC** by design — the
> emulator contract. DoS guards stay active (`MaxRecvMsgSize`, `MaxInflightPerSubscription`,
> `MaxConcurrentPolls`, `MaxSeekReplay`, push backoff), but you should **not expose port 8085 to
> untrusted networks**. The connector is **gRPC only** — there is no REST/JSON v1 surface.
> `CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT` is a cosmetic dashboard hint that shows the
> `PUBSUB_EMULATOR_HOST` value to copy.

## Channel mapping

Pub/Sub resources map onto native KubeMQ surfaces, so the same messages are visible to both
Pub/Sub clients and native KubeMQ consumers:

| Pub/Sub resource | KubeMQ object | Channel |
|------------------|---------------|---------|
| Topic `projects/{p}/topics/{t}` | Events Store log | `pubsub.{t}` |
| Subscription `projects/{p}/subscriptions/{s}` | Queue | `pubsub.sub.{s}` |
| Snapshot / Schema | Registry record (BoltDB) | — |

> **Mental model.** A `Publish` writes **once** to the topic's Events Store log
> `pubsub.{t}` (`Array.SendEventsStore`) — the authoritative, replayable, cross-protocol source —
> then fans out one queue copy per subscription on `pubsub.sub.{s}` (`Array.SendQueueMessage`),
> applying each subscription's filter. The `{p}` **project segment is parsed and validated but
> ignored** (single-tenant, like the emulator); resource ids are global across projects.
>
> A `PubsubMessage` becomes a KubeMQ message whose body is `data` and whose Tags are the message
> `attributes` plus **three reserved tags** carried across the wire — `_pubsub_message_id`,
> `_pubsub_publish_time`, `_pubsub_ordering_key`. Native consumers see these tags; they are
> stripped from `attributes` when delivered back to a Pub/Sub client. Topic ids may **not** start
> with `sub.` (reserved broker namespace).

## Repository map

| Path | What it is |
|------|------------|
| [`docs/`](docs/README.md) | Architecture, getting-started, configuration, [troubleshooting](docs/troubleshooting.md), [FAQ](docs/faq.md), plus `concepts/`, `guides/`, `reference/` (incl. `migration-from-gcp.md`) — every fact traced to the connector source |
| [`examples/`](examples/README.md) | 15 example variants × 6 languages (Go, Python, Java, JS/TS, C#, Ruby) = up to 90 programs, full parity |
| [`burnin/`](burnin/) | Standalone Go burn-in soak harness (one worker per Pub/Sub pattern), fixed port **8899** |
| [`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md) | Single source of truth for downstream agents: connection model, the 15-variant master table, README templates, the 9 Pub/Sub gotchas, per-language idiom traps, the verified connector-fact summary |
| [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md) | A verbatim copy of the root conventions, alongside the examples |
| `LICENSE` | Apache-2.0 |

## Languages & GCP Pub/Sub SDKs

| Language | Official GCP Pub/Sub client | Emulator construction | Manifest |
|----------|-----------------------------|------------------------|----------|
| Go | [`cloud.google.com/go/pubsub`](https://pkg.go.dev/cloud.google.com/go/pubsub) | `pubsub.NewClient(ctx, projectID)` — reads `PUBSUB_EMULATOR_HOST`, dials insecurely | `examples/go/go.mod` |
| Python | [`google-cloud-pubsub`](https://cloud.google.com/python/docs/reference/pubsub/latest) (via **uv**) | `pubsub_v1.PublisherClient()` — honours `PUBSUB_EMULATOR_HOST` | `examples/python/pyproject.toml` |
| Java | [`com.google.cloud:google-cloud-pubsub`](https://cloud.google.com/java/docs/reference/google-cloud-pubsub/latest/overview) (BOM `libraries-bom`) | point the channel at the emulator host with `NoCredentialsProvider` | `examples/java/pom.xml` |
| Node/TS | [`@google-cloud/pubsub`](https://cloud.google.com/nodejs/docs/reference/pubsub/latest) | `new PubSub({ projectId })` — auto-detects the emulator; run via `tsx` | `examples/javascript/package.json` |
| C# / .NET | [`Google.Cloud.PubSub.V1`](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.PubSub.V1/latest) | `…Builder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build()`; .NET 8 | `examples/csharp/Directory.Packages.props` |
| Ruby | [`google-cloud-pubsub`](https://cloud.google.com/ruby/docs/reference/google-cloud-pubsub/latest) | `Google::Cloud::PubSub.new(project_id:, emulator_host:)`; rbenv 3.3.7 | `examples/ruby/Gemfile` |

> The official first-party Google client is used in every language — that is the point of the
> emulator-protocol drop-in. There is **no Rust** example, because Google ships no first-party
> GCP Pub/Sub client for Rust. Exactly one variant per language — `interop/native-events-store` —
> additionally imports the per-language **KubeMQ SDK** to read the topic's Events Store log
> natively; that is demonstration-only and does NOT make this a KubeMQ-SDK package.

## Getting started

1. Ensure a KubeMQ server with the Pub/Sub connector is reachable on gRPC port 8085
   (`CONNECTORS_GCP_ENABLE=true`); export `PUBSUB_EMULATOR_HOST` + `PUBSUB_PROJECT_ID`
   (see **Connection**).
2. Pick a language under [`examples/`](examples/README.md) and start with
   `topics/basic-publish-subscribe`
   (`CreateTopic → CreateSubscription → Publish → Pull → Acknowledge`).
3. Read [`docs/getting-started.md`](docs/getting-started.md) for the first-message walkthrough and
   [`docs/guides/connectivity-and-emulator-mode.md`](docs/guides/connectivity-and-emulator-mode.md)
   for the emulator drop-in in detail.
4. Stuck? See [`docs/troubleshooting.md`](docs/troubleshooting.md) for symptom-to-fix diagnostics
   and [`docs/faq.md`](docs/faq.md) for the common "why does it behave this way?" questions.

## Protocol scope

The connector implements the real Pub/Sub v1 gRPC services — **38 RPCs**: `Publisher` (9),
`Subscriber` (16), `SchemaService` (10), and `IAMPolicy` (3 permissive stubs). It is dual
bidirectional: **source** external→KubeMQ via `Publish`, **target** KubeMQ→external via
`Pull`/`StreamingPull`. Coverage spans:

- **Topics & publish** — `CreateTopic`, batch `Publish` (1..1000 messages, atomic validation),
  message ids and publish time.
- **Subscriptions & pull** — `Pull`, `StreamingPull` with flow control and periodic reconnect,
  ack-deadline leases, `Acknowledge`, `ModifyAckDeadline` (0 = nack/redeliver, 10..600 s extend).
- **Delivery features** — ordering keys (one-in-flight-per-key, in-order redelivery), CEL-subset
  attribute **filtering**, **dead-letter** topics (`max_delivery_attempts` 5..100), **push**
  delivery (wrapped JSON envelope or raw body), **exactly-once** delivery.
- **Advanced** — `Seek` to a timestamp or **snapshot** (replay bounded by `MaxSeekReplay`,
  pre-window timestamps clamp to the earliest retained message), **schema** validation
  (Avro / Protobuf, enforce-on-publish, ≤ 300 KB definitions, revisions).
- **Interop** — native cross-protocol consume of a published message on the Events Store log
  `pubsub.{t}` via the per-language KubeMQ SDK.

**Known deviations (documented, not hidden):** authentication is **none** (emulator); **exactly-once
is node-local** (an `ack_id` minted on one node is invalid on another — pin a subscription's
StreamingPull to one node in a cluster, or accept at-least-once across nodes); a unary
invalid-ack returns `FAILED_PRECONDITION` + `ErrorInfo` (not literal `INVALID_ARGUMENT`); per-resource
retention is clamped to the broker's `Store.MaxRetention`; export subscriptions
(BigQuery/Cloud Storage/Bigtable) and ingestion are rejected; `kms_key_name` is accepted and
ignored; **gRPC only** (no REST/JSON v1). See
[`docs/reference/migration-from-gcp.md`](docs/reference/migration-from-gcp.md) and
[`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md) §5 for the full gotcha list.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
