# KubeMQ Google Cloud Pub/Sub — Documentation

Documentation for the KubeMQ embedded **Google Cloud Pub/Sub wire-protocol connector**.

The connector is a dedicated **gRPC listener** (default TCP **8085**, the Pub/Sub emulator
convention) inside the KubeMQ server that implements the real Pub/Sub v1 gRPC services —
`google.pubsub.v1.Publisher`, `Subscriber`, `SchemaService`, and permissive
`google.iam.v1.IAMPolicy` stubs (38 RPCs total). It runs in **emulator mode**: no authentication,
no TLS, exactly like Google's local emulator.

Because every official Pub/Sub client library (Go, Python, Java, Node.js, C#, Ruby) and `gcloud`
honour the standard `PUBSUB_EMULATOR_HOST` environment variable, pointing an unmodified
application at KubeMQ requires **zero code changes** — set the env var and a project id, and the
SDK clears its Google credentials and dials insecure gRPC:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default gRPC port
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
```

Topics map onto native KubeMQ **Events Store** logs (`pubsub.{topic}`) and each subscription onto
a native **Queue** channel (`pubsub.sub.{subscription}`), so Pub/Sub producers and native
gRPC/REST consumers interoperate on the same messages. A `Publish` is written **once** to the
topic log (the authoritative, cross-protocol, replayable source) and fanned out to one queue copy
per subscription, applying each subscription's filter.

## Contents

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | The gRPC listener inside the KubeMQ server; interceptor chain (Recovery → Logger → Traffic-gate); topic-log write + per-subscription queue fan-out; cross-protocol note |
| [getting-started.md](getting-started.md) | The `PUBSUB_EMULATOR_HOST` drop-in (no docker-compose, no emulator); `gcloud` + Go quickstart; create topic/subscription; publish/pull |
| [configuration.md](configuration.md) | The 11 `CONNECTORS_PUB_SUB_*` connector fields, defaults, and validation rules |
| [troubleshooting.md](troubleshooting.md) | Symptom → cause → fix diagnostics: connection refused / emulator not detected, missing topic/subscription, ack-deadline expiry & redelivery, exactly-once `FAILED_PRECONDITION`, dead-letter timing, filtering, schema, push, ordering, message-size / batch limits |
| [faq.md](faq.md) | Common "why does it behave this way?" questions: the `PUBSUB_EMULATOR_HOST` drop-in, ignored project id, no-auth/no-TLS posture, gRPC-only (no REST), node-local exactly-once, dead-letter redrive timing, retention clamp, rejected export/ingestion features |
| **Concepts** | |
| [concepts/topics-and-publish.md](concepts/topics-and-publish.md) | Topic → `pubsub.{t}`; batch publish; atomic batch validation; server-assigned ids / publish-time |
| [concepts/subscriptions-and-pull.md](concepts/subscriptions-and-pull.md) | Subscription → `pubsub.sub.{s}`; Pull vs StreamingPull; ack-deadline leases; ack / nack |
| [concepts/ordering-keys.md](concepts/ordering-keys.md) | `enable_message_ordering`; at-most-one-in-flight per key; in-order redelivery |
| [concepts/exactly-once.md](concepts/exactly-once.md) | The node-local boundary; StreamingPull confirmation messages; unary `FAILED_PRECONDITION` + `ErrorInfo` |
| [concepts/dead-letter-and-retry.md](concepts/dead-letter-and-retry.md) | `max_delivery_attempts` 5..100; retry backoff; connector-level dead-letter fan-out |
| [concepts/schema-validation.md](concepts/schema-validation.md) | Avro / Protobuf schemas; enforce-on-publish; revisions; ≤ 300 KB |
| [concepts/seek-and-snapshots.md](concepts/seek-and-snapshots.md) | Seek to timestamp / snapshot; topic-log replay; `MaxSeekReplay`; pre-window clamp |
| [concepts/cross-protocol-interop.md](concepts/cross-protocol-interop.md) | Pub/Sub publish → Events Store `pubsub.{t}`; native consume via the KubeMQ SDK; reserved tags |
| **Guides** | |
| [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md) | The env vars, no-auth / insecure-transport posture, `AdvertisedEndpoint` hint, gRPC-only, sticky-LB caveat |
| [guides/publishing.md](guides/publishing.md) | Basic publish, batch publish (≤ 1000), ordering keys, attributes, atomic validation |
| [guides/subscribing.md](guides/subscribing.md) | Pull, StreamingPull, ack deadlines, nack / extend, flow control, periodic reconnect |
| [guides/filtering.md](guides/filtering.md) | The CEL-subset attribute-filter syntax (attributes-only, ≤ 256 chars, immutable) with examples |
| [guides/push-delivery.md](guides/push-delivery.md) | `push_config` wrapped envelope, `no_wrapper`, OIDC bearer, HTTPS / localhost rule, retry → DLQ |
| [guides/reliability.md](guides/reliability.md) | Exactly-once (node-local), ordering, dead-letter, at-least-once, the sticky-LB caveat |
| **Reference** | |
| [reference/capabilities.md](reference/capabilities.md) | The 38-RPC matrix (Publisher 9 / Subscriber 16 / Schema 10 / IAM 3) with M1/M2/M3 tags |
| [reference/channel-mapping.md](reference/channel-mapping.md) | Topic / subscription → channel grammar; the reserved-tag table |
| [reference/limits-and-rules.md](reference/limits-and-rules.md) | The Google-exact resource / message / batch / attribute / retention limits |
| [reference/error-codes.md](reference/error-codes.md) | `INVALID_ARGUMENT`, `FAILED_PRECONDITION` + `ErrorInfo`, and the rest of the error table |
| [reference/migration-from-gcp.md](reference/migration-from-gcp.md) | Point the SDK at KubeMQ; recreate resources; mind the limits; the exactly-once decision; the interop bridge |
| [reference/connections-endpoint.md](reference/connections-endpoint.md) | The gRPC connection endpoints (`:8085` emulator listener, `:50000` native broker), Prometheus metrics, and dashboard surface; gRPC-only (no REST/JSON v1) |

> **No dedicated clustering guide** (deferred). The node-local caveats — exactly-once `ack_id`
> validity and StreamingPull lease state are per-node — appear as callouts in
> [guides/reliability.md](guides/reliability.md),
> [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md), and
> [reference/migration-from-gcp.md](reference/migration-from-gcp.md).

## Examples

Working code examples in **6 languages** (15 variants each) — across the `topics/`,
`subscriptions/`, `delivery/`, `advanced/`, and `interop/` groups — are in
[../examples/](../examples/README.md). Every example uses the **native** Google Cloud Pub/Sub
client library; only the `interop/native-events-store` variant additionally imports a KubeMQ SDK
for the native gRPC half.

## Prerequisites

All examples and documentation assume:

- A KubeMQ server running with the **Pub/Sub connector enabled** (`CONNECTORS_GCP_ENABLE=true`,
  the default) on port **8085**.
- The standard Google emulator env vars set: `PUBSUB_EMULATOR_HOST=localhost:8085` and a project
  id (`PUBSUB_PROJECT_ID` / `GOOGLE_CLOUD_PROJECT` — any value; the project segment is parsed but
  ignored).
- For the cross-protocol interop variant only, the **native KubeMQ gRPC broker** reachable at
  `localhost:50000`.

There is **no Pub/Sub emulator, no docker-compose, and no boot-the-server step** beyond running
KubeMQ — point your SDK at an existing connector. See
[getting-started.md](getting-started.md) and
[guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).
