# Architecture

## Overview

The KubeMQ **Google Cloud Pub/Sub connector** is an embedded, wire-protocol bridge inside
the KubeMQ server that speaks the genuine Pub/Sub v1 **gRPC** services on a dedicated gRPC listener
(default TCP **8085**, the Pub/Sub emulator convention). It is **opt-in** (disabled by default);
enable it with `CONNECTORS_GCP_ENABLE=true`. It runs in **emulator mode**: no authentication, no TLS,
insecure gRPC — exactly like Google's local emulator. Any standard Pub/Sub client library (Go,
Python, Java, Node.js, C#, Ruby) or `gcloud` connects to it by setting one environment variable,
`PUBSUB_EMULATOR_HOST` — **no code changes, no library swap, no emulator to install**.

The connector implements the **real Pub/Sub v1 gRPC services** — **38 RPCs** total across four
services:

- `google.pubsub.v1.Publisher` (9 RPCs)
- `google.pubsub.v1.Subscriber` (16 RPCs)
- `google.pubsub.v1.SchemaService` (10 RPCs)
- `google.iam.v1.IAMPolicy` (3 permissive stubs)

It is **dual bidirectional**: external producers source messages into KubeMQ via `Publish`, and
KubeMQ delivers them out to external consumers via `Pull` / `StreamingPull`. See
[reference/capabilities.md](reference/capabilities.md) for the full RPC matrix with M1/M2/M3 tags.

## gRPC Only

> **The connector is gRPC-only (gotcha #9).** There is **no REST/JSON v1** surface (grpc-gateway
> is not used). Clients and tools that only speak the Pub/Sub REST API will not work; use a gRPC
> client library or `gcloud` (which uses gRPC for the emulator). See
> [reference/capabilities.md](reference/capabilities.md).

## Stack Diagram

```
Pub/Sub client (Go / Python / Java / Node.js / C# / Ruby) or gcloud
         │
         │  PUBSUB_EMULATOR_HOST=host:8085   → SDK clears credentials, skips auth,
         │  PUBSUB_PROJECT_ID=my-project       dials INSECURE gRPC (emulator path)
         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  Google Pub/Sub connector  (in the KubeMQ server, dedicated gRPC listener on 8085) │
│                                                                                    │
│   gRPC interceptor chain:  Recovery → Logger → Traffic-gate                        │
│     (no auth interceptor — a nil auth seam is reserved for a future release)        │
│            │                                                                       │
│            ▼                                                                       │
│   Pub/Sub v1 services (38 RPCs):                                                   │
│     Publisher (9) · Subscriber (16) · SchemaService (10) · IAMPolicy (3 stubs)     │
│            │                                                                       │
│            ▼                                                                       │
│   ┌────────────────────────────────┐     ┌─────────────────────────────────────┐  │
│   │ Publish  (source ext→KubeMQ)   │     │ Pull / StreamingPull (target →ext)   │  │
│   │  Array.SendEventsStore(once)   │     │  lease per delivery (ack_id);        │  │
│   │  → fan out one queue copy per  │     │  250 ms sweeper expires leases,      │  │
│   │     subscription (apply filter)│     │  retries / dead-letters              │  │
│   └───────────────┬────────────────┘     └──────────────────┬──────────────────┘  │
└───────────────────┼─────────────────────────────────────────┼─────────────────────┘
                    ▼                                          ▼
   Topic log (write-once):   pubsub.{t}   ────fan-out────▶  Sub queue:  pubsub.sub.{s}
   (Events Store; the authoritative,                       (Queue; one copy per sub,
    cross-protocol, replayable source)                      filtered)
                    │
                    ▼
               NATS broker  (durable store)
```

A `Publish` is written **once** to the topic log (the authoritative, cross-protocol, replayable
source) and fanned out to **one queue copy per subscription**, applying each subscription's filter.

## Interceptor Chain

The gRPC server runs a fixed three-stage interceptor chain — **Recovery → Logger → Traffic-gate**:

- **Recovery** — recovers from handler panics and converts them to gRPC errors.
- **Logger** — structured request logging.
- **Traffic-gate** — while the broker is **not ready**, short-circuits requests with transient
  `UNAVAILABLE`; on a not-ready → ready transition the connector **drops all in-memory leases**
  (their downstream transactions are dead) and the poller rebuilds. SDKs see transient
  `UNAVAILABLE` and retry.

There is **no auth interceptor** — the connector runs in emulator mode (no Google OAuth2/JWT, no
TLS, IAM RPCs are permissive stubs). A nil auth seam is reserved for a future release so handlers
stay untouched. **Do not expose port 8085 to untrusted networks.** DoS guards
(`MaxMessageBytes`, `MaxInflightPerSubscription`, `MaxConcurrentPolls`, `MaxSeekReplay`, push
backoff) remain active regardless. See
[guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

## Service Model & Channel Mapping

This is the single most important mental model: a Pub/Sub `Publish` and a native KubeMQ consume
meet on the same channel.

| Concept | Behavior |
|---------|----------|
| **Listener** | A dedicated gRPC server on `Connectors.PubSub.Port` (default **8085**, the emulator convention). Insecure gRPC, no auth, no TLS. |
| **Topic → Events Store log** | Topic `projects/{p}/topics/{t}` ↔ native KubeMQ **Events Store** log `pubsub.{t}` — the authoritative, cross-protocol, replayable source. A `Publish` writes **once** here (`Array.SendEventsStore(pubsub.{t})`). |
| **Subscription → Queue** | Subscription `projects/{p}/subscriptions/{s}` ↔ native KubeMQ **Queue** channel `pubsub.sub.{s}`. One queue copy is fanned out per subscription (`Array.SendQueueMessage`), applying that subscription's filter. |
| **Snapshots / Schemas** | BoltDB **registry** records — no native channel. |
| **Project segment** | `{p}` is **parsed and validated but ignored** — the connector is single-tenant (like the emulator), so resource ids are **global across projects** (gotcha #2). |
| **Reserved tags** | `_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key` are carried across the wire — visible to native consumers, stripped from `attributes` for Pub/Sub clients. |
| **Resource id rules** | Topic ids may **not** start with `sub.` (the reserved broker namespace for subscription queues, gotcha #7). |

See [reference/channel-mapping.md](reference/channel-mapping.md) for the full grammar and the
reserved-tag table, and [configuration.md](configuration.md) for the connector knobs.

## Write-Once Topic Log + Per-Subscription Queue Fan-Out

A `Publish` is the heart of the model:

1. **Validate the whole batch** (1..1000 messages) before enqueuing anything — per message total
   ≤ 10 MiB, ≤ 100 attributes, attr key ≤ 256 B (no `goog`), attr value ≤ 1024 B, ordering key
   ≤ 1024 B, `data` OR `attributes` non-empty. Any failure rejects the entire batch with
   `INVALID_ARGUMENT` and nothing is published (atomic).
2. **Write once** to the topic's Events Store log `pubsub.{t}` (`Array.SendEventsStore`),
   assigning a server message id and publish-time. This single record is the authoritative,
   cross-protocol, replayable source.
3. **Fan out one queue copy per subscription** — for each subscription bound to the topic, evaluate
   its CEL-subset attribute filter; matching messages are sent (`Array.SendQueueMessage`) to that
   subscription's queue `pubsub.sub.{s}`.

Delivery is then driven from the per-subscription queue: every delivered message gets an opaque
`ack_id` under an ack-deadline lease; a 250 ms sweeper expires overdue leases, applies retry
backoff, and redelivers or dead-letters. See
[concepts/topics-and-publish.md](concepts/topics-and-publish.md) and
[concepts/subscriptions-and-pull.md](concepts/subscriptions-and-pull.md).

## Cross-Protocol Note

Because every topic is a normal KubeMQ Events Store log, a Pub/Sub `Publish` to topic `orders`
(written to `pubsub.orders`) is consumable by a **native** KubeMQ Events Store subscriber on the
same channel — and the native side sees the three reserved tags (`_pubsub_message_id`,
`_pubsub_publish_time`, `_pubsub_ordering_key`) that are stripped from `attributes` for Pub/Sub
clients. This is proven by the `interop/native-events-store` example variant and the burn-in
`cross_protocol_interop` worker, both of which open a second connection to the native KubeMQ gRPC
broker at `localhost:50000` (NOT the emulator host). See
[concepts/cross-protocol-interop.md](concepts/cross-protocol-interop.md).

## Cluster Behavior (Node-Local State)

Topic / subscription / snapshot / schema **records** are synced cluster-wide (BoltDB + NATS), but
two pieces of delivery state are **node-local**:

- **Exactly-once `ack_id`s** — an `ack_id` minted on one node is invalid on another (its node id
  won't match), so an exactly-once subscription's `StreamingPull` traffic must be pinned to one
  node (gotcha #1);
- **`StreamingPull` leases / in-flight tracking** — leased messages and flow-control counters live
  on the node that delivered them.

Cluster deployments therefore need a **sticky load balancer** (session affinity) in front of the
connector. Single-node deployments are unaffected. Full multi-node guidance is deferred (no
clustering guide in v1). See [guides/reliability.md](guides/reliability.md) and
[reference/migration-from-gcp.md](reference/migration-from-gcp.md).

## See Also

- [getting-started.md](getting-started.md) — first message in minutes via the emulator drop-in.
- [configuration.md](configuration.md) — the 11 `CONNECTORS_PUB_SUB_*` fields and defaults.
- [reference/capabilities.md](reference/capabilities.md) — the 38-RPC matrix with M1/M2/M3 tags.
- [reference/channel-mapping.md](reference/channel-mapping.md) — the resource-to-channel grammar.
- [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md) — the
  env vars, no-auth / insecure posture, gRPC-only, sticky-LB caveat.
