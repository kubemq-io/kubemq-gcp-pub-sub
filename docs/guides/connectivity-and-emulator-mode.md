# Connectivity & Emulator Mode

This guide covers how clients reach the connector: the standard `PUBSUB_EMULATOR_HOST` env var
(the zero-code-change drop-in), the no-auth / insecure-transport posture, the cosmetic
`AdvertisedEndpoint` dashboard hint, the gRPC-only constraint, and the sticky-load-balancer caveat
for clusters.

## The Emulator Protocol — Zero Code Changes

The connector is a dedicated **gRPC listener** (default port **8085**, the Pub/Sub emulator
convention) inside the KubeMQ server. Every official Pub/Sub client library (Go, Python, Java,
Node.js, C#, Ruby) and `gcloud` honour the standard `PUBSUB_EMULATOR_HOST` environment variable.
When that variable is set, the SDK **clears its Google credentials, skips Google auth, and dials
insecure gRPC** — exactly as it would against Google's own local emulator. Pointing an unmodified
Pub/Sub application at KubeMQ therefore requires **no code changes**:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default gRPC port; SDK uses the insecure path
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
# Some clients and gcloud also read this alias:
# export GOOGLE_CLOUD_PROJECT=my-project
```

There is **no bespoke wrapper variable** — the standard Google env var *is* the contract, and that
zero-code-change drop-in is the connector's headline value proposition.

## Per-Language Emulator Detection

Most clients auto-detect the emulator from `PUBSUB_EMULATOR_HOST` with no extra code; two need an
explicit flag or constructor argument:

| Language | Construction (emulator) |
|----------|-------------------------|
| Go | `pubsub.NewClient(ctx, projectID)` — reads `PUBSUB_EMULATOR_HOST` and dials insecurely. |
| Python | `pubsub_v1.PublisherClient()` / `SubscriberClient()` — honours the env var. |
| Node/TS | `new PubSub({ projectId })` — auto-detects the emulator from the env var. |
| Java | `Publisher.newBuilder(...)` / admin clients — set a plaintext `ManagedChannel` to the emulator host with `NoCredentialsProvider` when `PUBSUB_EMULATOR_HOST` is set. |
| C# | `new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build()` (or `EmulatorOrProduction`). |
| Ruby | `Google::Cloud::PubSub.new(project_id: ENV["PUBSUB_PROJECT_ID"], emulator_host: ENV["PUBSUB_EMULATOR_HOST"])`. |

> **Gotcha — project id is parsed but ignored.** The connector validates the `projects/{p}`
> segment but is **single-tenant** (like the emulator): resource ids are global across "projects".
> Any project id works; topic `orders` is always Events Store channel `pubsub.orders` regardless
> of project. See [../reference/channel-mapping.md](../reference/channel-mapping.md).

## No Auth, No TLS

The connector runs in **emulator mode**: no Google OAuth2/JWT validation, no IAM enforcement (the
`IAMPolicy` RPCs are permissive stubs), and **no TLS**. The gRPC interceptor chain is
**Recovery → Logger → Traffic-gate** — there is no auth interceptor (a nil auth seam is reserved
for a future release, so handlers stay untouched).

> **Security posture.** This is by design and matches Google's local emulator. The transport is
> **insecure gRPC**. **Do not expose port 8085 to untrusted networks.** Run it on a trusted
> network or behind your own perimeter.

DoS guards remain active even with no auth:

- `MaxMessageBytes` — a gRPC `MaxRecvMsgSize` body cap (default 10 MiB);
- `MaxInflightPerSubscription` — hard cap on leased (un-acked) messages per subscription (20,000);
- `MaxConcurrentPolls` — poller slot budget (1,024);
- `MaxSeekReplay` — max messages a single `Seek` may replay (1,000,000);
- push delivery backoff.

These knobs are documented in [../configuration.md](../configuration.md).

## The `AdvertisedEndpoint` Hint

`CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT` (default empty) is a **cosmetic** `host:port` string the
dashboard shows as the suggested `PUBSUB_EMULATOR_HOST` value. It does not change how the listener
binds or behaves; set it to the externally reachable address (e.g.
`kubemq.mycompany.svc:8085`) so operators copy the right host into their env.

## gRPC Only — No REST/JSON

> **Gotcha — the connector is gRPC-only.** There is **no REST/JSON v1** (grpc-gateway) surface.
> Clients and tools that only speak the Pub/Sub REST API will not work; use a gRPC client library
> or `gcloud` (which uses gRPC for the emulator). See
> [../reference/capabilities.md](../reference/capabilities.md).

## The Native Interop Connection

One example variant — `interop/native-events-store` — and the burn-in `cross_protocol_interop`
worker open a **second** connection: the native KubeMQ SDK over gRPC at **`localhost:50000`** (NOT
the emulator host). A Pub/Sub publish to topic `t` writes once to Events Store channel `pubsub.{t}`,
which the native side consumes — showing the three reserved tags (`_pubsub_message_id`,
`_pubsub_publish_time`, `_pubsub_ordering_key`) that are stripped from `attributes` for Pub/Sub
clients. The burn-in harness reads `KUBEMQ_BROKER_ADDRESS` (the emulator host) and `grpc_address`
(`localhost:50000`) for the two halves. See
[../concepts/cross-protocol-interop.md](../concepts/cross-protocol-interop.md).

## Periodic Stream Reconnect

A `StreamingPull` stream is closed by the server after `StreamCloseSeconds` (default **1800 s** /
30 min) with `UNAVAILABLE`; client libraries **transparently reconnect**. This bounds per-stream
resource lifetime and is normal — your `Subscribe` callback keeps running across the reconnect. See
[subscribing.md](subscribing.md).

## Sticky-LB Caveat (Cluster)

> **Gotcha — node-local state needs a sticky LB.** Topic/subscription/snapshot/schema **records**
> are synced cluster-wide (BoltDB + NATS), but two pieces of delivery state are **node-local**:
>
> - **Exactly-once `ack_id`s** — an `ack_id` minted on one node is invalid on another (its node id
>   won't match), so an exactly-once subscription's StreamingPull traffic must be pinned to one
>   node;
> - **StreamingPull leases / in-flight tracking** — leased messages and flow-control counters live
>   on the node that delivered them.
>
> Cluster deployments must therefore put a **sticky load balancer** (session affinity) in front of
> the connector so each subscriber sticks to one node for the lifetime of its in-flight messages.
> Single-node deployments are unaffected. Full multi-node guidance is deferred (no clustering guide
> in v1). See [reliability.md](reliability.md) and
> [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md).

Message **data** itself is not replicated by the connector — it rides the existing Events Store /
Queues replication.

## Traffic Gate

While the broker is not ready, the Traffic-gate interceptor short-circuits requests; on a
not-ready → ready transition the connector **drops all in-memory leases** (their downstream
transactions are dead) and the poller rebuilds. SDKs see transient `UNAVAILABLE` and retry.

## See Also

- [../getting-started.md](../getting-started.md) — first message in minutes via the emulator drop-in.
- [../configuration.md](../configuration.md) — the 11 `CONNECTORS_PUB_SUB_*` fields and defaults.
- [reliability.md](reliability.md) — the node-local exactly-once / sticky-LB caveat in depth.
- [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md) — endpoint swap, the exactly-once decision.
