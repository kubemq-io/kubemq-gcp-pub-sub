# Endpoints, Metrics & Dashboard

The connector exposes a single **gRPC connection endpoint** (the emulator listener), plus a
read-only observability surface — Prometheus metrics and a dashboard page. There is **no REST/JSON
v1 endpoint** (gRPC only, gotcha #9) and **no management REST API** distinct from the broker's.

## Connection Endpoints

| Endpoint | Default | Transport | Who connects | How |
|----------|---------|-----------|--------------|-----|
| Pub/Sub emulator listener | `:8085` | gRPC, **insecure** (no TLS, no auth) | unmodified Google Pub/Sub SDKs + `gcloud pubsub` | `export PUBSUB_EMULATOR_HOST=<host>:8085` |
| Native KubeMQ broker | `:50000` | gRPC | the `interop/native-events-store` example + burn-in's native half | KubeMQ SDK against `localhost:50000` |
| KubeMQ REST | `:9090` | HTTP | _not used by this connector_ (gRPC only) | — |

### The emulator drop-in

Setting `PUBSUB_EMULATOR_HOST` is the entire contract — every official client library and `gcloud`
honour it: when set, the SDK **clears credentials, skips Google auth, and dials insecure gRPC**,
exactly as against Google's local emulator.

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default port; SDK uses the insecure path
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
# alias honoured by some clients/gcloud:
# export GOOGLE_CLOUD_PROJECT=my-project
```

`gcloud` uses an explicit endpoint override instead of the env var:

```bash
gcloud config set api_endpoint_overrides/pubsub http://localhost:8085/
```

> **`AdvertisedEndpoint` hint.** The cosmetic `CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT` config
> value (`host:port`) is what the dashboard shows in its `PUBSUB_EMULATOR_HOST` hint — it does not
> change the listen address. See [limits-and-rules.md](limits-and-rules.md).

### Security posture

No authentication, no TLS — emulator mode by design. DoS guards (`MaxMessageBytes` /
`MaxInflightPerSubscription` / `MaxConcurrentPolls` / `MaxSeekReplay` / push backoff) stay active.
**Do not expose port 8085 to untrusted networks.** See
[../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md).

## Prometheus Metrics

The connector registers these metrics; they are persisted
(time-bucket history survives a restart) like every other connector.

### Counters

| Metric | Labels | Meaning |
|--------|--------|---------|
| `kubemq_pubsub_operations_total` | `service`, `operation`, `status` | RPCs, by service / op / status |
| `kubemq_pubsub_push_deliveries_total` | `result` | push deliveries, by result |

### Histogram

| Metric | Labels | Meaning |
|--------|--------|---------|
| `kubemq_pubsub_operation_duration_seconds` | `service`, `operation` | per-operation latency |

### Gauges

| Metric | Meaning |
|--------|---------|
| `kubemq_pubsub_topics` | registered topics |
| `kubemq_pubsub_subscriptions` | registered subscriptions |
| `kubemq_pubsub_snapshots` | registered snapshots |
| `kubemq_pubsub_schemas` | registered schemas |
| `kubemq_pubsub_inflight_messages` | leased (received-but-un-acked) messages |
| `kubemq_pubsub_streaming_pull_streams` | open StreamingPull streams |

The gauge reporter refreshes every 5 s (one of the connector's three sweepers — lease 250 ms,
gauges 5 s, snapshot expiry hourly).

## Dashboard

The KubeMQ web dashboard has a **Pub/Sub** page (route `/pubsub`, in the Connectors group):
summary cards; topics / subscriptions / snapshots / schemas tables; topic & subscription detail
panels; an overview throughput chart; a resource gauge chart (topics / subscriptions / pull /
push / inflight); and an operations table + per-operation chart. The operation key is the bare
method name (e.g. `Publish`). A settings page edits the connector config and shows the
`PUBSUB_EMULATOR_HOST` hint (from `AdvertisedEndpoint`).

## Cluster Notes

Topic / subscription / snapshot / schema records live in a per-node BoltDB registry
(`{StorePath}/pubsub/registry.db`) synchronized over the NATS subject prefix `_PUBSUB_META_.`
with a last-writer-wins rule. **Message data itself is not replicated by the connector** — it
rides the existing Events Store / Queues replication. **Leases and exactly-once are node-local**
(gotcha #1) — see [error-codes.md](error-codes.md) and [migration-from-gcp.md](migration-from-gcp.md).

## See Also

- [../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md) — env vars, no-auth, insecure transport.
- [limits-and-rules.md](limits-and-rules.md) — the config knobs (port, advertised endpoint, DoS guards).
- [capabilities.md](capabilities.md) — no REST/JSON v1; gRPC only.
