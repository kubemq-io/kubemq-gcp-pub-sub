# Burn-in dependencies (spec S6)

Module path: `github.com/kubemq-io/kubemq-gcp-pub-sub/burnin` (standalone Go
module, mirroring the sibling `kubemq-aws/burnin`). Go 1.25.

The transport drives the official Google Cloud Pub/Sub client
(`cloud.google.com/go/pubsub`), connecting to the connector via the standard
`PUBSUB_EMULATOR_HOST` env var (no-auth, insecure gRPC — the connector's
zero-code-change drop-in). The `cross_protocol_interop` worker is the ONLY user
of `kubemq-go/v2` (the native Events Store side on channel `pubsub.{topic}`,
opened with `StartFromNewEvents()` subscribe-before-publish). There is **no gRPC
RPC responder** — Pub/Sub is pub/sub, not request/reply.

| Module | Version | Used by |
|--------|---------|---------|
| `cloud.google.com/go/pubsub` | latest stable | transport/pubsub_client.go, worker/ (topic/sub create, Publish, Pull/StreamingPull, Ack) |
| `google.golang.org/api` | latest stable | transport/pubsub_client.go (option.WithEndpoint / WithoutAuthentication) |
| `google.golang.org/grpc` | latest stable | transport/pubsub_client.go (insecure dial credentials) |
| `github.com/kubemq-io/kubemq-go/v2` | v2.0.3 | transport/grpc_client.go, worker/cross_protocol_interop.go (native side ONLY) |
| `github.com/HdrHistogram/hdrhistogram-go` | v1.1.2 | metrics/ latency histograms |
| `github.com/prometheus/client_golang` | v1.20.5 | metrics/, server/ |
| `golang.org/x/time` | v0.15.0 | worker/ rate limiting |
| `gopkg.in/yaml.v3` | v3.0.1 | config/ |

> Versions are the latest stable resolved by `go mod tidy` as of 2026-06.
> `kubemq-go` resolves to the **v2** module (`/v2`), matching the in-monorepo
> `kubemq-keda` scaler; the v1 line is not used. Commit `go.sum`.

## Connectivity

- Pub/Sub endpoint: bare host:port via `PUBSUB_EMULATOR_HOST` (default
  `localhost:8085`). The SDK clears credentials, skips Google auth, and uses
  insecure gRPC. `KUBEMQ_BROKER_ADDRESS` is mirrored into `PUBSUB_EMULATOR_HOST`
  by the harness for parity with the sibling SDK burn-in apps. `gcp.project_id`
  is any non-empty id (the project segment is parsed but ignored by the
  connector).
- Native gRPC (interop worker): `KUBEMQ_GRPC_ADDRESS` (default `localhost:50000`).

## Channel / resource mapping

- topic `{t}` → Events Store channel `pubsub.{t}`
- subscription `{s}` → Queue channel `pubsub.sub.{s}`
- resource ids: `gcp_burnin_{worker}_{idx:04d}` (overridable via
  `BURNIN_RESOURCE_PREFIX` so concurrent per-language burn-in agents never
  collide on the global canonical channels).

## Fixed port

`8899` — the first clean port above the SDK burn-in range (8888–8897) and the
AWS m3-diag port (8898), collision-free for concurrent dashboard control. It
lives in exactly three places: the `metrics.port` of each `burnin*.yaml`, the
`config/config.go` `Metrics.Port` default, and the connector-harness tables in
the `burnin-start` / `burnin-stop` / `stress-test` skills.
