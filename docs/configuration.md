# Configuration

The Google Cloud Pub/Sub connector is configured under `Connectors.PubSub` in the KubeMQ server
configuration. It is **opt-in** (disabled by default); set `CONNECTORS_GCP_ENABLE=true` to enable
it. All **11** fields have environment-variable overrides of the form `CONNECTORS_PUB_SUB_*`.

This page is the prose walkthrough of the config knobs; the at-a-glance table (with the
Google-exact rules alongside) lives in [reference/limits-and-rules.md](reference/limits-and-rules.md).

## PubSubConfig Fields

Use these values **verbatim**. They were verified against the `PubSubConfig` struct / defaults and
the `Validate()` method.

| Env var | Default | Type | Meaning / validation | Config field |
|---------|---------|------|----------------------|--------------|
| `CONNECTORS_GCP_ENABLE` | `false` | bool | Opt-in (disabled by default); `true` enables the connector (opens port 8085). | `Enable` |
| `CONNECTORS_PUB_SUB_PORT` | `"8085"` | string | gRPC listen port (the emulator convention); must be a valid port and **distinct** from the gRPC/REST/HTTP/AWS-connector ports. | `Port` |
| `CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT` | `""` | string | `host:port` shown in the dashboard's `PUBSUB_EMULATOR_HOST` hint. **Cosmetic** — does not change listen behavior. | `AdvertisedEndpoint` |
| `CONNECTORS_PUB_SUB_MAX_MESSAGE_BYTES` | `10485760` (10 MiB) | int | Max total message size; also sizes the gRPC frame (`MaxRecvMsgSize`). Must be **> 0**. | `MaxMessageBytes` |
| `CONNECTORS_PUB_SUB_DEFAULT_ACK_DEADLINE_SECONDS` | `10` | int | Default ack deadline; must be **10..600**. | `DefaultAckDeadlineSeconds` |
| `CONNECTORS_PUB_SUB_MAX_OUTSTANDING_MESSAGES` | `1000` | int | Per-stream flow-control ceiling for clients that request unlimited. Must be **> 0**. | `MaxOutstandingMessages` |
| `CONNECTORS_PUB_SUB_MAX_INFLIGHT_PER_SUBSCRIPTION` | `20000` | int | Hard cap on leased (un-acked) messages per subscription. Must be **> 0**. | `MaxInflightPerSubscription` |
| `CONNECTORS_PUB_SUB_MAX_CONCURRENT_POLLS` | `1024` | int | Poller slot budget. Must be **> 0**. | `MaxConcurrentPolls` |
| `CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS` | `1800` | int | Periodic `StreamingPull` close interval (forces SDK reconnect; bounds per-stream lifetime). Must be **> 0**. | `StreamCloseSeconds` |
| `CONNECTORS_PUB_SUB_MAX_SEEK_REPLAY` | `1000000` | int | Max messages replayed by a single `Seek` (hits cap → WARN, no silent loss). Must be **> 0**. | `MaxSeekReplay` |
| `CONNECTORS_GCP_ENABLE_REFLECTION` | `false` | bool | Register gRPC server reflection (debugging only). | `EnableReflection` |

> **Env-form rule.** Compound camelCase fields are snake-split:
> `MaxInflightPerSubscription` → `CONNECTORS_PUB_SUB_MAX_INFLIGHT_PER_SUBSCRIPTION`,
> `DefaultAckDeadlineSeconds` → `CONNECTORS_PUB_SUB_DEFAULT_ACK_DEADLINE_SECONDS`. Note the prefix
> has the underscore (`CONNECTORS_PUB_SUB_*`).

All eleven fields take their env-var form as a plain string value (bools as `true`/`false`, the
numeric knobs as base-10 integers, `Port` as a quoted-or-bare port number). For example:

```bash
# The connector is opt-in (disabled by default); enable it explicitly.
export CONNECTORS_GCP_ENABLE=true                          # bool
export CONNECTORS_PUB_SUB_PORT=8085                            # string (port)
export CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT=kubemq.svc:8085  # host:port (cosmetic)
export CONNECTORS_PUB_SUB_MAX_MESSAGE_BYTES=10485760           # int (10 MiB), > 0
export CONNECTORS_PUB_SUB_DEFAULT_ACK_DEADLINE_SECONDS=10      # int, 10..600
export CONNECTORS_PUB_SUB_MAX_OUTSTANDING_MESSAGES=1000        # int, > 0
export CONNECTORS_PUB_SUB_MAX_INFLIGHT_PER_SUBSCRIPTION=20000  # int, > 0
export CONNECTORS_PUB_SUB_MAX_CONCURRENT_POLLS=1024            # int, > 0
export CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS=1800            # int, > 0
export CONNECTORS_PUB_SUB_MAX_SEEK_REPLAY=1000000              # int, > 0
export CONNECTORS_GCP_ENABLE_REFLECTION=false             # bool
```

## Field Notes

- **`Enable`** — **opt-in** (disabled by default); a stock server does NOT open port 8085 unless
  `CONNECTORS_GCP_ENABLE=true` is set. Rollback is config-only — no data migration (topics remain
  as Events Store logs, subscriptions as Queue channels).
- **`Port`** — default `"8085"` (the Pub/Sub emulator convention). Must be a valid port and differ
  from any enabled Grpc/Rest/Http/AWS-connector port; cross-connector uniqueness is enforced by
  the server.
- **`AdvertisedEndpoint`** — purely **cosmetic**. The dashboard shows it as the suggested
  `PUBSUB_EMULATOR_HOST` value; set it to the externally reachable address (e.g.
  `kubemq.mycompany.svc:8085`) so operators copy the right host. It does **not** change how the
  listener binds.
- **`MaxMessageBytes`** — the max total message size (body + attributes) and the gRPC
  `MaxRecvMsgSize` body cap. Default 10 MiB, matching the Google-exact per-message limit. A
  DoS guard that stays active even in no-auth emulator mode.
- **`DefaultAckDeadlineSeconds`** — the default ack deadline applied when a subscription does not
  set one. Must be in **10..600** (the same range a per-message `ModifyAckDeadline(>0)` extend
  honours; `ModifyAckDeadline(0)` is an immediate nack/redeliver).
- **`MaxOutstandingMessages`** — the per-stream flow-control ceiling used for clients that request
  unlimited outstanding messages on `StreamingPull`.
- **`MaxInflightPerSubscription`** — the hard cap on leased (un-acked) in-flight messages per
  subscription. A DoS guard.
- **`MaxConcurrentPolls`** — the poller slot budget. A DoS guard.
- **`StreamCloseSeconds`** — the server periodically closes a `StreamingPull` stream after this
  interval (default 1800 s / 30 min) with `UNAVAILABLE`; client libraries **transparently
  reconnect**. This bounds per-stream resource lifetime and is normal.
- **`MaxSeekReplay`** — the ceiling on how many messages a single `Seek` may replay from the topic
  log into the subscription queue. Hitting the cap logs a **WARN** (no silent loss). A DoS guard.
- **`EnableReflection`** — registers gRPC server reflection for debugging (e.g. `grpcurl`). Off by
  default.

## Validation (`Validate()`)

`Validate()` returns `nil` when `!Enable` (a disabled connector skips all other checks). Otherwise
it:

1. Parses `Port` with `strconv.Atoi` and runs `validatePort` (must be a valid port number);
2. Requires `DefaultAckDeadlineSeconds` to be in **10..600**;
3. Requires the **six numeric knobs** — `MaxMessageBytes`, `MaxOutstandingMessages`,
   `MaxInflightPerSubscription`, `MaxConcurrentPolls`, `StreamCloseSeconds`, `MaxSeekReplay` — to be
   **> 0**.

Cross-connector **port uniqueness** (the Pub/Sub port must differ from the enabled
gRPC / REST / HTTP / AWS-connector ports) is enforced separately by the server.

## DoS Guards (Always Active in Emulator Mode)

Even though the connector has **no authentication** (emulator mode), these guards remain on and
cannot be disabled:

- `MaxMessageBytes` — the gRPC body cap (`MaxRecvMsgSize`);
- `MaxInflightPerSubscription` — the leased-message ceiling per subscription;
- `MaxConcurrentPolls` — the poller slot budget;
- `MaxSeekReplay` — the replay ceiling for a single `Seek`;
- push delivery backoff — bounds retry pressure on push endpoints.

> **Do not expose port 8085 to untrusted networks** — there is no auth or TLS. See
> [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

## Enable / Disable

```bash
# The connector is opt-in (disabled by default). To enable it (opens port 8085):
export CONNECTORS_GCP_ENABLE=true

# To disable it again (config-only; no data migration):
export CONNECTORS_GCP_ENABLE=false
```

## Cross-References

- **Google-exact rules** (resource ids, message / batch / attribute sizes, ack deadline,
  retention, `max_delivery_attempts`, filter, schema) are **not configurable** — see
  [reference/limits-and-rules.md](reference/limits-and-rules.md).
- **Retention is clamped to the broker's `Store.MaxRetention`** (gotcha #5) — `GetTopic` /
  `GetSubscription` echo the requested value; fan-out / seek / dashboard use the clamped value. See
  [concepts/seek-and-snapshots.md](concepts/seek-and-snapshots.md).
- **What each limit violation returns on the wire** — see
  [reference/error-codes.md](reference/error-codes.md).
