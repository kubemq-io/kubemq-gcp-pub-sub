# Limits & Rules

Two kinds of limit apply to the connector: **Google-exact rules** (fixed, not configurable — the
connector enforces the same values real Pub/Sub does) and **connector config knobs** (the
`CONNECTORS_PUB_SUB_*` settings that tune DoS guards and defaults). Both are listed here; the
full prose walkthrough of the config knobs lives in [../configuration.md](../configuration.md).

## Google-Exact Rules (not configurable)

| Rule | Value |
|------|-------|
| Resource id | 3..255 chars, must start with a letter, charset `[A-Za-z0-9._~%+-]`, no `goog` prefix; **topic ids may not start with `sub.`** (reserved broker namespace) |
| Message size | ≤ 10 MiB total (body + attributes) |
| Batch size | ≤ 1000 messages |
| Attributes | ≤ 100 per message; key ≤ 256 B (no `goog`); value ≤ 1024 B; ordering key ≤ 1024 B |
| Ack deadline | `0` (nack) or **10..600 s** |
| Retention | 10 min .. 31 days, **clamped to the broker's `Store.MaxRetention`** |
| `max_delivery_attempts` | **5..100** (`0` = unset) |
| Filter | attributes-only CEL-subset, ≤ 256 chars, immutable (compiled at create) |
| Schema definition | ≤ 300 KB |

A violation of any of these is reported as a Google-shaped gRPC error (`INVALID_ARGUMENT` for most;
see [error-codes.md](error-codes.md)).

### Notes on the tricky ones

- **Resource id grammar (gotcha #7).** Ids must *start with a letter*; a topic id that starts
  with `sub.` collides with the subscription-queue namespace (`pubsub.sub.{s}`) and is rejected.
  The charset allows `._~%+-` but not `/` or spaces.
- **Retention clamp (gotcha #5).** You may request 10 min..31 days, but the effective retention is
  capped at the broker's global `Store.MaxRetention`. `GetTopic`/`GetSubscription` **echo your
  requested value**, while fan-out, `Seek`, and the dashboard use the **clamped** value. If
  retention matters for `Seek`, check the broker ceiling, not the topic config.
- **`max_delivery_attempts` (gotcha #6).** Dead-letter requires 5..100; values 1..4 are rejected.
  `0` means "no dead-letter policy" (unset), not "deliver zero times".
- **Batch atomicity.** The full batch is validated before *anything* is enqueued; the first
  offending message rejects the entire `Publish` with `INVALID_ARGUMENT` and nothing is published.
- **Attributes / ordering key sizing.** A message must have **`data` or `attributes` non-empty**.
  Attribute keys beginning `goog` are reserved by Google and rejected.

## Connector Config Knobs (`CONNECTORS_PUB_SUB_*`)

Server-side settings in `[Connectors.PubSub]`; env vars use the
`CONNECTORS_PUB_SUB_*` prefix. Defaults shown are the connector defaults.

| Field | Env var | Default | Meaning / constraint |
|-------|---------|---------|----------------------|
| `Enable` | `CONNECTORS_GCP_ENABLE` | `false` | Opt-in (disabled by default); `true` enables the connector (opens port 8085). |
| `Port` | `CONNECTORS_PUB_SUB_PORT` | `"8085"` | gRPC listen port (emulator convention); must be a valid port and **distinct** from the gRPC/REST/HTTP/AWS-connector ports. |
| `AdvertisedEndpoint` | `CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT` | `""` | `host:port` shown in the dashboard `PUBSUB_EMULATOR_HOST` hint. **Cosmetic** — does not change listen behavior. |
| `MaxMessageBytes` | `CONNECTORS_PUB_SUB_MAX_MESSAGE_BYTES` | `10485760` (10 MiB) | Max total message size; also sizes the gRPC frame (`MaxRecvMsgSize`). Must be > 0. |
| `DefaultAckDeadlineSeconds` | `CONNECTORS_PUB_SUB_DEFAULT_ACK_DEADLINE_SECONDS` | `10` | Default ack deadline; must be **10..600**. |
| `MaxOutstandingMessages` | `CONNECTORS_PUB_SUB_MAX_OUTSTANDING_MESSAGES` | `1000` | Per-stream flow-control ceiling for clients that request unlimited. Must be > 0. |
| `MaxInflightPerSubscription` | `CONNECTORS_PUB_SUB_MAX_INFLIGHT_PER_SUBSCRIPTION` | `20000` | Hard cap on leased (un-acked) messages per subscription. Must be > 0. |
| `MaxConcurrentPolls` | `CONNECTORS_PUB_SUB_MAX_CONCURRENT_POLLS` | `1024` | Poller slot budget. Must be > 0. |
| `StreamCloseSeconds` | `CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS` | `1800` | Periodic StreamingPull close interval (forces SDK reconnect; bounds per-stream lifetime). Must be > 0. |
| `MaxSeekReplay` | `CONNECTORS_PUB_SUB_MAX_SEEK_REPLAY` | `1000000` | Max messages replayed by a single `Seek` (hits cap → WARN, no silent loss). Must be > 0. |
| `EnableReflection` | `CONNECTORS_GCP_ENABLE_REFLECTION` | `false` | Register gRPC server reflection (debugging). |

### Validation

`Validate()` returns nil when `!Enable`. Otherwise it parses `Port` (`strconv.Atoi`) and runs
`validatePort`, requires `DefaultAckDeadlineSeconds` in **10..600**, and requires the six numeric
knobs (`MaxMessageBytes`, `MaxOutstandingMessages`, `MaxInflightPerSubscription`,
`MaxConcurrentPolls`, `StreamCloseSeconds`, `MaxSeekReplay`) to be **> 0**. Cross-connector port
uniqueness (vs gRPC/REST/HTTP/AWS) is enforced by the server.

## DoS Guards (always active, even in no-auth emulator mode)

Even though the connector has no authentication, these guards remain on and cannot be disabled:

- `MaxMessageBytes` — gRPC body cap (`MaxRecvMsgSize`).
- `MaxInflightPerSubscription` — leased-message ceiling per subscription.
- `MaxConcurrentPolls` — poller slot budget.
- `MaxSeekReplay` — replay ceiling for a single `Seek`.
- push delivery backoff — bounds retry pressure on push endpoints.

> Do **not** expose port 8085 to untrusted networks — there is no auth or TLS. See
> [connections-endpoint.md](connections-endpoint.md) and
> [../guides/connectivity-and-emulator-mode.md](../guides/connectivity-and-emulator-mode.md).

## See Also

- [../configuration.md](../configuration.md) — the prose walkthrough of every config field.
- [capabilities.md](capabilities.md) — what the connector supports / rejects.
- [error-codes.md](error-codes.md) — what a limit violation returns on the wire.
- [channel-mapping.md](channel-mapping.md) — the resource-id-to-channel grammar.
