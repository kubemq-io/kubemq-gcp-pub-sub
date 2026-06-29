# SHARED CONVENTIONS — kubemq-gcp-pub-sub

**Single source of truth for all downstream agents** building the up-to-90 examples (15
variants × 6 languages), the docs, and the burn-in harness. Where this file and the spec agree,
follow it; where the spec is more detailed, defer to
`.work/tasks/gcp-pub-sub-connector-repo/spec.md` (at the **clients root** `.work/`, NOT inside
this repo).

This file is duplicated verbatim at [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md);
keep the two copies in sync.

---

## 1. The connection — `PUBSUB_EMULATOR_HOST` + project id

The KubeMQ Pub/Sub connector is a **dedicated gRPC listener** inside the KubeMQ server (default
port **8085**, the emulator convention) implementing the real Pub/Sub v1 gRPC services. Examples
assume a running connector reachable at the emulator host; there is **NO emulator to install, NO
docker-compose, NO boot-the-server step** — the connector *is* the emulator.

### Canonical dev environment (USE THIS)

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default port; SDK uses the insecure path
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
# alias accepted by some clients/gcloud:
# export GOOGLE_CLOUD_PROJECT=my-project
```

- **Zero code changes.** Every official Pub/Sub client library and `gcloud` honour
  `PUBSUB_EMULATOR_HOST`: when set, the SDK clears credentials, skips Google auth, and uses
  **insecure gRPC** — exactly as against Google's local emulator. This drop-in is the repo's
  headline value prop. There is **no bespoke wrapper var** — the standard Google env var IS the
  contract.
- **No auth, no TLS.** Emulator posture by design. DoS guards stay active (`MaxRecvMsgSize`,
  `MaxInflightPerSubscription`, `MaxConcurrentPolls`, `MaxSeekReplay`, push backoff). Do **not**
  expose port 8085 to untrusted networks.
- **`AdvertisedEndpoint` hint** — cosmetic `host:port` shown in the dashboard's
  `PUBSUB_EMULATOR_HOST` hint (`CONNECTORS_PUB_SUB_ADVERTISED_ENDPOINT`).
- **gRPC only** — no REST/JSON v1 (grpc-gateway not used).
- **Burn-in / interop native half** uses native KubeMQ gRPC at **`localhost:50000`** (NOT the
  emulator host) for the `kubemq-go` / per-language KubeMQ SDK side. The harness reads
  `KUBEMQ_BROKER_ADDRESS` (emulator host) + `grpc_address` / `KUBEMQ_GRPC_ADDRESS`
  (`localhost:50000`).

### Per-language client construction (emulator auto-detect)

All snippets assume `PUBSUB_EMULATOR_HOST` is exported, so the client auto-detects the emulator
and uses insecure transport with no credentials.

| Lang | GCP Pub/Sub client construction | Native KubeMQ interop subscribe (interop variant only) |
|------|---------------------------------|--------------------------------------------------------|
| **Go** | `client, _ := pubsub.NewClient(ctx, os.Getenv("PUBSUB_PROJECT_ID"))` — the lib reads `PUBSUB_EMULATOR_HOST` and dials insecurely. | `client.SubscribeToEventsStore(ctx, "pubsub.orders", "", kubemq.StartFromNewEvents())` (`github.com/kubemq-io/kubemq-go/v2@v2.0.3`) |
| **Python** | `from google.cloud import pubsub_v1; pub = pubsub_v1.PublisherClient()` — honours `PUBSUB_EMULATOR_HOST`; manage with **uv**. | `EventsStoreSubscription(channel="pubsub.orders", events_store_type=EventStoreStartPosition.StartFromNew)` (`kubemq==4.1.5`) |
| **Java** | `TopicAdminClient`/`Publisher.newBuilder(...)`; when `PUBSUB_EMULATOR_HOST` is set, point the channel at it with plaintext creds (`NoCredentialsProvider` + `ManagedChannel` to the emulator host). BOM `libraries-bom`. | events-store subscribe with `StartNewOnly`/`StartFromNewEvents` start type (`io.kubemq.sdk:kubemq-sdk-Java:3.1.1`) |
| **Node/TS** | `const {PubSub} = require('@google-cloud/pubsub'); const ps = new PubSub({projectId});` — auto-detects the emulator from the env var. Run via `tsx`. | events-store subscribe with `StartNewOnly`/`StartFromNewEvents` start type (`kubemq-js@3.0.1`) |
| **C#** | `new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build()` (or `.EmulatorOrProduction`); .NET 8. | events-store subscribe with `StartNewOnly`/`StartFromNewEvents` start type (`KubeMQ.SDK.csharp@3.0.1`) |
| **Ruby** | `require "google/cloud/pubsub"; pubsub = Google::Cloud::PubSub.new(project_id: ENV["PUBSUB_PROJECT_ID"], emulator_host: ENV["PUBSUB_EMULATOR_HOST"])`. rbenv 3.3.7; `eval "$(rbenv init - zsh)"` first. | `KubeMQ::PubSub::EventsStoreSubscription.new(channel: "pubsub.orders", start_position: KubeMQ::PubSub::EventStoreStartPosition::START_NEW_ONLY)` (gem `kubemq ~> 1.0`, v1.0.0) |

### Channel mapping (the mental model every example/doc relies on)

- **Topic `projects/{p}/topics/{t}` ↔ Events Store log `pubsub.{t}`** — the authoritative,
  cross-protocol, replayable source. A `Publish` writes **once** here
  (`Array.SendEventsStore(pubsub.{t})`).
- **Subscription `projects/{p}/subscriptions/{s}` ↔ Queue `pubsub.sub.{s}`** — one queue copy is
  fanned out per subscription (`Array.SendQueueMessage`), applying that sub's filter.
- The `{p}` **project segment is parsed and validated but ignored** (single-tenant, like the
  emulator); resource ids are global across projects.
- **Reserved tags** carried across the wire (visible to native consumers, stripped from
  `attributes` for Pub/Sub clients): `_pubsub_message_id`, `_pubsub_publish_time`,
  `_pubsub_ordering_key`.
- **Resource id rules:** topic ids may **not** start with `sub.` (reserved broker namespace).

### Channel isolation (parallel-safe; MUST follow)

The canonical channels are shared and global: topic `orders` is always `pubsub.orders`;
subscription `s` is always `pubsub.sub.{s}`; the project segment is ignored, so ids are global
across "projects". If two runs (different languages, or `/examples` + `/burnin`) use the **same**
hard-coded topic/sub names concurrently, they cross-talk on the same channels and **false-fail**
assertions (extra / lost / out-of-order). To avoid this:

1. **Unique per-run resource names.** Never hard-code a bare topic/sub id. Generate a per-run
   suffix and build ids like `orders-<uuid8>` / `sub-orders-<uuid8>` so each run occupies distinct
   channels (`pubsub.orders-<uuid8>` / `pubsub.sub.sub-orders-<uuid8>`). Respect the resource-id
   rules (§7 limits): 3..255 chars, start with a letter, charset `[A-Za-z0-9._~%+-]`, no `goog`
   prefix; **topic ids may not start with `sub.`**.
2. **Best-effort cleanup.** Each example deletes the topics/subscriptions it created on exit
   (DeleteSubscription → DeleteTopic); cleanup must not fail the run if a resource is already gone.
3. **Harness namespace.** The burn-in harness derives its channel namespace from
   `broker.client_id_prefix` (e.g. `burnin-gcp`) so its topics/subs never collide with example
   runs or a second harness instance.

---

## 2. The 15-variant master table (VERBATIM from spec S5.2 — source of truth)

> Each per-example README's "Pub/Sub specifics" table is populated directly from the
> corresponding row. Every fact below is the connector's verified behaviour (spec S3 / §7).

| # | Variant (dir) | Group | RPC(s) exercised | Topic / Sub | Feature flag | What it prints / asserts | Canonical connector behaviour |
|---|---------------|-------|------------------|-------------|--------------|--------------------------|-------------------------------|
| 1 | `topics/basic-publish-subscribe` | topics | CreateTopic → CreateSubscription → Publish → Pull → Acknowledge | `orders` / `sub-orders` | none | full round-trip; one message published, pulled, acked; prints `<message-id>` | `Publish` writes once to `pubsub.orders`, fans out one queue copy to `pubsub.sub.sub-orders` |
| 2 | `topics/batch-publish` | topics | Publish (batch ≤ 1000) | `events` / `sub-events` | none | N message-ids returned in request order; atomic batch validation | batch 1..1000; whole batch rejected on first invalid message |
| 3 | `topics/topic-management` | topics | List/Get/UpdateTopic, DeleteTopic, ListTopicSubscriptions | `managed` / `sub-managed` | none | topic listed/updated; DeleteTopic tombstone; subscriptions listed | topic ids may not start with `sub.`; retention clamped to broker `Store.MaxRetention` |
| 4 | `subscriptions/streaming-pull` | subscriptions | StreamingPull + flow control | `stream` / `sub-stream` | none | bidirectional stream drains under flow control; survives periodic reconnect | `StreamCloseSeconds` (1800) periodic close forces SDK reconnect |
| 5 | `subscriptions/ack-deadline-nack` | subscriptions | ModifyAckDeadline (extend + 0-nack) | `redeliver` / `sub-redeliver` | none | extend then 0-nack → redelivery; `<ack-id>` shown | ack deadline 0 (nack) or 10..600 s; 250 ms sweeper expires leases |
| 6 | `delivery/ordering-keys` | delivery | Publish(ordering_key) + enable_message_ordering | `ordered` / `sub-ordered` | ordering | per-key in-order delivery; at-most-one-in-flight per key | round-robin cursor spreads delivery across contended keys; keyless = unordered |
| 7 | `delivery/message-filtering` | delivery | CreateSubscription(filter) → Publish matching + non-matching | `filtered` / `sub-filtered` | filter | matching delivered, non-matching suppressed (0 delivered) | CEL-subset attributes-only filter, ≤ 256 chars, immutable; malformed → `INVALID_ARGUMENT` |
| 8 | `delivery/dead-letter-topic` | delivery | dead_letter_topic + max_delivery_attempts | `work` + `work-dlq` / `sub-work` | dead-letter | message dead-lettered after `max_delivery_attempts`; DLQ republish + original ack | `max_delivery_attempts` 5..100; sweeper republishes then acks original |
| 9 | `delivery/push-delivery` | delivery | CreateSubscription(push_config) → local endpoint receives envelope | `pushed` / `sub-pushed` | push | per-sub worker POSTs wrapped JSON envelope; 2xx → ack | wrapped `{message:{data(b64),attributes,messageId,publishTime,orderingKey},subscription}` or `no_wrapper`; HTTPS required (HTTP only for localhost) |
| 10 | `delivery/exactly-once` | delivery | enable_exactly_once_delivery + ack confirmation | `eos` / `sub-eos` | exactly-once | StreamingPull returns `AcknowledgeConfirmation`; unary invalid-ack → `FAILED_PRECONDITION` + `ErrorInfo` | **node-local**: an `ack_id` minted on one node is invalid on another |
| 11 | `advanced/seek-timestamp` | advanced | Seek(timestamp) → replay | `seekable` / `sub-seekable` | none | rewind to timestamp; replay re-applies filter | pre-window timestamp clamps to earliest retained (not an error); replay bounded by `MaxSeekReplay` |
| 12 | `advanced/snapshots` | advanced | CreateSnapshot → Seek(snapshot) | `snapped` / `sub-snapped` | none | snapshot captures cursor; seek-to-snapshot replays | 7-day default expiry, swept hourly; snapshot of detached sub → `FAILED_PRECONDITION` |
| 13 | `advanced/schema-avro` | advanced | CreateSchema(Avro) + topic schema_settings; enforce-on-publish | `avro-topic` / `sub-avro` | schema | conforming publish ok; non-conforming → batch rejected `INVALID_ARGUMENT` | Avro via `hamba/avro/v2`; definition ≤ 300 KB; full revision history |
| 14 | `advanced/schema-protobuf` | advanced | CreateSchema(Protobuf) + enforce-on-publish | `proto-topic` / `sub-proto` | schema | conforming publish ok; non-conforming → batch rejected `INVALID_ARGUMENT` | Protobuf via `jhump/protoreflect`; definition ≤ 300 KB; revisions kept ≥ 1 |
| 15 | `interop/native-events-store` | interop | Pub/Sub Publish → native Events Store subscribe on `pubsub.{t}` | `orders` / Events Store `pubsub.orders` | none (interop) | same payload read natively with the three reserved tags present | native gRPC Events Store subscribe via the per-language KubeMQ SDK; subscribe-before-publish + `startAt = "new"` |

> 15 variants × 6 languages = up to **90 example programs** (full parity; no N/A capability
> cells). Any genuine per-language SDK gap is recorded in the spec's S10, not silently dropped.

### Directory naming

- **kebab-case** variant dirs for `go`, `javascript`, `java`, `csharp`
  (e.g. `topics/basic-publish-subscribe`, `delivery/ordering-keys`, `interop/native-events-store`).
- **snake_case** variant dirs for `python`, `ruby`
  (e.g. `topics/basic_publish_subscribe`, `delivery/ordering_keys`, `interop/native_events_store`).

The five concept groups (`topics/`, `subscriptions/`, `delivery/`, `advanced/`, `interop/`) are
the same kebab tokens in every language; only the leaf variant dir switches case.

---

## 3. Per-example README template (consistent across all up-to-90 = 15 × 6; spec S5.5)

H1: `{Language} — {Group}: {Variant}` + a one-line flow — then these **8 sections in order**
(headings verbatim; full skeleton in [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md)):

1. **Prerequisites** — runtime floor, GCP Pub/Sub SDK pin, a running connector, and the
   `PUBSUB_EMULATOR_HOST` + project-id env note. The `interop/native-events-store` variant ALSO
   notes the native KubeMQ gRPC broker at `localhost:50000` and the pinned KubeMQ SDK.
2. **How to Run** — the exact env exports + the run command (§4 run-command form for the language).
3. **Expected Output** — the **literal stdout** the program prints on a successful run (copy the
   real lines once built — not a paraphrase), with placeholders only for non-deterministic values
   (`<message-id>`, `<publish-time>`, `<ack-id>`) + the channel-isolation/concurrency note.
4. **What's Happening** — prose walkthrough; ENDS referencing the connector behaviour from §7
   (e.g. "a Publish writes once to the topic log `pubsub.{t}`, then fans out one queue copy per
   subscription `pubsub.sub.{s}`, applying each subscription's filter").
5. **Pub/Sub specifics** — this exact table (populated from the §2 master-table row):

   ```
   | RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
   ```

   `Feature flag` = ordering / filter / exactly-once / schema / dead-letter / push / none.
6. **Related Examples** — cross-links to the same variant in the other 5 languages + the matching
   doc under `../../../docs/`.
7. A **gotcha callout box** where one applies (see §5).
8. The **connectivity banner** (one line) linking
   `../../../docs/guides/connectivity-and-emulator-mode.md`.

### Expected-output & exit-code conventions

- "Expected Output" shows the literal stdout on a successful run against the dev connector.
  **Negative-path** variants MUST print the negative result explicitly — e.g.
  `non-matching attribute → suppressed (0 delivered)`,
  `schema-invalid msg → batch rejected INVALID_ARGUMENT`,
  `seek to pre-window timestamp → clamped to earliest retained`,
  `unary invalid-ack → FAILED_PRECONDITION (ErrorInfo PERMANENT_FAILURE_INVALID_ACK_ID)`.
  End with an explicit **PASS** line.
- **Exit code:** every example exits **0 on success** and **non-zero on any failed assertion or
  unexpected error**. Examples are runnable proofs, not demos — a missing delivery, an
  out-of-order ordered receive, or a wrong reserved tag must fail the process.
- **Channel isolation:** resource ids are uuid-suffixed so concurrent runs use distinct
  `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; `<message-id>` / `<publish-time>`
  vary per run.

---

## 4. Per-language root README template

Each `examples/<lang>/README.md` carries:

1. H1 `{Language} — KubeMQ Google Cloud Pub/Sub examples`.
2. **Prerequisites** — runtime floor + the pinned GCP Pub/Sub SDK package (§8).
3. **Setup** — install/restore command (`go build ./...`, `uv sync`, `npm install`,
   `mvn -q compile`, `dotnet build`, `bundle install`).
4. **Run a variant** — the exact run command form (table below) + the env exports.
5. **Idiom notes** — the per-language SDK traps (§6).
6. **Variant index** — the 15 variants as links into `topics/`, `subscriptions/`, `delivery/`,
   `advanced/`, `interop/`.

### Environment variable & per-language run commands (spec S5.6)

**Env var:** `PUBSUB_EMULATOR_HOST` (default `localhost:8085`) + `PUBSUB_PROJECT_ID` (any id). The
`interop/native-events-store` variant ALSO uses the native KubeMQ gRPC broker (`localhost:50000`).
The burn-in harness uses `KUBEMQ_BROKER_ADDRESS` (emulator host) + `grpc_address` (`localhost:50000`).

| Language | Dir (variant naming) | GCP Pub/Sub SDK (pin/floor) | Build / restore | Run a variant | Lint / Format |
|----------|----------------------|-----------------------------|-----------------|---------------|---------------|
| Go | `examples/go` (kebab) | `cloud.google.com/go/pubsub` | `go build ./...` | `go run ./topics/basic-publish-subscribe` | `gofumpt -w .` + `golangci-lint run ./... --fix` |
| Python | `examples/python` (snake) | `google-cloud-pubsub` (uv) | `uv sync` | `uv run python -m topics.basic_publish_subscribe` | `uv run ruff format .` + `uv run ruff check --fix .` |
| Java | `examples/java` (kebab) | `com.google.cloud:google-cloud-pubsub` (BOM) | `mvn -q compile` | `mvn -q exec:java -Dexec.mainClass=...` | `mvn compile` (compile-as-lint) |
| JS/TS | `examples/javascript` (kebab) | `@google-cloud/pubsub` | `npm install` | `npx tsx topics/basic-publish-subscribe/index.ts` | `npx tsc --noEmit` |
| C# | `examples/csharp` (kebab) | `Google.Cloud.PubSub.V1` (.NET 8) | `dotnet build` | `dotnet run --project topics/basic-publish-subscribe/<Project>.csproj` | `dotnet format` |
| Ruby | `examples/ruby` (snake) | `google-cloud-pubsub` | `bundle install` | `bundle exec ruby topics/basic_publish_subscribe/main.rb` | `bundle exec rubocop -a` (or `ruby -c`) |

> Exact SDK versions are pinned to the latest stable at implementation; bump-and-lock via
> `/check-deps` (no floating ranges in committed lockfiles). Commit `go.sum`, `uv.lock`,
> `package-lock.json`, and the C# central pins; gitignore Ruby `Gemfile.lock` (AWS convention).
> Ruby: run `eval "$(rbenv init - zsh)"` before bundle commands; rbenv 3.3.7.

---

## 5. The nine Pub/Sub gotchas (surface in docs + relevant example READMEs; spec S9)

| # | Gotcha | Where it shows up |
|---|--------|-------------------|
| 1 | **Exactly-once is node-local** — an `ack_id` minted on one node is invalid on another; pin a subscription's StreamingPull to one node (sticky LB) or accept at-least-once across nodes | `docs/concepts/exactly-once.md`, `docs/guides/reliability.md`, `delivery/exactly-once` READMEs |
| 2 | **Project id parsed but ignored** — resource ids are global across projects | `docs/getting-started.md`, every README's env note |
| 3 | **Credentials cleared / insecure path when `PUBSUB_EMULATOR_HOST` set** — SDK skips Google auth | `docs/guides/connectivity-and-emulator-mode.md`, every README |
| 4 | **Filtering is attributes-only CEL-subset** — `attributes:KEY`, `= / != "v"`, `hasPrefix(...)`, `AND/OR/NOT`; ≤ 256 chars; immutable; malformed → `INVALID_ARGUMENT` | `docs/guides/filtering.md`, `delivery/message-filtering` |
| 5 | **Retention clamped to broker `Store.MaxRetention`** — `GetTopic`/`GetSubscription` echo the requested value; fan-out/seek use the clamped value | `docs/reference/limits-and-rules.md`, `docs/concepts/seek-and-snapshots.md` |
| 6 | **`max_delivery_attempts` must be 5..100** (0 = unset) | `docs/concepts/dead-letter-and-retry.md`, `delivery/dead-letter-topic` |
| 7 | **Topic ids may not start with `sub.`** (reserved broker namespace) | `docs/reference/limits-and-rules.md`, `topics/topic-management` |
| 8 | **Seek of a pre-window timestamp clamps to earliest retained** (not an error) | `docs/concepts/seek-and-snapshots.md`, `advanced/seek-timestamp` |
| 9 | **gRPC only — no REST/JSON v1** (grpc-gateway not used) | `docs/reference/capabilities.md`, `docs/architecture.md` |

> Two further documented deviations are surfaced in reference docs but are NOT headline gotchas:
> a **unary invalid-ack** returns `FAILED_PRECONDITION` + `ErrorInfo(reason:
> PERMANENT_FAILURE_INVALID_ACK_ID)` (matching the real SDK contract, NOT a literal
> `INVALID_ARGUMENT`), and **export subscriptions / ingestion are rejected** while `kms_key_name`
> is accepted-and-ignored. Both live in `docs/reference/error-codes.md` /
> `docs/reference/migration-from-gcp.md`.

---

## 6. Per-language idiom traps (carry in each root README)

- **Go (`cloud.google.com/go/pubsub`):** `pubsub.NewClient(ctx, projectID)` auto-reads
  `PUBSUB_EMULATOR_HOST` and dials insecurely — no explicit emulator flag. Enable ordering with
  `topic.EnableMessageOrdering = true` BEFORE publishing keyed messages, and set the same on the
  subscription. For exactly-once, read `AckResult` from `Ack()`/`Nack()` to observe the
  confirmation. Interop half uses `github.com/kubemq-io/kubemq-go/v2@v2.0.3`.
- **Python (`google-cloud-pubsub`, via uv):** `pubsub_v1.PublisherClient()` /
  `SubscriberClient()` honour `PUBSUB_EMULATOR_HOST`. Ordering needs
  `publisher_options=PublisherOptions(enable_message_ordering=True)` AND a per-message
  `ordering_key`; the subscription must enable ordering at create. Exactly-once surfaces via the
  `AcknowledgeStatus` future. Manage with **uv** only. Interop half uses `kubemq==4.1.5`.
- **Java (`google-cloud-pubsub`, BOM `libraries-bom`):** when `PUBSUB_EMULATOR_HOST` is set, point
  the channel at the emulator host explicitly — build a plaintext `ManagedChannel` and pass
  `NoCredentialsProvider.create()` + a `FixedTransportChannelProvider` to `Publisher`/admin
  clients. `Publisher.setEnableMessageOrdering(true)` for ordering keys. Interop half uses
  `io.kubemq.sdk:kubemq-sdk-Java:3.1.1`.
- **Node/TS (`@google-cloud/pubsub`):** `new PubSub({ projectId })` auto-detects the emulator from
  the env var; run via `tsx`. Ordering needs `{ enableMessageOrdering: true }` on the publisher
  and `messageOrdering: true` on the subscription. Exactly-once is observed via the message
  `ackWithResponse()` promise. Interop half uses `kubemq-js@3.0.1`.
- **C# / .NET 8 (`Google.Cloud.PubSub.V1`):** the emulator is NOT auto-detected — set
  `EmulatorDetection = EmulatorDetection.EmulatorOnly` on the client builders
  (`PublisherServiceApiClientBuilder` / `SubscriberServiceApiClientBuilder`). Ordering needs
  `EnableMessageOrdering = true` and a per-message `OrderingKey`. Async `*Async` API throughout.
  Interop half uses `KubeMQ.SDK.csharp@3.0.1`.
- **Ruby (`google-cloud-pubsub`):** pass `emulator_host: ENV["PUBSUB_EMULATOR_HOST"]` explicitly to
  `Google::Cloud::PubSub.new` (it does not always pick it up from the env). Ordering needs
  `message_ordering: true` on the subscription and a per-message `ordering_key`. rbenv 3.3.7;
  `eval "$(rbenv init - zsh)"` first. Interop half uses gem `kubemq ~> 1.0` (v1.0.0,
  `subscribe_to_events_store`).

> **Interop start-position (all six languages).** The `interop/native-events-store` variant and
> the burn-in `cross_protocol_interop` worker open the native Events Store **subscribe** stream
> with the "new only" start policy and confirm the stream is open **before** publishing, so the
> published message is in-window (deterministic, no race). Start-position enums (all value `1`):
> Go `StartFromNewEvents()`, Python `EventStoreStartPosition.StartFromNew`, Ruby
> `EventStoreStartPosition::START_NEW_ONLY`; Java/Node/C# use the equivalent
> `StartNewOnly`/`StartFromNewEvents`.

---

## 7. Connector facts every example/doc relies on (verified, spec S3)

- **Listener:** dedicated gRPC port, default **8085** (the emulator convention); enabled by
  default (`Connectors.PubSub.Enable = true`). Interceptor chain: Recovery → Logger → Traffic-gate
  (no auth interceptor; a nil auth seam is reserved).
- **Services / RPCs (38):** `google.pubsub.v1.Publisher` (9), `google.pubsub.v1.Subscriber` (16),
  `google.pubsub.v1.SchemaService` (10), `google.iam.v1.IAMPolicy` (3 permissive stubs). Dual
  bidirectional: source external→KubeMQ via `Publish`; target KubeMQ→external via
  `Pull`/`StreamingPull`.
- **Config (11 `CONNECTORS_PUB_SUB_*` env vars):** `Enable` (`true`), `Port` (`8085`),
  `AdvertisedEndpoint` (`""`, cosmetic), `MaxMessageBytes` (`10485760` = 10 MiB),
  `DefaultAckDeadlineSeconds` (`10`, must be 10..600), `MaxOutstandingMessages` (`1000`),
  `MaxInflightPerSubscription` (`20000`), `MaxConcurrentPolls` (`1024`), `StreamCloseSeconds`
  (`1800`), `MaxSeekReplay` (`1000000`), `EnableReflection` (`false`). `Validate()` returns nil if
  `!Enable`; otherwise the port must be valid + distinct from gRPC/REST/HTTP/AWS connector ports,
  the ack deadline 10..600, and the six numeric knobs > 0.
- **Channel mapping:** topic `t` → Events Store log `pubsub.{t}`; subscription `s` → Queue
  `pubsub.sub.{s}`; project segment parsed-but-ignored; snapshots/schemas are BoltDB registry
  records.
- **Message mapping / reserved tags:** body = `data`; Tags = `attributes` + the three reserved
  tags `_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key` (visible to native
  consumers, stripped from `attributes` for Pub/Sub clients).
- **Ack / nack:** every delivered message gets an opaque `ack_id` =
  `base64(JSON{subscription, channel, node id, broker txn id, seq, receive count, lease id,
  deadline})` under an ack-deadline lease (subscription-owned, so cross-stream ack works).
  `Acknowledge` acks the broker seq; `ModifyAckDeadline(0)` = immediate nack/redeliver; `>0`
  extends (10..600 s). A 250 ms sweeper expires overdue leases, applies retry backoff, and
  redelivers or dead-letters.
- **Ordering keys:** with `enable_message_ordering`, messages sharing an `ordering_key` are
  delivered in publish order with **at most one in flight per key**; redelivery is in order; a
  round-robin cursor spreads delivery across contended keys; keyless messages are unordered.
- **Dead-letter:** connector-level fan-out — when receive count exceeds `max_delivery_attempts`
  (5..100; 0 = unset) and the sub has a `dead_letter_topic`, the sweeper republishes to that topic
  and acks the original.
- **Exactly-once:** `enable_exactly_once_delivery` — StreamingPull returns
  `AcknowledgeConfirmation`/`ModifyAckDeadlineConfirmation` (expired → `invalid_ack_ids`,
  transient → `temporary_failed_ack_ids`); unary returns `FAILED_PRECONDITION` +
  `ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID)`. **Node-local**: an `ack_id` minted on one
  node is invalid on another — pin a subscription's StreamingPull to one node in a cluster.
- **Seek / snapshots:** `Seek` rewinds to a timestamp or snapshot — resolve start seq from the
  topic log (a pre-window timestamp clamps to the earliest retained message, NOT an error), purge
  the sub queue + drop leases, replay the topic log via `Array.RangeEventsStore` re-applying the
  filter, bounded by `MaxSeekReplay` (hits cap → WARN, no silent loss). Snapshots capture a cursor
  (7-day default expiry, swept hourly); a snapshot of a detached subscription →
  `FAILED_PRECONDITION`.
- **Schema:** topics may reference an Avro (`hamba/avro/v2`) or Protobuf (`jhump/protoreflect`)
  schema; when set, publish is enforced and the whole batch is rejected (`INVALID_ARGUMENT`) on
  the first non-conforming message; definitions ≤ 300 KB; full revision history
  (`CommitSchema`/`RollbackSchema`/`DeleteSchemaRevision`, keeping ≥ 1).
- **Filtering:** `CreateSubscription` may set a CEL-subset `filter` (attributes-only, ≤ 256 chars,
  immutable, applied at publish fan-out): `attributes:KEY`, `attributes.KEY = "v"` / `!= "v"`,
  `hasPrefix(attributes.KEY, "p")`, `AND OR NOT` + parentheses. Malformed → `INVALID_ARGUMENT`.
- **Push delivery:** a sub with `push_config` is served by a per-sub worker that POSTs a wrapped
  JSON envelope (`{message:{data(base64),attributes,messageId,publishTime,orderingKey},
  subscription}`) or raw body (`no_wrapper`); 2xx → ack, non-2xx/timeout → retry w/ backoff,
  exhaustion → DLQ if set else drop; optional OIDC Bearer; HTTPS required (HTTP only for localhost).
- **Publish atomicity:** validate the whole batch before enqueuing anything — batch 1..1000; per
  message total ≤ 10 MiB, ≤ 100 attributes, attr key ≤ 256 B (no `goog`), attr value ≤ 1024 B,
  ordering key ≤ 1024 B, `data` OR `attributes` non-empty; any failure rejects the entire batch.
- **Limits (Google-exact, not configurable):** resource id 3..255 chars, start with a letter,
  charset `[A-Za-z0-9._~%+-]`, no `goog` prefix, topic ids may not start with `sub.`; message
  ≤ 10 MiB; batch ≤ 1000; ≤ 100 attributes; ack deadline 0 or 10..600 s; retention 10 min..31 days
  clamped to broker `Store.MaxRetention`; `max_delivery_attempts` 5..100; schema ≤ 300 KB.
- **Emulator model:** no authentication, no TLS, insecure gRPC. Clients set `PUBSUB_EMULATOR_HOST`
  + a project-id env var; the SDK skips Google auth and uses insecure gRPC. DoS guards remain
  active. Do not expose port 8085 to untrusted networks. **gRPC only** — no REST/JSON v1.

---

## 8. Skeleton manifest summary (created by the per-language scaffold agents)

| Artifact | Path | GCP Pub/Sub SDK pin/floor |
|----------|------|---------------------------|
| Go examples module | `examples/go/go.mod` (+ `go.sum`) | `cloud.google.com/go/pubsub` (interop: `github.com/kubemq-io/kubemq-go/v2@v2.0.3`) |
| Python project | `examples/python/pyproject.toml` (+ `uv.lock`) | `google-cloud-pubsub` (interop: `kubemq==4.1.5`) |
| JS package | `examples/javascript/package.json` (+ `package-lock.json`, `tsconfig.json`) | `@google-cloud/pubsub` (interop: `kubemq-js@3.0.1`) |
| Java POM | `examples/java/pom.xml` | `com.google.cloud:google-cloud-pubsub` via `libraries-bom` (interop: `io.kubemq.sdk:kubemq-sdk-Java:3.1.1`) |
| C# props/sln | `examples/csharp/Directory.Packages.props`, `Directory.Build.props`, `*.sln` | `Google.Cloud.PubSub.V1`, `net8.0` (interop: `KubeMQ.SDK.csharp@3.0.1`) |
| Ruby Gemfile | `examples/ruby/Gemfile` | `google-cloud-pubsub` (interop: gem `kubemq ~> 1.0`) |
| Burn-in module | `burnin/go.mod` | `cloud.google.com/go/pubsub` (+ `github.com/kubemq-io/kubemq-go/v2@v2.0.3` for the interop worker); see `burnin/DEPENDENCIES.md` |

> **Lockfile commit policy:** commit `go.sum`, `uv.lock`, `package-lock.json`, and the C#
> central-package pins; **gitignore Ruby `Gemfile.lock`** (AWS convention). Pin the latest stable
> first-party Google client per language at implementation (bump-and-lock via `/check-deps`; no
> floating ranges in committed lockfiles). Record the chosen versions here and in the per-language
> root READMEs. The concept-group dirs (`topics/`, `subscriptions/`, `delivery/`, `advanced/`,
> `interop/`) exist as scaffolds; the per-language scaffold agents add the manifests and the
> per-variant leaf dirs.
