# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repository ships **documentation, runnable examples, and a burn-in soak harness** for the
`kubemq-gcp-pub-sub` connector — there is **no published package**. Version numbers track the
state of the docs and examples in this repo, not a client library release.

## [Unreleased]

### Added

- Initial release of the `kubemq-gcp-pub-sub` direct-connect documentation and examples
  repository.
- Connector overview: bridging the **Google Cloud Pub/Sub** v1 gRPC wire protocol onto KubeMQ's
  native messaging via the embedded connector (a dedicated gRPC listener on port **8085**, the
  emulator convention; 38 RPCs across `Publisher`/`Subscriber`/`SchemaService`/`IAMPolicy`),
  driven by the standard, unmodified first-party Google Cloud Pub/Sub client libraries with **only
  the `PUBSUB_EMULATOR_HOST` environment variable** — zero code changes.
- Service model: topics map onto KubeMQ **Events Store** logs (`pubsub.{t}`, the authoritative
  cross-protocol source); subscriptions map onto KubeMQ **Queues** (`pubsub.sub.{s}`, one queue
  copy fanned out per subscription); the project segment is parsed-but-ignored; three reserved tags
  (`_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key`) cross the wire.
- `docs/`: architecture, getting-started, configuration, per-concept docs (topics & publish,
  subscriptions & pull, ordering keys, exactly-once, dead-letter & retry, schema validation,
  seek & snapshots, cross-protocol interop), task guides (connectivity & emulator mode, publishing,
  subscribing, filtering, push delivery, reliability), and reference (capabilities, channel
  mapping, limits & rules, error codes, migration-from-gcp) — every behavioral claim traced to
  the connector reference or a named test.
- `examples/`: per-pattern runnable examples across 6 languages (Go, Python, Java, JS/TS, C#,
  Ruby) — 15 variants × 6 languages = up to **90 examples** at full parity — using the standard
  first-party Google Cloud Pub/Sub client libraries only (no KubeMQ proto bindings). Variants cover
  topics (basic-publish-subscribe, batch-publish, topic-management), subscriptions (streaming-pull,
  ack-deadline-nack), delivery (ordering-keys, message-filtering, dead-letter-topic, push-delivery,
  exactly-once), advanced (seek-timestamp, snapshots, schema-avro, schema-protobuf), and interop
  (native-events-store).
- `burnin/`: standalone Go soak-test harness exercising the connector under sustained
  multi-pattern load (one worker per Pub/Sub pattern; `pendingLost` loss reconciliation), on fixed
  port **8899**.
- `SHARED-CONVENTIONS.md` (and its `examples/` copy): the connection model, the 15-variant master
  table, the per-example & per-language README templates, the nine Pub/Sub gotchas, the
  per-language idiom traps, and the verified connector-fact summary.
- `docs/troubleshooting.md`: a symptom → cause → fix diagnostics guide covering emulator
  not-detected (per language), connection failures / periodic `UNAVAILABLE`, the exactly-once /
  sticky-LB `FAILED_PRECONDITION` ack errors, schema & filter `INVALID_ARGUMENT` rejection,
  dead-letter redrive timing ("nothing in the DLQ yet"), project-id confusion, the gRPC-only / no-REST
  reminders, rejected resource ids, plus a diagnostics section (gRPC reflection + `grpcurl`,
  per-language SDK debug logging, what to capture when reporting a problem).
- `docs/faq.md`: answers to the common "what is this / why does it behave this way?" questions — the
  `PUBSUB_EMULATOR_HOST` drop-in vs. the real Google service, zero code changes, supported languages,
  no emulator/docker-compose step, REST/HTTP and auth/TLS posture, the ignored project id,
  node-local exactly-once, dead-letter timing, `max_delivery_attempts` 5..100, Ruby v3.x / C#
  `EmulatorDetection` idioms, `Seek` clamp behaviour, and rejected export/ingestion features.
- `docs/reference/error-codes.md`: a **Recovery** column on the error table mapping each status to
  its remediation.
- `docs/configuration.md` and `docs/reference/capabilities.md`: enriched with additional verified
  connector detail.
- CI: a GitHub Actions workflow (`.github/workflows/examples.yml`) that builds / type-checks the
  example programs across the six languages.

### Changed

- Aligned the toolchain version floors stated across the READMEs with the actual manifests:
  **Go 1.25+** (`go.mod`), **JDK 21+** / Maven 3.9+ (`pom.xml` `maven.compiler.release=21`), and
  **Ruby 3.1+** (verified on 3.3.7 via rbenv) in `examples/README.md`, so the index and per-language
  READMEs agree.
- Added outbound links to the official Google Cloud Pub/Sub client SDK reference docs (per language)
  in the root `README.md` SDK table, and links to the new troubleshooting guide and FAQ from the
  root `README.md` and the `docs/` index.

[Unreleased]: https://github.com/kubemq-io/kubemq-gcp-pub-sub/commits/main
