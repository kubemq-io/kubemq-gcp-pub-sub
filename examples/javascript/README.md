# KubeMQ Google Cloud Pub/Sub — JavaScript/TypeScript Examples

Runnable examples that drive the KubeMQ embedded Google Cloud Pub/Sub wire-protocol connector using the **official Google Cloud Pub/Sub client for Node.js** (`@google-cloud/pubsub`) — no KubeMQ SDK on the Pub/Sub side. The connector is **emulator-protocol**: you point an unmodified Pub/Sub application at KubeMQ by setting the standard `PUBSUB_EMULATOR_HOST` environment variable, exactly as you would against Google's local emulator. **Zero code changes** — the SDK skips Google auth and uses insecure gRPC when that variable is set. All examples are written in TypeScript and run with `tsx`.

The single exception is `interop/native-events-store`, whose native-KubeMQ half uses [`kubemq-js`](https://www.npmjs.com/package/kubemq-js) to read the topic's Events Store log (`pubsub.{topic}`) over native gRPC, demonstrating cross-protocol interop on the same channel.

## Prerequisites

- **Node.js 18+** (developed against Node 20+; works on current LTS).
- **npm**.
- A running **KubeMQ server with the Pub/Sub connector enabled** (`CONNECTORS_GCP_ENABLE=true`), listening on gRPC port **8085** (emulator convention).
- For `interop/native-events-store` only: the native KubeMQ **gRPC broker** on `localhost:50000`.

Pinned dependencies (see `package.json`; lockfile committed):

| Package | Version | Purpose |
|---------|---------|---------|
| `@google-cloud/pubsub` | `5.3.1` | Official Google Cloud Pub/Sub client (v1) |
| `kubemq-js` | `3.0.1` | native KubeMQ gRPC peer — `interop/native-events-store` only |
| `typescript`, `tsx`, `@types/node` | dev | TS compile + run |

## Setup

```bash
cd examples/javascript
npm install
```

> Lockfile policy: `package-lock.json` is **committed** in this repo (the pinned versions in `package.json` plus the lockfile are the reproducible source of truth). Run `npm ci` for an exact, lock-faithful install.

## Connection

Every example reads the standard Google emulator environment variables and the official client honors them automatically — there is no endpoint override in code:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085   # connector default gRPC port
export PUBSUB_PROJECT_ID=my-project          # any id; the project segment is parsed but ignored
```

When `PUBSUB_EMULATOR_HOST` is set, `@google-cloud/pubsub` clears credentials and connects over insecure gRPC — no Google authentication, no TLS. This is the connector's emulator-protocol drop-in. A `Publish` writes once to the topic log `pubsub.{topic}` (the authoritative, replayable, cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{subscription}`, applying each subscription's filter.

## Run a Variant

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
npx tsx topics/basic-publish-subscribe/index.ts
```

Each example prints human-readable progress, asserts its invariants, and **exits non-zero on any failed assertion or unexpected error** (they are runnable proofs, not demos).

Type-check (lint equivalent for this repo) without running:

```bash
npm run typecheck   # tsc --noEmit
```

## Idiom Notes (`@google-cloud/pubsub` / Node TS traps)

- **Emulator detection is automatic:** set `PUBSUB_EMULATOR_HOST` *before* `new PubSub(...)`. The library reads it at construction, clears credentials, and switches to insecure gRPC. No `apiEndpoint` / `servicePath` override is needed.
- **`PUBSUB_PROJECT_ID`** (or `GOOGLE_CLOUD_PROJECT`) must still be set so the client can form resource paths; the connector parses but **ignores** the project segment (single-tenant).
- **Ordering keys** require `enableMessageOrdering: true` on the publisher options *and* a non-empty `orderingKey` per message; a failed publish on an ordered key wedges the key until `topic.resumePublish(key)`.
- **Exactly-once delivery** changes ack semantics: `message.ackWithResponse()` / `nackWithResponse()` return promises you must await and reconcile (the connector replies with confirmation / `ErrorInfo`), and the boundary is **node-local** — pin a subscription's StreamingPull to one node in a cluster.
- **Self-contained examples:** there is no shared helper module — each example is fully self-contained, inlining its own helpers (e.g. the emulator-aware `v1.SubscriberClient` factory) per file so it can be copy-pasted and run on its own. The examples therefore have no intra-repo relative imports; they import only from `@google-cloud/pubsub`, `google-gax`, `kubemq-js`, and the Node standard library. (This package is `"type": "module"` with `moduleResolution: NodeNext`, so *if* a relative import is ever introduced it must carry the `.js` extension even though the source is `.ts`.)
- **`interop/native-events-store`** uses `kubemq-js` `KubeMQClient.create({ address })` and an Events Store subscribe with the `StartFromNew` start position (`EventStoreStartPosition.StartFromNew`) — established **before** the Pub/Sub publish so the native consumer sees the message (no startup race).

## Variant Index

The per-variant program sources live under the concept-group directories below and are added by the language example authors. Each variant directory carries its own `README.md` (8-section per-example template) once implemented.

| Group | Directory | Variants (kebab-case) |
|-------|-----------|-----------------------|
| topics | [`topics/`](topics/) | `basic-publish-subscribe`, `batch-publish`, `topic-management` |
| subscriptions | [`subscriptions/`](subscriptions/) | `streaming-pull`, `ack-deadline-nack` |
| delivery | [`delivery/`](delivery/) | `ordering-keys`, `message-filtering`, `dead-letter-topic`, `push-delivery`, `exactly-once` |
| advanced | [`advanced/`](advanced/) | `seek-timestamp`, `snapshots`, `schema-avro`, `schema-protobuf` |
| interop | [`interop/`](interop/) | `native-events-store` (the only variant that imports `kubemq-js`) |

## Related Resources

- [`../../docs/`](../../docs/) — full connector contract, channel mapping, configuration, error codes.
- [`../../docs/guides/connectivity-and-emulator-mode.md`](../../docs/guides/connectivity-and-emulator-mode.md) — env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
- [`../`](../) — examples in the other five languages (Go, Python, Java, C#, Ruby).
- [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md) — the cross-language conventions this repo follows.
