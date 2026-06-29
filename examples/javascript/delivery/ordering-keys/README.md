# JavaScript — delivery: Ordering keys

Per-key in-order delivery against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription(enableMessageOrdering) → Publish(orderingKey) → Pull → Acknowledge` — each key's messages arrive in publish order, at most one in flight per key.

## Prerequisites

- **Node.js 18+** (developed against Node 20+) and the official GCP Pub/Sub client
  `@google-cloud/pubsub` **5.3.1** (pinned in `examples/javascript/package.json`); run with `tsx`.
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/javascript && npx tsx delivery/ordering-keys/index.ts
```

## Expected Output

```
=== Pub/Sub ordering keys — topic 'ordered-<uuid8>' / sub 'sub-ordered-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)

CreateTopic            -> projects/my-project/topics/ordered-<uuid8>
CreateSubscription     -> sub-ordered-<uuid8>  (enableMessageOrdering=true)

Publish (publisher ordering enabled; interleaved across two keys):
  publish body='A1'       ordering_key=cust-7       -> id=<message-id>
  publish body='B1'       ordering_key=cust-9       -> id=<message-id>
  publish body='A2'       ordering_key=cust-7       -> id=<message-id>
  publish body='keyless'  ordering_key=(keyless)    -> id=<message-id>
  publish body='B2'       ordering_key=cust-9       -> id=<message-id>
  publish body='A3'       ordering_key=cust-7       -> id=<message-id>
  publish body='B3'       ordering_key=cust-9       -> id=<message-id>

Pull (one in flight per key; ack drives the next per-key message):
  received body='A1'       ordering_key=cust-7
  received body='B1'       ordering_key=cust-9
  received body='A2'       ordering_key=cust-7
  received body='keyless'  ordering_key=(keyless)
  received body='B2'       ordering_key=cust-9
  received body='A3'       ordering_key=cust-7
  received body='B3'       ordering_key=cust-9

Per-key delivery order:
  cust-7: [A1, A2, A3]
  cust-9: [B1, B2, B3]
  keyless: [keyless]
  [OK] key 'cust-7' delivered in publish order (A1,A2,A3)
  [OK] key 'cust-9' delivered in publish order (B1,B2,B3)
  [OK] every published message was delivered exactly once

PASS — ordering keys: per-key in-order delivery, one in flight per key.
cleanup: DeleteSubscription ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.ordered-<uuid8>` / `pubsub.sub.sub-ordered-<uuid8>` channels and do not collide; the
> `<uuid8>` suffix and each `<message-id>` vary per run. The **interleave** of `cust-7` vs `cust-9`
> deliveries may differ between runs (a round-robin cursor spreads delivery across contended keys),
> but each key's relative order is always `A1,A2,A3` / `B1,B2,B3`. Exits `0` on success, non-zero on
> any failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a uuid-suffixed topic and a subscription with `enableMessageOrdering: true`, and
sets `messageOrdering: true` on the publisher as well — both halves are required (the publisher
serializes publishes per key, the subscription enables ordered delivery). It then publishes seven
messages interleaved across two ordering keys (`cust-7`, `cust-9`) plus one keyless message.

Delivery is driven with **unary Pull + immediate Acknowledge, one message at a time**: by acking the
head of a key before pulling again, the program makes the connector's *one-in-flight-per-key*
guarantee directly observable — the next message for that key is only released once the previous one
is acked. (The high-level streaming subscriber buffers and acks asynchronously, which obscures the
strict per-key sequencing, so this example uses unary pull instead.) The program asserts each key was
delivered in publish order and that every message was delivered exactly once.

A Publish writes once to the topic log `pubsub.{t}` (the authoritative, replayable, cross-protocol
source), then fans out one queue copy per subscription `pubsub.sub.{s}`. With `enable_message_ordering`
the connector delivers messages sharing an `ordering_key` in publish order, at most one in flight per
key — the head of a key blocks until acked or redelivered (`ordering.go`); the key rides across the
wire as the reserved tag `_pubsub_ordering_key`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish` (with `orderingKey`), `Pull`, `Acknowledge` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | ordering | ordering key ≤ 1024 B; at-most-one-in-flight per key; redelivery is in order; round-robin cursor spreads delivery across contended keys; keyless messages unordered |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/ordering-keys`](../../../go/delivery/ordering-keys),
  [`../../../python/delivery/ordering_keys`](../../../python/delivery/ordering_keys),
  [`../../../java/delivery/ordering-keys`](../../../java/delivery/ordering-keys),
  [`../../../csharp/delivery/ordering-keys`](../../../csharp/delivery/ordering-keys),
  [`../../../ruby/delivery/ordering_keys`](../../../ruby/delivery/ordering_keys)
- Docs: [`../../../../docs/concepts/ordering-keys.md`](../../../../docs/concepts/ordering-keys.md)

> The project id is parsed and validated but **ignored** (single-tenant, like the emulator):
> resource ids are global across "projects", which is exactly why this example uuid-suffixes the
> topic/subscription names so parallel runs occupy private `pubsub.{t}` / `pubsub.sub.{s}` channels.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
