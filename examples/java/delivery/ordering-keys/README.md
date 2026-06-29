# Java — delivery: Ordering Keys

An ordering-enabled subscription delivers messages that share an `ordering_key` in
publish order, at most one in flight per key:
`CreateTopic → CreateSubscription(enable_message_ordering) → Publish(orderingKey) → Pull → Acknowledge`.

## Prerequisites

- **JDK 21+** (the examples target `maven.compiler.release=21`) and Maven 3.9+, and the official
  GCP Pub/Sub client **`com.google.cloud:google-cloud-pubsub`**, version-managed via
  **`com.google.cloud:libraries-bom` 26.84.0** (pinned in `examples/java/pom.xml`; the Pub/Sub
  dependency itself carries no `<version>`).
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
cd examples/java
mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.delivery.orderingkeys.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/ordering-keys | emulator=localhost:8085 project=my-project
CreateTopic        -> orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription -> sub-orders-<uuid8> (channel pubsub.sub.sub-orders-<uuid8>, enable_message_ordering=true)
Publish            -> 'm1' orderingKey='cust-7'
Publish            -> 'm2' orderingKey='cust-7'
Publish            -> 'm3' orderingKey='cust-7'
Publish            -> 'keyless' (no ordering key)
Pull               -> 'm1' orderingKey='cust-7' -> ack
Pull               -> 'm2' orderingKey='cust-7' -> ack
Pull               -> 'm3' orderingKey='cust-7' -> ack
Pull               -> 'keyless' (keyless) -> ack

PASS: ordered key 'cust-7' delivered in publish order: m1 -> m2 -> m3
DeleteSubscription -> ok
DeleteTopic -> ok
```

Exit code is `0` on success and non-zero on any failed assertion (e.g. an out-of-order delivery).

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`), so concurrent runs use
> distinct `pubsub.orders-<uuid8>` / `pubsub.sub.sub-orders-<uuid8>` channels and do not collide.
> The `<uuid8>` suffix varies per run. (See `../../../docs/guides/connectivity-and-emulator-mode.md`
> and the isolation note in `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates a topic and a subscription with `enable_message_ordering = true`. It publishes
three messages — `m1`, `m2`, `m3` — sequentially on the ordering key `cust-7` (sequential low-level
`Publish` calls fix the publish order for the key), plus one keyless message. It then pulls one
message at a time and acks it before pulling the next. Because the connector keeps **at most one
message in flight per ordering key** and delivers a key's messages in publish order, the keyed
messages arrive as `m1 → m2 → m3`; the assertion fails (non-zero exit) on any other order. The
keyless message is unordered and is delivered alongside the keyed sequence.

A `Publish` writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy to the subscription `pubsub.sub.sub-orders-<uuid8>`.
The ordering key rides across the wire as the reserved tag `_pubsub_ordering_key`; the connector's
round-robin cursor keeps distinct keys independent and in-order per key.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic → CreateSubscription → Publish (×4) → Pull (×N, MaxMessages=1) → Acknowledge | topic `orders-<uuid8>` (`pubsub.orders-<uuid8>`) → sub `sub-orders-<uuid8>` (`pubsub.sub.sub-orders-<uuid8>`) | ordering (`enable_message_ordering`) | ordering key ≤ 1024 B; ack deadline 10 s (10..600); at most one message in flight per key; redelivery stays in order |

## Related Examples

- Same variant in the other 5 languages:
  [go](../../../go/delivery/ordering-keys/),
  [python](../../../python/delivery/ordering_keys/),
  [javascript](../../../javascript/delivery/ordering-keys/),
  [csharp](../../../csharp/delivery/ordering-keys/),
  [ruby](../../../ruby/delivery/ordering_keys/)
- Docs: [`../../../docs/concepts/ordering-keys.md`](../../../docs/concepts/ordering-keys.md)

> **Gotcha — ordering needs publisher-side enablement too.** In-order delivery only holds if the
> publisher published in order for that key. With the high-level `Publisher` you must call
> `Publisher.newBuilder(topic).setEnableMessageOrdering(true)` (which serializes per-key publishes);
> this example uses the low-level `PublisherStub` and publishes sequentially, so the publish order on
> the key is the order of the calls. A single hot key is throttled by its round-trip ack latency
> (one in flight per key) — spread load across many keys.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
