# Python — delivery: Ordering Keys

Per-key in-order delivery: `CreateTopic → CreateSubscription(enable_message_ordering) → Publish (publisher ordering enabled) → StreamingPull → Ack`, asserting each ordering key's messages arrive in publish order while keyless messages stay unordered.

## Prerequisites

- Python 3.10+ and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `examples/python/pyproject.toml` + `uv.lock`).
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
cd examples/python && uv run python -m delivery.ordering_keys
# or, equivalently: uv run python delivery/ordering_keys/main.py
```

## Expected Output

```
=== Pub/Sub ordering keys — topic 'orders-<uuid8>' / sub 'sub-orders-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)

CreateTopic            -> projects/my-project/topics/orders-<uuid8>
CreateSubscription     -> projects/my-project/subscriptions/sub-orders-<uuid8>  (enable_message_ordering=True)

Publish (publisher ordering enabled; interleaved across two keys):
  publish body='A1'       ordering_key=cust-7       -> id=<message-id>
  publish body='B1'       ordering_key=cust-9       -> id=<message-id>
  publish body='A2'       ordering_key=cust-7       -> id=<message-id>
  publish body='keyless'  ordering_key=(keyless)    -> id=<message-id>
  publish body='B2'       ordering_key=cust-9       -> id=<message-id>
  publish body='A3'       ordering_key=cust-7       -> id=<message-id>
  publish body='B3'       ordering_key=cust-9       -> id=<message-id>

StreamingPull (one in flight per key; ack drives the next per-key message):
  received body='keyless'  ordering_key=(keyless)
  received body='B1'       ordering_key=cust-9
  received body='A1'       ordering_key=cust-7
  received body='A2'       ordering_key=cust-7
  received body='B2'       ordering_key=cust-9
  received body='B3'       ordering_key=cust-9
  received body='A3'       ordering_key=cust-7

Per-key delivery order:
  cust-7: ['A1', 'A2', 'A3']
  cust-9: ['B1', 'B2', 'B3']
  keyless: ['keyless']
  [OK] key 'cust-7' delivered in publish order (A1,A2,A3)
  [OK] key 'cust-9' delivered in publish order (B1,B2,B3)
  [OK] every published message was delivered exactly once

PASS — ordering keys: per-key in-order delivery, one in flight per key.
cleanup: DeleteSubscription ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid8-suffixed (`orders-<uuid8>` / `sub-orders-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the `<message-id>` values and
> the cross-key interleave order vary per run. The per-key order (A1,A2,A3 / B1,B2,B3) is the
> invariant being asserted. (See `connection-and-isolation`.)

## What's Happening

- The **publisher** is built with `PublisherOptions(enable_message_ordering=True)` — this serializes
  publishes per key client-side, which is required for in-order delivery; setting ordering only on
  the subscription is not enough.
- The **subscription** is created with `enable_message_ordering=True`.
- Three messages are published on each of two keys (`cust-7`, `cust-9`) interleaved, plus one keyless
  message.
- `StreamingPull` delivers, and acking each message releases the next message for that key. The two
  keys interleave freely (and the keyless message is unordered), but within each key the order is
  strictly preserved.
- Connector behavior (spec S3.5): a `Publish` writes once to the topic log `pubsub.{t}` (the
  authoritative, replayable, cross-protocol source), then fans out one queue copy per subscription
  `pubsub.sub.{s}`. With `enable_message_ordering` the connector delivers each `ordering_key` in
  publish order with **at most one message in flight per key**; the key is carried as the reserved
  tag `_pubsub_ordering_key`. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish (ordering), StreamingPull, Acknowledge | topic `orders-<uuid8>` / sub `sub-orders-<uuid8>` | ordering | ordering key ≤ 1024 B; one in flight per key; keyless messages unordered |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/ordering-keys`](../../../go/delivery/ordering-keys),
  [`../../../javascript/delivery/ordering-keys`](../../../javascript/delivery/ordering-keys),
  [`../../../java/delivery/ordering-keys`](../../../java/delivery/ordering-keys),
  [`../../../csharp/delivery/ordering-keys`](../../../csharp/delivery/ordering-keys),
  [`../../../ruby/delivery/ordering_keys`](../../../ruby/delivery/ordering_keys)
- Docs: [`../../../../docs/concepts/ordering-keys.md`](../../../../docs/concepts/ordering-keys.md)

> **Gotcha — one in flight per key throttles a hot key.** A single ordering key is effectively
> serialized: its throughput is bounded by round-trip ack latency. Spread load across many keys
> (per-customer / per-entity) rather than funneling everything through one key.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
