# Go — delivery: Ordering Keys

Per-key in-order delivery against the KubeMQ Pub/Sub connector:
`CreateTopic(EnableMessageOrdering) → CreateSubscription(EnableMessageOrdering) → Publish(orderingKey) → Pull → Acknowledge`.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
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
cd examples/go && go run ./delivery/ordering-keys
```

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/ordering-keys | emulator=localhost:8085 project=my-project
CreateTopic: ordered-<uuid8> (channel pubsub.ordered-<uuid8>, EnableMessageOrdering=true)
CreateSubscription: sub-ordered-<uuid8> (channel pubsub.sub.sub-ordered-<uuid8>, EnableMessageOrdering=true)
Publish (publisher ordering enabled; interleaved across two keys):
  publish body="A1"      ordering_key=cust-7     -> id=<message-id>
  publish body="B1"      ordering_key=cust-9     -> id=<message-id>
  publish body="A2"      ordering_key=cust-7     -> id=<message-id>
  publish body="keyless" ordering_key=(keyless)  -> id=<message-id>
  publish body="B2"      ordering_key=cust-9     -> id=<message-id>
  publish body="A3"      ordering_key=cust-7     -> id=<message-id>
  publish body="B3"      ordering_key=cust-9     -> id=<message-id>
Pull (one in flight per key; ack drives the next per-key message):
  received body="A1"      ordering_key=cust-7
  received body="B1"      ordering_key=cust-9
  received body="A2"      ordering_key=cust-7
  received body="keyless" ordering_key=(keyless)
  received body="B2"      ordering_key=cust-9
  received body="A3"      ordering_key=cust-7
  received body="B3"      ordering_key=cust-9
Per-key delivery order:
  cust-7: [A1 A2 A3]
  cust-9: [B1 B2 B3]
  keyless: [keyless]
PASS: per-key in-order delivery, one in flight per key (A1->A2->A3, B1->B2->B3)
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`ordered-<uuid8>` / `sub-ordered-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; `<message-id>` values and the
> interleaving of distinct keys vary per run (only the per-key order is fixed). (See
> `../../../SHARED-CONVENTIONS.md` §1.) Exits 0 on success, non-zero on any out-of-order delivery.

## What's Happening

The program creates a topic and enables `EnableMessageOrdering` on the **publisher handle** (the
publisher serializes publishes per key), and creates a subscription with `EnableMessageOrdering` so
the connector enforces per-key ordering on delivery — both sides must opt in. It publishes three
messages each on two ordering keys (`cust-7`, `cust-9`), interleaved, plus one keyless message,
awaiting each publish in turn to fix the publish order per key. It then pulls one message at a time
with the low-level `apiv1.SubscriberClient` and acks each before the next pull; because the connector
keeps **at most one message in flight per ordering key** and delivers a key's messages in publish
order, acking the head of a key releases the next. It asserts `cust-7` arrives as `A1→A2→A3` and
`cust-9` as `B1→B2→B3`, and that every message is delivered exactly once. (The bare `apiv1` client
does not auto-detect the emulator, so the program wires the insecure/no-auth options explicitly;
`pubsub.NewClient` auto-detects them.) Any out-of-order delivery exits non-zero.

A `Publish` writes once to the topic log `pubsub.ordered-<uuid8>` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy to the per-subscription queue
`pubsub.sub.sub-ordered-<uuid8>`. The ordering key rides across the wire as the reserved tag
`_pubsub_ordering_key` (stripped from `attributes` and surfaced as `Message.OrderingKey`); the
connector's round-robin cursor keeps distinct keys independent and in-order per key (spec S3.5).

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription, Publish (×7), Pull (×N, MaxMessages=1), Acknowledge, DeleteSubscription, DeleteTopic | `ordered-<uuid8>` / `sub-ordered-<uuid8>` | ordering (`EnableMessageOrdering`) | At most one message in flight per ordering key; redelivery stays in order; keyless messages are unordered. Ack deadline 10s (valid 0 or 10..600s). |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/delivery/ordering_keys`](../../../python/delivery/ordering_keys),
  [`../../../javascript/delivery/ordering-keys`](../../../javascript/delivery/ordering-keys),
  [`../../../java/delivery/ordering-keys`](../../../java/delivery/ordering-keys),
  [`../../../csharp/delivery/ordering-keys`](../../../csharp/delivery/ordering-keys),
  [`../../../ruby/delivery/ordering_keys`](../../../ruby/delivery/ordering_keys)
- Docs: [`../../../docs/concepts/ordering-keys.md`](../../../docs/concepts/ordering-keys.md)
- Related: [`../message-filtering`](../message-filtering), [`../exactly-once`](../exactly-once).

> **Gotcha — ordering needs publisher-side enablement too.** In-order delivery only holds if the
> publisher published in order for that key, which requires `Topic.EnableMessageOrdering = true` (it
> serializes per-key publishes); enabling it only on the subscription is not enough. A single hot key
> is throttled by its round-trip ack latency (one in flight per key) — spread load across many keys.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
