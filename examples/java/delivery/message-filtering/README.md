# Java — delivery: Message Filtering

Two subscriptions on one topic — one unfiltered, one with a CEL-subset attribute filter — show the
filter suppressing non-matching messages at publish fan-out:
`CreateTopic → CreateSubscription(all) + CreateSubscription(filter) → Publish(mixed batch) → Pull → Acknowledge`.

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
mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.delivery.messagefiltering.Main
```

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/message-filtering | emulator=localhost:8085 project=my-project
CreateTopic        -> orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription -> sub-all-<uuid8> (unfiltered)
CreateSubscription -> sub-eu-<uuid8> (filter: attributes.region = "eu")
Publish            -> batch: o1(region=eu), o2(region=us), o3(region=eu)
Pull (unfiltered)  -> sub-all-<uuid8> received: [o1, o2, o3]
Pull (filtered)    -> sub-eu-<uuid8> received: [o1, o3]  (o2/region=us -> suppressed)

PASS: filter suppressed the non-matching message (region=us): 3 delivered to all, 2 to eu
DeleteSubscription(eu) -> ok
DeleteSubscription(all) -> ok
DeleteTopic -> ok
```

The non-matching message (`o2`, `region=us`) is **suppressed → 0 delivered to the filtered
subscription** (it is never enqueued for that subscription). Exit code is `0` on success and
non-zero on any failed assertion.

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-all-<uuid8>` / `sub-eu-<uuid8>`), so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the
> `<uuid8>` suffix varies per run. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates one topic and two subscriptions: `sub-all` (no filter) and `sub-eu` with the
filter `attributes.region = "eu"`. It publishes a mixed batch of three messages tagged
`region=eu`, `region=us`, `region=eu`. The filter is **compiled once at `CreateSubscription` and is
immutable**; it is applied at **publish fan-out**, so a message that does not match a
subscription's filter is **never enqueued** for it. As a result the unfiltered subscription receives
all three (`o1, o2, o3`) and the filtered subscription receives only the two `eu` messages
(`o1, o3`); the `us` message is suppressed. The assertions fail (non-zero exit) if either
subscription receives the wrong set (the example also does a final probe pull to prove the filtered
subscription has no leftover `us` message).

A `Publish` writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy per subscription
(`pubsub.sub.sub-all-<uuid8>`, `pubsub.sub.sub-eu-<uuid8>`), applying each subscription's filter at
fan-out time. The topic log itself is unfiltered.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic → CreateSubscription (×2) → Publish (batch of 3) → Pull → Acknowledge | topic `orders-<uuid8>` → subs `sub-all-<uuid8>` + `sub-eu-<uuid8>` (`pubsub.sub.{s}`) | filter (CEL subset, attributes-only) | filter ≤ 256 chars; immutable; compiled at create; malformed → `INVALID_ARGUMENT`; batch ≤ 1000; ≤ 100 attrs |

## Related Examples

- Same variant in the other 5 languages:
  [go](../../../go/delivery/message-filtering/),
  [python](../../../python/delivery/message_filtering/),
  [javascript](../../../javascript/delivery/message-filtering/),
  [csharp](../../../csharp/delivery/message-filtering/),
  [ruby](../../../ruby/delivery/message_filtering/)
- Docs: [`../../../docs/guides/filtering.md`](../../../docs/guides/filtering.md)

> **Gotcha — filtering is attributes-only and a CEL subset.** Supported forms are `attributes:KEY`,
> `attributes.KEY = "v"` / `!= "v"`, `hasPrefix(attributes.KEY, "p")`, and `AND`/`OR`/`NOT` with
> parentheses; there is no `data`-based filtering and no numeric comparisons. The expression must be
> ≤ 256 chars and is immutable after `CreateSubscription`; a malformed filter is rejected at create
> with `INVALID_ARGUMENT`, not silently ignored.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
