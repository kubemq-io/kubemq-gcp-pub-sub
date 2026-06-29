# JavaScript — delivery: Message filtering

A subscription-level CEL-subset attribute filter against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription(filter) + CreateSubscription(no filter) → Publish mixed batch → Pull both → Acknowledge` — the filtered subscription receives only the matching subset.

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
cd examples/javascript && npx tsx delivery/message-filtering/index.ts
```

## Expected Output

```
=== Pub/Sub message filtering — topic 'filtered-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  filter   : attributes.region = "eu"

CreateTopic            -> projects/my-project/topics/filtered-<uuid8>
CreateSubscription     -> sub-filtered-<uuid8>  (filter=attributes.region = "eu")
CreateSubscription     -> sub-all-<uuid8>  (no filter)

Publish mixed batch (one Publish per message; region attribute drives the filter):
  publish body='order-1' region=eu  -> id=<message-id>
  publish body='order-2' region=us  -> id=<message-id>
  publish body='order-3' region=eu  -> id=<message-id>
  publish body='order-4' region=ap  -> id=<message-id>
  publish body='order-5' region=eu  -> id=<message-id>

Pull filtered subscription (expects EU only):
  filtered received: [order-1, order-3, order-5]
Pull unfiltered subscription (expects everything):
  unfiltered received: [order-1, order-2, order-3, order-4, order-5]

  [OK] filtered sub received only region=eu ([order-1, order-3, order-5])
  [OK] unfiltered sub received the full batch
  [OK] non-matching attribute -> suppressed (2 not enqueued for filtered sub)

Negative path: a malformed filter is rejected at CreateSubscription:
  malformed filter -> INVALID_ARGUMENT (gRPC code 3)
  [OK] malformed filter rejected at create-time (INVALID_ARGUMENT)

PASS — message filtering: attributes-only CEL-subset applied at publish fan-out.
cleanup: DeleteSubscription(sub-filtered-<uuid8>) ok
cleanup: DeleteSubscription(sub-all-<uuid8>) ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.filtered-<uuid8>` / `pubsub.sub.sub-filtered-<uuid8>` channels and do not collide; the
> `<uuid8>` suffix and each `<message-id>` vary per run. Two `region != eu` messages are
> **suppressed** (never enqueued for the filtered sub → 0 delivered there). Exits `0` on success,
> non-zero on any failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program creates one uuid-suffixed topic and two subscriptions: one with the CEL-subset filter
`attributes.region = "eu"` and one with no filter. It publishes a mixed five-message batch whose
`region` attribute is `eu`/`us`/`ap`, then drains both subscriptions with unary `Pull` and asserts:

- the **filtered** subscription receives only the three `region=eu` messages;
- the **unfiltered** subscription receives all five;
- the two non-matching messages are **suppressed** for the filtered sub.

It then drives the negative path: creating a subscription with a malformed filter
(`"this is not a filter"`) is rejected at create-time with gRPC `INVALID_ARGUMENT` (status code 3).

A Publish writes once to the topic log `pubsub.{t}` (unfiltered — the authoritative, replayable,
cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`, **applying
each subscription's filter at publish fan-out**. A filter is attributes-only, ≤ 256 chars, compiled
once at `CreateSubscription` and immutable thereafter; a non-matching message is never enqueued for
that subscription (effectively auto-acked for it). Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription` (`filter`), `Publish`, `Pull`, `Acknowledge` | topic `pubsub.{t}` / subs `pubsub.sub.{s}` | filter | CEL-subset, attributes-only (`attributes:KEY`, `= / != "v"`, `hasPrefix(...)`, `AND/OR/NOT`); ≤ 256 chars; immutable after create; applied at publish fan-out; malformed → `INVALID_ARGUMENT` |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/message-filtering`](../../../go/delivery/message-filtering),
  [`../../../python/delivery/message_filtering`](../../../python/delivery/message_filtering),
  [`../../../java/delivery/message-filtering`](../../../java/delivery/message-filtering),
  [`../../../csharp/delivery/message-filtering`](../../../csharp/delivery/message-filtering),
  [`../../../ruby/delivery/message_filtering`](../../../ruby/delivery/message_filtering)
- Docs: [`../../../../docs/guides/filtering.md`](../../../../docs/guides/filtering.md)

> **Gotcha — filtering is an attributes-only CEL-subset.** The filter operates on message
> `attributes`, never on the body, and supports only `attributes:KEY`, `= / != "v"`,
> `hasPrefix(...)`, and `AND/OR/NOT` with parentheses; it is immutable after `CreateSubscription`,
> and a malformed expression is rejected with `INVALID_ARGUMENT`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
