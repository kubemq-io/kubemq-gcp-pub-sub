# Python — delivery: Message Filtering

Attribute filtering at publish fan-out: `CreateTopic → CreateSubscription(filter) + CreateSubscription(no filter) → Publish a mixed batch → Pull both`, asserting the filtered subscription receives only the matching subset while the unfiltered one receives everything.

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
cd examples/python && uv run python -m delivery.message_filtering
# or, equivalently: uv run python delivery/message_filtering/main.py
```

## Expected Output

```
=== Pub/Sub message filtering — topic 'events-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  filter   : attributes.region = "eu"

CreateTopic            -> projects/my-project/topics/events-<uuid8>
CreateSubscription     -> projects/my-project/subscriptions/sub-eu-<uuid8>  (filter=attributes.region = "eu")
CreateSubscription     -> projects/my-project/subscriptions/sub-all-<uuid8>  (no filter)

Publish mixed batch (one Publish per message; region attribute drives the filter):
  publish body='order-1' region=eu  -> id=<message-id>
  publish body='order-2' region=us  -> id=<message-id>
  publish body='order-3' region=eu  -> id=<message-id>
  publish body='order-4' region=ap  -> id=<message-id>
  publish body='order-5' region=eu  -> id=<message-id>

Pull filtered subscription (expects EU only):
  filtered received: ['order-1', 'order-3', 'order-5']
Pull unfiltered subscription (expects everything):
  unfiltered received: ['order-1', 'order-2', 'order-3', 'order-4', 'order-5']

  [OK] filtered sub received only region=eu (['order-1', 'order-3', 'order-5'])
  [OK] unfiltered sub received the full batch
  [OK] non-matching attribute -> suppressed (2 not enqueued for filtered sub)

Negative path: a malformed filter is rejected at CreateSubscription:
  malformed filter -> INVALID_ARGUMENT
  [OK] malformed filter rejected at create-time (INVALID_ARGUMENT)

PASS — message filtering: attributes-only CEL-subset applied at publish fan-out.
cleanup: DeleteSubscription(filtered) ok
cleanup: DeleteSubscription(all) ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid8-suffixed (`events-<uuid8>` / `sub-eu-<uuid8>` / `sub-all-<uuid8>`) so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the
> `<message-id>` values vary per run. The non-matching subset (`us`, `ap`) is suppressed for the
> filtered subscription — this is the explicit negative result.

## What's Happening

- One topic is created with **two subscriptions**: one carries the CEL-subset filter
  `attributes.region = "eu"`, the other has no filter.
- A mixed batch of five messages with a `region` attribute (`eu`/`us`/`ap`) is published.
- The filtered subscription receives only the three `region=eu` messages; the unfiltered one receives
  all five. The two non-matching messages were **never enqueued** for the filtered subscription
  (effectively auto-acked for it).
- A malformed filter is rejected at `CreateSubscription` with `INVALID_ARGUMENT` — filters are parsed
  and compiled at create-time, not silently ignored.
- Connector behavior (spec S3.5): a `Publish` writes once to the topic log `pubsub.{t}` (unfiltered,
  authoritative, replayable), then fans out one queue copy per subscription `pubsub.sub.{s}`,
  **applying each subscription's filter at fan-out**. The filter is attributes-only, ≤ 256 chars, and
  immutable after create. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription ×2, Publish, Pull, Acknowledge | topic `events-<uuid8>` / subs `sub-eu-<uuid8>` (filtered) + `sub-all-<uuid8>` | filter | filter ≤ 256 chars, attributes-only CEL-subset, immutable; malformed → INVALID_ARGUMENT |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/message-filtering`](../../../go/delivery/message-filtering),
  [`../../../javascript/delivery/message-filtering`](../../../javascript/delivery/message-filtering),
  [`../../../java/delivery/message-filtering`](../../../java/delivery/message-filtering),
  [`../../../csharp/delivery/message-filtering`](../../../csharp/delivery/message-filtering),
  [`../../../ruby/delivery/message_filtering`](../../../ruby/delivery/message_filtering)
- Docs: [`../../../../docs/guides/filtering.md`](../../../../docs/guides/filtering.md)

> **Gotcha — filtering is an attributes-only CEL subset.** There is no `data`-based filtering and no
> numeric comparisons beyond `=` / `!=` / `hasPrefix`; the `filter` is immutable after
> `CreateSubscription` and a malformed expression is rejected with `INVALID_ARGUMENT`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
