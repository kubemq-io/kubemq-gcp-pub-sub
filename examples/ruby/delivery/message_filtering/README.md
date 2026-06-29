# Ruby — delivery: message filtering

A CEL-subset attribute filter applied at subscription create:
`CreateTopic → CreateSubscription(filter) → Publish(matching + non-matching) → Pull → Acknowledge`, where only the matching message is delivered.

## Prerequisites

- **Ruby 3.3.7** (rbenv; floor 3.1) and the official GCP Pub/Sub client for Ruby
  `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`; exact version in
  the gitignored `Gemfile.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=kubemq`             (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=kubemq
eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7
cd examples/ruby && bundle exec ruby delivery/message_filtering/main.rb
```

## Expected Output

```
[pubsub] emulator=localhost:8085 project=kubemq (insecure / no-auth)
CreateTopic            -> message-filtering-<uuid8>
CreateSubscription     -> sub-message-filtering-<uuid8>
  filter               -> attributes.region = "eu" AND attributes:tier
Publish                -> 1 matching + 2 non-matching
Pull+Ack               -> ["delivered"]
Assert                 -> 2 non-matching messages suppressed (0 delivered)
Malformed filter       -> rejected INVALID_ARGUMENT (3:invalid filter: unexpected token "this" in filter ...)
Cleanup                -> deleted subscription + topic
PASS: only the matching message delivered; non-matching suppressed; malformed filter rejected
```

> Resource ids are uuid-suffixed (`message-filtering-<uuid8>` / `sub-message-filtering-<uuid8>`),
> so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide.
> Exits non-zero on any failed assertion.

## What's Happening

The subscription is created with a `filter` —
`attributes.region = "eu" AND attributes:tier` — a CEL-subset expression that is
attributes-only, ≤ 256 chars, and **compiled once at create time (immutable)**. Three
messages are published: one matching (`region=eu` and a `tier` attribute set) and two that
do not (wrong region; missing `tier`). A pull returns only the matching message, and a
second drain proves the two non-matching messages were never enqueued. Finally a deliberately
malformed filter is rejected with `INVALID_ARGUMENT`.

A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`,
**applying that subscription's filter at fan-out** — a filtered-out message is never enqueued
(≈ auto-acked) and does not count against delivery attempts. Any failed assertion exits
non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription (filter), Publish, Pull, Acknowledge | `pubsub.message-filtering-<uuid8>` / `pubsub.sub.sub-message-filtering-<uuid8>` | filter (CEL-subset) | filter attributes-only, ≤ 256 chars, immutable; malformed → `INVALID_ARGUMENT`; ack deadline 30 s |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/message-filtering`](../../../go/delivery/message-filtering),
  [`../../../python/delivery/message_filtering`](../../../python/delivery/message_filtering),
  [`../../../javascript/delivery/message-filtering`](../../../javascript/delivery/message-filtering),
  [`../../../java/delivery/message-filtering`](../../../java/delivery/message-filtering),
  [`../../../csharp/delivery/message-filtering`](../../../csharp/delivery/message-filtering)
- Docs: [`../../../../docs/guides/filtering.md`](../../../../docs/guides/filtering.md)

> Filtering is attributes-only — a CEL subset: `attributes:KEY`, `= / != "v"`,
> `hasPrefix(attributes.KEY, "p")`, `AND/OR/NOT`. It cannot match the message body, and a
> malformed filter is rejected at create with `INVALID_ARGUMENT`.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
