# Ruby — interop: native Events Store consume

A message published with the official Google Cloud Pub/Sub client to topic `orders`
is consumed **natively** from the Events Store log `pubsub.orders` via the native
KubeMQ gRPC SDK: `CreateTopic → (native) SubscribeToEventsStore(startAt=new) → Publish → native receive on pubsub.{t}`.

## Prerequisites

- **Ruby 3.1+** (built/verified on **3.3.7** via rbenv) and the official GCP Pub/Sub
  client `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`; the
  resolved version is captured in the gitignored `Gemfile.lock`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)
- **interop only:** this variant also requires the native KubeMQ gRPC broker at
  `localhost:50000` and the pinned native KubeMQ SDK — gem `kubemq` `~> 1.0` (`1.0.0`,
  Ruby ≥ 3.1; also pinned in `examples/ruby/Gemfile`), which provides
  `KubeMQ::PubSub::PubSubClient#subscribe_to_events_store`. This is the only example
  in the repo that imports a KubeMQ SDK; every other variant uses only `google-cloud-pubsub`.

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
export KUBEMQ_GRPC_ADDRESS=localhost:50000     # native KubeMQ broker (interop half)
eval "$(rbenv init - zsh)"                     # selects Ruby 3.3.7 (rbenv global)
cd examples/ruby && bundle exec ruby interop/native_events_store/main.rb
```

## Expected Output

```
[*] topic id            : orders-<uuid8>  (Events Store channel 'pubsub.orders-<uuid8>')
[*] PUBSUB_EMULATOR_HOST: localhost:8085
[*] native gRPC broker  : localhost:50000
[*] CreateTopic 'orders-<uuid8>' (project 'my-project' parsed but ignored)
[*] Connected native KubeMQ client to localhost:50000
[*] Events Store subscribe stream open on 'pubsub.orders-<uuid8>' (startAt=new)
[>] Pub/Sub Publish to 'orders-<uuid8>' → messageId=<message-id>
[<] native Events Store received on 'pubsub.orders-<uuid8>' (seq 1) → 'order #4242 — cross-protocol via Pub/Sub'
[v] reserved tag _pubsub_message_id   = <message-id>
[v] reserved tag _pubsub_publish_time = <publish-time>
[v] reserved tag _pubsub_ordering_key = customer-42
[v] attribute region = eu-west-1
[v] attribute priority = high
[*] Cleaned up topic 'orders-<uuid8>'
[PASS] Pub/Sub publish consumed natively on Events Store with reserved tags present
```

> Channel-isolation note: the topic id is uuid-suffixed (`orders-<uuid8>`), so concurrent
> runs (other languages, or `/examples` + `/burnin`) use distinct `pubsub.orders-<uuid8>`
> channels and never cross-talk. `<message-id>` and `<publish-time>` vary per run. The
> reserved-tag `_pubsub_message_id` is asserted to equal the Pub/Sub `messageId`; any
> missing tag, body mismatch, or a no-event timeout exits non-zero.

## What's Happening

The producer is a plain `google-cloud-pubsub` client pointed at the connector via
`PUBSUB_EMULATOR_HOST` — it never speaks KubeMQ. It creates topic `orders-<uuid8>` and
publishes one message (`data` + two attributes + an `ordering_key`).

The consumer is the **native** KubeMQ SDK (`KubeMQ::PubSubClient`) connected over gRPC at
`localhost:50000`. To make the publish-then-consume deterministic, the example follows the
**subscribe-before-publish, `startAt = "new"`** pattern: it opens an Events Store *subscribe*
stream on `pubsub.orders-<uuid8>` with `EventStoreStartPosition::START_NEW_ONLY` (enum value
`1`) and confirms the background subscribe thread is active **before** issuing the Pub/Sub
publish, so the published message is guaranteed in-window (no startup race). It then awaits
exactly one event and asserts the body, the three reserved tags, and the two attributes.

A Pub/Sub `Publish` writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative,
replayable, cross-protocol source) via `Array.SendEventsStore`; the connector stamps the body
as `data` and carries the message `attributes` plus the **three reserved tags**
(`_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key`) — stripped from
`attributes` for Pub/Sub clients but visible to the native Events Store consumer here. Any
failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| Pub/Sub `CreateTopic` + `Publish`; native gRPC Events Store **subscribe** on `pubsub.{t}` | `orders-<uuid8>` / Events Store `pubsub.orders-<uuid8>` | none (interop) | same payload read natively with the three reserved tags present; native subscribe via the `kubemq` gem; subscribe-before-publish + `startAt = "new"` (`START_NEW_ONLY`); msg ≤ 10 MiB, ≤ 100 attributes |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/interop/native-events-store`](../../../go/interop/native-events-store),
  [`../../../python/interop/native_events_store`](../../../python/interop/native_events_store),
  [`../../../javascript/interop/native-events-store`](../../../javascript/interop/native-events-store),
  [`../../../java/interop/native-events-store`](../../../java/interop/native-events-store),
  [`../../../csharp/interop/native-events-store`](../../../csharp/interop/native-events-store)
- Docs: [`../../../../docs/concepts/cross-protocol-interop.md`](../../../../docs/concepts/cross-protocol-interop.md)
  and [`../../../../docs/reference/channel-mapping.md`](../../../../docs/reference/channel-mapping.md)

> Gotcha: the native side must **subscribe before** the Pub/Sub publish and use
> `START_NEW_ONLY` — an Events Store subscribe is a stream, not a one-shot read, so a publish
> issued before the stream is open would be missed. To consume messages published *before* the
> subscriber attaches, use `START_AT_SEQUENCE` / `START_AT_TIME` instead. The project id is
> parsed but ignored, so resource ids are global across "projects" — hence the uuid suffix.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
