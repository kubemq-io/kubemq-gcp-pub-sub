# Ruby — subscriptions: streaming pull with flow control

A flow-controlled `StreamingPull` round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish N → listen (StreamingPull) → drain+Ack all N → stop`.

## Prerequisites

- Ruby **3.3.7** (rbenv; floor is 3.1) and the official GCP Pub/Sub client
  `google-cloud-pubsub` (pinned `~> 3.3` in `examples/ruby/Gemfile`).
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
eval "$(rbenv init - zsh)"   # rbenv 3.3.7
cd examples/ruby
bundle install
bundle exec ruby subscriptions/streaming_pull/main.rb
```

## Expected Output

```
[PUBSUB] emulator_host=localhost:8085 project=my-project (no-auth / insecure gRPC)
CreateTopic         -> stream-<uuid8>
CreateSubscription  -> sub-stream-<uuid8>
Publish             -> 5 messages
StreamingPull       -> listening (flow control: max 10 outstanding)
Drained             -> 5 messages acked over the stream
Stop                -> listener stopped cleanly
PASS: StreamingPull delivered all 5 messages under flow control and acked them
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and never collide; the `<uuid8>` suffix and per-message
> `<message-id>` / `<publish-time>` vary per run. (See `../../../../docs/guides/connectivity-and-emulator-mode.md`.)

## What's Happening

The program publishes 5 messages, then calls `Subscriber#listen`, which opens a long-lived
**StreamingPull** stream and pushes each message to a callback as it arrives. Delivery is bounded
by client-side **flow control** (`inventory: { max_outstanding_messages: 10 }`): the client holds
at most that many un-acked messages at once, so a fast producer can never overwhelm the consumer.
The callback acknowledges each message; once all 5 have been drained, the program calls `stop!` to
shut the stream down gracefully. The official client also re-establishes the stream automatically
when the connector performs its periodic server-initiated close (`StreamCloseSeconds`, default
1800 s), so a long-running listener survives the reconnect with no message loss. Any failed
assertion exits non-zero.

A `Publish` writes once to the topic log `pubsub.{t}` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy per subscription `pubsub.sub.{s}`; the
StreamingPull stream drains that per-subscription queue under the ack-deadline lease.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `StreamingPull`, `Acknowledge`, `DeleteSubscription`, `DeleteTopic` | `stream-<uuid8>` / `sub-stream-<uuid8>` | none | client flow control `max_outstanding_messages`; periodic stream close (`StreamCloseSeconds`, 1800 s) forces a transparent SDK reconnect |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/subscriptions/streaming-pull`](../../../go/subscriptions/streaming-pull),
  [`../../../python/subscriptions/streaming_pull`](../../../python/subscriptions/streaming_pull),
  [`../../../java/subscriptions/streaming-pull`](../../../java/subscriptions/streaming-pull),
  [`../../../javascript/subscriptions/streaming-pull`](../../../javascript/subscriptions/streaming-pull),
  [`../../../csharp/subscriptions/streaming-pull`](../../../csharp/subscriptions/streaming-pull)
- Docs: [`../../../../docs/guides/subscribing.md`](../../../../docs/guides/subscribing.md)
  (and the concept doc [`../../../../docs/concepts/subscriptions-and-pull.md`](../../../../docs/concepts/subscriptions-and-pull.md))

> **Gotcha — StreamingPull leases are node-local.** Each StreamingPull stream and its ack-deadline
> leases are owned by the node that serves them; in a cluster, pin a subscription's StreamingPull
> traffic to one node (sticky LB) so deliveries and acks stay on the same node.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
