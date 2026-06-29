# Ruby — topics: Batch publish

One Publish RPC carrying many messages (1..1000); the server returns one
`message_id` per message **in request order** after validating the whole batch
atomically.

## Prerequisites

- **Ruby 3.3.7** (rbenv; floor 3.1) and the official Google Cloud Pub/Sub client
  for Ruby `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**).
- The standard Google emulator env vars — **no code changes**, the SDK skips
  Google auth and uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7 (rbenv global)
cd examples/ruby && bundle exec ruby topics/batch_publish/main.rb
```

## Expected Output

```
=== topics/batch_publish — topic 'events-<run>' (5 messages, one RPC) ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

[*] CreateTopic -> projects/my-project/topics/events-<run>  (channel pubsub.events-<run>)
[*] CreateSubscription -> projects/my-project/subscriptions/sub-events-<run>  (channel pubsub.sub.sub-events-<run>)
[x] Publish -> 5 messages in one batch RPC
  [OK] got 5 message ids back
  [OK] message 0 got a server-assigned id
  [OK] published[0] body is in request order
[*]   ids[0] = <message-id>  <->  "order #1001"
  [OK] message 1 got a server-assigned id
  [OK] published[1] body is in request order
[*]   ids[1] = <message-id>  <->  "order #1002"
  [OK] message 2 got a server-assigned id
  [OK] published[2] body is in request order
[*]   ids[2] = <message-id>  <->  "order #1003"
  [OK] message 3 got a server-assigned id
  [OK] published[3] body is in request order
[*]   ids[3] = <message-id>  <->  "order #1004"
  [OK] message 4 got a server-assigned id
  [OK] published[4] body is in request order
[*]   ids[4] = <message-id>  <->  "order #1005"
[v] Pulled and acked 5 messages
  [OK] pulled all 5 messages
  [OK] pulled a known body "order #1001"
  [OK] message_id matches for "order #1001"
  [OK] pulled a known body "order #1002"
  [OK] message_id matches for "order #1002"
  [OK] pulled a known body "order #1003"
  [OK] message_id matches for "order #1003"
  [OK] pulled a known body "order #1004"
  [OK] message_id matches for "order #1004"
  [OK] pulled a known body "order #1005"
  [OK] message_id matches for "order #1005"

PASS — batch publish of 5 messages complete (ids returned in request order).
[*] Cleaned up subscription 'sub-events-<run>'
[*] Cleaned up topic 'events-<run>'
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.events-<run>` / `pubsub.sub.sub-events-<run>` channels and never
> cross-talk; the `<run>` suffix and each `<message-id>` vary per run. Cleanup is
> best-effort. The example exits `0` on success and non-zero on any failed
> assertion.

## What's Happening

The synchronous **block form** of `publisher.publish` collects several messages
and sends them as a single Publish RPC:

```ruby
published = publisher.publish do |batch|
  bodies.each_with_index { |b, i| batch.publish(b, seq: (i + 1).to_s) }
end
```

It returns an `Array<Message>` whose order matches the request, so `published[i]`
carries the server `message_id` for `bodies[i]`. The connector validates the
**whole batch** before enqueuing anything (publish atomicity: ≤1000 messages,
≤10 MiB per message, ≤100 attributes; any single invalid message rejects the
entire batch with `INVALID_ARGUMENT` and enqueues nothing), then writes each
accepted message **once** to the topic log `pubsub.{topic}` (the authoritative,
replayable, cross-protocol source) and fans out one copy per subscription to
`pubsub.sub.{sub}`. We then drain all five from the subscription queue (one Pull
may return fewer than requested, so we loop and ack as we go) and assert every
pulled body maps back to the `message_id` returned by the Publish response. Any
failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| Publish (batch ≤ 1000) | `events` / `sub-events` | none | whole batch validated atomically; rejected on first invalid message (`INVALID_ARGUMENT`); ids returned in request order; per message ≤ 10 MiB, ≤ 100 attrs |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/batch-publish`](../../../go/topics/batch-publish),
  [`../../../python/topics/batch_publish`](../../../python/topics/batch_publish),
  [`../../../javascript/topics/batch-publish`](../../../javascript/topics/batch-publish),
  [`../../../java/topics/batch-publish`](../../../java/topics/batch-publish),
  [`../../../csharp/topics/batch-publish`](../../../csharp/topics/batch-publish)
- Docs: [`../../../../docs/guides/publishing.md`](../../../../docs/guides/publishing.md)

> The project id is parsed and validated but **ignored** — the connector is
> single-tenant like Google's emulator, so resource ids are global across
> "projects". Use uuid-suffixed names (as here) to keep concurrent runs isolated.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
