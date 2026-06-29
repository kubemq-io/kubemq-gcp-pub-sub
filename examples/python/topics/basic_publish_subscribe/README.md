# Python — topics: Basic Publish & Subscribe

The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
`CreateTopic → CreateSubscription → Publish → Pull → Acknowledge`.

## Prerequisites

- **Python 3.10+** (uv) and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `../../pyproject.toml` + committed `uv.lock`).
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
cd examples/python && uv run python -m topics.basic_publish_subscribe
# or, equivalently:
cd examples/python && uv run python topics/basic_publish_subscribe/main.py
```

## Expected Output

```
=== topics/basic_publish_subscribe — topic 'orders-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  auth     : none / insecure gRPC (emulator drop-in)

CreateTopic -> projects/my-project/topics/orders-<uuid8>
  [OK] created topic name matches the requested path
CreateSubscription -> projects/my-project/subscriptions/sub-orders-<uuid8>  (ack_deadline=10s)
  [OK] default ack deadline is the connector floor (10s)
Publish -> message_id=<message-id>
  [OK] publish returned a server-assigned message id
  [OK] pulled exactly 1 message
Pull -> data='hello from google-cloud-pubsub'  message_id=<message-id>
        publish_time=<publish-time>  attributes={'source': 'basic_publish_subscribe'}
  [OK] pulled body equals the published body
  [OK] pulled message_id matches the publish id
  [OK] attributes round-trip; reserved _pubsub_* tags are not exposed
Acknowledge -> 1 ack_id(s)
  [OK] subscription empty after ack (no redelivery)

PASS — basic publish/subscribe round-trip complete.
```

> Resource ids are uuid-suffixed, so concurrent runs use distinct `pubsub.{t}` /
> `pubsub.sub.{s}` channels and do not collide; `<message-id>` / `<publish-time>` vary per run.
> Each failed `[OK]` assertion exits non-zero. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program builds a `PublisherClient` and a `SubscriberClient` from the official
`google-cloud-pubsub` package. Because `PUBSUB_EMULATOR_HOST` is set, both clients clear
credentials and dial insecure gRPC at the connector — no `EmulatorDetection` flag, no auth, no
code changes. `CreateTopic` and `CreateSubscription` register the resources; `publish()` returns a
future whose `.result()` is the server-assigned message id; `pull()` returns the queued copy; and
`acknowledge()` removes it from the subscription queue (a re-`pull` then finds it empty). Every
expected outcome is asserted with `[OK]` lines; any mismatch exits non-zero.

On the connector side, a `Publish` writes **once** to the topic log `pubsub.{t}` — the
authoritative, replayable, cross-protocol source — then fans out one queue copy per subscription
`pubsub.sub.{s}`. Each delivered message carries an opaque `ack_id` under a 10s ack-deadline lease;
`Acknowledge` releases the lease and drops the broker seq, so the message is not redelivered.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription`, `Publish`, `Pull`, `Acknowledge` | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | none | Default ack deadline **10s** (must be 0 or 10..600s); message body ≤ 10 MiB; ≤ 100 attributes; reserved `_pubsub_*` tags are stripped for Pub/Sub clients. |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/topics/basic-publish-subscribe`](../../../go/topics/basic-publish-subscribe),
  [`../../../javascript/topics/basic-publish-subscribe`](../../../javascript/topics/basic-publish-subscribe),
  [`../../../java/topics/basic-publish-subscribe`](../../../java/topics/basic-publish-subscribe),
  [`../../../csharp/topics/basic-publish-subscribe`](../../../csharp/topics/basic-publish-subscribe),
  [`../../../ruby/topics/basic_publish_subscribe`](../../../ruby/topics/basic_publish_subscribe)
- Docs: [`../../../../docs/concepts/topics-and-publish.md`](../../../../docs/concepts/topics-and-publish.md)

> Gotcha (#2): the project id is **parsed but ignored** — resource ids are global across "projects",
> so two runs that reuse a bare topic name collide on the same `pubsub.{t}` channel. This example
> uuid-suffixes its ids to keep each run on a private channel.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
