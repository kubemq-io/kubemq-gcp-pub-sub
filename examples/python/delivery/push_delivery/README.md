# Python — delivery: Push Delivery

Server-pushed delivery: stand up a local HTTP endpoint → `CreateTopic → CreateSubscription(push_config) → Publish`, and the connector's per-subscription push worker POSTs the wrapped JSON envelope to the endpoint, which returns `2xx` (ack).

## Prerequisites

- Python 3.10+ and the official GCP Pub/Sub client `google-cloud-pubsub==2.39.0`
  (pinned in `examples/python/pyproject.toml` + `uv.lock`). The local push receiver uses only the
  standard-library `http.server`.
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**). The connector must be able to reach the
  local push endpoint (it runs as a local process, so a `localhost` endpoint works).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/python && uv run python -m delivery.push_delivery
# or, equivalently: uv run python delivery/push_delivery/main.py
```

## Expected Output

```
=== Pub/Sub push delivery — topic 'events-<uuid8>' / sub 'sub-push-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)
  endpoint : http://localhost:<port>/push  (plain HTTP allowed for localhost)

Local push endpoint listening on http://localhost:<port>/push
CreateTopic            -> projects/my-project/topics/events-<uuid8>
CreateSubscription     -> projects/my-project/subscriptions/sub-push-<uuid8>  (push_config -> http://localhost:<port>/push)

Publish -> body='push-me' attrs={'type': 'order', 'region': 'eu'} id=<message-id>

Waiting for the connector push worker to POST the wrapped envelope...
  [OK] endpoint received a push POST within 30s
Received envelope:
{
  "message": {
    "data": "cHVzaC1tZQ==",
    "attributes": {
      "region": "eu",
      "type": "order"
    },
    "messageId": "<message-id>",
    "publishTime": "<publish-time>"
  },
  "subscription": "projects/my-project/subscriptions/sub-push-<uuid8>"
}

  [OK] envelope is wrapped (has 'message')
  [OK] envelope carries the 'subscription' field
  [OK] message.data base64-decodes to 'push-me'
  [OK] message.attributes carries the user attributes
  [OK] message.messageId is present
  [OK] message.publishTime is present

PASS — push delivery: wrapped envelope POSTed to the localhost endpoint, 2xx acked.
cleanup: DeleteSubscription ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid8-suffixed (`events-<uuid8>` / `sub-push-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never collide; the listening `<port>` is an
> OS-assigned free port and the `<message-id>` / `<publish-time>` values vary per run.

## What's Happening

- A local HTTP server is started on an OS-assigned free port; its `do_POST` handler captures the body
  and returns `204` (a `2xx`, which the connector treats as an ack).
- A subscription is created with a `push_config` pointing at `http://localhost:<port>/push` — plain
  HTTP is permitted because the endpoint is `localhost` (non-localhost endpoints must be HTTPS).
- One message (`push-me`) with two attributes is published. The connector's per-subscription push
  worker pulls it from the sub queue and **POSTs the wrapped JSON envelope** to the endpoint.
- The example asserts the envelope shape: `message.data` is base64-encoded (`cHVzaC1tZQ==` →
  `push-me`), `message.attributes` carries the user attributes, `messageId` / `publishTime` are
  present, and the enclosing object carries the `subscription`.
- Connector behavior (spec S3.5): a `Publish` writes once to the topic log `pubsub.{t}`, fans out a
  queue copy to `pubsub.sub.{s}`, and the push worker drains that queue, POSTing each message. `2xx` →
  ack; non-`2xx`/timeout → retry with backoff; exhaustion → dead-letter topic if set, else drop. The
  reserved `_pubsub_*` tags are not surfaced in the envelope's `attributes`. Any failed assertion
  exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription (push_config), Publish; connector → HTTP POST | topic `events-<uuid8>` / sub `sub-push-<uuid8>` | push | wrapped envelope `{message:{data(base64),attributes,messageId,publishTime,orderingKey},subscription}`; HTTPS required (HTTP only for localhost) |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/push-delivery`](../../../go/delivery/push-delivery),
  [`../../../javascript/delivery/push-delivery`](../../../javascript/delivery/push-delivery),
  [`../../../java/delivery/push-delivery`](../../../java/delivery/push-delivery),
  [`../../../csharp/delivery/push-delivery`](../../../csharp/delivery/push-delivery),
  [`../../../ruby/delivery/push_delivery`](../../../ruby/delivery/push_delivery)
- Docs: [`../../../../docs/guides/push-delivery.md`](../../../../docs/guides/push-delivery.md)

> **Gotcha — push endpoints must be HTTPS (HTTP only for localhost).** This example runs against a
> `localhost` endpoint where plain HTTP is permitted; any non-localhost push endpoint must be
> `https://`. Push delivery state is also **node-local** — use a sticky load balancer so a
> subscription's push worker and its retries stay on one node in a cluster.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
