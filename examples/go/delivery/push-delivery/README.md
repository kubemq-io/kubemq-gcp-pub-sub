# Go — delivery: Push Delivery

Push subscriptions against the KubeMQ Pub/Sub connector:
`CreateSubscription(PushConfig{Endpoint}) → Publish → connector POSTs a wrapped JSON envelope to a local endpoint → 2xx acks`.

## Prerequisites

- **Go 1.25+** and the official GCP Pub/Sub client `cloud.google.com/go/pubsub v1.50.2`
  (pinned in `../../go.mod`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**). The connector's push worker must be able
  to reach the local HTTP endpoint this example starts (loopback, ephemeral port).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/go && go run ./delivery/push-delivery
```

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/push-delivery | emulator=localhost:8085 project=my-project
Push receiver listening at http://127.0.0.1:<port>/push (HTTP allowed for localhost)
CreateTopic: pushed-<uuid8> (channel pubsub.pushed-<uuid8>)
CreateSubscription: sub-pushed-<uuid8> (push_config endpoint=http://127.0.0.1:<port>/push)
Publish: messageId=<message-id> body="push me" attr[priority]="high"
Waiting for the push worker to POST the wrapped envelope...
Push received: messageId=<message-id> body="push me" attr[priority]="high" subscription="projects/my-project/subscriptions/sub-pushed-<uuid8>" publishTime=<publish-time>
PASS: push worker POSTed the wrapped envelope; 2xx acked it
DeleteSubscription: ok
DeleteTopic: ok
```

> Resource ids are uuid-suffixed (`pushed-<uuid8>` / `sub-pushed-<uuid8>`) so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not collide; the receiver binds an
> ephemeral loopback port, so `<port>`, `<message-id>`, and `<publish-time>` vary per run. (See
> `../../../SHARED-CONVENTIONS.md` §1.) Exits 0 on success, non-zero if no push arrives within 30 s.

## What's Happening

The program starts a local HTTP receiver on an ephemeral loopback port, creates a topic, and creates
a subscription with a `PushConfig` whose `Endpoint` is that receiver. It publishes one message with a
`priority` attribute. The connector's per-subscription **push worker** then POSTs a wrapped JSON
envelope to the endpoint; the receiver base64-decodes `message.data`, replies `204 No Content`
(a 2xx → the worker acks the message), and the program asserts the decoded body, the attribute, and
the `messageId` match what was published. Any failure (including no delivery within 30 s) exits
non-zero.

A subscription with a `push_config` is served by a per-sub worker that drains the per-subscription
queue `pubsub.sub.sub-pushed-<uuid8>` and POSTs the Pub/Sub-wrapper envelope (spec S3.5):

```json
{"message":{"data":"<base64>","attributes":{"priority":"high"},"messageId":"<id>","publishTime":"<ts>","orderingKey":""},"subscription":"projects/my-project/subscriptions/sub-pushed-<uuid8>"}
```

A 2xx acks; a non-2xx/timeout retries with backoff; exhaustion dead-letters (if a DLQ is configured)
else drops. HTTPS is required for non-localhost endpoints; plain HTTP is allowed only for localhost,
which is what this example uses.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription (push_config), Publish, (push worker POST + ack), DeleteSubscription, DeleteTopic | `pushed-<uuid8>` / `sub-pushed-<uuid8>` | push | Wrapped `{message:{data(b64),attributes,messageId,publishTime,orderingKey},subscription}` envelope (default) or `no_wrapper` raw body; 2xx → ack; HTTPS required (HTTP only for localhost). |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../python/delivery/push_delivery`](../../../python/delivery/push_delivery),
  [`../../../javascript/delivery/push-delivery`](../../../javascript/delivery/push-delivery),
  [`../../../java/delivery/push-delivery`](../../../java/delivery/push-delivery),
  [`../../../csharp/delivery/push-delivery`](../../../csharp/delivery/push-delivery),
  [`../../../ruby/delivery/push_delivery`](../../../ruby/delivery/push_delivery)
- Docs: [`../../../docs/guides/push-delivery.md`](../../../docs/guides/push-delivery.md)
- Related: [`../dead-letter-topic`](../dead-letter-topic), [`../exactly-once`](../exactly-once).

> **Gotcha — push endpoints must be HTTPS (except localhost), and the body is base64.** A non-loopback
> push endpoint must use HTTPS; the wrapped envelope carries the payload as base64 in `message.data`,
> so the receiver must decode it. A 2xx response is the ack — return non-2xx to trigger retry/backoff.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
