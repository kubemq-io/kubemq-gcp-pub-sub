# Java — delivery: Push Delivery

A subscription with a `push_config` makes the connector POST each message to a local HTTP endpoint
(no client pull); the wrapped JSON envelope arrives and a 2xx response acks it:
`CreateTopic → CreateSubscription(push_config) → Publish → connector POSTs wrapped envelope → 200 ack`.

## Prerequisites

- **JDK 21+** (the examples target `maven.compiler.release=21`) and Maven 3.9+, and the official
  GCP Pub/Sub client **`com.google.cloud:google-cloud-pubsub`**, version-managed via
  **`com.google.cloud:libraries-bom` 26.84.0** (pinned in `examples/java/pom.xml`; the Pub/Sub
  dependency itself carries no `<version>`). The local HTTP sink uses the JDK's built-in
  `com.sun.net.httpserver` and the envelope is parsed with `gson` (already on the BOM-managed
  classpath) — no extra dependency.
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**). The connector must be able to reach the
  example's local push endpoint (`http://localhost:<port>/push/`).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/java
mvn -q exec:java -Dexec.mainClass=io.kubemq.examples.gcp.delivery.pushdelivery.Main
```

The example binds a local HTTP sink on the first free port in `18080..18130`.

## Expected Output

```
[kubemq-gcp-pub-sub] delivery/push-delivery | emulator=localhost:8085 project=my-project
LocalSink          -> listening at http://localhost:18080/push/
CreateTopic        -> orders-<uuid8> (channel pubsub.orders-<uuid8>)
CreateSubscription -> sub-push-<uuid8> (push_endpoint=http://localhost:18080/push/)
Publish            -> 'hello-push' (attributes: kind=demo)
PushPOST           -> data(decoded)='hello-push', attributes.kind='demo'
PushPOST           -> envelope.subscription='projects/my-project/subscriptions/sub-push-<uuid8>', messageId=<message-id>

PASS: push delivery: wrapped envelope POSTed to the localhost endpoint; 2xx acked, no redelivery
DeleteSubscription -> ok
DeleteTopic -> ok
```

The bound port (`18080` here) and `<message-id>` vary per run. Exit code is `0` on success and
non-zero if no push arrives within 30 s, the envelope is malformed, or the message is redelivered
(more than one POST for a single publish).

> Resource ids are uuid-suffixed (`orders-<uuid8>` / `sub-push-<uuid8>`), so concurrent runs use
> distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and a free local port, so they do not collide;
> the `<uuid8>` suffix and the bound port vary per run. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program stands up a local `com.sun.net.httpserver.HttpServer` (plain HTTP — permitted because
the host is localhost), creates a subscription with a `PushConfig { pushEndpoint }` pointing at it,
and publishes one message tagged `kind=demo`. Instead of the client pulling, the connector's
per-subscription push worker **POSTs the message to the endpoint** as a **wrapped JSON envelope**:

```json
{
  "message": {
    "data": "<base64>",
    "attributes": { "kind": "demo" },
    "messageId": "...",
    "publishTime": "..."
  },
  "subscription": "projects/{p}/subscriptions/sub-push-<uuid8>"
}
```

`data` is base64-encoded (the example decodes it back to `hello-push`); the user attributes are
surfaced (the reserved `_pubsub_*` tags are not). The endpoint returns **200**, which is the ack
signal — a 2xx acks the message, while a non-2xx or timeout would retry with backoff and, on
exhaustion, dead-letter (if a topic is set) or drop. The example asserts the decoded body, the
attribute, the subscription path, and that exactly one POST arrived (no redelivery after the ack).

A `Publish` writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative, replayable,
cross-protocol source), then fans out one queue copy to `pubsub.sub.sub-push-<uuid8>`, from which
the push worker delivers.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic → CreateSubscription (PushConfig) → Publish → (connector POSTs envelope) | topic `orders-<uuid8>` → sub `sub-push-<uuid8>` (`pubsub.sub.sub-push-<uuid8>`) | push (`push_config`, wrapped envelope) | endpoint must be HTTPS (HTTP allowed only for localhost); `data` base64-encoded; 2xx → ack, non-2xx/timeout → retry then DLQ/drop; ack deadline 10 s |

## Related Examples

- Same variant in the other 5 languages:
  [go](../../../go/delivery/push-delivery/),
  [python](../../../python/delivery/push_delivery/),
  [javascript](../../../javascript/delivery/push-delivery/),
  [csharp](../../../csharp/delivery/push-delivery/),
  [ruby](../../../ruby/delivery/push_delivery/)
- Docs: [`../../../docs/guides/push-delivery.md`](../../../docs/guides/push-delivery.md)

> **Gotcha — push endpoints must be HTTPS (localhost may use HTTP).** Plain `http://` is allowed
> only for `localhost` (local development); any non-localhost endpoint must be `https://`. The push
> worker and its in-flight retries are **node-local** in a cluster — use a sticky load balancer so a
> subscription's push worker stays on one node.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../docs/guides/connectivity-and-emulator-mode.md`](../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
