# JavaScript — delivery: Push delivery

Push subscriptions against the KubeMQ Pub/Sub connector:
`bind local endpoint → CreateTopic → CreateSubscription(pushConfig) → Publish → connector POSTs the wrapped envelope → 2xx acks it`.

## Prerequisites

- **Node.js 18+** (developed against Node 20+) and the official GCP Pub/Sub client
  `@google-cloud/pubsub` **5.3.1** (pinned in `examples/javascript/package.json`); run with `tsx`.
  The local push receiver uses Node's built-in `node:http` (no extra dependency).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**). The connector must be able to reach the
  local push endpoint this example binds (it runs on the same host by default, so `127.0.0.1` works).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=my-project`         (any id; the project segment is parsed but ignored)
  - `PUSH_ENDPOINT_HOST` (optional) — host the connector dials back to; defaults to `127.0.0.1`.

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project
cd examples/javascript && npx tsx delivery/push-delivery/index.ts
```

## Expected Output

```
=== Pub/Sub push delivery — topic 'pushed-<uuid8>' / sub 'sub-pushed-<uuid8>' ===
  emulator : localhost:8085
  project  : my-project (parsed but ignored by the connector)

Local push receiver listening -> http://127.0.0.1:<port>/push
CreateTopic            -> projects/my-project/topics/pushed-<uuid8>
CreateSubscription     -> sub-pushed-<uuid8>  (push_config -> http://127.0.0.1:<port>/push)

Publish -> 'push-payload'  id=<message-id>  attributes={"region":"eu","source":"push-example"}

Awaiting the connector push POST (wrapped JSON envelope):
  envelope.message.data (base64-decoded) = 'push-payload'
  envelope.message.messageId             = <message-id>
  envelope.message.attributes            = {"region":"eu","source":"push-example"}
  envelope.subscription                  = projects/my-project/subscriptions/sub-pushed-<uuid8>

  [OK] pushed envelope carries the base64-encoded original body
  [OK] pushed envelope messageId matches the published id
  [OK] pushed envelope preserves the message attributes
  [OK] pushed envelope names the subscription

PASS — push delivery: connector POSTed the wrapped envelope; 2xx acked it.
cleanup: DeleteSubscription ok
cleanup: DeleteTopic ok
```

> Resource ids are uuid-suffixed per run, so concurrent runs use distinct
> `pubsub.pushed-<uuid8>` / `pubsub.sub.sub-pushed-<uuid8>` channels and do not collide; the
> `<uuid8>` suffix, the ephemeral local `<port>`, and the `<message-id>` vary per run. Exits `0` on
> success, non-zero on any failed assertion. (See `../../../SHARED-CONVENTIONS.md` §1.)

## What's Happening

The program binds a tiny local `node:http` server on an ephemeral port, then creates a uuid-suffixed
topic and a subscription whose `pushConfig.pushEndpoint` points at that server (plain **HTTP** is
allowed because the endpoint host is `localhost`/`127.0.0.1`; HTTPS is required for any other host).
It publishes one message with a body and two attributes.

The connector's per-subscription **push worker** then POSTs a **wrapped JSON envelope** to the
endpoint:

```json
{ "message": { "data": "<base64>", "attributes": {…}, "messageId": "…",
               "publishTime": "…", "orderingKey": "…" },
  "subscription": "…" }
```

The handler returns **HTTP 200**, which the connector treats as an ack (a non-2xx response or timeout
would trigger retry with backoff; exhaustion dead-letters if a DLQ is set, else drops). The program
asserts the base64-decoded body matches the published payload, the envelope `messageId` matches the
publish-returned id, the attributes are preserved, and the envelope names the subscription.

A Publish writes once to the topic log `pubsub.{t}` then fans out one queue copy per subscription
`pubsub.sub.{s}`; the push worker drains that per-sub queue and POSTs each message (`push.go`). An
optional OIDC `Bearer` token can be attached when configured. Any failed assertion exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| `CreateTopic`, `CreateSubscription` (`pushConfig.pushEndpoint`), `Publish`; connector → HTTP POST | topic `pubsub.{t}` / sub `pubsub.sub.{s}` | push | wrapped `{message:{data(base64),attributes,messageId,publishTime,orderingKey},subscription}` (or `no_wrapper` raw body); 2xx → ack, non-2xx/timeout → retry w/ backoff, exhaustion → DLQ if set else drop; optional OIDC Bearer; **HTTPS required except for localhost** |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/push-delivery`](../../../go/delivery/push-delivery),
  [`../../../python/delivery/push_delivery`](../../../python/delivery/push_delivery),
  [`../../../java/delivery/push-delivery`](../../../java/delivery/push-delivery),
  [`../../../csharp/delivery/push-delivery`](../../../csharp/delivery/push-delivery),
  [`../../../ruby/delivery/push_delivery`](../../../ruby/delivery/push_delivery)
- Docs: [`../../../../docs/guides/push-delivery.md`](../../../../docs/guides/push-delivery.md)

> **Gotcha — HTTPS is required except for localhost.** The connector accepts a plain `http://`
> push endpoint only when the host is `localhost`/`127.0.0.1` (as used here); any other host must use
> `https://`. The connector must also be able to reach the endpoint — if the connector runs on a
> different host or in a container, set `PUSH_ENDPOINT_HOST` to an address it can dial.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
