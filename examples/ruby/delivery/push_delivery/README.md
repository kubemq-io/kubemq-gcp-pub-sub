# Ruby — delivery: push delivery

A push subscription delivers via HTTP POST instead of pull:
`start local endpoint → CreateTopic → CreateSubscription(push_config) → Publish → connector POSTs wrapped envelope → assert`.

## Prerequisites

- **Ruby 3.3.7** (rbenv; floor 3.1) and the official GCP Pub/Sub client for Ruby
  `google-cloud-pubsub` (`~> 3.3`, pinned in `examples/ruby/Gemfile`; exact version in
  the gitignored `Gemfile.lock`). The local webhook uses only Ruby stdlib (`socket`).
- A running KubeMQ broker with the Pub/Sub connector enabled
  (`CONNECTORS_GCP_ENABLE=true`, gRPC port **8085**). The connector must be able to reach
  back to this process's webhook port (default `127.0.0.1`; set `WEBHOOK_HOST` for a remote or
  containerized connector, e.g. `host.docker.internal`).
- The standard Google emulator env vars — **no code changes**, the SDK skips Google auth and
  uses insecure gRPC when `PUBSUB_EMULATOR_HOST` is set:
  - `PUBSUB_EMULATOR_HOST=localhost:8085`  (connector default port)
  - `PUBSUB_PROJECT_ID=kubemq`             (any id; the project segment is parsed but ignored)

## How to Run

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=kubemq
export WEBHOOK_HOST=127.0.0.1   # address the connector calls back on (override if remote)
eval "$(rbenv init - zsh)"      # selects Ruby 3.3.7
cd examples/ruby && bundle exec ruby delivery/push_delivery/main.rb
```

## Expected Output

```
[pubsub] emulator=localhost:8085 project=kubemq (insecure / no-auth)
Webhook listening      -> http://127.0.0.1:<port>/push
CreateTopic            -> push-delivery-<uuid8>
CreateSubscription     -> sub-push-delivery-<uuid8> (push_config.push_endpoint=http://127.0.0.1:<port>/push)
Publish                -> id=<message-id>
Push received          -> POST envelope keys=["message", "subscription"]
  message.data         -> "hello-push" (base64-decoded)
  message.attributes   -> {"color"=>"blue"}
  message.messageId    -> <message-id>
  subscription         -> projects/kubemq/subscriptions/sub-push-delivery-<uuid8>
Assert                 -> wrapped envelope shape + base64 data + attributes verified
Cleanup                -> deleted subscription + topic
PASS: push subscription delivered the wrapped JSON envelope to the local endpoint
```

> Resource ids are uuid-suffixed and the webhook binds an ephemeral port, so concurrent runs
> use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and ports and do not collide.
> `<port>` and `<message-id>` vary per run. Exits non-zero on any failed assertion.

## What's Happening

The example starts a tiny stdlib HTTP endpoint on an ephemeral port, then creates a
subscription whose `push_config.push_endpoint` points at it. After a single `Publish`, the
client does **no pull loop** — the connector's per-subscription delivery worker POSTs the
message to the endpoint. The example captures the request and asserts the wrapped envelope
shape (`{ "message": { "data", "attributes", "messageId", "publishTime" }, "subscription" }`),
base64-decodes `message.data` to `hello-push`, and checks the attributes.

A `Publish` writes once to the topic log `pubsub.{t}`, then fans out one queue copy per
subscription `pubsub.sub.{s}`; the push worker pulls from that queue and POSTs the wrapped JSON
envelope (or the raw body when `no_wrapper` is set). 2xx → ack; non-2xx/timeout → retry with
backoff; exhaustion → dead-letter topic if set, else drop. Endpoints must be HTTPS, except HTTP
is allowed for localhost (used here so the example needs no TLS cert). Any failed assertion
exits non-zero.

## Pub/Sub specifics

| RPC(s) | Topic / Sub | Feature flag | Notes (limits applied) |
|--------|-------------|--------------|------------------------|
| CreateTopic, CreateSubscription (push_config), Publish | `pubsub.push-delivery-<uuid8>` / `pubsub.sub.sub-push-delivery-<uuid8>` | push | wrapped envelope; HTTPS required (HTTP only for localhost); 2xx→ack, else retry/backoff→DLQ/drop; optional OIDC Bearer |

## Related Examples

- Same variant in the other 5 languages:
  [`../../../go/delivery/push-delivery`](../../../go/delivery/push-delivery),
  [`../../../python/delivery/push_delivery`](../../../python/delivery/push_delivery),
  [`../../../javascript/delivery/push-delivery`](../../../javascript/delivery/push-delivery),
  [`../../../java/delivery/push-delivery`](../../../java/delivery/push-delivery),
  [`../../../csharp/delivery/push-delivery`](../../../csharp/delivery/push-delivery)
- Docs: [`../../../../docs/guides/push-delivery.md`](../../../../docs/guides/push-delivery.md)

> Push endpoints must be HTTPS — HTTP is permitted **only for localhost**. On dual-stack
> hosts prefer the `127.0.0.1` literal over `localhost` (which may resolve to `::1` first) so
> the connector's POST lands on the IPv4 listener.

> Connectivity: this example uses the emulator-protocol drop-in. See
> [`../../../../docs/guides/connectivity-and-emulator-mode.md`](../../../../docs/guides/connectivity-and-emulator-mode.md)
> for env vars, the no-auth/insecure posture, and the `AdvertisedEndpoint` hint.
