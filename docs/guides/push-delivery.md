# Push Delivery

A subscription with a `push_config` is delivered **push-style**: instead of the client pulling, a
per-subscription connector worker pulls from the queue and **POSTs** each message to your HTTP(S)
endpoint. This guide covers the envelope shape, the `no_wrapper` mode, optional OIDC auth, the
HTTPS / localhost rule, and the retry → dead-letter behavior.

## Pull ↔ Push

A subscription is either pull or push:

- `CreateSubscription` with a `push_config`, or `ModifyPushConfig` with one, starts a per-subscription
  **delivery worker**.
- `ModifyPushConfig` with an **empty** config returns the subscription to pull.
- Workers start on `CreateSubscription`/`ModifyPushConfig` (push), stop on switch-to-pull or delete,
  and **drain on connector shutdown**.

See [subscribing.md](subscribing.md) for the pull paths.

## The Wrapped Envelope

By default the worker POSTs a **wrapped JSON envelope**:

```json
{
  "message": {
    "data": "<base64>",
    "attributes": { "...": "..." },
    "messageId": "...",
    "publishTime": "...",
    "orderingKey": "..."
  },
  "subscription": "..."
}
```

- `data` is **base64-encoded** (decode it on receipt).
- `attributes` are the user attributes (the reserved `_pubsub_*` tags are not surfaced here).
- `messageId` / `publishTime` / `orderingKey` mirror the message metadata.

### `no_wrapper` mode

When the subscription's push config sets `no_wrapper`, the worker POSTs the **raw message body**
instead of the envelope, with the attributes surfaced as `x-goog-*` headers when configured. Use
this for endpoints that expect the payload directly.

## Acknowledgement

The HTTP response status is the ack signal:

| Endpoint response | Effect |
|-------------------|--------|
| `2xx` | The message is **acked**. |
| Non-`2xx` / timeout | **Retried** with backoff. |
| Retry exhaustion | Republished to the subscription's **dead-letter topic** if one is set, else **dropped**. |

The retry → dead-letter pipeline shares the dead-letter machinery described in
[reliability.md](reliability.md) and
[../concepts/dead-letter-and-retry.md](../concepts/dead-letter-and-retry.md).

## OIDC Authentication

When the push config sets an `oidc_token`, the worker adds an **OIDC JWT** as
`Authorization: Bearer <jwt>`. The token audience defaults to the endpoint URL. This lets your
endpoint verify the request originated from the connector. Without `oidc_token`, no Authorization
header is sent.

## HTTPS / Localhost Rule

> **Endpoints must be HTTPS.** Plain `http://` is allowed **only for localhost** (local
> development). Any non-localhost endpoint must be `https://`. This matches Google's push-endpoint
> requirement and keeps message data off the wire in cleartext.

This is the connector's own transport rule for the **outbound** push and is independent of the
inbound emulator mode (which is insecure gRPC by design — see
[connectivity-and-emulator-mode.md](connectivity-and-emulator-mode.md)).

## The `delivery/push-delivery` Example

The `delivery/push-delivery` example stands up a local HTTP(S) endpoint, creates a subscription
with a `push_config` pointing at it, publishes a message, and shows the wrapped envelope arriving
at the endpoint with a `2xx` ack. It runs against a `localhost` endpoint (where plain HTTP is
permitted).

## Node-Local Caveat

> Push delivery state — the per-subscription worker and its in-flight retries — lives on the node
> that runs the worker. In a cluster, push subscriptions are part of the **node-local** family; use
> a sticky load balancer so a subscription's push worker and its retries stay on one node. See
> [reliability.md](reliability.md) and
> [../reference/migration-from-gcp.md](../reference/migration-from-gcp.md).

## Error Quick Reference

| Trigger | Result |
|---------|--------|
| Non-localhost endpoint over plain HTTP | rejected (HTTPS required) |
| Endpoint returns non-`2xx` / times out | retried with backoff |
| Retries exhausted, DLQ set | republished to the dead-letter topic |
| Retries exhausted, no DLQ | dropped (+ metric) |

## See Also

- [subscribing.md](subscribing.md) — the pull paths and `ModifyPushConfig`.
- [reliability.md](reliability.md) — retry, dead-letter, at-least-once.
- [../concepts/dead-letter-and-retry.md](../concepts/dead-letter-and-retry.md) — the dead-letter concept.
