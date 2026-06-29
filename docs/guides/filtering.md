# Message Filtering

A subscription may carry a `filter` so it only receives messages whose **attributes** match. The
connector implements a hand-written **CEL-subset** (a strict subset of Google's filter expression
language) — attributes-only, compiled once at create-time, and applied at publish fan-out.

## Where the Filter Runs

A filter is set on `CreateSubscription` and is **compiled once and immutable** thereafter
(`UpdateSubscription` cannot change `filter`). It is applied at **publish fan-out**: when a
`Publish` writes the topic log and fans copies out to each subscription, a message that does not
match a subscription's filter is **never enqueued** for that subscription (it is effectively
auto-acked for it). The topic log `pubsub.{topic}` itself is unfiltered — the filter only governs
which subscription queues receive a copy. See [publishing.md](publishing.md) and
[../reference/channel-mapping.md](../reference/channel-mapping.md).

## Supported Syntax

Filters operate on the message's **`attributes`** map only (not `data`, not the ordering key):

```
attributes:KEY                       -- attribute KEY exists  (KEY may be quoted: attributes:"k")
attributes.KEY = "v"                 -- equality
attributes.KEY != "v"                -- inequality
hasPrefix(attributes.KEY, "p")       -- value has the prefix "p"
AND  OR  NOT  -                      -- boolean operators (NOT and unary - both negate)
( … )                                -- parentheses for grouping
```

Rules:

- **Attributes-only.** There is no `data`-based filtering and no numeric / comparison operators
  beyond `=` / `!=` / `hasPrefix`.
- **≤ 256 characters.** A filter expression longer than 256 chars is rejected.
- **Immutable.** Compiled at `CreateSubscription`; cannot be changed by `UpdateSubscription`.
- **Malformed → `INVALID_ARGUMENT`.** A filter that fails to parse is rejected at create-time, not
  silently ignored.

## Examples

| Goal | Filter |
|------|--------|
| Only messages tagged `region=eu` | `attributes.region = "eu"` |
| Everything except `region=eu` | `attributes.region != "eu"` |
| Has a `priority` attribute (any value) | `attributes:priority` |
| `type=order` **and** high priority | `attributes.type = "order" AND attributes.priority = "high"` |
| EU **or** US region | `attributes.region = "eu" OR attributes.region = "us"` |
| Order events but not test traffic | `attributes.type = "order" AND NOT attributes:test` |
| Event names starting with `user.` | `hasPrefix(attributes.event, "user.")` |
| A quoted key with special chars | `attributes:"x-tenant"` |

The `delivery/message-filtering` example creates two subscriptions on one topic — one with a
filter, one without — publishes a mixed batch, and shows the filtered subscription receives only
the matching subset while the unfiltered one receives everything.

## How Attributes Map

Message `attributes` round-trip as KubeMQ message tags. The connector also carries three reserved
tags (`_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key`); filters operate on the
**user attributes**, not the reserved tags. See [publishing.md](publishing.md#message-attributes).

## Fan-Out Pattern

Filtering is the basis of the fan-out pattern: attach **N subscriptions** to one topic, each with
its own filter, and a single `Publish` is fanned out only to the subscriptions whose filter matches.
The burn-in `fan_out` worker stresses this — one topic, multiple subscriptions, with and without
filters — and gates on per-subscription completeness.

## Error Quick Reference

| Trigger | Result |
|---------|--------|
| Malformed / unparseable filter | `INVALID_ARGUMENT` at `CreateSubscription` |
| Filter > 256 characters | `INVALID_ARGUMENT` |
| Attempt to change `filter` via `UpdateSubscription` | rejected (immutable) |
| Non-attribute (e.g. `data`-based) expression | `INVALID_ARGUMENT` |

## See Also

- [publishing.md](publishing.md) — attributes and the publish fan-out.
- [subscribing.md](subscribing.md) — `CreateSubscription` and the immutable `filter`.
- [../reference/limits-and-rules.md](../reference/limits-and-rules.md) — the 256-char filter limit.
