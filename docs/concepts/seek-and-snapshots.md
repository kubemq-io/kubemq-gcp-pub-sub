# Seek & Snapshots

## Concept

Because every topic is backed by a durable, replayable **Events Store log** (`pubsub.{t}`), a
subscription can be **rewound**. `Seek` resets a subscription's position to a point in the past —
either a **timestamp** or a saved **snapshot** — and replays the topic log from there into the
subscription's queue. This is how you reprocess messages: redeploy a consumer with a bug fix, then
seek the subscription back to before the bad window and let it re-consume.

## How It Works

A `Seek` against a subscription:

1. **Resolves the start sequence** from the topic log — from a timestamp (the first message
   at/after that time) or from a snapshot's captured cursor.
2. **Purges the subscription queue and drops outstanding leases** — in-flight `ack_id`s become
   invalid (this is the reset).
3. **Replays the topic log** from the start sequence via `Array.RangeEventsStore`, **re-applying
   the subscription's filter** as it fans the replayed messages back into `pubsub.sub.{s}`.

```
Seek(subscription, time|snapshot)
   │  resolve start seq from pubsub.{t}
   ▼
purge sub queue + drop leases  (in-flight ack_ids now invalid)
   │
   ▼
replay pubsub.{t} from start seq ──(re-apply filter)──▶ refill pubsub.sub.{s}
   │
   ▼  bounded by MaxSeekReplay (default 1,000,000) → hit cap = WARN, no silent loss
```

## Timestamp Clamping

> **Gotcha — seeking before the retained window clamps; it is not an error.** A `Seek` to a
> timestamp **older than the earliest retained message** does not fail — it clamps to the
> **earliest retained message** and replays from there. (Retention itself is clamped to the
> broker's `Store.MaxRetention`, so what is "retained" depends on the broker ceiling — see
> [topics-and-publish.md](topics-and-publish.md).) Don't rely on a pre-window seek returning an
> error to detect "too far back"; it silently starts at the oldest available message.

## Replay Cap

A single `Seek` replays at most `CONNECTORS_PUB_SUB_MAX_SEEK_REPLAY` messages (default
**1,000,000**). Hitting the cap logs a **WARN** and stops at the cap — there is **no silent loss**;
you simply do not replay beyond the limit in one seek. Raise the config value or seek in smaller
windows if you need to replay more.

## Snapshots

A **snapshot** captures a subscription's current cursor so you can seek back to it later without
knowing an exact timestamp:

- `CreateSnapshot(subscription)` records the cursor as a registry record.
- `Seek(subscription, snapshot)` rewinds to that captured cursor.
- Snapshots have a **7-day default expiry** and are swept hourly.

> **Gotcha — you cannot snapshot a detached subscription.** `CreateSnapshot` on a subscription
> whose topic has been deleted/detached returns `FAILED_PRECONDITION`. Snapshot before you detach.

## Examples

| Variant | Go | Python | Java | JS/TS | C# | Ruby |
|---------|----|--------|------|-------|----|------|
| seek-timestamp | [go](../../examples/go/advanced/seek-timestamp/) | [python](../../examples/python/advanced/seek_timestamp/) | [java](../../examples/java/advanced/seek-timestamp/) | [js](../../examples/javascript/advanced/seek-timestamp/) | [csharp](../../examples/csharp/advanced/seek-timestamp/) | [ruby](../../examples/ruby/advanced/seek_timestamp/) |
| snapshots | [go](../../examples/go/advanced/snapshots/) | [python](../../examples/python/advanced/snapshots/) | [java](../../examples/java/advanced/snapshots/) | [js](../../examples/javascript/advanced/snapshots/) | [csharp](../../examples/csharp/advanced/snapshots/) | [ruby](../../examples/ruby/advanced/snapshots/) |

## See Also

- [topics-and-publish.md](topics-and-publish.md) — the topic log is the replay source; retention clamping.
- [subscriptions-and-pull.md](subscriptions-and-pull.md) — leases/`ack_id`s that a seek invalidates.
- [../reference/limits-and-rules.md](../reference/limits-and-rules.md) — retention range + `MaxSeekReplay`.
