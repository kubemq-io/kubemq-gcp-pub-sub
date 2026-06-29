# Ruby — KubeMQ GCP Pub/Sub examples

Native **Google Cloud Pub/Sub client for Ruby** (`google-cloud-pubsub`) examples
that talk to the **KubeMQ GCP Pub/Sub connector** by exporting one environment
variable. This is **not** an SDK — it is native `google-cloud-pubsub` apps
pointed at KubeMQ.

> Set `PUBSUB_EMULATOR_HOST` to the connector's listener and the Google client
> "just works" — it skips Google auth and speaks the Pub/Sub gRPC emulator
> protocol to KubeMQ. **Zero code changes.**

Shared conventions (the 15-variant master table, the env vars, the per-example
README template, the gotchas) live in
[`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md). Every variant has its own
`README.md` next to its `main.rb`.

## Prerequisites

- **Ruby** — built/verified on **3.3.7** (rbenv). The native KubeMQ gem (`kubemq`,
  used only by the interop variant) requires **Ruby ≥ 3.1**, so 3.1+ is the floor;
  see the **Toolchain note** below.
- **Google Cloud Pub/Sub client for Ruby:** `google-cloud-pubsub` (latest stable
  floor pinned `~> 3.3` in `Gemfile`; the exact resolved version is captured in
  the gitignored `Gemfile.lock`).
- **Native KubeMQ SDK (interop variant only):** gem `kubemq` `~> 1.0` (`1.0.0`),
  which provides `KubeMQ::PubSub::PubSubClient#subscribe_to_events_store` for the
  native gRPC half. Pinned in `Gemfile`. Every other variant uses only
  `google-cloud-pubsub`.
- A running KubeMQ broker with the **GCP Pub/Sub connector** enabled, reachable
  at `PUBSUB_EMULATOR_HOST` (the Pub/Sub emulator-protocol gRPC listener).
- The `interop/native_events_store` variant additionally needs the KubeMQ
  **native gRPC broker** (`localhost:50000`) for its native half — it consumes
  the topic's Events Store log `pubsub.{topic}` via the `kubemq` gem.

## Setup

```bash
export PUBSUB_EMULATOR_HOST="localhost:8085"   # the connector's Pub/Sub listener
export PUBSUB_PROJECT_ID="kubemq"              # any project id; NOT enforced
# (GOOGLE_CLOUD_PROJECT is honored too)

eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7 (see Toolchain note)
bundle install
```

If `bundle` wants to install into a system gem path, vendor the gems locally:

```bash
bundle config --local path vendor/bundle
bundle install
```

`Gemfile.lock` and `vendor/bundle/` are gitignored (AWS convention).

## Run a variant

```bash
# topics
bundle exec ruby topics/basic_publish_subscribe/main.rb
bundle exec ruby topics/batch_publish/main.rb
bundle exec ruby topics/topic_management/main.rb

# subscriptions
bundle exec ruby subscriptions/streaming_pull/main.rb
bundle exec ruby subscriptions/ack_deadline_nack/main.rb

# delivery
bundle exec ruby delivery/ordering_keys/main.rb
bundle exec ruby delivery/message_filtering/main.rb
bundle exec ruby delivery/dead_letter_topic/main.rb
bundle exec ruby delivery/push_delivery/main.rb
bundle exec ruby delivery/exactly_once/main.rb

# advanced
bundle exec ruby advanced/seek_timestamp/main.rb
bundle exec ruby advanced/snapshots/main.rb
bundle exec ruby advanced/schema_avro/main.rb
bundle exec ruby advanced/schema_protobuf/main.rb

# interop (native half via the kubemq gem over gRPC at localhost:50000)
bundle exec ruby interop/native_events_store/main.rb
```

Each script prints human-readable progress, asserts the documented behavior, and
**exits non-zero on any failure**.

## The 15 variants

| Group | Variant (dir) | What it demonstrates |
|-------|---------------|----------------------|
| topics | [`basic_publish_subscribe`](topics/basic_publish_subscribe) | CreateTopic → CreateSubscription → Publish → Pull → Ack |
| topics | [`batch_publish`](topics/batch_publish) | Batch publish (≤1000); message ids in request order |
| topics | [`topic_management`](topics/topic_management) | List/Get/UpdateTopic; DeleteTopic tombstone; ListTopicSubscriptions |
| subscriptions | [`streaming_pull`](subscriptions/streaming_pull) | StreamingPull; flow control; periodic reconnect |
| subscriptions | [`ack_deadline_nack`](subscriptions/ack_deadline_nack) | ModifyAckDeadline extend + 0-nack redelivery |
| delivery | [`ordering_keys`](delivery/ordering_keys) | `enable_message_ordering`; per-key in-order delivery |
| delivery | [`message_filtering`](delivery/message_filtering) | CEL-subset attribute filter applied at subscription create |
| delivery | [`dead_letter_topic`](delivery/dead_letter_topic) | `dead_letter_topic` + `max_delivery_attempts` (5..100) |
| delivery | [`push_delivery`](delivery/push_delivery) | `push_config` wrapped envelope to a local HTTPS/localhost endpoint |
| delivery | [`exactly_once`](delivery/exactly_once) | `enable_exactly_once_delivery`; ack confirmation / ErrorInfo handling |
| advanced | [`seek_timestamp`](advanced/seek_timestamp) | Seek to a timestamp; replay |
| advanced | [`snapshots`](advanced/snapshots) | CreateSnapshot → Seek-to-snapshot |
| advanced | [`schema_avro`](advanced/schema_avro) | CreateSchema (Avro) + topic `schema_settings`; enforce-on-publish |
| advanced | [`schema_protobuf`](advanced/schema_protobuf) | CreateSchema (Protobuf) + enforce-on-publish |
| interop | [`native_events_store`](interop/native_events_store) | Pub/Sub publish → consume natively from Events Store `pubsub.{t}` via the **native KubeMQ SDK** |

## Idiom notes

- **`PUBSUB_EMULATOR_HOST` is the whole story.** When set, the Google client
  routes all RPCs to that host over plaintext gRPC and skips Google credentials
  entirely — no service account, no `GOOGLE_APPLICATION_CREDENTIALS`. The
  `google-cloud-pubsub` constructor only needs a project id (any value;
  `PUBSUB_PROJECT_ID` / `GOOGLE_CLOUD_PROJECT`).
- **Project id is not enforced.** The connector ignores the project segment; use
  any value (`"kubemq"` here). Resource names are ephemeral/uuid-based so
  concurrent runs don't collide.
- **Errors** raise `Google::Cloud::Error` subclasses (e.g.
  `Google::Cloud::NotFoundError`, `Google::Cloud::InvalidArgumentError`,
  `Google::Cloud::FailedPreconditionError`).
- **Channel mapping.** A topic `X` maps to the KubeMQ Events Store log
  `pubsub.X`; each subscription maps to a per-sub queue `pubsub.sub.{s}`.
- **Interop native half (native gRPC subscribe).** Unlike the sibling
  `kubemq-aws` Ruby examples — whose interop variant used the KubeMQ REST queue
  gateway because no native Ruby gRPC SDK then existed — the `kubemq` gem v1.0.0
  now ships a native gRPC Events Store subscribe. `interop/native_events_store`
  subscribes with start policy **`START_NEW_ONLY`** and **establishes the
  subscription before** issuing the Pub/Sub publish, so the published message is
  deterministically in-window. Documented in that variant's README.

### Toolchain note (RECORDED CONSTRAINT)

The login-shell default `ruby` is the macOS **system Ruby 2.6.10**. These
examples target a modern CI floor and were **built and verified on
rbenv-provided Ruby 3.3.7** (the `rbenv global`). The native `kubemq` gem
requires **Ruby ≥ 3.1**, so 3.1 is the hard floor (reflected in
`.rubocop.yml` `TargetRubyVersion: 3.1`). Activate the toolchain with:

```bash
eval "$(rbenv init - zsh)"   # selects Ruby 3.3.7 (rbenv global)
ruby --version               # ruby 3.3.7
```

## Lint / Format

```bash
bundle exec rubocop -a            # auto-fix (config in .rubocop.yml, TargetRubyVersion 3.1)
# or, toolchain-free syntax check:
find . -name "*.rb" -not -path "./vendor/*" -exec ruby -c {} +
```

---

Connectivity: the connector speaks the Pub/Sub emulator protocol — point any
Google Pub/Sub client at it with `PUBSUB_EMULATOR_HOST` and it works with zero
code changes. See
[`../../docs/guides/connectivity-and-emulator-mode.md`](../../docs/guides/connectivity-and-emulator-mode.md).
