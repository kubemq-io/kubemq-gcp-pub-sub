# Getting Started

Get a message flowing through the KubeMQ Google Cloud Pub/Sub connector in minutes.

## Connection Assumption

This repo assumes a **running KubeMQ server with the Pub/Sub connector enabled**
(`CONNECTORS_GCP_ENABLE=true`, on port **8085**). There is **no Pub/Sub emulator
to install, no docker-compose, and no boot-the-server step** here — the connector *is* the
emulator. Point your Pub/Sub SDK at the existing connector.

Because every official Pub/Sub client library (Go, Python, Java, Node.js, C#, Ruby) and `gcloud`
honour the standard `PUBSUB_EMULATOR_HOST` environment variable, pointing an unmodified
application at KubeMQ requires **zero code changes** — set the env var and a project id, and the
SDK clears its Google credentials, skips Google auth, and dials insecure gRPC:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085     # connector default gRPC port; SDK uses the insecure path
export PUBSUB_PROJECT_ID=my-project            # any id; the project segment is parsed but ignored
# Some clients and gcloud also read this alias:
# export GOOGLE_CLOUD_PROJECT=my-project
```

There is **no bespoke wrapper variable** — the standard Google env var *is* the contract, and that
zero-code-change drop-in is the connector's headline value proposition.

> **Project id is parsed but ignored (gotcha #2).** The connector validates the `projects/{p}`
> segment but is **single-tenant** (like the emulator): resource ids are **global across
> projects**. Any project id works; topic `orders` is always Events Store channel `pubsub.orders`
> regardless of project.

> **No auth, no TLS (gotcha #3).** When `PUBSUB_EMULATOR_HOST` is set, the SDK clears its Google
> credentials and uses **insecure gRPC** — exactly as against Google's local emulator. The
> connector enforces no Google OAuth2/JWT, no IAM, and no TLS. **Do not expose port 8085 to
> untrusted networks.** DoS guards (`MaxMessageBytes`, `MaxInflightPerSubscription`,
> `MaxConcurrentPolls`, `MaxSeekReplay`, push backoff) stay active. See
> [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

> **gRPC only (gotcha #9).** There is **no REST/JSON v1**. Use a gRPC client library or `gcloud`
> (which uses gRPC for the emulator).

## Topic → `pubsub.{t}` / Subscription → `pubsub.sub.{s}` Channel Mapping

Every Pub/Sub topic maps onto a native KubeMQ **Events Store** log, and every subscription onto a
native **Queue** channel:

| Pub/Sub resource | KubeMQ object | Channel |
|------------------|---------------|---------|
| Topic `orders` | Events Store log | `pubsub.orders` |
| Topic `events` | Events Store log | `pubsub.events` |
| Subscription `sub-orders` | Queue | `pubsub.sub.sub-orders` |

A `Publish` writes **once** to the topic log (the authoritative, cross-protocol, replayable source)
and fans out one queue copy per subscription, applying each subscription's filter. So Pub/Sub
producers and native gRPC/REST consumers interoperate on the same messages. See
[architecture.md](architecture.md) and [reference/channel-mapping.md](reference/channel-mapping.md).

## `gcloud` Quickstart (No SDK Code)

With the env vars exported above, `gcloud pubsub` talks straight to the connector:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=my-project

gcloud pubsub topics create orders
gcloud pubsub subscriptions create sub-orders --topic=orders
gcloud pubsub topics publish orders --message="hello kubemq"
gcloud pubsub subscriptions pull sub-orders --auto-ack
```

## First Message in Go (CreateTopic → CreateSubscription → Publish → Pull → Ack)

The `topics/basic-publish-subscribe` variant runs the full round-trip. The flow is identical in
every language:

1. `CreateTopic("orders")` → maps to KubeMQ Events Store log `pubsub.orders`.
2. `CreateSubscription("sub-orders", topic="orders")` → maps to KubeMQ Queue `pubsub.sub.sub-orders`.
3. `Publish` a message → returns a server-assigned `<message-id>` (the `Publish` is written **once**
   to the topic log, then fanned out one queue copy to the subscription).
4. `Pull` (or `StreamingPull`) → returns the message + an opaque `ack_id`.
5. `Acknowledge(ack_id)` → acks the message off the subscription queue.

A minimal Go program (the SDK reads `PUBSUB_EMULATOR_HOST` and dials insecurely — no explicit
emulator flag):

```go
ctx := context.Background()
client, _ := pubsub.NewClient(ctx, os.Getenv("PUBSUB_PROJECT_ID"))
defer client.Close()

topic, _ := client.CreateTopic(ctx, "orders")                         // → pubsub.orders
sub, _ := client.CreateSubscription(ctx, "sub-orders", pubsub.SubscriptionConfig{Topic: topic})

id, _ := topic.Publish(ctx, &pubsub.Message{Data: []byte("hello kubemq")}).Get(ctx)
fmt.Println("published:", id)                                          // <message-id>

cctx, cancel := context.WithCancel(ctx)
sub.Receive(cctx, func(_ context.Context, m *pubsub.Message) {         // Pull/StreamingPull
    fmt.Println("received:", string(m.Data))
    m.Ack()                                                            // Acknowledge
    cancel()
})
```

Run it (and the same variant in any language):

| Language | GCP Pub/Sub SDK | Run command |
|----------|-----------------|-------------|
| Go | `cloud.google.com/go/pubsub` | `cd examples/go && go run ./topics/basic-publish-subscribe` |
| Python | `google-cloud-pubsub` (uv) | `cd examples/python && uv run python -m topics.basic_publish_subscribe` |
| Java | `com.google.cloud:google-cloud-pubsub` (BOM) | `cd examples/java && mvn -q exec:java -Dexec.mainClass=...` |
| JavaScript / TS | `@google-cloud/pubsub` | `cd examples/javascript && npx tsx topics/basic-publish-subscribe/index.ts` |
| C# / .NET 8 | `Google.Cloud.PubSub.V1` | `cd examples/csharp && dotnet run --project topics/basic-publish-subscribe/<Project>.csproj` |
| Ruby | `google-cloud-pubsub` | `cd examples/ruby && bundle exec ruby topics/basic_publish_subscribe/main.rb` |

> **Channel isolation.** Examples uuid-suffix their resource ids (e.g. `orders-<uuid8>`) so
> concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do not cross-talk. See
> [`../examples/SHARED-CONVENTIONS.md`](../examples/SHARED-CONVENTIONS.md) §1.

> C# and Ruby need an explicit emulator flag — C# sets
> `EmulatorDetection = EmulatorDetection.EmulatorOnly` on the client builders; Ruby passes
> `emulator_host: ENV["PUBSUB_EMULATOR_HOST"]`. All other languages auto-detect from the env var.
> See [guides/connectivity-and-emulator-mode.md](guides/connectivity-and-emulator-mode.md).

## Next Steps

- [architecture.md](architecture.md) — the gRPC listener, interceptor chain, and fan-out model.
- [configuration.md](configuration.md) — the 11 `CONNECTORS_PUB_SUB_*` env vars and defaults.
- [concepts/topics-and-publish.md](concepts/topics-and-publish.md) — topic → `pubsub.{t}`, batch
  publish, atomic batch validation, server-assigned ids / publish-time.
- [concepts/subscriptions-and-pull.md](concepts/subscriptions-and-pull.md) — subscription →
  `pubsub.sub.{s}`, Pull vs StreamingPull, ack-deadline leases, ack / nack.
- [../examples/README.md](../examples/README.md) — all 15 variants in 6 languages.
