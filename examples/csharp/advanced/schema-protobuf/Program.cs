using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;

// ─── advanced/schema-protobuf ───────────────────────────────────────────────
//
// Register a Protocol Buffers schema, bind a topic to it, and prove
// enforce-on-publish:
//   CreateSchema(PROTOCOL_BUFFER) → CreateTopic(schema_settings → schema, JSON)
//     → Publish(conforming JSON)        → accepted
//     → Publish(batch w/ 1 bad message) → WHOLE batch rejected INVALID_ARGUMENT
//     → Pull → only the 1 conforming message landed (the bad batch enqueued nothing)
//
// The message `data` is validated against the FIRST message type declared in the
// .proto. Validation is part of the atomic batch check, so the first
// non-conforming message rejects the ENTIRE Publish call and nothing is enqueued
// (spec S3.5 schema).
//
// Uses ONLY Google.Cloud.PubSub.V1. The Publisher/Subscriber builders honour
// EmulatorDetection.EmulatorOnly; the SchemaServiceClientBuilder does NOT expose
// EmulatorDetection, so it is pointed at the emulator explicitly via Endpoint +
// insecure channel credentials.

string suffix = Guid.NewGuid().ToString("N")[..8];
string projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
string emulatorHost = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST") ?? "localhost:8085";
var schemaName = SchemaName.FromProjectSchema(projectId, $"proto-order-{suffix}");
var topicName = TopicName.FromProjectTopic(projectId, $"proto-orders-{suffix}");
var subName = SubscriptionName.FromProjectSubscription(projectId, $"proto-sub-{suffix}");

// Protobuf schema: a single self-contained .proto with one Order message.
const string protoDefinition = """
syntax = "proto3";

message Order {
  string id = 1;
  int32 amount = 2;
}
""";

int exitCode = 0;
PublisherServiceApiClient? publisher = null;
SubscriberServiceApiClient? subscriber = null;
SchemaServiceClient? schemas = null;
bool schemaCreated = false;

void Step(string m) => Console.WriteLine($"[*] {m}");
void Ok(string m) => Console.WriteLine($"[ok] {m}");
void Fail(string m)
{
    Console.Error.WriteLine($"[FAIL] {m}");
    exitCode = 1;
}

try
{
    publisher = new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.Build();
    subscriber = new SubscriberServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.Build();
    // SchemaServiceClientBuilder has no EmulatorDetection — point it at the
    // emulator endpoint with insecure credentials (no auth, no TLS).
    schemas = new SchemaServiceClientBuilder
    {
        Endpoint = emulatorHost,
        ChannelCredentials = ChannelCredentials.Insecure,
    }.Build();

    // CreateSchema(PROTOCOL_BUFFER) — the definition must be a valid .proto and ≤ 300 KB.
    var schema = schemas.CreateSchema(new CreateSchemaRequest
    {
        Parent = $"projects/{projectId}",
        SchemaId = schemaName.SchemaId,
        Schema = new Schema { Type = Schema.Types.Type.ProtocolBuffer, Definition = protoDefinition },
    });
    schemaCreated = true;
    Step($"CreateSchema {schemaName.SchemaId} (PROTOCOL_BUFFER, revision {schema.RevisionId})");

    // Bind a topic to the schema with JSON encoding (message `data` is JSON text
    // validated against the proto message type on every Publish).
    publisher.CreateTopic(new Topic
    {
        TopicName = topicName,
        SchemaSettings = new SchemaSettings
        {
            SchemaAsSchemaName = schemaName,
            Encoding = Encoding.Json,
        },
    });
    Step($"Created topic {topicName.TopicId} bound to schema (JSON encoding)");

    subscriber.CreateSubscription(new Subscription
    {
        SubscriptionName = subName,
        TopicAsTopicName = topicName,
        AckDeadlineSeconds = 10,
    });
    Step($"Created subscription {subName.SubscriptionId}");

    // ── Conforming publish: accepted ────────────────────────────────────────
    var good = publisher.Publish(topicName, new[]
    {
        new PubsubMessage { Data = ByteString.CopyFromUtf8("""{"id":"A-1","amount":100}""") },
    });
    Ok($"Conforming message accepted id={good.MessageIds[0]}");

    // ── Non-conforming BATCH: rejected whole (INVALID_ARGUMENT) ─────────────
    // The batch mixes one valid message with one whose `amount` is a string, not
    // an int32. Because publish is atomic, the entire batch is rejected and
    // NOTHING is enqueued — not even the valid message.
    Step("Publishing a batch with one schema-invalid message (amount is a string)…");
    try
    {
        publisher.Publish(topicName, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFromUtf8("""{"id":"B-1","amount":200}""") },
            new PubsubMessage { Data = ByteString.CopyFromUtf8("""{"id":"B-2","amount":"not-a-number"}""") },
        });
        Fail("expected INVALID_ARGUMENT for the schema-invalid batch, but it succeeded");
    }
    catch (RpcException rex) when (rex.StatusCode == StatusCode.InvalidArgument)
    {
        Ok("Schema-invalid batch rejected whole → INVALID_ARGUMENT (nothing enqueued)");
    }

    // ── Verify only the conforming message landed ───────────────────────────
    var received = PullAll(subscriber, subName, 5);
    var bodies = received.Select(m => m.Message.Data.ToStringUtf8()).ToList();
    if (bodies.Count == 1 && bodies[0] == """{"id":"A-1","amount":100}""")
    {
        Ok($"Pulled exactly 1 message (the conforming one); the rejected batch enqueued nothing");
    }
    else
    {
        Fail($"expected exactly 1 conforming message, got {bodies.Count}: [{string.Join(", ", bodies)}]");
    }

    if (exitCode == 0)
    {
        Ok("PASS — Protobuf schema enforce-on-publish (accept conforming, reject batch on first non-conforming) verified");
    }
}
catch (Exception ex)
{
    Fail(ex.Message);
}
finally
{
    try { subscriber?.DeleteSubscription(subName); Step($"Deleted subscription {subName.SubscriptionId}"); }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] subscription cleanup: {ex.Message}"); }
    try { publisher?.DeleteTopic(topicName); Step($"Deleted topic {topicName.TopicId}"); }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] topic cleanup: {ex.Message}"); }
    try { if (schemaCreated) { schemas?.DeleteSchema(schemaName); Step($"Deleted schema {schemaName.SchemaId}"); } }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] schema cleanup: {ex.Message}"); }
}

return exitCode;

// PullAll pulls until `max` messages are collected or two consecutive empty
// pulls occur, acknowledging each batch. Returns the received messages.
static List<ReceivedMessage> PullAll(SubscriberServiceApiClient sub, SubscriptionName name, int max)
{
    var all = new List<ReceivedMessage>();
    int emptyStreak = 0;
    while (all.Count < max && emptyStreak < 2)
    {
        var pull = sub.Pull(name, maxMessages: max);
        if (pull.ReceivedMessages.Count == 0)
        {
            emptyStreak++;
            continue;
        }
        emptyStreak = 0;
        all.AddRange(pull.ReceivedMessages);
        sub.Acknowledge(name, pull.ReceivedMessages.Select(m => m.AckId));
    }
    return all;
}
