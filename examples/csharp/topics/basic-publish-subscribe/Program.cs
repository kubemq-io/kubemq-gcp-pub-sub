using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

// ─── topics 1: Basic publish / subscribe ─────────────────────────────────────
//
// The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
//   CreateTopic → CreateSubscription → Publish → Pull → Acknowledge.
//
// The official Google.Cloud.PubSub.V1 client is pointed at the connector purely
// by PUBSUB_EMULATOR_HOST (no Google auth, insecure gRPC). Unlike the Go/Python/
// Node clients, the .NET client does NOT auto-detect the emulator from the env
// var — every client is built with EmulatorDetection.EmulatorOnly.
//
// Topic "t" maps to Events Store log "pubsub.{t}" (the authoritative, replayable,
// cross-protocol source); subscription "s" maps to queue "pubsub.sub.{s}".
// Resource ids are uuid-suffixed per run so concurrent runs occupy distinct
// channels and never cross-talk.

return await Demo.RunAsync(async () =>
{
    var projectId = Demo.ProjectId();

    // Unique-per-run resource names (channel-isolation rule): distinct
    // pubsub.{t} / pubsub.sub.{s} channels per run, no collisions across
    // languages or with the burn-in harness.
    var run = Guid.NewGuid().ToString("N")[..8];
    var topicName = TopicName.FromProjectTopic(projectId, $"orders-{run}");
    var subName = SubscriptionName.FromProjectSubscription(projectId, $"sub-orders-{run}");

    // Build both API clients with explicit emulator detection (the .NET idiom trap).
    var publisher = await new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.BuildAsync();
    var subscriber = await new SubscriberServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.BuildAsync();

    await publisher.CreateTopicAsync(topicName);
    Demo.Step($"Created topic '{topicName.TopicId}' (→ channel pubsub.{topicName.TopicId})");

    try
    {
        // CreateSubscription wires the sub's queue (pubsub.sub.{s}) to the topic log.
        await subscriber.CreateSubscriptionAsync(subName, topicName, pushConfig: null, ackDeadlineSeconds: 10);
        Demo.Step($"Created subscription '{subName.SubscriptionId}' (→ channel pubsub.sub.{subName.SubscriptionId})");

        try
        {
            // Publish a single message with a user attribute. The connector writes
            // it once to the topic log, then fans out one queue copy per sub.
            var msg = new PubsubMessage
            {
                Data = ByteString.CopyFromUtf8("order #1001"),
                Attributes = { ["priority"] = "high" },
            };
            var publishResponse = await publisher.PublishAsync(topicName, new[] { msg });
            var messageId = publishResponse.MessageIds[0];
            Demo.Sent($"Published MessageId={messageId}");
            Demo.Require(!string.IsNullOrEmpty(messageId), "server did not assign a MessageId");

            // Pull the message back, retrying briefly while the publish fan-out
            // lands in the sub queue (pubsub.sub.{s}).
            PullResponse? pull = null;
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < deadline)
            {
                pull = await subscriber.PullAsync(subName, maxMessages: 1);
                if (pull.ReceivedMessages.Count > 0) break;
            }
            Demo.Require(pull is { ReceivedMessages.Count: 1 },
                $"expected 1 message, got {pull?.ReceivedMessages.Count ?? 0}");

            var received = pull!.ReceivedMessages[0];
            var body = received.Message.Data.ToStringUtf8();
            Demo.Got($"Pulled '{body}' MessageId={received.Message.MessageId}");

            // Assert the body, server id, attribute, and the server-stamped publish time.
            Demo.RequireEqual("order #1001", body, "message body");
            Demo.RequireEqual(messageId, received.Message.MessageId, "MessageId");
            Demo.RequireEqual("high", received.Message.Attributes["priority"], "priority attribute");
            Demo.Require(received.Message.PublishTime is not null, "missing server publish_time");

            // The three reserved pubsub tags are stripped from `attributes` for
            // Pub/Sub clients (native consumers still see them) — assert no leak.
            Demo.Require(!received.Message.Attributes.ContainsKey("_pubsub_message_id"),
                "reserved tag _pubsub_message_id leaked into attributes");

            // Acknowledge by ack_id (subscription-owned lease).
            await subscriber.AcknowledgeAsync(subName, new[] { received.AckId });
            Demo.Step("Acknowledged the message by ack_id");

            Demo.Ok("Basic publish/subscribe round-trip complete");
        }
        finally
        {
            await Demo.BestEffort(() => subscriber.DeleteSubscriptionAsync(subName),
                $"Cleaned up subscription '{subName.SubscriptionId}'");
        }
    }
    finally
    {
        await Demo.BestEffort(() => publisher.DeleteTopicAsync(topicName),
            $"Cleaned up topic '{topicName.TopicId}'");
    }
});

/// <summary>
/// Console-output + assertion helpers (inlined per example — the examples carry
/// no shared project). Examples are runnable proofs: each prints clear progress
/// and exits non-zero on any failed assertion or unexpected error.
/// </summary>
internal static class Demo
{
    public static void Step(string m) => Console.WriteLine($"[*] {m}");
    public static void Sent(string m) => Console.WriteLine($"[x] {m}");
    public static void Got(string m) => Console.WriteLine($"[v] {m}");
    public static void Ok(string m) => Console.WriteLine($"[ok] {m}");

    /// <summary>The project id is parsed but ignored by the connector; any id works.</summary>
    public static string ProjectId() =>
        Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID")
        ?? Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT")
        ?? "my-project";

    public static void Require(bool condition, string message)
    {
        if (!condition) throw new DemoFailure(message);
    }

    public static void RequireEqual<T>(T expected, T actual, string what)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            throw new DemoFailure($"{what}: expected '{expected}', got '{actual}'");
    }

    /// <summary>Best-effort cleanup that never fails the run if the resource is already gone.</summary>
    public static async Task BestEffort(Func<Task> action, string okMessage)
    {
        try
        {
            await action();
            Step(okMessage);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[warn] cleanup skipped: {ex.GetType().Name}: {ex.Message}");
        }
    }

    public static async Task<int> RunAsync(Func<Task> body)
    {
        try
        {
            await body();
            return 0;
        }
        catch (DemoFailure ex)
        {
            Console.Error.WriteLine($"[FAIL] {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ERROR] {ex.GetType().Name}: {ex.Message}");
            return 1;
        }
    }
}

internal sealed class DemoFailure(string message) : Exception(message);
