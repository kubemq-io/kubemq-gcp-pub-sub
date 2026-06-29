using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

// ─── topics 2: Batch publish ─────────────────────────────────────────────────
//
// One Publish RPC carrying many messages (1..1000). The connector validates the
// WHOLE batch before enqueuing anything (publish atomicity), then returns one
// server message id per message IN REQUEST ORDER. We publish a 5-message batch,
// assert the returned ids line up with the request order, then pull all five
// back and confirm every body/index round-trips.
//
// The official Google.Cloud.PubSub.V1 client is pointed at the connector purely
// by PUBSUB_EMULATOR_HOST; every client is built with
// EmulatorDetection.EmulatorOnly (the .NET idiom trap). Resource ids are
// uuid-suffixed per run so concurrent runs occupy distinct pubsub.{t} /
// pubsub.sub.{s} channels and never cross-talk.

return await Demo.RunAsync(async () =>
{
    var projectId = Demo.ProjectId();
    const int batchSize = 5;

    var run = Guid.NewGuid().ToString("N")[..8];
    var topicName = TopicName.FromProjectTopic(projectId, $"orders-{run}");
    var subName = SubscriptionName.FromProjectSubscription(projectId, $"sub-orders-{run}");

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
        await subscriber.CreateSubscriptionAsync(subName, topicName, pushConfig: null, ackDeadlineSeconds: 10);
        Demo.Step($"Created subscription '{subName.SubscriptionId}' (→ channel pubsub.sub.{subName.SubscriptionId})");

        try
        {
            // Build a batch of distinct messages. Each carries a 1-based `seq`
            // attribute so we can verify request-order id correspondence.
            var messages = new List<PubsubMessage>(batchSize);
            for (var i = 1; i <= batchSize; i++)
            {
                messages.Add(new PubsubMessage
                {
                    Data = ByteString.CopyFromUtf8($"order #{1000 + i}"),
                    Attributes = { ["seq"] = i.ToString() },
                });
            }

            // ONE Publish RPC for the whole batch. The connector validates the
            // entire batch atomically (≤1000 messages, ≤10 MiB/msg, ≤100 attrs);
            // any single invalid message rejects the whole batch with
            // INVALID_ARGUMENT and enqueues nothing.
            var response = await publisher.PublishAsync(topicName, messages);
            Demo.Sent($"Published {response.MessageIds.Count} messages in one Publish RPC");
            Demo.Require(response.MessageIds.Count == batchSize,
                $"expected {batchSize} message ids, got {response.MessageIds.Count}");

            // The ids come back in request order: ids[i] is the id of messages[i].
            var idByBody = new Dictionary<string, string>();
            for (var i = 0; i < batchSize; i++)
            {
                var id = response.MessageIds[i];
                Demo.Require(!string.IsNullOrEmpty(id), $"message {i} got an empty id");
                idByBody[messages[i].Data.ToStringUtf8()] = id;
                Demo.Step($"  ids[{i}] = {id}  ↔  '{messages[i].Data.ToStringUtf8()}'");
            }

            // Pull all five back (one Pull may return fewer than requested, so loop).
            var pulled = new List<ReceivedMessage>();
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (pulled.Count < batchSize && DateTime.UtcNow < deadline)
            {
                var pull = await subscriber.PullAsync(subName, maxMessages: batchSize);
                pulled.AddRange(pull.ReceivedMessages);
                if (pull.ReceivedMessages.Count > 0)
                {
                    await subscriber.AcknowledgeAsync(
                        subName, pull.ReceivedMessages.Select(r => r.AckId));
                }
            }

            Demo.Got($"Pulled and acked {pulled.Count} messages");
            Demo.Require(pulled.Count == batchSize,
                $"expected to pull {batchSize} messages, got {pulled.Count}");

            // Every published body must come back with the matching server id
            // assigned in the Publish response (id ↔ body correspondence).
            foreach (var r in pulled)
            {
                var body = r.Message.Data.ToStringUtf8();
                Demo.Require(idByBody.TryGetValue(body, out var expectedId),
                    $"pulled an unexpected body '{body}'");
                Demo.RequireEqual(expectedId, r.Message.MessageId, $"MessageId for '{body}'");
            }

            Demo.Ok($"Batch publish of {batchSize} messages complete (ids returned in request order)");
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
