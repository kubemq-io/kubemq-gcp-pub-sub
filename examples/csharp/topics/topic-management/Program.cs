using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;

// ─── topics 3: Topic management ──────────────────────────────────────────────
//
// The topic admin surface: CreateTopic → GetTopic → ListTopics →
// ListTopicSubscriptions → UpdateTopic (FieldMask) → DeleteTopic. DeleteTopic is
// a TOMBSTONE: the record is retained so existing subscriptions survive (they
// detach rather than disappear), which we prove by reading the subscription back
// after the topic is deleted.
//
// The official Google.Cloud.PubSub.V1 client is pointed at the connector purely
// by PUBSUB_EMULATOR_HOST; every client is built with
// EmulatorDetection.EmulatorOnly (the .NET idiom trap). Resource ids are
// uuid-suffixed per run so concurrent runs never collide.

return await Demo.RunAsync(async () =>
{
    var projectId = Demo.ProjectId();

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

    var subDeleted = false;
    var topicDeleted = false;

    // CreateTopic with an initial label and a requested retention.
    var created = await publisher.CreateTopicAsync(new Topic
    {
        TopicName = topicName,
        Labels = { ["env"] = "dev" },
        MessageRetentionDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
    });
    Demo.Step($"Created topic '{topicName.TopicId}' (→ channel pubsub.{topicName.TopicId})");

    try
    {
        // GetTopic echoes the requested (un-clamped) retention and labels.
        var got = await publisher.GetTopicAsync(topicName);
        Demo.Got($"GetTopic name={TopicName.Parse(got.Name).TopicId} " +
                 $"labels.env={got.Labels.GetValueOrDefault("env")} " +
                 $"retention={got.MessageRetentionDuration.ToTimeSpan().TotalHours}h");
        Demo.RequireEqual("dev", got.Labels.GetValueOrDefault("env"), "env label");
        Demo.RequireEqual(
            TimeSpan.FromHours(1), got.MessageRetentionDuration.ToTimeSpan(),
            "requested retention (GetTopic returns the un-clamped value)");

        // ListTopics must include the topic we just created.
        var found = false;
        await foreach (var t in publisher.ListTopicsAsync(ProjectName.FromProject(projectId)))
        {
            if (t.Name == topicName.ToString()) found = true;
        }
        Demo.Step($"ListTopics contains '{topicName.TopicId}' → {found}");
        Demo.Require(found, "freshly created topic missing from ListTopics");

        // Attach a subscription, then ListTopicSubscriptions must report it.
        await subscriber.CreateSubscriptionAsync(subName, topicName, pushConfig: null, ackDeadlineSeconds: 10);
        Demo.Step($"Created subscription '{subName.SubscriptionId}' on the topic");

        var subs = new List<string>();
        await foreach (var s in publisher.ListTopicSubscriptionsAsync(topicName))
        {
            subs.Add(s);
        }
        Demo.Step($"ListTopicSubscriptions → [{string.Join(", ", subs.Select(s => SubscriptionName.Parse(s).SubscriptionId))}]");
        Demo.Require(subs.Contains(subName.ToString()),
            "subscription missing from ListTopicSubscriptions");

        // UpdateTopic via FieldMask: change only `labels`, leave retention untouched.
        var update = new Topic
        {
            TopicName = topicName,
            Labels = { ["env"] = "staging", ["team"] = "orders" },
        };
        var updated = await publisher.UpdateTopicAsync(update, new FieldMask { Paths = { "labels" } });
        Demo.Step($"UpdateTopic labels → env={updated.Labels.GetValueOrDefault("env")}, " +
                  $"team={updated.Labels.GetValueOrDefault("team")}");
        Demo.RequireEqual("staging", updated.Labels.GetValueOrDefault("env"), "updated env label");
        Demo.RequireEqual("orders", updated.Labels.GetValueOrDefault("team"), "added team label");

        // Confirm the label change is durable via a fresh GetTopic.
        var reread = await publisher.GetTopicAsync(topicName);
        Demo.RequireEqual("staging", reread.Labels.GetValueOrDefault("env"), "re-read env label");

        // DeleteTopic is a TOMBSTONE: the topic record is retained so the
        // subscription survives (detaches) rather than being destroyed.
        await publisher.DeleteTopicAsync(topicName);
        topicDeleted = true;
        Demo.Step($"DeleteTopic '{topicName.TopicId}' (tombstone — subscriptions survive)");

        // Prove the subscription still exists after the topic delete.
        var survivor = await subscriber.GetSubscriptionAsync(subName);
        Demo.Got($"Subscription '{subName.SubscriptionId}' survived the topic tombstone " +
                 $"(topic field='{survivor.Topic}')");
        Demo.Require(survivor.SubscriptionName.Equals(subName),
            "subscription did not survive the topic tombstone");

        // Clean the surviving subscription up ourselves.
        await subscriber.DeleteSubscriptionAsync(subName);
        subDeleted = true;
        Demo.Step($"Cleaned up subscription '{subName.SubscriptionId}'");

        Demo.Ok("Topic management lifecycle complete (create/get/list/update/tombstone)");
    }
    finally
    {
        // The topic was deleted in the happy path; on an early failure best-effort
        // remove whatever is still around so re-runs start clean.
        if (!subDeleted)
        {
            await Demo.BestEffort(() => subscriber.DeleteSubscriptionAsync(subName),
                $"Cleaned up subscription '{subName.SubscriptionId}'");
        }
        if (!topicDeleted)
        {
            await Demo.BestEffort(() => publisher.DeleteTopicAsync(topicName),
                $"Cleaned up topic '{topicName.TopicId}'");
        }
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
