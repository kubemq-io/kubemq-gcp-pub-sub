using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

// ─── advanced/seek-timestamp ────────────────────────────────────────────────
//
// Seek a subscription back to a wall-clock instant and replay the topic log:
//   CreateTopic → CreateSubscription → Publish(6) → Pull+Ack(6) (drain)
//     → Seek(time = T3)  → Pull (replays msgs published at/after T3)
//     → Seek(time = far past)  → clamps to earliest retained (replays all 6)
//
// Every topic maps to the Events Store log `pubsub.{t}` (the durable, replayable
// source); `Seek(time)` resolves the first sequence at/after the instant, purges
// the subscription queue, drops in-flight leases, then re-fans the log forward
// via Array.RangeEventsStore. A seek to a time BEFORE the retained window does
// NOT error — it clamps to the earliest retained message (spec S3.5 gotcha #8).
//
// Uses ONLY Google.Cloud.PubSub.V1. The .NET client does not auto-detect the
// emulator from PUBSUB_EMULATOR_HOST the way Go/Python/Node do, so every client
// is built with EmulatorDetection.EmulatorOnly (the C# idiom trap).

// ── unique-per-run resource names so concurrent runs never cross-talk ────────
// (channel-isolation rule: a bare `orders` topic is always channel `pubsub.orders`
//  globally, so two runs sharing it would drain each other's messages.)
string suffix = Guid.NewGuid().ToString("N")[..8];
string projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
var topicName = TopicName.FromProjectTopic(projectId, $"seek-ts-orders-{suffix}");
var subName = SubscriptionName.FromProjectSubscription(projectId, $"seek-ts-sub-{suffix}");

int exitCode = 0;
PublisherServiceApiClient? publisher = null;
SubscriberServiceApiClient? subscriber = null;

void Step(string m) => Console.WriteLine($"[*] {m}");
void Ok(string m) => Console.WriteLine($"[ok] {m}");
void Fail(string m)
{
    Console.Error.WriteLine($"[FAIL] {m}");
    exitCode = 1;
}

try
{
    // EmulatorDetection.EmulatorOnly: required for the .NET client to honour
    // PUBSUB_EMULATOR_HOST (clears credentials, uses insecure gRPC).
    publisher = new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.Build();
    subscriber = new SubscriberServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.Build();

    publisher.CreateTopic(topicName);
    Step($"Created topic {topicName.TopicId}");

    subscriber.CreateSubscription(new Subscription
    {
        SubscriptionName = subName,
        TopicAsTopicName = topicName,
        AckDeadlineSeconds = 10,
    });
    Step($"Created subscription {subName.SubscriptionId}");

    // Publish 6 messages, recording the publish-time boundary BEFORE message #3
    // so we can later seek back to exactly that instant.
    const int total = 6;
    Timestamp seekFrom = Timestamp.FromDateTime(DateTime.UtcNow);
    for (int i = 1; i <= total; i++)
    {
        if (i == 3)
        {
            // Boundary captured just before publishing #3 → seek replays 3..6.
            seekFrom = Timestamp.FromDateTime(DateTime.UtcNow);
            await Task.Delay(50);
        }
        var resp = publisher.Publish(topicName, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFromUtf8($"order-{i}") },
        });
        await Task.Delay(30);
        Console.WriteLine($"[x] Published order-{i} id={resp.MessageIds[0]}");
    }

    // Drain all 6 once so the subscription cursor is at the head.
    var drained = PullAll(subscriber, subName, total);
    if (drained.Count != total)
    {
        Fail($"initial drain expected {total} messages, got {drained.Count}");
    }
    else
    {
        Ok($"Initial drain delivered all {total} messages");
    }

    // ── Seek to the boundary instant: replays the messages at/after T3 ───────
    subscriber.Seek(new SeekRequest { Subscription = subName.ToString(), Time = seekFrom });
    Step($"Seek(time = boundary before order-3)");
    var replayed = PullAll(subscriber, subName, total);
    var bodies = replayed.Select(m => m.Message.Data.ToStringUtf8()).OrderBy(s => s).ToList();
    var expected = new[] { "order-3", "order-4", "order-5", "order-6" };
    if (bodies.SequenceEqual(expected))
    {
        Ok($"Seek-to-timestamp replayed [{string.Join(", ", bodies)}] (4 messages at/after the boundary)");
    }
    else
    {
        Fail($"seek-to-timestamp expected [{string.Join(", ", expected)}], got [{string.Join(", ", bodies)}]");
    }

    // ── Seek to a pre-window timestamp: clamps to earliest retained, NOT error ─
    var farPast = Timestamp.FromDateTime(DateTime.UtcNow.AddYears(-5));
    subscriber.Seek(new SeekRequest { Subscription = subName.ToString(), Time = farPast });
    Step("Seek(time = 5 years ago) → clamps to earliest retained (not an error)");
    var clamped = PullAll(subscriber, subName, total);
    if (clamped.Count == total)
    {
        Ok($"Pre-window seek clamped to earliest retained → replayed all {total} messages");
    }
    else
    {
        Fail($"pre-window clamp expected {total} messages, got {clamped.Count}");
    }

    if (exitCode == 0)
    {
        Ok("PASS — seek-to-timestamp replay + pre-window clamp verified");
    }
}
catch (Exception ex)
{
    Fail(ex.Message);
}
finally
{
    // Best-effort cleanup so re-runs start clean (must not fail the run).
    try { subscriber?.DeleteSubscription(subName); Step($"Deleted subscription {subName.SubscriptionId}"); }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] subscription cleanup: {ex.Message}"); }
    try { publisher?.DeleteTopic(topicName); Step($"Deleted topic {topicName.TopicId}"); }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] topic cleanup: {ex.Message}"); }
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
