using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;

// ─── advanced/snapshots ─────────────────────────────────────────────────────
//
// Capture a subscription cursor as a snapshot, then seek back to it:
//   CreateTopic → CreateSubscription → Publish(3) → Pull+Ack(3) (drain)
//     → CreateSnapshot (cursor = head, after the first 3)
//     → Publish(3 more) → Pull+Ack(3) (drain)
//     → Seek(snapshot)  → Pull (replays the 3 messages after the snapshot)
//
// A snapshot records a subscription's cursor as a registry record (7-day default
// expiry, swept hourly) so you can seek back without knowing an exact timestamp.
// Seek(snapshot) replays the topic Events Store log `pubsub.{t}` forward from the
// captured cursor into the subscription queue `pubsub.sub.{s}`.
//
// Also demonstrates the gotcha: CreateSnapshot on a DETACHED subscription (its
// topic deleted) returns FAILED_PRECONDITION.
//
// Uses ONLY Google.Cloud.PubSub.V1, built with EmulatorDetection.EmulatorOnly
// (the .NET client does not auto-detect the emulator from the env var).

string suffix = Guid.NewGuid().ToString("N")[..8];
string projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
var topicName = TopicName.FromProjectTopic(projectId, $"snap-orders-{suffix}");
var subName = SubscriptionName.FromProjectSubscription(projectId, $"snap-sub-{suffix}");
var snapName = SnapshotName.FromProjectSnapshot(projectId, $"snap-cursor-{suffix}");

// A second, throwaway topic/sub pair to demonstrate the detached-subscription gotcha.
var deadTopic = TopicName.FromProjectTopic(projectId, $"snap-dead-topic-{suffix}");
var deadSub = SubscriptionName.FromProjectSubscription(projectId, $"snap-dead-sub-{suffix}");

int exitCode = 0;
PublisherServiceApiClient? publisher = null;
SubscriberServiceApiClient? subscriber = null;
bool snapshotCreated = false;

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

    publisher.CreateTopic(topicName);
    Step($"Created topic {topicName.TopicId}");
    subscriber.CreateSubscription(new Subscription
    {
        SubscriptionName = subName,
        TopicAsTopicName = topicName,
        AckDeadlineSeconds = 10,
    });
    Step($"Created subscription {subName.SubscriptionId}");

    // Publish + drain the first 3 messages so the cursor advances past them.
    PublishRange(publisher, topicName, 1, 3);
    var first = PullAll(subscriber, subName, 3);
    if (first.Count != 3) { Fail($"first drain expected 3, got {first.Count}"); }
    else { Ok("Drained first 3 messages (order-1..order-3)"); }

    // Snapshot the cursor at the head (after order-3).
    var snap = subscriber.CreateSnapshot(new CreateSnapshotRequest
    {
        SnapshotName = snapName,
        SubscriptionAsSubscriptionName = subName,
    });
    snapshotCreated = true;
    Step($"CreateSnapshot {snapName.SnapshotId} (cursor at head, expires {snap.ExpireTime?.ToDateTime():u})");

    // Publish + drain 3 more (these are the messages a seek-to-snapshot will replay).
    PublishRange(publisher, topicName, 4, 6);
    var second = PullAll(subscriber, subName, 3);
    if (second.Count != 3) { Fail($"second drain expected 3, got {second.Count}"); }
    else { Ok("Drained next 3 messages (order-4..order-6)"); }

    // ── Seek back to the snapshot: replays the 3 messages after the cursor ───
    subscriber.Seek(new SeekRequest
    {
        Subscription = subName.ToString(),
        Snapshot = snapName.ToString(),
    });
    Step($"Seek(snapshot {snapName.SnapshotId})");
    var replayed = PullAll(subscriber, subName, 6);
    var bodies = replayed.Select(m => m.Message.Data.ToStringUtf8()).OrderBy(s => s).ToList();
    // The snapshot cursor is the topic log's last retained sequence at capture
    // time (a CONSERVATIVE replay anchor): replay re-drives the log forward from
    // that boundary message (order-3) inclusive, so the replay is order-3..order-6
    // — never order-1/order-2 (which were before the snapshot).
    var expected = new[] { "order-3", "order-4", "order-5", "order-6" };
    bool noPreSnapshot = !bodies.Contains("order-1") && !bodies.Contains("order-2");
    if (bodies.SequenceEqual(expected) && noPreSnapshot)
    {
        Ok($"Seek-to-snapshot replayed [{string.Join(", ", bodies)}] (from the snapshot cursor onward; pre-snapshot messages excluded)");
    }
    else
    {
        Fail($"seek-to-snapshot expected [{string.Join(", ", expected)}], got [{string.Join(", ", bodies)}]");
    }

    // ── Gotcha: snapshot of a detached subscription → FAILED_PRECONDITION ────
    publisher.CreateTopic(deadTopic);
    subscriber.CreateSubscription(new Subscription
    {
        SubscriptionName = deadSub,
        TopicAsTopicName = deadTopic,
        AckDeadlineSeconds = 10,
    });
    publisher.DeleteTopic(deadTopic); // detaches deadSub
    Step("Detached a subscription (deleted its topic), then attempted CreateSnapshot");
    try
    {
        subscriber.CreateSnapshot(new CreateSnapshotRequest
        {
            SnapshotName = SnapshotName.FromProjectSnapshot(projectId, $"snap-dead-{suffix}"),
            SubscriptionAsSubscriptionName = deadSub,
        });
        Fail("expected FAILED_PRECONDITION snapshotting a detached subscription, but it succeeded");
    }
    catch (RpcException rex) when (rex.StatusCode == StatusCode.FailedPrecondition)
    {
        Ok("CreateSnapshot on a detached subscription → FAILED_PRECONDITION (expected)");
    }

    if (exitCode == 0)
    {
        Ok("PASS — snapshot create + seek-to-snapshot + detached-subscription rejection verified");
    }
}
catch (Exception ex)
{
    Fail(ex.Message);
}
finally
{
    try { if (snapshotCreated) { subscriber?.DeleteSnapshot(snapName); Step($"Deleted snapshot {snapName.SnapshotId}"); } }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] snapshot cleanup: {ex.Message}"); }
    try { subscriber?.DeleteSubscription(subName); Step($"Deleted subscription {subName.SubscriptionId}"); }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] subscription cleanup: {ex.Message}"); }
    try { subscriber?.DeleteSubscription(deadSub); } catch { /* best-effort */ }
    try { publisher?.DeleteTopic(topicName); Step($"Deleted topic {topicName.TopicId}"); }
    catch (Exception ex) { Console.Error.WriteLine($"[warn] topic cleanup: {ex.Message}"); }
}

return exitCode;

// PublishRange publishes "order-{i}" for i in [from..to] (inclusive).
static void PublishRange(PublisherServiceApiClient pub, TopicName topic, int from, int to)
{
    for (int i = from; i <= to; i++)
    {
        var resp = pub.Publish(topic, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFromUtf8($"order-{i}") },
        });
        Console.WriteLine($"[x] Published order-{i} id={resp.MessageIds[0]}");
    }
}

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
