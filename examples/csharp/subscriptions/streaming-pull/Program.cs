using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

// ─── Subscriptions 4: Streaming pull (flow control + ack over the duplex stream) ───
//
// Open a StreamingPull bidirectional stream against the KubeMQ Pub/Sub connector,
// publish a small batch, receive every message over the long-lived stream applying
// per-stream flow control (MaxOutstandingMessages), and ack the delivered ack_ids
// back through the same stream.
//
// A topic `t` maps to the Events Store log `pubsub.{t}`; the subscription `s` maps
// to the per-sub queue `pubsub.sub.{s}`. StreamingPull reads from that queue through
// a poller and leases each delivered message under an ack-deadline lease. Resource
// ids are uuid-suffixed per run so concurrent runs occupy distinct channels and
// never cross-talk (see ../../../SHARED-CONVENTIONS.md §1).

return await Demo.RunAsync(async () =>
{
    // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels.
    var suffix = Guid.NewGuid().ToString("N")[..8];
    var topicId = $"orders-{suffix}";
    var subId = $"sub-orders-{suffix}";
    const int messageCount = 5;

    // EmulatorDetection.EmulatorOnly: the .NET client does NOT auto-detect the
    // emulator from PUBSUB_EMULATOR_HOST the way Go/Python/Node do — it must be
    // told explicitly. With the env var set it clears credentials and uses
    // insecure gRPC (idiom trap; see ../../README.md).
    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    var publisher = await new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.BuildAsync();
    var subscriber = await new SubscriberServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly,
    }.BuildAsync();

    var topicName = TopicName.FromProjectTopic(projectId, topicId);
    var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subId);

    await publisher.CreateTopicAsync(topicName);
    Demo.Step($"Created topic '{topicId}' → channel pubsub.{topicId}");

    // ackDeadlineSeconds 10 = connector minimum (0 or 10..600).
    await subscriber.CreateSubscriptionAsync(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 10);
    Demo.Step($"Created subscription '{subId}' → channel pubsub.sub.{subId}");

    try
    {
        // Publish a small batch; ids come back in request order.
        var messages = Enumerable.Range(1, messageCount).Select(i => new PubsubMessage
        {
            Data = ByteString.CopyFrom($"order #{1000 + i}", System.Text.Encoding.UTF8),
            Attributes = { ["seq"] = i.ToString() },
        });
        var publish = await publisher.PublishAsync(topicName, messages);
        Demo.Sent($"Published {publish.MessageIds.Count} messages (ids in request order)");
        Demo.Require(publish.MessageIds.Count == messageCount, $"expected {messageCount} ids, got {publish.MessageIds.Count}");

        // Open the StreamingPull duplex stream. The first request carries the
        // subscription + per-stream flow control; subsequent requests carry acks.
        using var stream = subscriber.StreamingPull();
        await stream.WriteAsync(new StreamingPullRequest
        {
            Subscription = subscriptionName.ToString(),
            StreamAckDeadlineSeconds = 30,
            MaxOutstandingMessages = 10, // per-stream flow-control ceiling
        });
        Demo.Step("Opened StreamingPull stream (StreamAckDeadlineSeconds=30, MaxOutstandingMessages=10)");

        // Drain the response stream until all messages are received + acked.
        var seen = new HashSet<string>();
        var responseStream = stream.GetResponseStream();
        while (seen.Count < messageCount && await responseStream.MoveNextAsync())
        {
            var ackIds = new List<string>();
            foreach (var rm in responseStream.Current.ReceivedMessages)
            {
                var body = rm.Message.Data.ToStringUtf8();
                Demo.Got($"Received '{body}' messageId={rm.Message.MessageId} (over stream)");
                seen.Add(rm.Message.MessageId);
                ackIds.Add(rm.AckId);
            }
            // Ack the delivered ack_ids back through the same stream. Leases are
            // subscription-owned, so the stream-ack resolves the broker seq.
            if (ackIds.Count > 0)
            {
                await stream.WriteAsync(new StreamingPullRequest { AckIds = { ackIds } });
                Demo.Step($"Acked {ackIds.Count} message(s) over the stream");
            }
        }

        // Half-close our side of the duplex stream; the connector closes its half
        // (periodic StreamCloseSeconds close also forces a reconnect in steady state).
        await stream.WriteCompleteAsync();

        Demo.Require(seen.Count == messageCount, $"expected {messageCount} distinct messages, got {seen.Count}");
        Demo.Ok($"StreamingPull delivered + acked all {messageCount} messages");
    }
    finally
    {
        // Best-effort cleanup (DeleteSubscription → DeleteTopic); never fail the run.
        try { await subscriber.DeleteSubscriptionAsync(subscriptionName); } catch { /* already gone */ }
        try { await publisher.DeleteTopicAsync(topicName); } catch { /* already gone */ }
        Demo.Step($"Cleaned up subscription '{subId}' and topic '{topicId}'");
    }
});

// ─── Inlined console-output + assertion helpers ───
//
// The C# examples have no shared helper project, so each program inlines this
// tiny harness. Examples are runnable proofs: clear progress to stdout, and a
// non-zero process exit on any failed assertion or unexpected error.
internal static class Demo
{
    public static void Step(string m) => Console.WriteLine($"[*] {m}");
    public static void Sent(string m) => Console.WriteLine($"[x] {m}");
    public static void Got(string m) => Console.WriteLine($"[v] {m}");
    public static void Ok(string m) => Console.WriteLine($"[ok] {m}");

    public static void Require(bool condition, string message)
    {
        if (!condition) throw new DemoFailure(message);
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
