using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

// ─── Subscriptions 5: Ack deadline + nack (extend, then 0-nack redelivery) ───
//
// Demonstrate the connector's ack-deadline lease semantics with unary Pull:
//   1. Pull a message (it is leased, not removed) and DO NOT ack it.
//   2. ModifyAckDeadline(>0) EXTENDS the lease (10..600s) — the message stays
//      invisible and is NOT redelivered.
//   3. ModifyAckDeadline(0) is an immediate NACK — the connector redelivers the
//      message at once (bypassing retry backoff).
//   4. The same message is redelivered (verified by server-assigned messageId);
//      ack it to finish. (DeliveryAttempt is only populated when the subscription
//      has a dead_letter_topic — see delivery/dead-letter-topic.)
//
// A topic `t` maps to the Events Store log `pubsub.{t}`; the subscription `s` maps
// to the per-sub queue `pubsub.sub.{s}`. Every delivered message gets an opaque
// ack_id under a subscription-owned lease; a 250ms sweeper expires overdue leases.
// Resource ids are uuid-suffixed per run so concurrent runs occupy distinct
// channels and never cross-talk (see ../../../SHARED-CONVENTIONS.md §1).

return await Demo.RunAsync(async () =>
{
    var suffix = Guid.NewGuid().ToString("N")[..8];
    var topicId = $"orders-{suffix}";
    var subId = $"sub-orders-{suffix}";

    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    // EmulatorDetection.EmulatorOnly: the .NET client must be told explicitly to
    // honour PUBSUB_EMULATOR_HOST (idiom trap; see ../../README.md).
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

    // ackDeadlineSeconds 10 = connector minimum (valid values: 0 or 10..600).
    await subscriber.CreateSubscriptionAsync(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 10);
    Demo.Step($"Created subscription '{subId}' → channel pubsub.sub.{subId} (ackDeadline=10s)");

    try
    {
        await publisher.PublishAsync(topicName, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFrom("retry-me", System.Text.Encoding.UTF8) },
        });
        Demo.Sent("Published 'retry-me'");

        // (1) Pull the message and leave it leased (do NOT ack).
        var first = await subscriber.PullAsync(subscriptionName, maxMessages: 1);
        Demo.Require(first.ReceivedMessages.Count == 1, $"expected 1 message on first pull, got {first.ReceivedMessages.Count}");
        var m1 = first.ReceivedMessages[0];
        var firstId = m1.Message.MessageId;
        Demo.Got($"First pull: '{m1.Message.Data.ToStringUtf8()}' messageId={firstId} (leased, not acked)");

        // (2) Extend the lease with ModifyAckDeadline(>0). The message stays
        // invisible, so an immediate-return pull sees nothing.
        await subscriber.ModifyAckDeadlineAsync(subscriptionName, new[] { m1.AckId }, ackDeadlineSeconds: 30);
        Demo.Step("ModifyAckDeadline(30) → extended lease; message stays invisible");
#pragma warning disable CS0612 // ReturnImmediately is deprecated in the public API but the connector honours it for a non-blocking probe pull
        var whileLeased = await subscriber.PullAsync(new PullRequest
        {
            Subscription = subscriptionName.ToString(),
            ReturnImmediately = true,
            MaxMessages = 1,
        });
#pragma warning restore CS0612
        Demo.Require(whileLeased.ReceivedMessages.Count == 0,
            $"extended lease should hide the message, but got {whileLeased.ReceivedMessages.Count}");
        Demo.Got("Immediate pull during extended lease → 0 messages (still leased)");

        // (3) ModifyAckDeadline(0) is an immediate NACK → redeliver at once.
        await subscriber.ModifyAckDeadlineAsync(subscriptionName, new[] { m1.AckId }, ackDeadlineSeconds: 0);
        Demo.Step("ModifyAckDeadline(0) → immediate nack (redeliver, bypassing backoff)");

        // (4) Re-pull: the SAME message is redelivered (same server-assigned
        // messageId). DeliveryAttempt is only populated when the subscription has
        // a dead_letter_topic policy (Google's contract — see delivery/dead-letter-topic),
        // so here we verify redelivery by message identity instead.
        var second = await subscriber.PullAsync(subscriptionName, maxMessages: 1);
        Demo.Require(second.ReceivedMessages.Count == 1, "expected redelivery after nack");
        var m2 = second.ReceivedMessages[0];
        Demo.Got($"Second pull: '{m2.Message.Data.ToStringUtf8()}' messageId={m2.Message.MessageId} (redelivered)");
        Demo.Require(m2.Message.MessageId == firstId,
            $"redelivered messageId should match the first delivery (was {firstId}, now {m2.Message.MessageId})");

        await subscriber.AcknowledgeAsync(subscriptionName, new[] { m2.AckId });
        Demo.Step("Acknowledged the redelivered message");
        Demo.Ok("Ack-deadline extend + 0-nack redelivery verified");
    }
    finally
    {
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
