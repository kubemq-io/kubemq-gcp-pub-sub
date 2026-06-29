using System.Diagnostics;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using KubeMQ.GcpPubSub.Examples.Delivery;

// ─── Delivery 8: dead-letter topic ───────────────────────────────────────────
//
// A source subscription with a DeadLetterPolicy (max_delivery_attempts = 5)
// moves a poison message to a separate dead-letter topic once its receive count
// exceeds the budget. A second subscription on the dead-letter topic consumes it.
//
// IMPORTANT (verified against the connector): dead-lettering is driven by NATURAL
// ack-deadline EXPIRY, not by an explicit nack. Every expiry of the unacked lease
// raises the receive count via the connector's 250 ms lease sweeper; once it
// exceeds max_delivery_attempts the sweeper republishes the message to the
// dead-letter topic and acks the original off the source. (An explicit
// ModifyAckDeadline(0) nack redelivers but does NOT raise the count toward the
// DLQ threshold — Google semantics.) So we deliberately pull WITHOUT acking and
// wait for the message to surface on the dead-letter subscription.
//
// With the minimum ack deadline (10 s) and max_delivery_attempts = 5, the message
// dead-letters after ~5 expiry cycles — expect roughly 50-65 s. A tight
// RetryPolicy (10-50 ms) makes the re-arm/redelivery after each expiry immediate.

return await Demo.RunAsync(async () =>
{
    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    var suffix = Demo.RunSuffix();
    var srcTopic = TopicName.FromProjectTopic(projectId, $"orders-{suffix}");
    var dltTopic = TopicName.FromProjectTopic(projectId, $"dead-letters-{suffix}");
    var workSub = SubscriptionName.FromProjectSubscription(projectId, $"sub-worker-{suffix}");
    var dltSub = SubscriptionName.FromProjectSubscription(projectId, $"sub-dlq-{suffix}");
    const int maxDeliveryAttempts = 5; // 5..100 (Google's minimum); 0 = unset

    var publisher = await new PublisherServiceApiClientBuilder
    { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();
    var subscriber = await new SubscriberServiceApiClientBuilder
    { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();

    await publisher.CreateTopicAsync(srcTopic);
    await publisher.CreateTopicAsync(dltTopic);
    Demo.Step($"Created source topic '{srcTopic.TopicId}' + dead-letter topic '{dltTopic.TopicId}'");

    await subscriber.CreateSubscriptionAsync(new Subscription
    {
        SubscriptionName = workSub,
        TopicAsTopicName = srcTopic,
        AckDeadlineSeconds = 10, // minimum; the unacked lease expires after this
        DeadLetterPolicy = new DeadLetterPolicy
        {
            DeadLetterTopic = dltTopic.ToString(),
            MaxDeliveryAttempts = maxDeliveryAttempts,
        },
        // Tightest redelivery backoff: a swept lease re-arms and redelivers almost
        // immediately, so each expiry cycle is bounded by the 10 s ack deadline.
        RetryPolicy = new RetryPolicy
        {
            MinimumBackoff = Duration.FromTimeSpan(TimeSpan.FromMilliseconds(10)),
            MaximumBackoff = Duration.FromTimeSpan(TimeSpan.FromMilliseconds(50)),
        },
    });
    Demo.Step($"Created source subscription '{workSub.SubscriptionId}' (dead_letter_topic='{dltTopic.TopicId}', max_delivery_attempts={maxDeliveryAttempts})");

    await subscriber.CreateSubscriptionAsync(new Subscription
    {
        SubscriptionName = dltSub,
        TopicAsTopicName = dltTopic,
        AckDeadlineSeconds = 10,
    });
    Demo.Step($"Created dead-letter subscription '{dltSub.SubscriptionId}'");

    try
    {
        await publisher.PublishAsync(srcTopic, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFromUtf8("poison") },
        });
        Demo.Sent("Published 'poison' to the source topic");

        var sw = Stopwatch.StartNew();
        var srcDeliveries = 0;
        string? dead = null;

        // Single drive loop. Each iteration: (1) try to re-lease the source message
        // — a successful Pull means the previous lease expired and the sweeper
        // re-armed it (raising the receive count); (2) check whether it has
        // surfaced on the dead-letter subscription yet. Frequent source pulls keep
        // the receive count climbing toward max_delivery_attempts as fast as the
        // 10 s ack deadline allows. Bounded at 90 s.
        while (dead is null && sw.Elapsed < TimeSpan.FromSeconds(90))
        {
            var src = await subscriber.PullAsync(new PullRequest
            {
                SubscriptionAsSubscriptionName = workSub,
                MaxMessages = 1,
            });
            if (src.ReceivedMessages.Count > 0)
            {
                srcDeliveries++;
                var deliveryAttempt = src.ReceivedMessages[0].DeliveryAttempt;
                Demo.Got($"source delivery #{srcDeliveries} (DeliveryAttempt={deliveryAttempt}) @ {sw.Elapsed.TotalSeconds:F0}s — NOT acking, letting the lease expire");
                // deliberately no ack / no nack — let the lease expire naturally
            }

            var dlt = await subscriber.PullAsync(new PullRequest
            {
                SubscriptionAsSubscriptionName = dltSub,
                MaxMessages = 1,
            });
            if (dlt.ReceivedMessages.Count > 0)
            {
                var rm = dlt.ReceivedMessages[0];
                dead = rm.Message.Data.ToStringUtf8();
                await subscriber.AcknowledgeAsync(dltSub, new[] { rm.AckId });
                break;
            }

            await Task.Delay(1000);
        }

        Demo.Require(dead is not null,
            $"the poison message must be dead-lettered after exceeding max_delivery_attempts (src deliveries={srcDeliveries})");
        Demo.Got($"dead-letter subscription received '{dead}' after {srcDeliveries} source deliveries (~{sw.Elapsed.TotalSeconds:F0}s)");
        Demo.RequireEqual("poison", dead!, "dead-letter message body");
        Demo.Ok($"poison message dead-lettered to '{dltTopic.TopicId}' once receive count exceeded max_delivery_attempts={maxDeliveryAttempts}");
    }
    finally
    {
        try { await subscriber.DeleteSubscriptionAsync(dltSub); } catch { /* best-effort */ }
        try { await subscriber.DeleteSubscriptionAsync(workSub); } catch { /* best-effort */ }
        try { await publisher.DeleteTopicAsync(dltTopic); } catch { /* best-effort */ }
        try { await publisher.DeleteTopicAsync(srcTopic); } catch { /* best-effort */ }
        Demo.Step($"Cleaned up subscriptions + topics '{srcTopic.TopicId}' / '{dltTopic.TopicId}'");
    }
});
