using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using KubeMQ.GcpPubSub.Examples.Delivery;

// ─── Delivery 6: ordering keys ───────────────────────────────────────────────
//
// An ordering-enabled subscription delivers messages that share an ordering_key
// in publish order, with at most one message in flight per key. We publish three
// messages on key "cust-7" (m1, m2, m3) plus one keyless message, then pull and
// ack one at a time and assert the three keyed messages arrive in publish order.
//
// Enable ordering at BOTH ends: set enable_message_ordering on the subscription
// at create time, and (for the high-level PublisherClient) set
// EnableMessageOrdering on its Settings. Here we use the low-level
// PublisherServiceApiClient and publish sequentially, so publish order on the key
// is the order of the calls.
//
// The connector maps topic t to the Events Store log `pubsub.{t}` and the
// subscription s to the queue `pubsub.sub.{s}`; the ordering key rides as the
// reserved tag `_pubsub_ordering_key`.

return await Demo.RunAsync(async () =>
{
    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    var suffix = Demo.RunSuffix();
    var topicName = TopicName.FromProjectTopic(projectId, $"orders-{suffix}");
    var subName = SubscriptionName.FromProjectSubscription(projectId, $"sub-orders-{suffix}");
    const string key = "cust-7";

    // Emulator detection is explicit in the .NET client (idiom trap): it does NOT
    // auto-detect PUBSUB_EMULATOR_HOST the way the Go/Python/Node clients do.
    var publisher = await new PublisherServiceApiClientBuilder
    { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();
    var subscriber = await new SubscriberServiceApiClientBuilder
    { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();

    await publisher.CreateTopicAsync(topicName);
    Demo.Step($"Created topic '{topicName.TopicId}'");

    await subscriber.CreateSubscriptionAsync(new Subscription
    {
        SubscriptionName = subName,
        TopicAsTopicName = topicName,
        AckDeadlineSeconds = 10,
        EnableMessageOrdering = true, // ordering enabled on the subscription
    });
    Demo.Step($"Created ordering-enabled subscription '{subName.SubscriptionId}' (enable_message_ordering=true)");

    try
    {
        // Publish three ordered messages on one key (sequentially → publish order)
        // plus one keyless message (unordered).
        var ordered = new[] { "m1", "m2", "m3" };
        foreach (var body in ordered)
        {
            await publisher.PublishAsync(topicName, new[]
            {
                new PubsubMessage { Data = ByteString.CopyFromUtf8(body), OrderingKey = key },
            });
            Demo.Sent($"Published '{body}' orderingKey='{key}'");
        }
        await publisher.PublishAsync(topicName, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFromUtf8("keyless") },
        });
        Demo.Sent("Published 'keyless' (no ordering key)");

        // Pull + ack one at a time. The connector keeps at most one message per key
        // in flight, so each keyed message is only delivered after the previous one
        // is acked — we observe m1, m2, m3 in that order on the key.
        var keyed = new List<string>();
        var keyless = new List<string>();
        for (var round = 0; round < 8 && keyed.Count + keyless.Count < ordered.Length + 1; round++)
        {
            var resp = await subscriber.PullAsync(new PullRequest
            {
                SubscriptionAsSubscriptionName = subName,
                MaxMessages = 1, // one at a time so per-key in-flight is observable
            });
            foreach (var rm in resp.ReceivedMessages)
            {
                var body = rm.Message.Data.ToStringUtf8();
                var mk = rm.Message.OrderingKey;
                if (string.IsNullOrEmpty(mk))
                {
                    keyless.Add(body);
                    Demo.Got($"Pulled '{body}' (keyless) → ack");
                }
                else
                {
                    keyed.Add(body);
                    Demo.Got($"Pulled '{body}' orderingKey='{mk}' → ack");
                }
                await subscriber.AcknowledgeAsync(subName, new[] { rm.AckId });
            }
        }

        Demo.RequireEqual(string.Join(",", ordered), string.Join(",", keyed),
            "keyed delivery order");
        Demo.Require(keyless.Count == 1 && keyless[0] == "keyless",
            "the keyless message must also be delivered");
        Demo.Ok($"ordered key '{key}' delivered in publish order: {string.Join(" -> ", keyed)}");
    }
    finally
    {
        try { await subscriber.DeleteSubscriptionAsync(subName); } catch { /* best-effort */ }
        try { await publisher.DeleteTopicAsync(topicName); } catch { /* best-effort */ }
        Demo.Step($"Cleaned up subscription '{subName.SubscriptionId}' + topic '{topicName.TopicId}'");
    }
});
