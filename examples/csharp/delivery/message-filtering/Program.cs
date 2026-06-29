using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using KubeMQ.GcpPubSub.Examples.Delivery;

// ─── Delivery 7: message filtering (CEL-subset, attributes-only) ─────────────
//
// One topic, two subscriptions: one unfiltered, one with a CEL-subset filter
// (attributes.region = "eu"). A single mixed batch is published; the filter is
// applied at publish fan-out, so the filtered subscription only ever enqueues
// the matching subset, while the unfiltered subscription receives everything.
//
// The connector implements a hand-written CEL subset over message ATTRIBUTES
// only (not data, not the ordering key): attributes:KEY, = / != "v",
// hasPrefix(attributes.KEY, "p"), AND/OR/NOT, parentheses; <=256 chars;
// compiled once at CreateSubscription and immutable; malformed -> INVALID_ARGUMENT.

return await Demo.RunAsync(async () =>
{
    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    var suffix = Demo.RunSuffix();
    var topicName = TopicName.FromProjectTopic(projectId, $"orders-{suffix}");
    var allSub = SubscriptionName.FromProjectSubscription(projectId, $"sub-all-{suffix}");
    var euSub = SubscriptionName.FromProjectSubscription(projectId, $"sub-eu-{suffix}");
    const string filter = "attributes.region = \"eu\"";

    var publisher = await new PublisherServiceApiClientBuilder
    { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();
    var subscriber = await new SubscriberServiceApiClientBuilder
    { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();

    await publisher.CreateTopicAsync(topicName);
    Demo.Step($"Created topic '{topicName.TopicId}'");

    await subscriber.CreateSubscriptionAsync(new Subscription
    {
        SubscriptionName = allSub,
        TopicAsTopicName = topicName,
        AckDeadlineSeconds = 10,
    });
    Demo.Step($"Created unfiltered subscription '{allSub.SubscriptionId}'");

    await subscriber.CreateSubscriptionAsync(new Subscription
    {
        SubscriptionName = euSub,
        TopicAsTopicName = topicName,
        AckDeadlineSeconds = 10,
        Filter = filter, // attributes-only CEL subset, compiled once at create
    });
    Demo.Step($"Created filtered subscription '{euSub.SubscriptionId}' (filter: {filter})");

    try
    {
        // Mixed batch: two eu, one us. Filtering is applied at publish fan-out.
        PubsubMessage Msg(string body, string region)
        {
            var m = new PubsubMessage { Data = ByteString.CopyFromUtf8(body) };
            m.Attributes["region"] = region;
            return m;
        }

        await publisher.PublishAsync(topicName, new[]
        {
            Msg("o1", "eu"),
            Msg("o2", "us"),
            Msg("o3", "eu"),
        });
        Demo.Sent("Published batch: o1(region=eu), o2(region=us), o3(region=eu)");

        // Drain helper: pull + ack until we have what we expect (or give up).
        async Task<List<string>> Drain(SubscriptionName sub, int expected)
        {
            var acc = new List<string>();
            for (var round = 0; round < 6 && acc.Count < expected; round++)
            {
                var resp = await subscriber.PullAsync(new PullRequest
                {
                    SubscriptionAsSubscriptionName = sub,
                    MaxMessages = 10,
                });
                foreach (var rm in resp.ReceivedMessages)
                {
                    acc.Add(rm.Message.Data.ToStringUtf8());
                    await subscriber.AcknowledgeAsync(sub, new[] { rm.AckId });
                }
                if (resp.ReceivedMessages.Count == 0) await Task.Delay(200);
            }
            acc.Sort(StringComparer.Ordinal);
            return acc;
        }

        var all = await Drain(allSub, 3);
        Demo.Got($"unfiltered '{allSub.SubscriptionId}' received: [{string.Join(", ", all)}]");
        var eu = await Drain(euSub, 2);
        Demo.Got($"filtered   '{euSub.SubscriptionId}' received: [{string.Join(", ", eu)}]  (o2/region=us → suppressed)");

        Demo.RequireEqual("o1,o2,o3", string.Join(",", all), "unfiltered subscription delivery");
        Demo.RequireEqual("o1,o3", string.Join(",", eu), "filtered subscription delivery");
        Demo.Ok("filter suppressed the non-matching message (region=us): 3 delivered to all, 2 to eu");
    }
    finally
    {
        try { await subscriber.DeleteSubscriptionAsync(euSub); } catch { /* best-effort */ }
        try { await subscriber.DeleteSubscriptionAsync(allSub); } catch { /* best-effort */ }
        try { await publisher.DeleteTopicAsync(topicName); } catch { /* best-effort */ }
        Demo.Step($"Cleaned up subscriptions + topic '{topicName.TopicId}'");
    }
});
