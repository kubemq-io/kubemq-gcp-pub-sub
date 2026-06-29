using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using KubeMQ.GcpPubSub.Examples.Delivery;

// ─── Delivery 10: exactly-once delivery ──────────────────────────────────────
//
// A subscription created with enable_exactly_once_delivery strengthens the ack
// contract: once a message is successfully acknowledged it will not be
// redelivered, and a stale/duplicate ack of an already-used ack_id is REJECTED
// rather than silently accepted.
//
// We publish one message, pull it, and ack it (the durable ack). We then replay
// the SAME ack_id — a stale ack — and assert the connector rejects it with a
// unary error: FAILED_PRECONDITION carrying a google.rpc.ErrorInfo whose
// reason is PERMANENT_FAILURE_INVALID_ACK_ID (domain pubsub.googleapis.com).
// This matches the real Google SDK contract (NOT a literal INVALID_ARGUMENT).
//
// HEADLINE CAVEAT: exactly-once is NODE-LOCAL. An ack_id is valid only on the
// cluster node that minted it; pin a subscription's StreamingPull traffic to one
// node (sticky load balancing) or accept at-least-once across nodes.

return await Demo.RunAsync(async () =>
{
    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    var suffix = Demo.RunSuffix();
    var topicName = TopicName.FromProjectTopic(projectId, $"orders-{suffix}");
    var subName = SubscriptionName.FromProjectSubscription(projectId, $"sub-orders-{suffix}");

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
        EnableExactlyOnceDelivery = true, // strengthened ack contract
    });
    Demo.Step($"Created exactly-once subscription '{subName.SubscriptionId}' (enable_exactly_once_delivery=true)");

    try
    {
        await publisher.PublishAsync(topicName, new[]
        {
            new PubsubMessage { Data = ByteString.CopyFromUtf8("order-42") },
        });
        Demo.Sent("Published 'order-42'");

        // Pull the message.
        PullResponse pull;
        var tries = 0;
        do
        {
            pull = await subscriber.PullAsync(new PullRequest
            {
                SubscriptionAsSubscriptionName = subName,
                MaxMessages = 1,
            });
            tries++;
        } while (pull.ReceivedMessages.Count == 0 && tries < 10);

        Demo.Require(pull.ReceivedMessages.Count == 1, "expected exactly one message on first pull");
        var rm = pull.ReceivedMessages[0];
        var ackId = rm.AckId;
        Demo.Got($"Pulled '{rm.Message.Data.ToStringUtf8()}'");

        // Durable ack: with exactly-once, a successful unary Acknowledge means the
        // ack took effect (the message will not be redelivered).
        await subscriber.AcknowledgeAsync(subName, new[] { ackId });
        Demo.Got("Acknowledged → durable (exactly-once ack confirmed)");

        // Replay the SAME ack_id. The connector must reject the stale ack with
        // FAILED_PRECONDITION + ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID).
        try
        {
            await subscriber.AcknowledgeAsync(subName, new[] { ackId });
            throw new DemoFailure("stale ack of an already-used ack_id was unexpectedly accepted");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.FailedPrecondition)
        {
            var (reason, domain) = ExtractErrorInfo(ex);
            Demo.Got($"Stale ack rejected: status={ex.StatusCode}, ErrorInfo.reason={reason}, domain={domain}");
            Demo.RequireEqual("FailedPrecondition", ex.StatusCode.ToString(), "stale-ack status code");
            Demo.RequireEqual("PERMANENT_FAILURE_INVALID_ACK_ID", reason, "stale-ack ErrorInfo.reason");
        }

        Demo.Ok("exactly-once: durable ack confirmed; stale ack rejected with PERMANENT_FAILURE_INVALID_ACK_ID");
    }
    finally
    {
        try { await subscriber.DeleteSubscriptionAsync(subName); } catch { /* best-effort */ }
        try { await publisher.DeleteTopicAsync(topicName); } catch { /* best-effort */ }
        Demo.Step($"Cleaned up subscription '{subName.SubscriptionId}' + topic '{topicName.TopicId}'");
    }
});

// Reads the google.rpc.ErrorInfo (reason + domain) out of the RpcException's
// rich-status trailer (grpc-status-details-bin) — the same detail the real Google
// client libraries map to a permanent per-message ack failure.
static (string reason, string domain) ExtractErrorInfo(RpcException ex)
{
    var entry = ex.Trailers.Get("grpc-status-details-bin");
    if (entry is null) return ("<no-status-details>", "<none>");
    var status = Google.Rpc.Status.Parser.ParseFrom(entry.ValueBytes);
    foreach (var detail in status.Details)
    {
        if (detail.TryUnpack<Google.Rpc.ErrorInfo>(out var info))
            return (info.Reason, info.Domain);
    }
    return ("<no-error-info>", "<none>");
}
