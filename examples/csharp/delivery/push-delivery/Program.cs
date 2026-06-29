using System.Net;
using System.Text.Json;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using KubeMQ.GcpPubSub.Examples.Delivery;

// ─── Delivery 9: push delivery ───────────────────────────────────────────────
//
// A subscription with a push_config is served by a per-subscription connector
// worker that POSTs each message to an HTTP(S) endpoint instead of the client
// pulling. This example stands up a local HTTP sink, creates a push subscription
// pointing at it, publishes one message, and shows the wrapped JSON envelope
// arriving — then returns 2xx so the connector acks the message.
//
// The connector requires HTTPS for push endpoints, with plain HTTP allowed ONLY
// for localhost (local development) — which is exactly what this example uses.
//
// Wrapped envelope (verified):
//   { "message": { "data": "<base64>", "attributes": {...},
//                   "messageId": "...", "publishTime": "..." },
//     "subscription": "projects/{p}/subscriptions/{s}" }
// `data` is base64-encoded; a 2xx response acks, non-2xx/timeout retries.

return await Demo.RunAsync(async () =>
{
    var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
    var suffix = Demo.RunSuffix();
    var topicName = TopicName.FromProjectTopic(projectId, $"orders-{suffix}");
    var subName = SubscriptionName.FromProjectSubscription(projectId, $"sub-push-{suffix}");

    // Stand up a local HTTP sink on the first free port in a small range. Plain
    // HTTP is permitted because the host is localhost.
    var (listener, pushEndpoint) = StartLocalSink();
    Demo.Step($"Local push sink listening at {pushEndpoint}");

    var envelopeReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    var hitCount = 0;
    using var listenerCts = new CancellationTokenSource();
    var serveTask = Task.Run(async () =>
    {
        while (!listenerCts.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try { ctx = await listener.GetContextAsync(); }
            catch { break; } // listener stopped
            using var reader = new StreamReader(ctx.Request.InputStream,
                ctx.Request.ContentEncoding ?? System.Text.Encoding.UTF8);
            var body = await reader.ReadToEndAsync();
            Interlocked.Increment(ref hitCount);
            envelopeReceived.TrySetResult(body);
            // 2xx → the connector acks the message (no retry, no redelivery).
            ctx.Response.StatusCode = 200;
            ctx.Response.Close();
        }
    });

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
        PushConfig = new PushConfig { PushEndpoint = pushEndpoint },
    });
    Demo.Step($"Created push subscription '{subName.SubscriptionId}' (push_endpoint={pushEndpoint})");

    try
    {
        var msg = new PubsubMessage { Data = ByteString.CopyFromUtf8("hello-push") };
        msg.Attributes["kind"] = "demo";
        await publisher.PublishAsync(topicName, new[] { msg });
        Demo.Sent("Published 'hello-push' (attributes: kind=demo)");

        // Wait for the connector's push worker to POST the envelope.
        var winner = await Task.WhenAny(envelopeReceived.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Demo.Require(winner == envelopeReceived.Task, "expected a push POST at the local endpoint within 30s");
        var raw = await envelopeReceived.Task;

        // Parse the wrapped envelope and assert its shape.
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;
        var message = root.GetProperty("message");
        var dataB64 = message.GetProperty("data").GetString()!;
        var data = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(dataB64));
        var kind = message.GetProperty("attributes").GetProperty("kind").GetString();
        var subscription = root.GetProperty("subscription").GetString();
        Demo.Got($"Push POST received: data(decoded)='{data}', attributes.kind='{kind}'");
        Demo.Got($"  envelope.subscription='{subscription}', messageId={message.GetProperty("messageId").GetString()}");

        Demo.RequireEqual("hello-push", data, "pushed message body (base64-decoded)");
        Demo.RequireEqual("demo", kind, "pushed message attribute 'kind'");
        Demo.Require(subscription!.EndsWith(subName.SubscriptionId, StringComparison.Ordinal),
            "envelope.subscription must name this subscription");

        // The 200 response acked the message; give the worker a moment and assert
        // it was not redelivered (exactly one POST for the single publish).
        await Task.Delay(2000);
        Demo.RequireEqual(1, Volatile.Read(ref hitCount), "push POST count (2xx → acked, no redelivery)");
        Demo.Ok("push delivery: wrapped envelope POSTed to the localhost endpoint; 2xx acked, no redelivery");
    }
    finally
    {
        listenerCts.Cancel();
        try { listener.Stop(); } catch { /* best-effort */ }
        try { await subscriber.DeleteSubscriptionAsync(subName); } catch { /* best-effort */ }
        try { await publisher.DeleteTopicAsync(topicName); } catch { /* best-effort */ }
        Demo.Step($"Cleaned up subscription '{subName.SubscriptionId}' + topic '{topicName.TopicId}'");
    }
});

// Binds an HttpListener on the first free port in [base, base+range). Returns the
// listener and the full push endpoint URL (http://localhost:<port>/push/).
static (HttpListener listener, string endpoint) StartLocalSink()
{
    const int basePort = 18080;
    for (var port = basePort; port < basePort + 50; port++)
    {
        var listener = new HttpListener();
        var endpoint = $"http://localhost:{port}/push/";
        listener.Prefixes.Add(endpoint);
        try
        {
            listener.Start();
            return (listener, endpoint);
        }
        catch (HttpListenerException)
        {
            // port in use — try the next one
        }
    }
    throw new DemoFailure($"could not bind a local push sink in ports {basePort}..{basePort + 50}");
}
