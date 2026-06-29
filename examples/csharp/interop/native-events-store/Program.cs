using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using KubeMQ.Sdk.Client;
using KubeMQ.Sdk.EventsStore;
using Encoding = System.Text.Encoding; // Google.Cloud.PubSub.V1 also defines an Encoding enum

// ─── interop 15: native-events-store ──────────────────────────────────────────
//
// The ONLY example in this repo that imports a KubeMQ SDK. It demonstrates the
// cross-protocol bridge: a message published with the official Google Pub/Sub
// client to topic `orders` is consumed NATIVELY from the Events Store log
// `pubsub.orders` via the native KubeMQ gRPC SDK — with the three reserved tags
// (_pubsub_message_id, _pubsub_publish_time, _pubsub_ordering_key) visible.
//
// Two endpoints are used:
//   - Pub/Sub half: PUBSUB_EMULATOR_HOST (default localhost:8085) — the connector.
//   - Native half:  KUBEMQ_GRPC_ADDRESS  (default localhost:50000) — native broker.
//
// Deterministic ordering (subscribe-before-publish, startAt = "new"): we open the
// native Events Store subscribe stream with StartFromNew and confirm it is open
// BEFORE issuing the Pub/Sub publish, so the published message is guaranteed
// in-window for the native consumer (no startup race). See spec S5.7 +
// docs/concepts/cross-protocol-interop.md.

// Reserved tags the connector stamps on every message (stripped from `attributes`
// for Pub/Sub clients, visible to native Events Store consumers).
const string TagMessageId = "_pubsub_message_id";
const string TagPublishTime = "_pubsub_publish_time";
const string TagOrderingKey = "_pubsub_ordering_key";

var exitCode = 0;
void Fail(string why)
{
    Console.Error.WriteLine($"[FAIL] {why}");
    exitCode = 1;
}

// Channel isolation: uuid-suffix the topic id so concurrent runs (other languages,
// or /examples + /burnin) occupy distinct channels and never cross-talk. The native
// side derives its `pubsub.{t}` channel name from the SAME generated topic id.
var suffix = Guid.NewGuid().ToString("N")[..8];
var projectId = Environment.GetEnvironmentVariable("PUBSUB_PROJECT_ID") ?? "my-project";
var topicId = $"orders-{suffix}";                 // Pub/Sub topic id
var nativeChannel = $"pubsub.{topicId}";          // Events Store log it maps to
var grpcAddress = Environment.GetEnvironmentVariable("KUBEMQ_GRPC_ADDRESS") ?? "localhost:50000";

// The Pub/Sub payload + the attributes we expect to surface natively as tags.
var payload = "order #4242 — cross-protocol via Pub/Sub";
var orderingKey = "customer-42";
var attributes = new Dictionary<string, string>
{
    ["region"] = "eu-west-1",
    ["priority"] = "high",
};

Console.WriteLine($"[*] topic id           : {topicId}  (Events Store channel '{nativeChannel}')");
Console.WriteLine($"[*] PUBSUB_EMULATOR_HOST: {Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST") ?? "<unset>"}");
Console.WriteLine($"[*] native gRPC broker  : {grpcAddress}");

// ── Pub/Sub half: build the publisher admin client (emulator drop-in) ──────────
// C# does NOT auto-detect the emulator from the env var — set EmulatorDetection
// explicitly. EmulatorOnly clears credentials and uses insecure gRPC.
var publisher = await new PublisherServiceApiClientBuilder
{
    EmulatorDetection = EmulatorDetection.EmulatorOnly,
}.BuildAsync();

var topicName = TopicName.FromProjectTopic(projectId, topicId);
await publisher.CreateTopicAsync(topicName);
Console.WriteLine($"[*] CreateTopic '{topicId}' (project '{projectId}' parsed but ignored)");

// ── Native half: connect the KubeMQ gRPC client ────────────────────────────────
await using var kube = new KubeMQClient(new KubeMQClientOptions
{
    Address = grpcAddress,
    ClientId = $"gcp-pubsub-interop-csharp-{suffix}",
});

using var subscriptionCts = new CancellationTokenSource();
try
{
    await kube.ConnectAsync();
    Console.WriteLine($"[*] Connected native KubeMQ client to {grpcAddress}");

    // Subscribe-before-publish with startAt = "new": establish the Events Store
    // subscribe stream FIRST. StartFromNew (the C# enum mapped to gRPC StartNewOnly
    // = 1) receives only events published after the subscription is established.
    var eventsStoreSub = new EventStoreSubscription
    {
        Channel = nativeChannel,
        StartPosition = EventStoreStartPosition.StartFromNew,
    };

    // Drive readiness deterministically rather than sleeping blind. The gRPC
    // subscribe stream is established lazily on the iterator's FIRST MoveNextAsync
    // (that is when the SDK issues the underlying SubscribeToEvents RPC). So we
    // obtain the enumerator and START — but do not yet await — that first
    // MoveNextAsync here, BEFORE publishing. By the time the task is observed to
    // have begun (it has yielded back to us while blocking on the first event),
    // the subscribe RPC has been dispatched and the stream is attached with
    // startAt=new, so the about-to-be-published message lands in-window.
    var enumerator = kube
        .SubscribeToEventsStoreAsync(eventsStoreSub, subscriptionCts.Token)
        .GetAsyncEnumerator(subscriptionCts.Token);

    // Kick off the first MoveNextAsync. This synchronously enters the iterator up
    // to the point where it dispatches the subscribe RPC and then awaits the first
    // event, at which point the returned task is pending and control returns here.
    var firstMove = enumerator.MoveNextAsync().AsTask();

    // Yield so the iterator's synchronous prologue (WaitForReady + dispatch of the
    // subscribe RPC) runs to its first real await before we publish.
    await Task.Yield();

    // The SDK currently exposes no explicit "stream attached" callback, so we add a
    // small bounded settle to cover the subscribe RPC's network round-trip to the
    // broker. This is a safety margin on top of the deterministic enumerator start —
    // NOT the primary readiness mechanism — so it can stay short. If the SDK later
    // surfaces an attach confirmation, await that here and drop the delay.
    await Task.Delay(TimeSpan.FromMilliseconds(250));
    Console.WriteLine($"[*] Events Store subscribe stream open on '{nativeChannel}' (startAt=new)");

    // Background consumer: convert the already-started first MoveNextAsync (and any
    // subsequent reads) into the single event we await. The first MoveNextAsync is
    // NOT restarted — we observe the in-flight task created above.
    var received = new TaskCompletionSource<EventStoreReceived>(
        TaskCreationOptions.RunContinuationsAsynchronously);
    var consumer = Task.Run(async () =>
    {
        try
        {
            if (await firstMove.ConfigureAwait(false))
            {
                received.TrySetResult(enumerator.Current);
            }
            else
            {
                received.TrySetException(
                    new InvalidOperationException("Events Store subscribe stream closed before any event"));
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on cleanup after we already have our event.
        }
        catch (Exception ex)
        {
            received.TrySetException(ex);
        }
    });

    // ── Pub/Sub publish (the producer never speaks KubeMQ) ─────────────────────
    var message = new PubsubMessage
    {
        Data = ByteString.CopyFrom(payload, Encoding.UTF8),
        OrderingKey = orderingKey,
    };
    foreach (var (k, v) in attributes)
    {
        message.Attributes[k] = v;
    }

    var publishResponse = await publisher.PublishAsync(topicName, new[] { message });
    var messageId = publishResponse.MessageIds[0];
    Console.WriteLine($"[>] Pub/Sub Publish to '{topicId}' → messageId={messageId}");

    // ── Await exactly one native event and verify the bridge ───────────────────
    var awaited = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(15)));
    if (awaited != received.Task)
    {
        Fail($"native Events Store consumer received no event on '{nativeChannel}' within 15s");
    }
    else
    {
        var ev = await received.Task;
        var body = Encoding.UTF8.GetString(ev.Body.Span);
        Console.WriteLine($"[<] native Events Store received on '{ev.Channel}' (seq {ev.Sequence}) → '{body}'");

        if (body != payload)
        {
            Fail($"native body mismatch: expected '{payload}', got '{body}'");
        }

        var tags = ev.Tags ?? new Dictionary<string, string>();

        // The three reserved tags must be present for the native consumer.
        foreach (var reserved in new[] { TagMessageId, TagPublishTime, TagOrderingKey })
        {
            if (!tags.ContainsKey(reserved))
            {
                Fail($"reserved tag '{reserved}' missing from native message");
            }
        }

        if (tags.TryGetValue(TagMessageId, out var nativeMsgId))
        {
            Console.WriteLine($"[v] reserved tag {TagMessageId}   = {nativeMsgId}");
            if (nativeMsgId != messageId)
            {
                Fail($"{TagMessageId} '{nativeMsgId}' != Pub/Sub messageId '{messageId}'");
            }
        }

        if (tags.TryGetValue(TagPublishTime, out var publishTime))
        {
            Console.WriteLine($"[v] reserved tag {TagPublishTime} = {publishTime}");
        }

        if (tags.TryGetValue(TagOrderingKey, out var nativeOrderingKey))
        {
            Console.WriteLine($"[v] reserved tag {TagOrderingKey} = {nativeOrderingKey}");
            if (nativeOrderingKey != orderingKey)
            {
                Fail($"{TagOrderingKey} '{nativeOrderingKey}' != published ordering key '{orderingKey}'");
            }
        }

        // The Pub/Sub attributes ride along as ordinary tags.
        foreach (var (k, v) in attributes)
        {
            if (!tags.TryGetValue(k, out var got) || got != v)
            {
                Fail($"attribute '{k}' expected '{v}', got '{(tags.ContainsKey(k) ? tags[k] : "<missing>")}'");
            }
            else
            {
                Console.WriteLine($"[v] attribute {k} = {got}");
            }
        }
    }

    subscriptionCts.Cancel();
    await consumer;
    await enumerator.DisposeAsync();
}
finally
{
    // Best-effort cleanup: cancel the native subscribe + delete the topic so
    // re-runs start clean. Must not fail the run if a resource is already gone.
    if (!subscriptionCts.IsCancellationRequested)
    {
        subscriptionCts.Cancel();
    }

    try
    {
        await publisher.DeleteTopicAsync(topicName);
        Console.WriteLine($"[*] Cleaned up topic '{topicId}'");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[*] Cleanup: topic '{topicId}' already gone ({ex.GetType().Name})");
    }
}

if (exitCode == 0)
{
    Console.WriteLine("[PASS] Pub/Sub publish consumed natively on Events Store with reserved tags present");
}

return exitCode;
