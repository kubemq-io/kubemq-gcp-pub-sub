package io.kubemq.examples.gcp.subscriptions.streamingpull;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * subscriptions/streaming-pull (master-table variant 4) — the high-throughput
 * consume path most client libraries use by default.
 *
 * <p>{@code CreateTopic -> CreateSubscription -> Publish(N) -> Subscriber (StreamingPull) -> Acknowledge}.
 *
 * <p>It publishes N messages, opens a long-lived bidirectional {@code StreamingPull}
 * stream via the high-level {@link Subscriber} (the Java client's StreamingPull
 * driver) with an explicit client-side flow-control ceiling
 * ({@code FlowControlSettings.setMaxOutstandingElementCount}), acks each delivered
 * message in the {@link MessageReceiver} callback, and asserts every published
 * message was delivered exactly once. The connector periodically closes each
 * StreamingPull stream ({@code CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS}, default
 * 1800 s) to force a transparent SDK reconnect — no messages are lost across it.
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client.
 * With {@code PUBSUB_EMULATOR_HOST} set, the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — the
 * emulator drop-in posture (no Google auth, insecure gRPC). The topic and
 * subscription are uuid-suffixed per run so concurrent runs use distinct
 * {@code pubsub.{t}} / {@code pubsub.sub.{s}} channels and never cross-talk; both
 * are deleted on exit (best-effort) so re-runs start clean.
 *
 * <p>A Publish writes once to the topic log {@code pubsub.orders-<uuid8>} (the
 * authoritative, replayable, cross-protocol source), then fans out one queue copy
 * to the per-sub queue {@code pubsub.sub.sub-orders-<uuid8>} (spec S3.3); the
 * StreamingPull stream drains that subscription's queue. Any failed assertion
 * exits non-zero.
 */
public final class Main {

    /** Number of messages published and then drained over the StreamingPull stream. */
    private static final int MESSAGE_COUNT = 5;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] subscriptions/streaming-pull | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids (channel isolation). The canonical master-table
        // names are orders / sub-orders; the uuid8 suffix gives this run a private
        // channel namespace so parallel runs never collide on the shared pubsub.{t}
        // channels. Ids stay within the connector rules (3..255 chars, start with a
        // letter, charset [A-Za-z0-9._~%+-]); topic ids must not start with "sub.".
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String topicId = "orders-" + suffix;
        String subId = "sub-orders-" + suffix;

        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subscriptionName = SubscriptionName.of(projectId, subId);

        // PUBSUB_EMULATOR_HOST -> a plaintext channel + NoCredentialsProvider. This is
        // the emulator drop-in posture: the Java client talks insecure gRPC with no
        // Google auth. One channel is shared by every client below.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        NoCredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        TopicAdminClient topicAdmin = TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());
        SubscriptionAdminClient subscriptionAdmin = SubscriptionAdminClient.create(
                SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());

        Subscriber subscriber = null;
        try {
            // 1. CreateTopic -> channel pubsub.{topicId}.
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic        -> %s (channel pubsub.%s)%n", topicId, topicId);

            // 2. CreateSubscription -> per-sub queue pubsub.sub.{subId}. ackDeadline 10s
            //    is the connector minimum; the valid range is 0 (nack) or 10..600s.
            subscriptionAdmin.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.printf("CreateSubscription -> %s (channel pubsub.sub.%s)%n", subId, subId);

            // 3. Publish a known set; remember each by its seq attribute and block for the id.
            Set<String> published = ConcurrentHashMap.newKeySet();
            Publisher publisher = Publisher.newBuilder(topicName)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
            try {
                for (int i = 0; i < MESSAGE_COUNT; i++) {
                    String seq = Integer.toString(i);
                    PubsubMessage message = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8("event-" + seq))
                            .putAttributes("seq", seq)
                            .build();
                    publisher.publish(message).get(15, TimeUnit.SECONDS); // blocks until the connector acks.
                    published.add(seq);
                }
            } finally {
                publisher.shutdown();
                publisher.awaitTermination(15, TimeUnit.SECONDS);
            }
            System.out.printf("Publish            -> %d messages to the topic log%n", MESSAGE_COUNT);

            // 4. Open a long-lived StreamingPull stream via the high-level Subscriber.
            //    FlowControlSettings.maxOutstandingElementCount is the client-side ceiling
            //    on outstanding (un-acked) messages — the connector also enforces its own
            //    server-side MaxOutstandingMessages / MaxInflightPerSubscription safety nets.
            //    setParallelPull(1) keeps the proof deterministic (one StreamingPull stream).
            Set<String> received = ConcurrentHashMap.newKeySet();
            AtomicInteger duplicates = new AtomicInteger();
            CountDownLatch drained = new CountDownLatch(MESSAGE_COUNT);
            MessageReceiver receiver = (PubsubMessage m, AckReplyConsumer reply) -> {
                String seq = m.getAttributesMap().getOrDefault("seq", m.getData().toStringUtf8());
                reply.ack(); // ack the broker seq under its ack-deadline lease.
                if (received.add(seq)) {
                    drained.countDown();
                } else {
                    duplicates.incrementAndGet();
                }
            };
            subscriber = Subscriber.newBuilder(subscriptionName.toString(), receiver)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .setParallelPullCount(1)
                    .setFlowControlSettings(FlowControlSettings.newBuilder()
                            .setMaxOutstandingElementCount((long) MESSAGE_COUNT)
                            .build())
                    .build();
            subscriber.startAsync().awaitRunning();
            System.out.println("StreamingPull      -> stream open (Subscriber); acking each via callback");

            boolean complete = drained.await(30, TimeUnit.SECONDS);
            subscriber.stopAsync().awaitTerminated(15, TimeUnit.SECONDS);
            subscriber = null;
            if (!complete) {
                fail(String.format("timed out: drained %d of %d distinct messages", received.size(), MESSAGE_COUNT));
            }

            // Assertions: exactly the published set, each delivered exactly once.
            System.out.printf("Received           -> %d messages over the StreamingPull stream%n", received.size());
            if (received.size() != MESSAGE_COUNT) {
                fail(String.format("received %d distinct messages, expected %d", received.size(), MESSAGE_COUNT));
            }
            System.out.printf("  [OK] received all %d published messages%n", MESSAGE_COUNT);
            for (String seq : published) {
                if (!received.contains(seq)) {
                    fail(String.format("published message seq=%s was never delivered", seq));
                }
            }
            System.out.println("  [OK] every published message was delivered exactly once");
            if (duplicates.get() != 0) {
                fail(String.format("%d duplicate delivery/deliveries (expected 0)", duplicates.get()));
            }
            System.out.println("  [OK] no duplicate deliveries (each acked once)");
            System.out.println();
            System.out.println("PASS: StreamingPull round-trip complete — all messages drained and acked.");
        } finally {
            if (subscriber != null && subscriber.state() != ApiService.State.TERMINATED) {
                try {
                    subscriber.stopAsync().awaitTerminated(15, TimeUnit.SECONDS);
                } catch (RuntimeException ignored) {
                    // best-effort: a cancelled live stream may surface as an expected error.
                }
            }
            // Best-effort teardown so re-runs start clean (DeleteSubscription -> DeleteTopic).
            try {
                subscriptionAdmin.deleteSubscription(subscriptionName);
                System.out.println("DeleteSubscription -> ok");
            } catch (RuntimeException e) {
                System.out.println("warning: DeleteSubscription: " + e.getMessage());
            }
            try {
                topicAdmin.deleteTopic(topicName);
                System.out.println("DeleteTopic        -> ok");
            } catch (RuntimeException e) {
                System.out.println("warning: DeleteTopic: " + e.getMessage());
            }
            subscriptionAdmin.close();
            topicAdmin.close();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Any id works — the connector parses but ignores the project segment. */
    private static String projectId() {
        String id = System.getenv("PUBSUB_PROJECT_ID");
        if (id == null || id.isBlank()) {
            id = System.getenv("GOOGLE_CLOUD_PROJECT");
        }
        return (id == null || id.isBlank()) ? "my-project" : id;
    }

    /** The connector's emulator host; PUBSUB_EMULATOR_HOST is the standard env contract. */
    private static String emulatorHost() {
        String host = System.getenv("PUBSUB_EMULATOR_HOST");
        return (host == null || host.isBlank()) ? "localhost:8085" : host;
    }

    /** Print a failure to stderr and exit non-zero — examples are runnable proofs. */
    private static void fail(String message) {
        System.err.println("FAIL: " + message);
        System.exit(1);
    }
}
