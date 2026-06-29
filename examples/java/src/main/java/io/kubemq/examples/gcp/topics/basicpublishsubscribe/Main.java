package io.kubemq.examples.gcp.topics.basicpublishsubscribe;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * topics: basic-publish-subscribe — master-table variant 1.
 *
 * <p>The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
 * {@code CreateTopic -> CreateSubscription -> Publish -> Pull (StreamingPull) -> Acknowledge}.
 *
 * <p>It publishes one message to a uuid-suffixed topic, pulls exactly that message
 * back through the subscription via a {@link Subscriber} StreamingPull loop, asserts
 * the body and the user attribute round-trip and that the received message id matches
 * the publish id, then acks it. Topic and subscription are uuid-suffixed per run so
 * concurrent runs use distinct {@code pubsub.{t}} / {@code pubsub.sub.{s}} channels and
 * never cross-talk; both are deleted on exit (best-effort) so re-runs start clean.
 *
 * <p>A Publish writes once to the topic log {@code pubsub.orders-<uuid8>} (the
 * authoritative, replayable, cross-protocol source), then fans out one queue copy to the
 * per-subscription queue {@code pubsub.sub.sub-orders-<uuid8>} (spec S3.3).
 *
 * <p>Connectivity: this example is driven entirely by {@code PUBSUB_EMULATOR_HOST}. When
 * it is set, every client is built against that host over a plaintext gRPC channel with
 * {@link NoCredentialsProvider} (the standard Google-emulator posture — no Google auth, no
 * TLS), using only official {@code google-cloud-pubsub} APIs. A failed assertion throws and
 * the JVM exits non-zero.
 */
public final class Main {

    private static final String BODY = "order #4242 — 3x widget, ship express";
    private static final String ATTR_KEY = "priority";
    private static final String ATTR_VAL = "express";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        System.out.printf(
                "[kubemq-gcp-pub-sub] topics/basic-publish-subscribe | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids (channel isolation). Canonical master-table names are
        // orders / sub-orders; the uuid8 suffix gives this run a private channel namespace.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topicId = "orders-" + suffix;
        String subId = "sub-orders-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subName = SubscriptionName.of(projectId, subId);

        // One plaintext channel to the connector; PUBSUB_EMULATOR_HOST IS the contract.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider noCreds = NoCredentialsProvider.create();

        try (TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(noCreds)
                .build());
                SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(noCreds)
                                .build())) {

            // 1. CreateTopic -> channel pubsub.{topicId}.
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic: %s (channel pubsub.%s)%n", topicId, topicId);

            // 2. CreateSubscription -> per-sub queue pubsub.sub.{subId}; ack deadline 10s
            //    (connector default; valid range 0 or 10..600s).
            subAdmin.createSubscription(subName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.printf("CreateSubscription: %s (channel pubsub.sub.%s)%n", subId, subId);

            try {
                // 3. Publish one message and block for the server-assigned id.
                Publisher publisher = Publisher.newBuilder(topicName)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(noCreds)
                        .build();
                String msgId;
                try {
                    PubsubMessage message = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(BODY))
                            .putAttributes(ATTR_KEY, ATTR_VAL)
                            .build();
                    msgId = publisher.publish(message).get(15, TimeUnit.SECONDS);
                } finally {
                    publisher.shutdown();
                    publisher.awaitTermination(15, TimeUnit.SECONDS);
                }
                System.out.printf("Publish: messageId=%s%n", msgId);

                // 4. Pull exactly one message via StreamingPull, ack it, then stop the loop.
                PubsubMessage got = pullOne(subName, channelProvider, noCreds);
                if (got == null) {
                    throw new AssertionError("FAIL: expected 1 message, received 0");
                }
                System.out.printf(
                        "Receive: messageId=%s body=%s attr[%s]=%s publishTime=%s%n",
                        got.getMessageId(), quote(got.getData().toStringUtf8()), ATTR_KEY,
                        quote(got.getAttributesOrDefault(ATTR_KEY, "")), publishTime(got));

                // Assertions: byte-for-byte body, attribute, reserved message id.
                String body = got.getData().toStringUtf8();
                if (!BODY.equals(body)) {
                    throw new AssertionError(
                            "FAIL: received body " + quote(body) + " != published " + quote(BODY));
                }
                if (!ATTR_VAL.equals(got.getAttributesOrDefault(ATTR_KEY, ""))) {
                    throw new AssertionError("FAIL: received attr " + ATTR_KEY + "="
                            + quote(got.getAttributesOrDefault(ATTR_KEY, "")) + " != published "
                            + quote(ATTR_VAL));
                }
                if (!msgId.equals(got.getMessageId())) {
                    throw new AssertionError("FAIL: received messageId " + quote(got.getMessageId())
                            + " != published " + quote(msgId));
                }
                System.out.println("Acknowledge: ok (acked by ack_id under its lease)");
                System.out.println(
                        "PASS: full Pub/Sub round-trip verified (publish -> pull -> ack)");
            } finally {
                // 5. Best-effort teardown (DeleteSubscription -> DeleteTopic) so re-runs are clean.
                deleteQuietly(() -> subAdmin.deleteSubscription(subName), "DeleteSubscription");
                deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            }
        } finally {
            channel.shutdown();
            channel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** Opens a StreamingPull Subscriber, acks the first message, and returns it (or null). */
    private static PubsubMessage pullOne(SubscriptionName subName,
            TransportChannelProvider channelProvider, CredentialsProvider noCreds)
            throws Exception {
        ProjectSubscriptionName name =
                ProjectSubscriptionName.of(subName.getProject(), subName.getSubscription());
        AtomicReference<PubsubMessage> holder = new AtomicReference<>();
        Subscriber subscriber = Subscriber.newBuilder(name,
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    consumer.ack(); // ack the broker seq under its ack-deadline lease.
                    holder.compareAndSet(null, message);
                })
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(noCreds)
                .build();
        subscriber.startAsync().awaitRunning();
        try {
            Instant deadline = Instant.now().plusSeconds(15);
            while (holder.get() == null && Instant.now().isBefore(deadline)) {
                Thread.sleep(50);
            }
        } finally {
            subscriber.stopAsync().awaitTerminated(15, TimeUnit.SECONDS);
        }
        return holder.get();
    }

    private static String publishTime(PubsubMessage m) {
        if (m.hasPublishTime()) {
            return Instant.ofEpochSecond(m.getPublishTime().getSeconds(),
                    m.getPublishTime().getNanos()).toString();
        }
        return "<publish-time>";
    }

    private static String quote(String s) {
        return "\"" + s + "\"";
    }

    /** Best-effort delete: a missing resource (already gone) must not fail the run. */
    private static void deleteQuietly(Runnable delete, String label) {
        try {
            delete.run();
            System.out.println(label + ": ok");
        } catch (NotFoundException e) {
            System.out.println(label + ": already gone");
        } catch (RuntimeException e) {
            System.out.println("warning: " + label + ": " + e.getMessage());
        }
    }
}
