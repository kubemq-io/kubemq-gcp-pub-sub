package io.kubemq.examples.gcp.subscriptions.ackdeadlinenack;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * subscriptions/ack-deadline-nack (master-table variant 5) — the ack-deadline
 * lease lifecycle, driven with unary {@code Pull} + {@code ModifyAckDeadline}.
 *
 * <p>{@code Publish -> Pull (lease) -> ModifyAckDeadline(>0) extend ->
 * ModifyAckDeadline(0) nack -> re-Pull (redelivered) -> Acknowledge}.
 *
 * <p>Every delivered message carries an opaque {@code ack_id} held under a
 * subscription-owned ack-deadline lease. {@code ModifyAckDeadline(ack_id, n>0)}
 * EXTENDS the lease (clamped to 10..600 s) — the message stays invisible and is
 * NOT redelivered. {@code ModifyAckDeadline(ack_id, 0)} is an immediate NACK that
 * redelivers the message at once (bypassing retry backoff). The connector's 250 ms
 * sweeper also redelivers any lease that expires un-acked. This example proves the
 * extend-then-nack path: the message reappears on a later Pull (same server-assigned
 * messageId), and a final {@code Acknowledge} drains it for good.
 *
 * <p>The high-level {@code Subscriber} auto-acks and auto-extends, so this variant
 * uses the low-level {@link SubscriberStub} ({@code pullCallable} /
 * {@code modifyAckDeadlineCallable} / {@code acknowledgeCallable}) to drive the
 * lease manually. Uses ONLY the official
 * {@code com.google.cloud:google-cloud-pubsub} client. With
 * {@code PUBSUB_EMULATOR_HOST} set the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — no
 * Google auth, insecure gRPC. Resource ids are uuid-suffixed per run so concurrent
 * runs occupy distinct {@code pubsub.{t}} / {@code pubsub.sub.{s}} channels and
 * never cross-talk; both are deleted on exit (best-effort).
 *
 * <p>A Publish writes once to the topic log {@code pubsub.{t}} (the authoritative,
 * replayable, cross-protocol source), then fans out one queue copy per subscription
 * {@code pubsub.sub.{s}} (spec S3.3). Any failed assertion exits non-zero.
 */
public final class Main {

    /** Lease extension applied before the nack — within the valid 10..600 s window. */
    private static final int EXTEND_SECONDS = 30;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] subscriptions/ack-deadline-nack | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels
        // (the project segment is ignored, so a bare "orders" would collide across runs).
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String topicId = "orders-" + suffix;
        String subId = "sub-orders-" + suffix;

        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subscriptionName = SubscriptionName.of(projectId, subId);
        String subscriptionPath = subscriptionName.toString();

        // PUBSUB_EMULATOR_HOST → plaintext channel + NoCredentialsProvider (emulator drop-in:
        // insecure gRPC, no Google auth). One channel is shared by every client below.
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
        SubscriberStub subscriber = GrpcSubscriberStub.create(
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());

        try {
            // CreateTopic -> channel pubsub.{topicId}.
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic        -> %s (channel pubsub.%s)%n", topicId, topicId);

            // CreateSubscription with ackDeadline 10s (connector minimum; valid 0 or 10..600).
            subscriptionAdmin.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.printf("CreateSubscription -> %s (channel pubsub.sub.%s, ack deadline 10s)%n", subId, subId);

            // Publish a single message via the Publisher stub (unary Publish RPC).
            PubsubMessage payload = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("retry-me"))
                    .putAttributes("seq", "0")
                    .build();
            String firstId = publishOne(channelProvider, credentialsProvider, topicName, payload);
            System.out.println("Publish            -> 1 message to the topic log");

            // (1) First delivery — Pull leaves the message LEASED (we do NOT ack it).
            ReceivedMessage first = pullOne(subscriber, subscriptionPath);
            require(first != null, "first Pull returns the message");
            String ackId = first.getAckId();
            System.out.printf("Pull #1            -> body='%s' messageId=%s (leased, not acked)%n",
                    first.getMessage().getData().toStringUtf8(), first.getMessage().getMessageId());

            // (2) Extend the lease with ModifyAckDeadline(>0). The message stays invisible,
            //     so an immediate-return probe pull sees nothing.
            subscriber.modifyAckDeadlineCallable().call(ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .addAckIds(ackId)
                    .setAckDeadlineSeconds(EXTEND_SECONDS)
                    .build());
            System.out.printf("ModifyAckDeadline  -> +%ds (lease extended; message stays invisible)%n", EXTEND_SECONDS);
            ReceivedMessage whileLeased = pullOne(subscriber, subscriptionPath);
            require(whileLeased == null,
                    "extended lease should hide the message, but a pull returned one");
            System.out.println("Pull (probe)       -> 0 messages (still leased)");

            // (3) ModifyAckDeadline(0) is an immediate NACK → redeliver at once (bypassing backoff).
            subscriber.modifyAckDeadlineCallable().call(ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .addAckIds(ackId)
                    .setAckDeadlineSeconds(0)
                    .build());
            System.out.println("ModifyAckDeadline  -> 0 (nack: redeliver now, bypassing backoff)");

            // (4) Re-pull: the SAME message is redelivered (same server-assigned messageId).
            //     DeliveryAttempt is only populated when the subscription has a dead_letter_topic
            //     policy (Google's contract — see delivery/dead-letter-topic), so we verify
            //     redelivery by message identity instead.
            ReceivedMessage second = pullWithRetry(subscriber, subscriptionPath);
            require(second != null, "the nacked message is redelivered on a later Pull");
            require(second.getMessage().getData().equals(payload.getData()),
                    "redelivered body matches the original");
            require(second.getMessage().getMessageId().equals(firstId),
                    String.format("redelivered messageId should match the first delivery (was %s, now %s)",
                            firstId, second.getMessage().getMessageId()));
            System.out.printf("Pull #2            -> body='%s' messageId=%s (redelivered)%n",
                    second.getMessage().getData().toStringUtf8(), second.getMessage().getMessageId());

            // (5) Acknowledge for good — the lease is released and the message is gone.
            subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .addAckIds(second.getAckId())
                    .build());
            System.out.println("Acknowledge        -> redelivered message acked");

            // (6) Prove the subscription is now empty (the ack removed it; no further redelivery).
            ReceivedMessage leftover = pullOne(subscriber, subscriptionPath);
            require(leftover == null, "subscription is empty after the ack (no further redelivery)");
            System.out.println("Pull (drain)       -> 0 messages (empty after ack)");

            System.out.println();
            System.out.println("PASS: ack-deadline extend + 0-nack redelivery verified.");
        } finally {
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
            subscriber.close();
            subscriptionAdmin.close();
            topicAdmin.close();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Publish one message via a short-lived Publisher stub on the shared channel; returns its message id. */
    private static String publishOne(TransportChannelProvider channelProvider,
                                     NoCredentialsProvider credentialsProvider,
                                     TopicName topicName,
                                     PubsubMessage message) throws Exception {
        try (com.google.cloud.pubsub.v1.stub.PublisherStub publisher =
                     com.google.cloud.pubsub.v1.stub.GrpcPublisherStub.create(
                             com.google.cloud.pubsub.v1.stub.PublisherStubSettings.newBuilder()
                                     .setTransportChannelProvider(channelProvider)
                                     .setCredentialsProvider(credentialsProvider)
                                     .build())) {
            return publisher.publishCallable().call(PublishRequest.newBuilder()
                    .setTopic(topicName.toString())
                    .addMessages(message)
                    .build()).getMessageIds(0);
        }
    }

    /** Unary Pull of a single message; returns the ReceivedMessage or null if the queue is empty. */
    private static ReceivedMessage pullOne(SubscriberStub subscriber, String subscriptionPath) {
        PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                .setSubscription(subscriptionPath)
                .setMaxMessages(1)
                .build());
        List<ReceivedMessage> msgs = response.getReceivedMessagesList();
        return msgs.isEmpty() ? null : msgs.get(0);
    }

    /** Poll for a (re)delivered message; the nacked copy reappears after a short backoff. */
    private static ReceivedMessage pullWithRetry(SubscriberStub subscriber, String subscriptionPath)
            throws InterruptedException {
        for (int attempt = 0; attempt < 20; attempt++) {
            ReceivedMessage msg = pullOne(subscriber, subscriptionPath);
            if (msg != null) {
                return msg;
            }
            Thread.sleep(300);
        }
        return null;
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

    /** Assert an expected outcome; exit non-zero on failure (examples are runnable proofs). */
    private static void require(boolean condition, String message) {
        if (!condition) {
            System.err.println("FAIL: " + message);
            System.exit(1);
        }
    }
}
