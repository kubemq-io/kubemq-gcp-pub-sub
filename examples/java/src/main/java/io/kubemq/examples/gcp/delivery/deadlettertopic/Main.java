package io.kubemq.examples.gcp.delivery.deadlettertopic;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcPublisherStub;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.RetryPolicy;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * delivery/dead-letter-topic (master-table variant 8) — a source subscription with
 * a dead-letter policy moves a poison message to a separate topic once its receive
 * count exceeds {@code max_delivery_attempts}.
 *
 * <p>{@code CreateTopic x2 -> CreateSubscription(dead_letter_topic,
 * max_delivery_attempts=5) -> Publish(poison) -> Pull-without-ack (let leases
 * expire) -> dead-letter subscription Receive}.
 *
 * <p><b>Dead-lettering on this connector is driven by NATURAL ack-deadline
 * EXPIRY</b>, not by an explicit nack. Every expiry of the unacked lease raises the
 * receive count via the connector's 250 ms lease sweeper; once the count
 * <b>exceeds</b> {@code max_delivery_attempts} the sweeper republishes the message
 * to the dead-letter topic and acks the original off the source. An explicit
 * {@code ModifyAckDeadline(0)} nack redelivers but does NOT raise the count toward
 * the threshold (Google semantics), so this example deliberately pulls the source
 * subscription <b>without ever acking it</b> and waits for the message to surface on
 * the dead-letter subscription. A tight {@link RetryPolicy} (10..50 ms) makes the
 * re-arm/redelivery after each expiry immediate, so each cycle is bounded by the
 * 10 s ack deadline; with {@code max_delivery_attempts=5} the message dead-letters
 * after ~5 expiry cycles (roughly 50-65 s).
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client.
 * With {@code PUBSUB_EMULATOR_HOST} set the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — no
 * Google auth, insecure gRPC.
 *
 * <p>This is connector-level fan-out to the DLQ: the source topic log
 * {@code pubsub.{t}} fans a queue copy to {@code pubsub.sub.{worker}}; on exhaustion
 * the sweeper publishes to the dead-letter topic log {@code pubsub.{dlt}}, which fans
 * out to its own subscription {@code pubsub.sub.{dlq}} (spec S3.5). Any failed
 * assertion exits non-zero.
 */
public final class Main {

    /** Google's minimum; valid range is 5..100 (0 = unset). */
    private static final int MAX_DELIVERY_ATTEMPTS = 5;
    /** Overall budget for the dead-letter to surface (~5 expiry cycles of a 10 s deadline). */
    private static final long DEADLINE_SECONDS = 90;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] delivery/dead-letter-topic | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels.
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String srcTopicId = "orders-" + suffix;
        String dltTopicId = "dead-letters-" + suffix;
        String workSubId = "sub-worker-" + suffix;
        String dlqSubId = "sub-dlq-" + suffix;
        TopicName srcTopic = TopicName.of(projectId, srcTopicId);
        TopicName dltTopic = TopicName.of(projectId, dltTopicId);
        SubscriptionName workSub = SubscriptionName.of(projectId, workSubId);
        SubscriptionName dlqSub = SubscriptionName.of(projectId, dlqSubId);

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
        PublisherStub publisher = GrpcPublisherStub.create(
                PublisherStubSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());
        SubscriberStub subscriber = GrpcSubscriberStub.create(
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());

        try {
            topicAdmin.createTopic(srcTopic);
            topicAdmin.createTopic(dltTopic);
            System.out.printf(
                    "CreateTopic        -> source '%s' + dead-letter '%s'%n", srcTopicId, dltTopicId);

            // Source subscription: dead_letter_topic + max_delivery_attempts=5, plus a
            // tight retry backoff so a swept lease re-arms and redelivers immediately.
            subscriptionAdmin.createSubscription(Subscription.newBuilder()
                    .setName(workSub.toString())
                    .setTopic(srcTopic.toString())
                    .setPushConfig(PushConfig.getDefaultInstance())
                    .setAckDeadlineSeconds(10)
                    .setDeadLetterPolicy(DeadLetterPolicy.newBuilder()
                            .setDeadLetterTopic(dltTopic.toString())
                            .setMaxDeliveryAttempts(MAX_DELIVERY_ATTEMPTS)
                            .build())
                    .setRetryPolicy(RetryPolicy.newBuilder()
                            .setMinimumBackoff(Duration.newBuilder().setNanos(10_000_000).build())
                            .setMaximumBackoff(Duration.newBuilder().setNanos(50_000_000).build())
                            .build())
                    .build());
            System.out.printf(
                    "CreateSubscription -> %s (dead_letter_topic='%s', max_delivery_attempts=%d)%n",
                    workSubId, dltTopicId, MAX_DELIVERY_ATTEMPTS);

            subscriptionAdmin.createSubscription(
                    dlqSub, dltTopic, PushConfig.getDefaultInstance(), 10);
            System.out.printf("CreateSubscription -> %s (dead-letter sink)%n", dlqSubId);

            // Publish a single poison message to the source topic.
            publisher.publishCallable().call(PublishRequest.newBuilder()
                    .setTopic(srcTopic.toString())
                    .addMessages(PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8("poison"))
                            .build())
                    .build());
            System.out.println("Publish            -> 'poison' to the source topic");

            // Drive loop: each iteration tries to re-lease the source message (a
            // successful pull means the previous lease expired and the sweeper re-armed
            // it, raising the receive count) and checks the dead-letter subscription.
            // We NEVER ack the source — natural expiry is what climbs toward the budget.
            String dead = null;
            int srcDeliveries = 0;
            long start = System.nanoTime();
            while (dead == null
                    && (System.nanoTime() - start) < DEADLINE_SECONDS * 1_000_000_000L) {
                ReceivedMessage src = pull(subscriber, workSub.toString());
                if (src != null) {
                    srcDeliveries++;
                    long elapsed = (System.nanoTime() - start) / 1_000_000_000L;
                    System.out.printf(
                            "Pull (source)      -> delivery #%d (DeliveryAttempt=%d) @ %ds "
                                    + "- NOT acking, letting the lease expire%n",
                            srcDeliveries, src.getDeliveryAttempt(), elapsed);
                    // deliberately no ack / no nack — let the lease expire naturally
                }

                ReceivedMessage dlt = pull(subscriber, dlqSub.toString());
                if (dlt != null) {
                    dead = dlt.getMessage().getData().toStringUtf8();
                    subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                            .setSubscription(dlqSub.toString())
                            .addAckIds(dlt.getAckId())
                            .build());
                    long elapsed = (System.nanoTime() - start) / 1_000_000_000L;
                    System.out.printf(
                            "Pull (dead-letter) -> received '%s' after %d source deliveries (~%ds)%n",
                            dead, srcDeliveries, elapsed);
                    break;
                }
                Thread.sleep(1000);
            }

            require(dead != null,
                    "the poison message must be dead-lettered after exceeding "
                            + "max_delivery_attempts (source deliveries=" + srcDeliveries + ")");
            require("poison".equals(dead),
                    "dead-letter message body must be 'poison' but was '" + dead + "'");

            System.out.println();
            System.out.printf(
                    "PASS: poison message dead-lettered to '%s' once receive count "
                            + "exceeded max_delivery_attempts=%d%n",
                    dltTopicId, MAX_DELIVERY_ATTEMPTS);
        } finally {
            deleteQuietly(() -> subscriptionAdmin.deleteSubscription(dlqSub), "DeleteSubscription(dlq)");
            deleteQuietly(() -> subscriptionAdmin.deleteSubscription(workSub), "DeleteSubscription(worker)");
            deleteQuietly(() -> topicAdmin.deleteTopic(dltTopic), "DeleteTopic(dead-letter)");
            deleteQuietly(() -> topicAdmin.deleteTopic(srcTopic), "DeleteTopic(source)");
            subscriber.close();
            publisher.close();
            subscriptionAdmin.close();
            topicAdmin.close();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Unary Pull of a single message; returns the ReceivedMessage or null if empty. */
    private static ReceivedMessage pull(SubscriberStub subscriber, String subscriptionPath) {
        PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                .setSubscription(subscriptionPath)
                .setMaxMessages(1)
                .build());
        List<ReceivedMessage> msgs = response.getReceivedMessagesList();
        return msgs.isEmpty() ? null : msgs.get(0);
    }

    private static String projectId() {
        String id = System.getenv("PUBSUB_PROJECT_ID");
        if (id == null || id.isBlank()) {
            id = System.getenv("GOOGLE_CLOUD_PROJECT");
        }
        return (id == null || id.isBlank()) ? "my-project" : id;
    }

    private static String emulatorHost() {
        String host = System.getenv("PUBSUB_EMULATOR_HOST");
        return (host == null || host.isBlank()) ? "localhost:8085" : host;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            System.err.println("FAIL: " + message);
            System.exit(1);
        }
    }

    private static void deleteQuietly(Runnable delete, String label) {
        try {
            delete.run();
            System.out.println(label + " -> ok");
        } catch (RuntimeException e) {
            System.out.println("warning: " + label + ": " + e.getMessage());
        }
    }
}
