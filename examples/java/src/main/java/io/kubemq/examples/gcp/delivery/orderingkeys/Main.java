package io.kubemq.examples.gcp.delivery.orderingkeys;

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
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * delivery/ordering-keys (master-table variant 6) — per-key in-order delivery,
 * at most one message in flight per key.
 *
 * <p>{@code CreateTopic -> CreateSubscription(enable_message_ordering) ->
 * Publish(orderingKey) x3 + keyless -> Pull -> Acknowledge}.
 *
 * <p>The subscription is created with {@code enable_message_ordering = true}. The
 * program publishes three messages — {@code m1, m2, m3} — sequentially on the
 * ordering key {@code cust-7} (sequential low-level {@code Publish} calls fix the
 * publish order for the key), plus one keyless message. It then pulls one message
 * at a time, acking each before pulling the next. Because the connector keeps
 * <b>at most one message in flight per ordering key</b> and delivers a key's
 * messages in publish order, the keyed messages arrive {@code m1 -> m2 -> m3}; the
 * assertion fails (non-zero exit) on any other order. The keyless message is
 * unordered and arrives alongside the keyed sequence.
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client.
 * With {@code PUBSUB_EMULATOR_HOST} set the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — no
 * Google auth, insecure gRPC. The low-level {@link PublisherStub} is used so the
 * per-key publish order is exactly the order of the sequential calls, and the
 * low-level {@link SubscriberStub} so each message is acked before the next pull
 * (the high-level {@code Subscriber} would buffer concurrently).
 *
 * <p>A Publish writes once to the topic log {@code pubsub.{t}} (the authoritative,
 * replayable, cross-protocol source), then fans out one queue copy to
 * {@code pubsub.sub.{s}} (spec S3.3). The ordering key rides the wire as the
 * reserved tag {@code _pubsub_ordering_key}; the connector's round-robin cursor
 * keeps distinct keys independent and in-order per key. Any failed assertion exits
 * non-zero.
 */
public final class Main {

    private static final String ORDERING_KEY = "cust-7";
    private static final List<String> ORDERED_BODIES = List.of("m1", "m2", "m3");
    private static final String KEYLESS_BODY = "keyless";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] delivery/ordering-keys | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels
        // (the project segment is ignored, so a bare "ordered" would collide across runs).
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
            // CreateTopic -> channel pubsub.{topicId}.
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic        -> %s (channel pubsub.%s)%n", topicId, topicId);

            // CreateSubscription with enable_message_ordering=true; ack deadline 10s
            // (connector minimum; valid 0 or 10..600). Ordering must be enabled at create.
            subscriptionAdmin.createSubscription(Subscription.newBuilder()
                    .setName(subscriptionPath)
                    .setTopic(topicName.toString())
                    .setPushConfig(PushConfig.getDefaultInstance())
                    .setAckDeadlineSeconds(10)
                    .setEnableMessageOrdering(true)
                    .build());
            System.out.printf(
                    "CreateSubscription -> %s (channel pubsub.sub.%s, enable_message_ordering=true)%n",
                    subId, subId);

            // Publish m1, m2, m3 sequentially on the SAME ordering key — sequential
            // calls fix the publish order for that key — plus one keyless message.
            for (String body : ORDERED_BODIES) {
                publishOne(publisher, topicName, body, ORDERING_KEY);
                System.out.printf("Publish            -> '%s' orderingKey='%s'%n", body, ORDERING_KEY);
            }
            publishOne(publisher, topicName, KEYLESS_BODY, "");
            System.out.printf("Publish            -> '%s' (no ordering key)%n", KEYLESS_BODY);

            // Pull one at a time and ack each before pulling the next: the connector
            // holds at most one in flight per key, so the keyed head only advances once
            // its predecessor is acked. Record the order keyed messages arrive in.
            List<String> orderedReceived = new ArrayList<>();
            int total = ORDERED_BODIES.size() + 1;
            for (int i = 0; i < total; i++) {
                ReceivedMessage rm = pullWithRetry(subscriber, subscriptionPath);
                require(rm != null, "expected " + total + " messages; the queue went empty early");
                PubsubMessage msg = rm.getMessage();
                String body = msg.getData().toStringUtf8();
                String key = msg.getOrderingKey();
                if (!key.isEmpty()) {
                    orderedReceived.add(body);
                    System.out.printf("Pull               -> '%s' orderingKey='%s' -> ack%n", body, key);
                } else {
                    System.out.printf("Pull               -> '%s' (keyless) -> ack%n", body);
                }
                subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionPath)
                        .addAckIds(rm.getAckId())
                        .build());
            }

            // The keyed messages MUST have arrived in publish order m1 -> m2 -> m3.
            require(orderedReceived.equals(ORDERED_BODIES),
                    "ordered key '" + ORDERING_KEY + "' must deliver in publish order "
                            + ORDERED_BODIES + " but arrived " + orderedReceived);

            System.out.println();
            System.out.printf(
                    "PASS: ordered key '%s' delivered in publish order: m1 -> m2 -> m3%n", ORDERING_KEY);
        } finally {
            deleteQuietly(() -> subscriptionAdmin.deleteSubscription(subscriptionName), "DeleteSubscription");
            deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            subscriber.close();
            publisher.close();
            subscriptionAdmin.close();
            topicAdmin.close();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Publish a single message (optionally with an ordering key) via the low-level stub. */
    private static void publishOne(PublisherStub publisher, TopicName topicName,
                                   String body, String orderingKey) {
        PubsubMessage.Builder msg = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(body));
        if (!orderingKey.isEmpty()) {
            msg.setOrderingKey(orderingKey);
        }
        publisher.publishCallable().call(PublishRequest.newBuilder()
                .setTopic(topicName.toString())
                .addMessages(msg.build())
                .build());
    }

    /** Poll for a single delivered message; the head of a key advances after its ack. */
    private static ReceivedMessage pullWithRetry(SubscriberStub subscriber, String subscriptionPath)
            throws InterruptedException {
        for (int attempt = 0; attempt < 40; attempt++) {
            PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .setMaxMessages(1)
                    .build());
            List<ReceivedMessage> msgs = response.getReceivedMessagesList();
            if (!msgs.isEmpty()) {
                return msgs.get(0);
            }
            Thread.sleep(250);
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

    /** Best-effort delete: a missing resource (already gone) must not fail the run. */
    private static void deleteQuietly(Runnable delete, String label) {
        try {
            delete.run();
            System.out.println(label + " -> ok");
        } catch (RuntimeException e) {
            System.out.println("warning: " + label + ": " + e.getMessage());
        }
    }
}
