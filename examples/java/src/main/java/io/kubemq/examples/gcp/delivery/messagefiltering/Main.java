package io.kubemq.examples.gcp.delivery.messagefiltering;

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
 * delivery/message-filtering (master-table variant 7) — a CEL-subset, attributes-only
 * subscription filter applied at publish fan-out.
 *
 * <p>{@code CreateTopic -> CreateSubscription(all) + CreateSubscription(filter) ->
 * Publish(mixed batch) -> Pull -> Acknowledge}.
 *
 * <p>One topic, two subscriptions: {@code sub-all} (no filter) and {@code sub-eu}
 * with the filter {@code attributes.region = "eu"}. The program publishes a mixed
 * batch of three messages tagged {@code region=eu}, {@code region=us},
 * {@code region=eu}. The filter is compiled once at {@code CreateSubscription} and
 * is immutable; it is applied at <b>publish fan-out</b>, so a non-matching message
 * is never enqueued for that subscription. The unfiltered subscription therefore
 * receives all three ({@code o1, o2, o3}); the filtered subscription receives only
 * the two {@code eu} messages ({@code o1, o3}); the {@code us} message is
 * suppressed (0 delivered to the filtered sub). The assertions fail (non-zero exit)
 * if either subscription receives the wrong set.
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client.
 * With {@code PUBSUB_EMULATOR_HOST} set the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — no
 * Google auth, insecure gRPC. The low-level {@link PublisherStub}/{@link SubscriberStub}
 * are used so the batch publish and the per-subscription pulls are explicit.
 *
 * <p>A Publish writes once to the topic log {@code pubsub.{t}} (the authoritative,
 * replayable, cross-protocol source), then fans out one queue copy per subscription
 * ({@code pubsub.sub.{s}}), applying each subscription's filter at fan-out time; the
 * topic log itself is unfiltered (spec S3.3 / S3.5). Any failed assertion exits non-zero.
 */
public final class Main {

    private static final String FILTER = "attributes.region = \"eu\"";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] delivery/message-filtering | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels.
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String topicId = "orders-" + suffix;
        String subAllId = "sub-all-" + suffix;
        String subEuId = "sub-eu-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subAllName = SubscriptionName.of(projectId, subAllId);
        SubscriptionName subEuName = SubscriptionName.of(projectId, subEuId);

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
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic        -> %s (channel pubsub.%s)%n", topicId, topicId);

            // Unfiltered subscription: receives everything.
            subscriptionAdmin.createSubscription(
                    subAllName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.printf("CreateSubscription -> %s (unfiltered)%n", subAllId);

            // Filtered subscription: CEL-subset attributes-only filter, compiled at create.
            subscriptionAdmin.createSubscription(Subscription.newBuilder()
                    .setName(subEuName.toString())
                    .setTopic(topicName.toString())
                    .setPushConfig(PushConfig.getDefaultInstance())
                    .setAckDeadlineSeconds(10)
                    .setFilter(FILTER)
                    .build());
            System.out.printf("CreateSubscription -> %s (filter: %s)%n", subEuId, FILTER);

            // Publish a mixed batch: o1(eu), o2(us), o3(eu) — one atomic PublishRequest.
            publisher.publishCallable().call(PublishRequest.newBuilder()
                    .setTopic(topicName.toString())
                    .addMessages(message("o1", "eu"))
                    .addMessages(message("o2", "us"))
                    .addMessages(message("o3", "eu"))
                    .build());
            System.out.println("Publish            -> batch: o1(region=eu), o2(region=us), o3(region=eu)");

            // The unfiltered sub must see all three; the filtered sub only the two eu.
            List<String> all = drain(subscriber, subAllName.toString(), 3);
            System.out.printf("Pull (unfiltered)  -> %s received: %s%n", subAllId, all);
            List<String> eu = drain(subscriber, subEuName.toString(), 2);
            System.out.printf("Pull (filtered)    -> %s received: %s  (o2/region=us -> suppressed)%n", subEuId, eu);

            require(all.equals(List.of("o1", "o2", "o3")),
                    "unfiltered subscription must receive [o1, o2, o3] but got " + all);
            require(eu.equals(List.of("o1", "o3")),
                    "filtered subscription must receive [o1, o3] (us suppressed) but got " + eu);

            // Prove the filtered sub has nothing more (the us message is never enqueued).
            require(extraPull(subscriber, subEuName.toString()) == null,
                    "filtered subscription must NOT receive the non-matching (region=us) message");

            System.out.println();
            System.out.println(
                    "PASS: filter suppressed the non-matching message (region=us): "
                            + "3 delivered to all, 2 to eu");
        } finally {
            deleteQuietly(() -> subscriptionAdmin.deleteSubscription(subEuName), "DeleteSubscription(eu)");
            deleteQuietly(() -> subscriptionAdmin.deleteSubscription(subAllName), "DeleteSubscription(all)");
            deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            subscriber.close();
            publisher.close();
            subscriptionAdmin.close();
            topicAdmin.close();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** A message body tagged with the {@code region} attribute. */
    private static PubsubMessage message(String body, String region) {
        return PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(body))
                .putAttributes("region", region)
                .build();
    }

    /** Pull and ack until {@code expected} bodies are gathered (in arrival order). */
    private static List<String> drain(SubscriberStub subscriber, String subscriptionPath, int expected)
            throws InterruptedException {
        List<String> bodies = new ArrayList<>();
        for (int attempt = 0; attempt < 40 && bodies.size() < expected; attempt++) {
            PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .setMaxMessages(expected)
                    .build());
            for (ReceivedMessage rm : response.getReceivedMessagesList()) {
                bodies.add(rm.getMessage().getData().toStringUtf8());
                subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionPath)
                        .addAckIds(rm.getAckId())
                        .build());
            }
            if (bodies.size() < expected) {
                Thread.sleep(250);
            }
        }
        return bodies;
    }

    /** One short probe pull (no ack needed when empty); returns a body or null. */
    private static String extraPull(SubscriberStub subscriber, String subscriptionPath) {
        PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                .setSubscription(subscriptionPath)
                .setMaxMessages(1)
                .build());
        List<ReceivedMessage> msgs = response.getReceivedMessagesList();
        if (msgs.isEmpty()) {
            return null;
        }
        subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionPath)
                .addAckIds(msgs.get(0).getAckId())
                .build());
        return msgs.get(0).getMessage().getData().toStringUtf8();
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
