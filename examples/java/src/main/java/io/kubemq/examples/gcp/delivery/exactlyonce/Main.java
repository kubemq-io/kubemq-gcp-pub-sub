package io.kubemq.examples.gcp.delivery.exactlyonce;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.StatusCode;
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
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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
import com.google.rpc.ErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.protobuf.StatusProto;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * delivery/exactly-once (master-table variant 10) — an
 * {@code enable_exactly_once_delivery} subscription strengthens the ack contract: a
 * durable ack is confirmed and a stale/duplicate ack is rejected.
 *
 * <p>{@code CreateTopic -> CreateSubscription(enable_exactly_once_delivery) ->
 * Publish -> Pull -> Acknowledge (durable) -> Acknowledge(same ack_id) ->
 * FAILED_PRECONDITION + ErrorInfo}.
 *
 * <p>The program creates an exactly-once subscription, publishes one message, pulls
 * it, and acks it. With exactly-once a successful unary {@code Acknowledge} means the
 * ack is <b>durable</b> — the message will not be redelivered. The program then
 * <b>replays the same {@code ack_id}</b>: because that lease is already consumed, the
 * connector rejects the stale ack with a unary {@code FAILED_PRECONDITION} carrying a
 * {@code google.rpc.ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID, domain:
 * pubsub.googleapis.com)}. This matches the real Google SDK contract (it is <b>not</b>
 * a literal {@code INVALID_ARGUMENT}); client libraries map this {@code ErrorInfo} to
 * a per-message permanent ack failure. The example reads the {@code ErrorInfo} from
 * the gax {@link ApiException} (the rich-status trailer) and asserts both the status
 * code and the reason. Over StreamingPull the same outcome surfaces as
 * {@code invalid_ack_ids} / {@code temporary_failed_ack_ids} on the
 * {@code AcknowledgeConfirmation}.
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client. With
 * {@code PUBSUB_EMULATOR_HOST} set the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — no Google
 * auth, insecure gRPC. The low-level {@link SubscriberStub} is used so the unary
 * {@code Acknowledge} (and its rejection) is observed directly.
 *
 * <p>A Publish writes once to the topic log {@code pubsub.{t}} (the authoritative,
 * replayable, cross-protocol source), then fans out one queue copy to
 * {@code pubsub.sub.{s}}; the lease, receive count, and the decoded {@code ack_id}
 * transaction all live on the node that issued the Pull (spec S3.5). Any failed
 * assertion exits non-zero.
 *
 * <p><b>HEADLINE CAVEAT: exactly-once is NODE-LOCAL.</b> An {@code ack_id} is valid
 * only on the cluster node that minted it; pin a subscription's StreamingPull traffic
 * to one node (sticky load balancing) or accept at-least-once across nodes.
 */
public final class Main {

    private static final String EXPECTED_REASON = "PERMANENT_FAILURE_INVALID_ACK_ID";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] delivery/exactly-once | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels.
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String topicId = "orders-" + suffix;
        String subId = "sub-orders-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subscriptionName = SubscriptionName.of(projectId, subId);
        String subscriptionPath = subscriptionName.toString();

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

            // CreateSubscription with enable_exactly_once_delivery=true; ack deadline 10s.
            subscriptionAdmin.createSubscription(Subscription.newBuilder()
                    .setName(subscriptionPath)
                    .setTopic(topicName.toString())
                    .setPushConfig(PushConfig.getDefaultInstance())
                    .setAckDeadlineSeconds(10)
                    .setEnableExactlyOnceDelivery(true)
                    .build());
            System.out.printf(
                    "CreateSubscription -> %s (enable_exactly_once_delivery=true)%n", subId);

            // Publish a single message.
            publisher.publishCallable().call(PublishRequest.newBuilder()
                    .setTopic(topicName.toString())
                    .addMessages(PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8("order-42"))
                            .build())
                    .build());
            System.out.println("Publish            -> 'order-42'");

            // Pull the message.
            ReceivedMessage rm = pullWithRetry(subscriber, subscriptionPath);
            require(rm != null, "expected exactly one message on the pull");
            String ackId = rm.getAckId();
            System.out.printf("Pull               -> '%s'%n", rm.getMessage().getData().toStringUtf8());

            // Durable ack: with exactly-once, a successful unary Acknowledge means the
            // ack took effect (the message will not be redelivered).
            subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .addAckIds(ackId)
                    .build());
            System.out.println("Acknowledge        -> durable (exactly-once ack confirmed)");

            // Replay the SAME ack_id. The connector must reject the stale ack with
            // FAILED_PRECONDITION + ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID).
            try {
                subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionPath)
                        .addAckIds(ackId)
                        .build());
                require(false, "stale ack of an already-used ack_id was unexpectedly accepted");
            } catch (ApiException ex) {
                require(ex.getStatusCode().getCode() == StatusCode.Code.FAILED_PRECONDITION,
                        "stale-ack status code must be FAILED_PRECONDITION but was "
                                + ex.getStatusCode().getCode());
                ErrorInfo info = errorInfo(ex);
                String reason = info != null ? info.getReason() : "<no-error-info>";
                String domain = info != null ? info.getDomain() : "<none>";
                System.out.printf(
                        "Acknowledge(stale) -> rejected: status=%s, ErrorInfo.reason=%s, domain=%s%n",
                        ex.getStatusCode().getCode(), reason, domain);
                require(EXPECTED_REASON.equals(reason),
                        "stale-ack ErrorInfo.reason must be " + EXPECTED_REASON + " but was " + reason);
            }

            System.out.println();
            System.out.println(
                    "PASS: exactly-once: durable ack confirmed; stale ack rejected with "
                            + EXPECTED_REASON);
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

    /**
     * Reads the {@code google.rpc.ErrorInfo} out of a gax {@link ApiException}: gax
     * surfaces it via {@code getErrorDetails().getErrorInfo()}; if that is absent the
     * fallback parses the rich-status trailer of the underlying gRPC cause. The same
     * detail the real Google client maps to a permanent per-message ack failure.
     */
    private static ErrorInfo errorInfo(ApiException ex) {
        if (ex.getErrorDetails() != null && ex.getErrorDetails().getErrorInfo() != null) {
            return ex.getErrorDetails().getErrorInfo();
        }
        com.google.rpc.Status status = StatusProto.fromThrowable(ex.getCause());
        if (status == null) {
            return null;
        }
        for (Any detail : status.getDetailsList()) {
            if (detail.is(ErrorInfo.class)) {
                try {
                    return detail.unpack(ErrorInfo.class);
                } catch (InvalidProtocolBufferException ignored) {
                    return null;
                }
            }
        }
        return null;
    }

    /** Unary Pull with a short retry until the published message is leased. */
    private static ReceivedMessage pullWithRetry(SubscriberStub subscriber, String subscriptionPath)
            throws InterruptedException {
        for (int attempt = 0; attempt < 20; attempt++) {
            PullResponse response = subscriber.pullCallable().call(PullRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .setMaxMessages(1)
                    .build());
            List<ReceivedMessage> msgs = response.getReceivedMessagesList();
            if (!msgs.isEmpty()) {
                return msgs.get(0);
            }
            Thread.sleep(300);
        }
        return null;
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
