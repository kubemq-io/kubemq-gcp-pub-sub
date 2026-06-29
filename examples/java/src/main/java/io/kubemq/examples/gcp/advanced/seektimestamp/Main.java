package io.kubemq.examples.gcp.advanced.seektimestamp;

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
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * advanced: seek-timestamp — master-table variant 11.
 *
 * <p>Rewind a subscription to a wall-clock instant and replay the topic log from there:
 * {@code CreateTopic -> CreateSubscription(retain_acked) -> Publish(3) -> Pull+Ack(drain)
 * -> Seek(marker between msg-1 and msg-2) -> Pull (replays msg-2,msg-3)
 * -> Seek(epoch / pre-window) -> Pull (clamps to earliest retained, replays all)}.
 *
 * <p>The topic is backed by a durable, replayable Events Store log ({@code pubsub.{t}}), so a
 * Seek to a timestamp rewinds the subscription's cursor and replays the topic log from the
 * first message published at/after that instant (spec S3.5 seek/snapshots). The connector
 * resolves the start seq from the topic log, purges the per-sub queue and drops in-flight
 * leases, then replays via {@code Array.RangeEventsStore} re-applying the subscription filter,
 * bounded by {@code MaxSeekReplay}. A seek to a timestamp older than the earliest retained
 * message CLAMPS to the earliest retained message — it is NOT an error.
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client. The
 * {@code PUBSUB_EMULATOR_HOST} env var selects the connector: every client (admin, publisher,
 * subscriber) is built on a plaintext gRPC channel to that host with {@link NoCredentialsProvider}
 * (the emulator skips Google auth). Resource ids are uuid-suffixed per run so concurrent runs use
 * distinct {@code pubsub.{t}} / {@code pubsub.sub.{s}} channels and never cross-talk; both are
 * deleted on exit (best-effort). Delivery is at-least-once, so each drain asserts on the DISTINCT
 * set of bodies (a redelivered duplicate is valid Pub/Sub semantics). Any failed assertion throws
 * and the JVM exits non-zero.
 */
public final class Main {

    private static final List<String> ORDER = List.of("msg-1", "msg-2", "msg-3");

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        System.out.printf(
                "[kubemq-gcp-pub-sub] advanced/seek-timestamp | emulator=%s project=%s%n",
                System.getenv("PUBSUB_EMULATOR_HOST"), projectId);

        // Unique-per-run resource ids (channel isolation). Canonical names: seekable / sub-seekable.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topicId = "seekable-" + suffix;
        String subId = "sub-seekable-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subName = SubscriptionName.of(projectId, subId);

        // Build a plaintext gRPC channel to PUBSUB_EMULATOR_HOST with no credentials — the
        // explicit, official-SDK emulator wiring (the emulator skips Google auth). Shared by the
        // admin clients, the Publisher, and the Subscriber.
        String emulatorHost = System.getenv().getOrDefault("PUBSUB_EMULATOR_HOST", "localhost:8085");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        try (TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());
                SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(credentialsProvider)
                                .build())) {

            // 1. CreateTopic -> channel pubsub.{topicId}.
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic        -> %s (channel pubsub.%s)%n", topicId, topicId);

            // 2. CreateSubscription with retain_acked_messages so the log stays replayable after
            //    ack; a Seek can then rewind past already-acknowledged messages (spec S3.5).
            //    Ack deadline 10s (connector default; valid 0 or 10..600s).
            Subscription subscription = Subscription.newBuilder()
                    .setName(subName.toString())
                    .setTopic(topicName.toString())
                    .setAckDeadlineSeconds(10)
                    .setRetainAckedMessages(true)
                    .setMessageRetentionDuration(Duration.newBuilder().setSeconds(600).build())
                    .build();
            subAdmin.createSubscription(subscription);
            System.out.printf(
                    "CreateSubscription -> %s (channel pubsub.sub.%s, retain_acked=true)%n",
                    subId, subId);

            try {
                // 3. Publish three messages with a recorded marker between msg-1 and msg-2/3.
                Publisher publisher = Publisher.newBuilder(topicName)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build();
                Instant marker;
                try {
                    publish(publisher, "msg-1");
                    Thread.sleep(1200);
                    marker = Instant.now();
                    System.out.printf(
                            "Marker timestamp   -> %s (between msg-1 and msg-2/msg-3)%n", marker);
                    Thread.sleep(1200);
                    publish(publisher, "msg-2");
                    publish(publisher, "msg-3");
                } finally {
                    publisher.shutdown();
                    publisher.awaitTermination(15, TimeUnit.SECONDS);
                }

                // 4. Initial drain: all three distinct messages delivered at least once.
                Set<String> first = new TreeSet<>(drainAck(
                        projectId, subId, "Pull (initial)", channelProvider, credentialsProvider));
                check(first.size() == 3,
                        "initial drain delivered all 3 distinct messages (" + first + ")");

                // 5. Seek to the marker: connector purges the queue and replays the topic log from
                //    that instant. msg-2 and msg-3 (at/after the marker) replay; msg-1 does not.
                subAdmin.seek(SeekRequest.newBuilder()
                        .setSubscription(subName.toString())
                        .setTime(toTimestamp(marker))
                        .build());
                System.out.printf("Seek(time=marker)  -> cursor rewound to %s%n", marker);
                Set<String> replay = new TreeSet<>(drainAck(projectId, subId,
                        "Pull (after marker seek)", channelProvider, credentialsProvider));
                check(replay.contains("msg-2") && replay.contains("msg-3"),
                        "marker seek replayed msg-2 and msg-3");
                check(!replay.contains("msg-1"),
                        "msg-1 (published before the marker) was NOT replayed");

                // 6. Seek to a pre-window timestamp (epoch). The connector CLAMPS to the earliest
                //    retained message rather than erroring, so the whole log replays.
                Instant epoch = Instant.EPOCH;
                subAdmin.seek(SeekRequest.newBuilder()
                        .setSubscription(subName.toString())
                        .setTime(toTimestamp(epoch))
                        .build());
                System.out.println(
                        "Seek(time=epoch)   -> pre-window (clamps to earliest retained, not an error)");
                Set<String> clamped = new TreeSet<>(drainAck(projectId, subId,
                        "Pull (after pre-window seek / clamp)", channelProvider, credentialsProvider));
                check(clamped.containsAll(ORDER),
                        "pre-window seek clamped to earliest retained (all 3 replayed)");

                System.out.println(
                        "PASS: timestamp seek replayed the in-window tail and clamped a pre-window seek");
            } finally {
                // 7. Best-effort teardown (DeleteSubscription -> DeleteTopic) so re-runs are clean.
                deleteQuietly(() -> subAdmin.deleteSubscription(subName), "DeleteSubscription");
                deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            }
        } finally {
            channel.shutdownNow();
        }
    }

    /** Publishes one message and blocks for the server-assigned id. */
    private static void publish(Publisher publisher, String body) throws Exception {
        String id = publisher.publish(PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(body))
                .build()).get(15, TimeUnit.SECONDS);
        System.out.printf("Publish            -> %s messageId=%s%n", body, id);
    }

    /**
     * Opens a short StreamingPull window, acks every delivered message, and returns the bodies
     * collected in that window (in arrival order). The connector replays asynchronously, so we
     * drain for a fixed window rather than expecting an exact count up front.
     */
    private static List<String> drainAck(String projectId, String subId, String label,
            TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider)
            throws Exception {
        ProjectSubscriptionName name = ProjectSubscriptionName.of(projectId, subId);
        List<String> bodies = new CopyOnWriteArrayList<>();
        Subscriber subscriber = Subscriber.newBuilder(name,
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    bodies.add(message.getData().toStringUtf8());
                    consumer.ack();
                })
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
        subscriber.startAsync().awaitRunning();
        try {
            Thread.sleep(2500); // collection window: let the replayed tail arrive.
        } finally {
            subscriber.stopAsync().awaitTerminated(15, TimeUnit.SECONDS);
        }
        // Report the DISTINCT set so output is stable across runs (a redelivered duplicate
        // would otherwise vary the raw list); delivery is at-least-once.
        Set<String> distinct = new TreeSet<>(bodies);
        System.out.printf("%-18s -> %d distinct %s%n", label, distinct.size(),
                new ArrayList<>(distinct));
        return new ArrayList<>(bodies);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    /** Asserts an expected outcome, printing an [OK] line or throwing (non-zero exit). */
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAIL: " + message);
        }
        System.out.println("[OK] " + message);
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
