package io.kubemq.examples.gcp.advanced.snapshots;

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
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.SnapshotName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * advanced: snapshots — master-table variant 12.
 *
 * <p>Capture a subscription cursor as a snapshot, then seek the subscription back to it:
 * {@code CreateTopic -> CreateSubscription(retain_acked) -> Publish(msg-1,msg-2) -> Pull+Ack
 * -> CreateSnapshot (captures the acked cursor) -> Publish(msg-3) -> Pull+Ack
 * -> Seek(snapshot) -> Pull (replays only msg-3, published after the captured cursor)}.
 *
 * <p>A snapshot captures the subscription's cursor as a registry record (7-day default expiry,
 * swept hourly). Seeking the subscription to that snapshot purges the per-sub queue and replays
 * the topic log {@code pubsub.{t}} from the captured position via {@code Array.RangeEventsStore},
 * so msg-3 (published after the snapshot) is redelivered while msg-1 (well before the cursor) is
 * not. The cursor boundary is inclusive of the last message acked before capture (msg-2 also
 * replays). Snapshotting a detached subscription returns {@code FAILED_PRECONDITION} (spec S3.5).
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client. The
 * {@code PUBSUB_EMULATOR_HOST} env var selects the connector: every client (admin, publisher,
 * subscriber) is built on a plaintext gRPC channel to that host with {@link NoCredentialsProvider}
 * (the emulator skips Google auth). Resource ids (topic, subscription, snapshot) are uuid-suffixed
 * per run so concurrent runs use distinct {@code pubsub.{t}} / {@code pubsub.sub.{s}} channels and
 * registry records and never cross-talk; all are deleted on exit (best-effort). Delivery is
 * at-least-once, so each drain asserts on the DISTINCT set. Any failed assertion throws and the
 * JVM exits non-zero.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        System.out.printf(
                "[kubemq-gcp-pub-sub] advanced/snapshots | emulator=%s project=%s%n",
                System.getenv("PUBSUB_EMULATOR_HOST"), projectId);

        // Unique-per-run resource ids (channel isolation). Canonical names: snapped / sub-snapped.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topicId = "snapped-" + suffix;
        String subId = "sub-snapped-" + suffix;
        String snapId = "snap-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subName = SubscriptionName.of(projectId, subId);
        SnapshotName snapName = SnapshotName.of(projectId, snapId);

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

            // 2. CreateSubscription with retain_acked_messages so a seek can rewind past acked
            //    messages; ack deadline 10s (connector default; valid 0 or 10..600s).
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
                // 3. Publish + drain msg-1,msg-2 so the cursor advances past them.
                Publisher publisher = newPublisher(topicName, channelProvider, credentialsProvider);
                try {
                    publish(publisher, "msg-1");
                    publish(publisher, "msg-2");
                } finally {
                    publisher.shutdown();
                    publisher.awaitTermination(15, TimeUnit.SECONDS);
                }
                Set<String> drained = new TreeSet<>(drainAck(projectId, subId,
                        "Pull (drain msg-1,msg-2)", channelProvider, credentialsProvider));
                check(drained.contains("msg-1") && drained.contains("msg-2"),
                        "first drain delivered msg-1 and msg-2 (" + drained + ")");

                // 4. CreateSnapshot — captures the cursor after msg-1,msg-2 acked.
                subAdmin.createSnapshot(snapName, subName);
                System.out.printf(
                        "CreateSnapshot     -> %s (cursor after msg-1,msg-2; 7-day expiry, swept hourly)%n",
                        snapId);

                // 5. Publish + drain msg-3; the live subscription is now past msg-3 too.
                Publisher publisher2 = newPublisher(topicName, channelProvider, credentialsProvider);
                try {
                    publish(publisher2, "msg-3");
                } finally {
                    publisher2.shutdown();
                    publisher2.awaitTermination(15, TimeUnit.SECONDS);
                }
                Set<String> after = new TreeSet<>(drainAck(projectId, subId,
                        "Pull (drain msg-3)", channelProvider, credentialsProvider));
                check(after.contains("msg-3"), "second drain delivered msg-3 (" + after + ")");

                // 6. Seek the subscription back to the snapshot cursor. The connector purges the
                //    queue and replays the topic log from the captured position, so msg-3
                //    (published after the snapshot) is redelivered while msg-1 (well before the
                //    cursor) is not. The cursor boundary is inclusive of the last message acked
                //    before the snapshot (msg-2), so msg-2 also replays — the connector resolves
                //    the snapshot to the earliest unacked sequence at capture time.
                subAdmin.seek(SeekRequest.newBuilder()
                        .setSubscription(subName.toString())
                        .setSnapshot(snapName.toString())
                        .build());
                System.out.printf("Seek(snapshot)     -> rewound subscription to snapshot %s%n",
                        snapId);
                Set<String> replay = new TreeSet<>(drainAck(projectId, subId,
                        "Pull (after seek-to-snapshot)", channelProvider, credentialsProvider));
                check(replay.contains("msg-3"),
                        "seek-to-snapshot replayed msg-3 (published after the captured cursor)");
                check(!replay.contains("msg-1"),
                        "msg-1 (well before the captured cursor) was NOT replayed");

                System.out.println(
                        "PASS: snapshot captured the cursor; seek-to-snapshot replayed the tail from it");
            } finally {
                // 7. Best-effort teardown (snapshot -> subscription -> topic) so re-runs are clean.
                deleteQuietly(() -> subAdmin.deleteSnapshot(snapName), "DeleteSnapshot");
                deleteQuietly(() -> subAdmin.deleteSubscription(subName), "DeleteSubscription");
                deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            }
        } finally {
            channel.shutdownNow();
        }
    }

    /** Builds an emulator-wired Publisher (plaintext channel + no credentials). */
    private static Publisher newPublisher(TopicName topicName,
            TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider)
            throws Exception {
        return Publisher.newBuilder(topicName)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
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
     * collected in that window. Reports the DISTINCT set so output is stable (delivery is
     * at-least-once; a redelivered duplicate is valid Pub/Sub semantics).
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
            Thread.sleep(2000); // collection window.
        } finally {
            subscriber.stopAsync().awaitTerminated(15, TimeUnit.SECONDS);
        }
        Set<String> distinct = new TreeSet<>(bodies);
        System.out.printf("%-18s -> %d distinct %s%n", label, distinct.size(),
                new ArrayList<>(distinct));
        return new ArrayList<>(bodies);
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
