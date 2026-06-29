package io.kubemq.examples.gcp.topics.topicmanagement;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient.ListTopicSubscriptionsPagedResponse;
import com.google.cloud.pubsub.v1.TopicAdminClient.ListTopicsPagedResponse;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.UpdateTopicRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * topics: topic-management — master-table variant 3.
 *
 * <p>The topic administrative surface against the KubeMQ Pub/Sub connector:
 * {@code CreateTopic -> ListTopics -> GetTopic -> UpdateTopic -> CreateSubscription
 * -> ListTopicSubscriptions -> DeleteTopic (tombstone)}.
 *
 * <p>It creates a topic (with labels and a 30-minute retention) plus one subscription on
 * it, confirms the topic appears in the project-wide {@code ListTopics}, reads its config
 * with {@code GetTopic}, mutates its labels via {@code UpdateTopic} (a {@link FieldMask}
 * targeting {@code labels}) and asserts the mutation stuck, lists the subscriptions
 * attached to the topic, then deletes the topic and asserts the delete is a tombstone
 * ({@code GetTopic} -&gt; {@code NotFound}). All ids are uuid-suffixed per run and cleaned
 * up on exit. Any mismatch exits non-zero.
 *
 * <p>Note (spec S3.6): topic ids may not start with {@code sub.} (reserved broker
 * namespace), and the requested retention is clamped to the broker's
 * {@code Store.MaxRetention} — {@code GetTopic} returns the requested value while
 * fan-out/seek use the clamped value.
 *
 * <p>Connectivity: this example is driven entirely by {@code PUBSUB_EMULATOR_HOST}. When
 * it is set, every client is built against that host over a plaintext gRPC channel with
 * {@link NoCredentialsProvider} (no Google auth, no TLS), using only official
 * {@code google-cloud-pubsub} APIs.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        System.out.printf("[kubemq-gcp-pub-sub] topics/topic-management | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids (channel isolation). Canonical names: managed / sub-managed.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topicId = "managed-" + suffix;
        String subId = "sub-managed-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subName = SubscriptionName.of(projectId, subId);

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

            // CreateTopic with labels and a 30-minute retention (10 min..31 days valid range,
            // clamped to the broker's Store.MaxRetention).
            Topic created = topicAdmin.createTopic(Topic.newBuilder()
                    .setName(topicName.toString())
                    .putLabels("team", "orders")
                    .putLabels("stage", "demo")
                    .setMessageRetentionDuration(Duration.newBuilder().setSeconds(30 * 60).build())
                    .build());
            System.out.printf("CreateTopic: %s (channel pubsub.%s)%n", topicId, topicId);

            // topicDeleted records whether the happy-path DeleteTopic already ran, so cleanup
            // does not re-delete it (which would log a spurious not-found).
            boolean[] topicDeleted = {false};
            try {
                // CreateSubscription so ListTopicSubscriptions has something to return.
                subAdmin.createSubscription(subName, topicName, PushConfig.getDefaultInstance(), 10);
                System.out.printf("CreateSubscription: %s (channel pubsub.sub.%s)%n", subId, subId);

                // ListTopics — confirm our topic is in the project-wide list.
                boolean found = false;
                ListTopicsPagedResponse topics = topicAdmin.listTopics(ProjectName.of(projectId));
                for (Topic t : topics.iterateAll()) {
                    if (t.getName().equals(topicName.toString())) {
                        found = true;
                    }
                }
                if (!found) {
                    throw new AssertionError(
                            "FAIL: created topic " + quote(topicId) + " not found in ListTopics");
                }
                System.out.printf("ListTopics: found %s%n", topicId);

                // GetTopic — read the config back.
                Topic cfg = topicAdmin.getTopic(topicName);
                System.out.printf("GetTopic: labels=%s retention=%s%n",
                        sortedLabels(cfg.getLabelsMap()),
                        durationOf(cfg.getMessageRetentionDuration()));
                if (!"orders".equals(cfg.getLabelsOrDefault("team", ""))) {
                    throw new AssertionError("FAIL: GetTopic label team="
                            + quote(cfg.getLabelsOrDefault("team", "")) + ", expected orders");
                }

                // UpdateTopic — replace the label set (FieldMask on "labels"), assert it stuck.
                Topic desired = created.toBuilder()
                        .clearLabels()
                        .putLabels("team", "orders")
                        .putLabels("stage", "production")
                        .build();
                Topic updated = topicAdmin.updateTopic(UpdateTopicRequest.newBuilder()
                        .setTopic(desired)
                        .setUpdateMask(FieldMask.newBuilder().addPaths("labels").build())
                        .build());
                System.out.printf("UpdateTopic: labels=%s%n", sortedLabels(updated.getLabelsMap()));
                if (!"production".equals(updated.getLabelsOrDefault("stage", ""))) {
                    throw new AssertionError("FAIL: UpdateTopic stage="
                            + quote(updated.getLabelsOrDefault("stage", "")) + ", expected production");
                }

                // ListTopicSubscriptions — the sub we created must be attached to this topic.
                boolean subFound = false;
                int subCount = 0;
                ListTopicSubscriptionsPagedResponse subs =
                        topicAdmin.listTopicSubscriptions(topicName);
                for (String s : subs.iterateAll()) {
                    subCount++;
                    if (s.equals(subName.toString())) {
                        subFound = true;
                    }
                }
                if (!subFound) {
                    throw new AssertionError("FAIL: subscription " + quote(subId)
                            + " not found among the topic's " + subCount + " subscriptions");
                }
                System.out.printf(
                        "ListTopicSubscriptions: found %s (%d total on topic)%n", subId, subCount);

                // DeleteTopic — tombstone: GetTopic now returns NotFound.
                topicAdmin.deleteTopic(topicName);
                topicDeleted[0] = true;
                boolean stillExists = true;
                try {
                    topicAdmin.getTopic(topicName);
                } catch (NotFoundException e) {
                    stillExists = false;
                }
                if (stillExists) {
                    throw new AssertionError("FAIL: topic " + quote(topicId)
                            + " still exists after DeleteTopic (expected tombstone)");
                }
                System.out.println("DeleteTopic: ok (tombstone — GetTopic=NotFound)");

                System.out.println("PASS: topic management verified "
                        + "(list/get/update/list-subs/delete-tombstone)");
            } finally {
                // The sub may outlive the topic (detached), so always clean it up here.
                deleteQuietly(() -> subAdmin.deleteSubscription(subName), "DeleteSubscription");
                if (!topicDeleted[0]) {
                    deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
                }
            }
        } finally {
            channel.shutdown();
            channel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** Renders {@code messageRetentionDuration} as a human string, or "unset" when zero. */
    private static String durationOf(Duration d) {
        long seconds = d.getSeconds();
        if (seconds == 0 && d.getNanos() == 0) {
            return "unset";
        }
        long minutes = seconds / 60;
        return minutes + "m" + (seconds % 60) + "s";
    }

    /** Labels in a stable, sorted order so the printed output is deterministic across runs. */
    private static String sortedLabels(Map<String, String> labels) {
        return new TreeMap<>(labels).toString();
    }

    private static String quote(String s) {
        return "\"" + s + "\"";
    }

    /** Best-effort delete: a missing resource (already gone) must not fail the run. */
    private static void deleteQuietly(Runnable delete, String label) {
        try {
            delete.run();
        } catch (NotFoundException e) {
            // already gone — fine
        } catch (RuntimeException e) {
            System.out.println("warning: " + label + ": " + e.getMessage());
        }
    }
}
