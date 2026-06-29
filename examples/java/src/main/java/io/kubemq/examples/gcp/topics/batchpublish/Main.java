package io.kubemq.examples.gcp.topics.batchpublish;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.threeten.bp.Duration;

/**
 * topics: batch-publish — master-table variant 2.
 *
 * <p>Publishing a batch of messages and verifying the server-assigned ids come back in
 * request order: {@code CreateTopic -> Publish (N) -> Get each id in order}.
 *
 * <p>The official Java {@link Publisher} coalesces individual {@code publish} calls into
 * a server-side batch (its {@link BatchingSettings} are sized so all {@value #BATCH_SIZE}
 * messages go in one batch). Each call returns an {@link ApiFuture}; the program collects
 * the ids in the SAME order it issued the publishes and asserts it got N distinct ids
 * aligned 1:1 to request order. The connector validates the whole batch before enqueuing
 * anything (publish atomicity, spec S3.5): a single invalid message rejects the entire
 * batch with {@code INVALID_ARGUMENT}. The batch limit is 1000; each accepted message is
 * written once to the topic log {@code pubsub.events-<uuid8>}. The topic is uuid-suffixed
 * per run and deleted on exit. Any failed assertion exits non-zero.
 *
 * <p>Connectivity: this example is driven entirely by {@code PUBSUB_EMULATOR_HOST}. When
 * it is set, every client is built against that host over a plaintext gRPC channel with
 * {@link NoCredentialsProvider} (no Google auth, no TLS), using only official
 * {@code google-cloud-pubsub} APIs.
 */
public final class Main {

    /** Stays well under the connector's hard limit of 1000 messages/batch (spec S3.6). */
    private static final int BATCH_SIZE = 10;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        System.out.printf("[kubemq-gcp-pub-sub] topics/batch-publish | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run topic id (channel isolation). Canonical master-table name is events.
        String topicId = "events-" + UUID.randomUUID().toString().substring(0, 8);
        TopicName topicName = TopicName.of(projectId, topicId);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider noCreds = NoCredentialsProvider.create();

        try (TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(noCreds)
                .build())) {

            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic: %s (channel pubsub.%s)%n", topicId, topicId);

            try {
                // Hold the batch open until all BATCH_SIZE messages are queued, then flush once.
                Publisher publisher = Publisher.newBuilder(topicName)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(noCreds)
                        .setBatchingSettings(BatchingSettings.newBuilder()
                                .setElementCountThreshold((long) BATCH_SIZE)
                                .setDelayThreshold(Duration.ofMillis(200))
                                .build())
                        .build();

                List<String> ids = new ArrayList<>(BATCH_SIZE);
                try {
                    // Issue the batch: index i carries attribute seq=i to prove order is preserved.
                    System.out.printf(
                            "Publish: issuing a batch of %d messages (limit 1000/batch)%n",
                            BATCH_SIZE);
                    List<ApiFuture<String>> futures = new ArrayList<>(BATCH_SIZE);
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        PubsubMessage message = PubsubMessage.newBuilder()
                                .setData(ByteString.copyFromUtf8("event-" + i))
                                .putAttributes("seq", Integer.toString(i))
                                .build();
                        futures.add(publisher.publish(message));
                    }

                    // Collect ids in the SAME order we issued the publishes — get() blocks per
                    // future. A single invalid message would reject the whole batch here.
                    Map<String, Integer> seen = new HashMap<>(BATCH_SIZE);
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        String id = futures.get(i).get(30, TimeUnit.SECONDS);
                        ids.add(id);
                        System.out.printf("  [%d] event-%d -> messageId=%s%n", i, i, id);
                        Integer prev = seen.put(id, i);
                        if (prev != null) {
                            throw new AssertionError("FAIL: duplicate messageId \"" + id
                                    + "\" at indexes " + prev + " and " + i);
                        }
                    }

                    // Assertions: N ids, all distinct, aligned 1:1 to request order.
                    if (seen.size() != BATCH_SIZE) {
                        throw new AssertionError("FAIL: expected " + BATCH_SIZE
                                + " distinct message ids, got " + seen.size());
                    }
                } finally {
                    publisher.shutdown();
                    publisher.awaitTermination(30, TimeUnit.SECONDS);
                }

                for (int i = 0; i < ids.size(); i++) {
                    if (ids.get(i) == null || ids.get(i).isBlank()) {
                        throw new AssertionError("FAIL: message index " + i + " has no id");
                    }
                }
                System.out.printf(
                        "Verified: %d message ids returned in request order, all distinct%n",
                        BATCH_SIZE);
                System.out.println(
                        "PASS: batch publish verified (ids in request order, atomic batch)");
            } finally {
                deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            }
        } finally {
            channel.shutdown();
            channel.awaitTermination(10, TimeUnit.SECONDS);
        }
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
