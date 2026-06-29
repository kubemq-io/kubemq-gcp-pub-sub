package io.kubemq.examples.gcp.advanced.schemaprotobuf;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SchemaServiceSettings;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * advanced: schema-protobuf — master-table variant 14.
 *
 * <p>Attach a Protobuf schema to a topic and enforce it on publish:
 * {@code CreateSchema(PROTOCOL_BUFFER) -> CreateTopic{ schema_settings } -> CreateSubscription
 * -> Publish(conforming)     -> accepted, message-id returned
 * -> Publish(non-conforming) -> whole batch rejected INVALID_ARGUMENT}.
 *
 * <p>When a topic references a schema the connector enforces it at publish time and rejects the
 * ENTIRE batch ({@code INVALID_ARGUMENT}) on the first non-conforming message (spec S3.5 schema).
 * Protobuf is handled connector-side via {@code jhump/protoreflect}; schema definitions are
 * &le; 300 KB and at least one revision is always kept. JSON-encoded messages must conform to
 * the proto message.
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client
 * ({@link SchemaServiceClient#createSchema} + a topic with {@link SchemaSettings}). The
 * {@code PUBSUB_EMULATOR_HOST} env var selects the connector: every client (schema, admin,
 * publisher) is built on a plaintext gRPC channel to that host with {@link NoCredentialsProvider}
 * (the emulator skips Google auth). Resource ids (schema, topic, subscription) are uuid-suffixed
 * per run so concurrent runs use distinct channels and registry records and never cross-talk; all
 * are deleted on exit (best-effort). Any failed assertion throws and the JVM exits non-zero.
 */
public final class Main {

    // proto3 message: Order { int64 id = 1; string name = 2; }. JSON messages must conform.
    private static final String PROTO_DEFINITION = String.join("\n",
            "syntax = \"proto3\";",
            "message Order {",
            "  int64 id = 1;",
            "  string name = 2;",
            "}");

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        System.out.printf(
                "[kubemq-gcp-pub-sub] advanced/schema-protobuf | emulator=%s project=%s%n",
                System.getenv("PUBSUB_EMULATOR_HOST"), projectId);

        // Unique-per-run resource ids (channel isolation). Canonical names: proto-topic / sub-proto.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String schemaId = "proto-schema-" + suffix;
        String topicId = "proto-topic-" + suffix;
        String subId = "sub-proto-" + suffix;
        ProjectName projectName = ProjectName.of(projectId);
        SchemaName schemaName = SchemaName.of(projectId, schemaId);
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subName = SubscriptionName.of(projectId, subId);

        // Build a plaintext gRPC channel to PUBSUB_EMULATOR_HOST with no credentials — the
        // explicit, official-SDK emulator wiring (the emulator skips Google auth). Shared by the
        // schema-service client, the admin clients, and each Publisher.
        String emulatorHost = System.getenv().getOrDefault("PUBSUB_EMULATOR_HOST", "localhost:8085");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        try (SchemaServiceClient schemaService = SchemaServiceClient.create(
                        SchemaServiceSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(credentialsProvider)
                                .build());
                TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());
                SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(credentialsProvider)
                                .build())) {

            // 1. CreateSchema(PROTOCOL_BUFFER) -> BoltDB registry record (<= 300 KB; >= 1 revision).
            schemaService.createSchema(projectName, Schema.newBuilder()
                    .setType(Schema.Type.PROTOCOL_BUFFER)
                    .setDefinition(PROTO_DEFINITION)
                    .build(), schemaId);
            System.out.printf("CreateSchema(Proto) -> %s (<= 300 KB; >= 1 revision kept)%n",
                    schemaId);

            try {
                // 2. CreateTopic bound to the schema with JSON encoding; publishes are now enforced.
                Topic topic = Topic.newBuilder()
                        .setName(topicName.toString())
                        .setSchemaSettings(SchemaSettings.newBuilder()
                                .setSchema(schemaName.toString())
                                .setEncoding(Encoding.JSON)
                                .build())
                        .build();
                topicAdmin.createTopic(topic);
                System.out.printf(
                        "CreateTopic         -> %s (schema %s, encoding JSON; channel pubsub.%s)%n",
                        topicId, schemaId, topicId);

                // 3. CreateSubscription -> per-sub queue pubsub.sub.{subId}; ack deadline 10s.
                subAdmin.createSubscription(subName, topicName, PushConfig.getDefaultInstance(), 10);
                System.out.printf("CreateSubscription  -> %s (channel pubsub.sub.%s)%n", subId, subId);

                // 4. Conforming message: JSON matching the proto message -> accepted.
                String id = publish(topicName, "{\"id\":1,\"name\":\"widget\"}",
                        channelProvider, credentialsProvider);
                System.out.printf("Publish (conforming)     -> accepted, id %s%n", id);
                check(id != null && !id.isBlank(),
                        "conforming publish returned a message id");

                // 5. Non-conforming message: an unknown field not in the proto -> the connector
                //    rejects the whole batch with INVALID_ARGUMENT.
                String detail = publishExpectingReject(topicName,
                        "{\"id\":2,\"name\":\"widget\",\"bogus\":\"extra-field\"}",
                        channelProvider, credentialsProvider);
                System.out.println("Publish (non-conforming) -> rejected (batch) INVALID_ARGUMENT");
                check(detail != null,
                        "schema-invalid message was rejected by the connector");
                check(matchesSchemaRejection(detail),
                        "rejection mentions INVALID_ARGUMENT/schema (was: " + detail + ")");

                System.out.println(
                        "PASS: Protobuf schema enforced — conforming accepted, non-conforming batch rejected");
            } finally {
                // 6. Best-effort teardown (subscription -> topic -> schema) so re-runs are clean.
                deleteQuietly(() -> subAdmin.deleteSubscription(subName), "DeleteSubscription");
                deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
                deleteQuietly(() -> schemaService.deleteSchema(schemaName), "DeleteSchema");
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

    /** Publishes one JSON body and blocks for the server-assigned id (conforming path). */
    private static String publish(TopicName topicName, String json,
            TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider)
            throws Exception {
        Publisher publisher = newPublisher(topicName, channelProvider, credentialsProvider);
        try {
            return publisher.publish(PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(json))
                    .build()).get(15, TimeUnit.SECONDS);
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(15, TimeUnit.SECONDS);
        }
    }

    /**
     * Publishes a non-conforming body expecting the connector to reject it. Returns the rejection
     * detail message; throws if the publish was (incorrectly) accepted.
     */
    private static String publishExpectingReject(TopicName topicName, String json,
            TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider)
            throws Exception {
        Publisher publisher = newPublisher(topicName, channelProvider, credentialsProvider);
        try {
            String id = publisher.publish(PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(json))
                    .build()).get(15, TimeUnit.SECONDS);
            throw new AssertionError(
                    "FAIL: non-conforming message was UNEXPECTEDLY ACCEPTED (id=" + id + ")");
        } catch (ExecutionException e) {
            // The publish future fails with the connector's INVALID_ARGUMENT status.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return cause.getMessage();
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(15, TimeUnit.SECONDS);
        }
    }

    private static boolean matchesSchemaRejection(String detail) {
        String d = detail == null ? "" : detail.toLowerCase();
        return d.contains("invalid_argument") || d.contains("invalid argument")
                || d.contains("invalid") || d.contains("schema") || d.contains("unknown");
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
