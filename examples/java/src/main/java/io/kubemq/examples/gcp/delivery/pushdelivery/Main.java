package io.kubemq.examples.gcp.delivery.pushdelivery;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcPublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * delivery/push-delivery (master-table variant 9) — a {@code push_config}
 * subscription makes the connector POST each message to a local HTTP endpoint; the
 * wrapped JSON envelope arrives and a 2xx response acks it.
 *
 * <p>{@code CreateTopic -> CreateSubscription(push_config) -> Publish -> connector
 * POSTs wrapped envelope -> 200 ack}.
 *
 * <p>The program stands up a local {@link HttpServer} (plain HTTP — permitted
 * because the host is localhost), creates a subscription with a {@link PushConfig}
 * pointing at it, and publishes one message tagged {@code kind=demo}. Instead of the
 * client pulling, the connector's per-subscription push worker POSTs the message as
 * a <b>wrapped JSON envelope</b>:
 *
 * <pre>{@code
 * { "message": { "data": "<base64>", "attributes": { "kind": "demo" },
 *                "messageId": "...", "publishTime": "..." },
 *   "subscription": "projects/{p}/subscriptions/{s}" }
 * }</pre>
 *
 * <p>{@code data} is base64-encoded (the example decodes it back to
 * {@code hello-push}); user attributes are surfaced (the reserved {@code _pubsub_*}
 * tags are not). The endpoint returns <b>200</b>, the ack signal — a 2xx acks the
 * message, while a non-2xx or timeout would retry with backoff and, on exhaustion,
 * dead-letter (if set) or drop. The example asserts the decoded body, the attribute,
 * the subscription path, and that exactly one POST arrived (no redelivery after the
 * ack).
 *
 * <p>Uses ONLY the official {@code com.google.cloud:google-cloud-pubsub} client for
 * Pub/Sub (gson, already on the BOM-managed classpath, parses the envelope; the
 * HTTP sink is the JDK's built-in {@code com.sun.net.httpserver}). With
 * {@code PUBSUB_EMULATOR_HOST} set the program wires a plaintext
 * {@link ManagedChannel} to that host with {@link NoCredentialsProvider} — no
 * Google auth, insecure gRPC.
 *
 * <p>A Publish writes once to the topic log {@code pubsub.{t}} (the authoritative,
 * replayable, cross-protocol source), then fans out one queue copy to
 * {@code pubsub.sub.{s}}, from which the push worker delivers (spec S3.5). Any
 * failed assertion exits non-zero.
 */
public final class Main {

    private static final String BODY = "hello-push";
    private static final int BASE_PORT = 18080;
    private static final int PORT_RANGE = 50;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String projectId = projectId();
        String emulatorHost = emulatorHost();
        System.out.printf(
                "[kubemq-gcp-pub-sub] delivery/push-delivery | emulator=%s project=%s%n",
                emulatorHost, projectId);

        // Unique-per-run resource ids → distinct pubsub.{t} / pubsub.sub.{s} channels.
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String topicId = "orders-" + suffix;
        String subId = "sub-push-" + suffix;
        TopicName topicName = TopicName.of(projectId, topicId);
        SubscriptionName subscriptionName = SubscriptionName.of(projectId, subId);

        // Stand up a local HTTP sink on the first free port; plain HTTP is allowed
        // because the host is localhost. A SynchronousQueue hands the first POST body
        // to main; an AtomicInteger counts hits to prove there is no redelivery.
        SynchronousQueue<String> received = new SynchronousQueue<>();
        AtomicInteger hits = new AtomicInteger();
        HttpServer sink = startLocalSink(received, hits);
        int port = sink.getAddress().getPort();
        String pushEndpoint = "http://localhost:" + port + "/push/";
        System.out.println("LocalSink          -> listening at " + pushEndpoint);

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

        try {
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic        -> %s (channel pubsub.%s)%n", topicId, topicId);

            // CreateSubscription with a push_config pointing at the local sink.
            subscriptionAdmin.createSubscription(Subscription.newBuilder()
                    .setName(subscriptionName.toString())
                    .setTopic(topicName.toString())
                    .setAckDeadlineSeconds(10)
                    .setPushConfig(PushConfig.newBuilder().setPushEndpoint(pushEndpoint).build())
                    .build());
            System.out.printf(
                    "CreateSubscription -> %s (push_endpoint=%s)%n", subId, pushEndpoint);

            // Publish one message; the connector's push worker will POST the envelope.
            publisher.publishCallable().call(PublishRequest.newBuilder()
                    .setTopic(topicName.toString())
                    .addMessages(PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(BODY))
                            .putAttributes("kind", "demo")
                            .build())
                    .build());
            System.out.println("Publish            -> 'hello-push' (attributes: kind=demo)");

            // Wait for the connector's push worker to POST the wrapped envelope.
            String raw = received.poll(30, TimeUnit.SECONDS);
            require(raw != null, "expected a push POST at the local endpoint within 30s");

            // Parse the wrapped envelope: { message: { data(b64), attributes, messageId,
            // publishTime }, subscription }.
            JsonObject root = JsonParser.parseString(raw).getAsJsonObject();
            JsonObject message = root.getAsJsonObject("message");
            String dataB64 = message.get("data").getAsString();
            String data = new String(Base64.getDecoder().decode(dataB64), StandardCharsets.UTF_8);
            String kind = message.getAsJsonObject("attributes").get("kind").getAsString();
            String messageId = message.get("messageId").getAsString();
            String subscription = root.get("subscription").getAsString();
            System.out.printf(
                    "PushPOST           -> data(decoded)='%s', attributes.kind='%s'%n", data, kind);
            System.out.printf(
                    "PushPOST           -> envelope.subscription='%s', messageId=%s%n",
                    subscription, messageId);

            require(BODY.equals(data),
                    "pushed message body (base64-decoded) must be '" + BODY + "' but was '" + data + "'");
            require("demo".equals(kind),
                    "pushed message attribute 'kind' must be 'demo' but was '" + kind + "'");
            require(subscription.endsWith(subId),
                    "envelope.subscription must name this subscription but was '" + subscription + "'");

            // The 200 acked the message; give the worker a moment and assert it was not
            // redelivered (exactly one POST for the single publish).
            Thread.sleep(2000);
            require(hits.get() == 1,
                    "expected exactly one push POST (2xx -> acked, no redelivery) but saw " + hits.get());

            System.out.println();
            System.out.println(
                    "PASS: push delivery: wrapped envelope POSTed to the localhost endpoint; "
                            + "2xx acked, no redelivery");
        } finally {
            deleteQuietly(() -> subscriptionAdmin.deleteSubscription(subscriptionName), "DeleteSubscription");
            deleteQuietly(() -> topicAdmin.deleteTopic(topicName), "DeleteTopic");
            publisher.close();
            subscriptionAdmin.close();
            topicAdmin.close();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            sink.stop(0);
        }
    }

    /** Binds an HttpServer on the first free port in [BASE_PORT, BASE_PORT+PORT_RANGE). */
    private static HttpServer startLocalSink(SynchronousQueue<String> received, AtomicInteger hits)
            throws IOException {
        IOException last = null;
        for (int port = BASE_PORT; port < BASE_PORT + PORT_RANGE; port++) {
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                server.createContext("/push/", exchange -> {
                    String body;
                    try (InputStream in = exchange.getRequestBody()) {
                        body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                    }
                    hits.incrementAndGet();
                    received.offer(body); // hand the first body to main; later POSTs are still counted
                    byte[] ok = "OK".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, ok.length); // 2xx -> the connector acks
                    exchange.getResponseBody().write(ok);
                    exchange.close();
                });
                server.start();
                return server;
            } catch (IOException e) {
                last = e; // port in use — try the next one
            }
        }
        throw new IOException("could not bind a local push sink in ports "
                + BASE_PORT + ".." + (BASE_PORT + PORT_RANGE), last);
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
