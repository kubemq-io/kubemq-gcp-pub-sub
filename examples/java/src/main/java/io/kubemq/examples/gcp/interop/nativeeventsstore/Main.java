package io.kubemq.examples.gcp.interop.nativeeventsstore;

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
import io.kubemq.sdk.pubsub.EventStoreMessageReceived;
import io.kubemq.sdk.pubsub.EventsStoreSubscription;
import io.kubemq.sdk.pubsub.EventsStoreType;
import io.kubemq.sdk.pubsub.PubSubClient;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * interop: native-events-store — master-table variant 15: cross-protocol interop.
 *
 * <p>The ONLY example in the repo that imports a KubeMQ SDK. It demonstrates the
 * connector's cross-protocol bridge end to end, in one process:
 *
 * <pre>{@code Subscribe(pubsub.{t}, startAt="new") -> Publish (GCP SDK) -> receive natively}</pre>
 *
 * <ol>
 *   <li>A native {@code io.kubemq.sdk:kubemq-sdk-Java:3.1.1} Events Store subscriber attaches
 *       to channel {@code pubsub.{topic}} with start type {@link EventsStoreType#StartNewOnly}
 *       ("new only").</li>
 *   <li>We block until the subscribe stream is confirmed open (subscribe-BEFORE-publish).</li>
 *   <li>The official {@code com.google.cloud:google-cloud-pubsub} client publishes one message to
 *       {@code topic} via {@code PUBSUB_EMULATOR_HOST} (zero code changes — the SDK skips Google
 *       auth and dials insecure gRPC).</li>
 *   <li>The native subscriber receives exactly that message on {@code pubsub.{topic}} and sees the
 *       three reserved tags the connector stamps on every message — {@code _pubsub_message_id},
 *       {@code _pubsub_publish_time}, {@code _pubsub_ordering_key} — which are stripped from
 *       {@code attributes} when delivered to a Pub/Sub client.</li>
 * </ol>
 *
 * <p>Why subscribe-before-publish + {@code StartNewOnly}: the native side attaches to a
 * <b>stream</b>, not a fixed offset. A naive "publish then subscribe" races — the publish could
 * land before the subscriber attaches and be missed. Establishing the "new only" subscription
 * first guarantees the published message is in-window (spec S5.7;
 * {@code kubemq-server/docs/20-google-pubsub.md:73-77}).
 *
 * <p>Channel mapping (spec S3.3): a Pub/Sub {@code Publish} writes once to the topic's Events
 * Store log {@code pubsub.{t}} (the authoritative, replayable, cross-protocol source) before
 * fanning out per-subscription queue copies. Here there is no subscription — we read the topic
 * log directly, natively.
 *
 * <p>The Pub/Sub half talks to the connector via {@code PUBSUB_EMULATOR_HOST} (default
 * {@code localhost:8085}); the native half talks to the gRPC broker via
 * {@code KUBEMQ_GRPC_ADDRESS} (default {@code localhost:50000}). A failed assertion throws and
 * the JVM exits non-zero.
 */
public final class Main {

    private static final String BODY = "from-gcp-pubsub";
    private static final String ORDERING_KEY = "shipments";
    private static final String ATTR_KEY = "region";
    private static final String ATTR_VAL = "emea";

    /**
     * The three reserved tags the connector stamps on every Pub/Sub message and that a native
     * consumer sees (stripped from {@code attributes} for Pub/Sub clients; spec S3.4).
     */
    private static final String[] RESERVED = {
        "_pubsub_message_id", "_pubsub_publish_time", "_pubsub_ordering_key"
    };

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String emulatorHost = System.getenv().getOrDefault("PUBSUB_EMULATOR_HOST", "localhost:8085");
        String projectId = System.getenv().getOrDefault("PUBSUB_PROJECT_ID", "my-project");
        String grpcAddress = grpcAddress();

        // Per-run isolation (parallel-safe; spec S9 / SHARED-CONVENTIONS §1). The canonical interop
        // name is `orders`, but the channel pubsub.orders is global and shared, so a fixed name
        // would cross-talk with a concurrent run (/examples + /burnin, other languages) and
        // false-fail the single-message assertion. uuid8-suffix the topic so each run reads its own
        // private Events Store channel pubsub.{topic}.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topicId = "orders-" + suffix;       // Pub/Sub topic id (starts with a letter; not `sub.`)
        String channel = "pubsub." + topicId;       // the native Events Store channel it maps to
        TopicName topicName = TopicName.of(projectId, topicId);

        System.out.printf(
                "=== interop: Pub/Sub publish -> native Events Store consume — topic %s / channel %s ===%n",
                quote(topicId), quote(channel));
        System.out.printf("  pubsub : %s (PUBSUB_EMULATOR_HOST; insecure, no Google auth)%n",
                emulatorHost);
        System.out.printf("  grpc   : %s (native KubeMQ broker)%n%n", grpcAddress);

        // Hand-off of exactly one received event from the native callback thread to main.
        SynchronousQueue<EventStoreMessageReceived> received = new SynchronousQueue<>();
        AtomicReference<String> subscribeError = new AtomicReference<>();

        // One plaintext channel to the connector; PUBSUB_EMULATOR_HOST IS the contract — every
        // Pub/Sub client is built against it with NoCredentialsProvider (the standard Google
        // emulator posture: no Google auth, no TLS), using only official google-cloud-pubsub APIs.
        ManagedChannel pubsubChannel =
                ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(pubsubChannel));
        CredentialsProvider noCreds = NoCredentialsProvider.create();

        // TopicAdminClient + Publisher are the Pub/Sub half; PubSubClient is the native KubeMQ
        // Java SDK peer (gRPC at KUBEMQ_GRPC_ADDRESS).
        try (TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(noCreds)
                        .build());
                PubSubClient native_ = PubSubClient.builder()
                        .address(grpcAddress)
                        .clientId("gcp-interop-java-" + suffix)
                        .build()) {

            // The topic must exist (creates the Events Store channel) before we publish.
            topicAdmin.createTopic(topicName);
            System.out.printf("CreateTopic -> projects/%s/topics/%s%n", projectId, topicId);

            EventsStoreSubscription subscription = EventsStoreSubscription.builder()
                    .channel(channel)
                    .eventsStoreType(EventsStoreType.StartNewOnly)
                    .onReceiveEventCallback(event -> {
                        // Best-effort hand-off; main consumes exactly one.
                        received.offer(event);
                    })
                    .onErrorCallback(err ->
                            subscribeError.compareAndSet(null, err.getMessage()))
                    .build();

            try {
                // --- 1. subscribe FIRST, with start type "new only" ----------------------
                native_.subscribeToEventsStore(subscription);
                // The SDK opens the Events Store stream on a background thread; wait a short,
                // bounded interval so the publish below is guaranteed in-window for the "new only"
                // subscription (subscribe-before-publish), then fail fast if the stream errored.
                Thread.sleep(1500);
                if (subscribeError.get() != null) {
                    throw new AssertionError(
                            "FAIL: native subscribe stream failed to open: " + subscribeError.get());
                }
                System.out.printf(
                        "native SubscribeToEventsStore(%s, startAt='new') -> stream open%n",
                        quote(channel));

                // --- 2. publish exactly one message via the Pub/Sub SDK ------------------
                // enableMessageOrdering is required to publish with an ordering key — it makes the
                // reserved tag _pubsub_ordering_key observable on the native side.
                Publisher publisher = Publisher.newBuilder(topicName)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(noCreds)
                        .setEnableMessageOrdering(true)
                        .build();
                String messageId;
                try {
                    PubsubMessage message = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(BODY))
                            .setOrderingKey(ORDERING_KEY)
                            .putAttributes(ATTR_KEY, ATTR_VAL) // ordinary attribute — rides along as a plain tag
                            .build();
                    messageId = publisher.publish(message).get(15, TimeUnit.SECONDS);
                } finally {
                    publisher.shutdown();
                    publisher.awaitTermination(15, TimeUnit.SECONDS);
                }
                System.out.printf("PublisherClient.publish(%s) -> message_id=%s%n",
                        quote(topicId), messageId);

                // --- 3. await exactly one event natively on pubsub.{topic} ---------------
                EventStoreMessageReceived event = received.poll(15, TimeUnit.SECONDS);
                check(event != null,
                        "native consumer received the published message on " + quote(channel));
                String eventBody = new String(event.getBody(), StandardCharsets.UTF_8);
                System.out.printf("native receive(%s) -> body=%s%n", quote(channel), quote(eventBody));
                for (String key : RESERVED) {
                    System.out.printf("    %s = %s%n", key, quote(event.getTags().get(key)));
                }
                System.out.printf("    %s (attribute) = %s%n",
                        ATTR_KEY, quote(event.getTags().get(ATTR_KEY)));

                // --- 4. assert the cross-protocol invariants -----------------------------
                check(BODY.equals(eventBody), "native side sees the Pub/Sub-published body");
                check(hasAll(event), "all three reserved tags are present (_pubsub_message_id/publish_time/ordering_key)");
                check(messageId.equals(event.getTags().get("_pubsub_message_id")),
                        "_pubsub_message_id matches the Pub/Sub-returned message id");
                check(ORDERING_KEY.equals(event.getTags().get("_pubsub_ordering_key")),
                        "_pubsub_ordering_key carries the publish ordering key");
                check(ATTR_VAL.equals(event.getTags().get(ATTR_KEY)),
                        "the message attribute rides along as an ordinary tag");

                // only one message was published — nothing more must arrive
                check(received.poll(1, TimeUnit.SECONDS) == null,
                        "exactly one message was consumed (no cross-talk)");
                check(subscribeError.get() == null,
                        "no subscription errors ([])");
            } finally {
                subscription.cancel();
                // Best-effort teardown so re-runs start clean (must not fail the run).
                deleteQuietly(() -> topicAdmin.deleteTopic(topicName), topicId);
            }
        } finally {
            pubsubChannel.shutdown();
            pubsubChannel.awaitTermination(10, TimeUnit.SECONDS);
        }

        System.out.println("\nPASS — Pub/Sub publish consumed natively on the Events Store topic log.");
    }

    /**
     * Native KubeMQ gRPC address. Reads {@code KUBEMQ_GRPC_ADDRESS} (default
     * {@code localhost:50000}). The emulator host and the gRPC broker are different listeners on
     * the same KubeMQ server.
     */
    private static String grpcAddress() {
        String a = System.getenv("KUBEMQ_GRPC_ADDRESS");
        return (a != null && !a.isBlank()) ? a : "localhost:50000";
    }

    /** True iff every reserved tag is present on the received event. */
    private static boolean hasAll(EventStoreMessageReceived event) {
        for (String key : RESERVED) {
            if (!event.getTags().containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Asserts an expected outcome; prints an [OK] line on success, throws (JVM exits non-zero) on
     * failure. These examples are runnable PROOFS, not demos — a missing message, a missing
     * reserved tag, or a mismatched payload must fail the process.
     */
    private static void check(boolean condition, String message) {
        if (condition) {
            System.out.printf("  [OK] %s%n", message);
        } else {
            throw new AssertionError("  [FAIL] " + message);
        }
    }

    private static String quote(String s) {
        return "'" + (s != null ? s : "") + "'";
    }

    /** Best-effort delete: a missing resource (already gone) must not fail the run. */
    private static void deleteQuietly(Runnable delete, String topicId) {
        try {
            delete.run();
            System.out.printf("DeleteTopic(%s) -> done%n", quote(topicId));
        } catch (NotFoundException e) {
            System.out.printf("DeleteTopic(%s) -> skipped (already gone)%n", quote(topicId));
        } catch (RuntimeException e) {
            System.out.printf("DeleteTopic(%s) -> skipped (%s)%n", quote(topicId),
                    e.getClass().getSimpleName());
        }
    }
}
