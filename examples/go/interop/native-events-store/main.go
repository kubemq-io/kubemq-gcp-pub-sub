// Command native-events-store is master-table variant 15: cross-protocol interop.
//
// It is the ONLY example in the repo that imports a KubeMQ SDK. It demonstrates
// the connector's cross-protocol bridge end to end, in one process:
//
//		Subscribe(pubsub.{t}, startAt="new") -> Publish (GCP SDK) -> receive natively
//
//	 1. A native github.com/kubemq-io/kubemq-go/v2@v2.0.3 Events Store subscriber
//	    attaches to channel "pubsub.{topic}" with start policy StartFromNewEvents()
//	    ("new only").
//	 2. We block until the subscribe stream is confirmed open (subscribe-BEFORE-publish).
//	 3. The official cloud.google.com/go/pubsub client publishes one message to
//	    "topic" via PUBSUB_EMULATOR_HOST (zero code changes — the SDK skips Google
//	    auth and dials insecure gRPC).
//	 4. The native subscriber receives exactly that message on "pubsub.{topic}" and
//	    sees the three reserved tags the connector stamps on every message —
//	    _pubsub_message_id, _pubsub_publish_time, _pubsub_ordering_key — which are
//	    stripped from `attributes` when delivered to a Pub/Sub client.
//
// Why subscribe-before-publish + StartFromNewEvents(): the native side attaches to
// a STREAM, not a fixed offset. A naive "publish then subscribe" races — the
// publish could land before the subscriber attaches and be missed. Establishing
// the "new only" subscription first guarantees the published message is in-window
// (spec S5.7; kubemq-server/docs/20-google-pubsub.md:73-77).
//
// Channel mapping (spec S3.3): a Pub/Sub Publish writes once to the topic's Events
// Store log pubsub.{t} (the authoritative, replayable, cross-protocol source)
// before fanning out per-subscription queue copies. Here there is no subscription
// — we read the topic log directly, natively.
//
// The Pub/Sub half talks to the connector via PUBSUB_EMULATOR_HOST (default
// localhost:8085); the native half talks to the gRPC broker via
// KUBEMQ_GRPC_ADDRESS (default localhost:50000).
//
// Run (needs BOTH the Pub/Sub connector on :8085 AND the gRPC broker on :50000):
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	export KUBEMQ_GRPC_ADDRESS=localhost:50000
//	go run ./interop/native-events-store
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	kubemq "github.com/kubemq-io/kubemq-go/v2"
)

// The three reserved tags the connector stamps on every Pub/Sub message and that a
// native consumer sees (stripped from `attributes` for Pub/Sub clients; spec S3.4).
var reservedTags = []string{"_pubsub_message_id", "_pubsub_publish_time", "_pubsub_ordering_key"}

const (
	body        = "from-gcp-pubsub"
	orderingKey = "shipments"
	attrKey     = "region"
	attrVal     = "emea"
)

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	emulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	if emulatorHost == "" {
		emulatorHost = "localhost:8085"
	}
	grpcAddress := os.Getenv("KUBEMQ_GRPC_ADDRESS")
	if grpcAddress == "" {
		grpcAddress = "localhost:50000"
	}

	// Per-run isolation (parallel-safe; spec S9 / SHARED-CONVENTIONS §1). The
	// canonical interop name is `orders`, but the channel pubsub.orders is global
	// and shared, so a fixed name would cross-talk with a concurrent run
	// (/examples + /burnin, other languages) and false-fail the single-message
	// assertion. uuid8-suffix the topic so each run reads its own private Events
	// Store channel pubsub.{topic}.
	run := uuid.NewString()[:8]
	topicID := "orders-" + run     // Pub/Sub topic id (starts with a letter; not `sub.`)
	channel := "pubsub." + topicID // the native Events Store channel it maps to

	fmt.Printf("=== interop: Pub/Sub publish -> native Events Store consume — topic %q / channel %q ===\n", topicID, channel)
	fmt.Printf("  pubsub : %s (PUBSUB_EMULATOR_HOST; insecure, no Google auth)\n", emulatorHost)
	fmt.Printf("  grpc   : %s (native KubeMQ broker)\n\n", grpcAddress)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- the Pub/Sub half (PUBSUB_EMULATOR_HOST routes the official client at the
	// connector over insecure gRPC with credentials cleared — no code change). ---
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	// Ordering must be enabled to publish with an OrderingKey — it makes the
	// reserved tag _pubsub_ordering_key observable on the native side.
	topic.EnableMessageOrdering = true
	fmt.Printf("CreateTopic -> projects/%s/topics/%s\n", projectID, topicID)

	// Best-effort teardown so re-runs start clean (must not fail the run).
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := topic.Delete(cleanupCtx); err != nil {
			fmt.Printf("DeleteTopic(%q) -> skipped (%v)\n", topicID, err)
		} else {
			fmt.Printf("DeleteTopic(%q) -> done\n", topicID)
		}
	}()

	// --- the native KubeMQ gRPC half (Events Store subscribe) ----------------
	host, port, err := splitHostPort(grpcAddress)
	if err != nil {
		log.Fatalf("parse KUBEMQ_GRPC_ADDRESS %q: %v", grpcAddress, err)
	}
	native, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId(fmt.Sprintf("gcp-interop-go-%s", run)),
	)
	if err != nil {
		log.Fatalf("connect native KubeMQ gRPC (%s): %v", grpcAddress, err)
	}
	defer func() { _ = native.Close() }()

	received := make(chan *kubemq.EventStoreReceive, 4)
	errs := make(chan error, 4)

	// --- 1. subscribe FIRST, with start policy "new only" --------------------
	// SubscribeToEventsStore returns only after the gRPC subscribe stream is
	// established (it opens the transport handle before returning), so a nil
	// error here means the stream is open — this is the subscribe-before-publish
	// barrier. With StartFromNewEvents() the published message is in-window.
	sub, err := native.SubscribeToEventsStore(ctx, channel, "", kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(ev *kubemq.EventStoreReceive) {
			received <- ev
		}),
		kubemq.WithOnError(func(e error) {
			errs <- e
		}),
	)
	if err != nil {
		log.Fatalf("native SubscribeToEventsStore(%q): %v", channel, err)
	}
	defer sub.Cancel()
	fmt.Printf("native SubscribeToEventsStore(%q, startAt='new') -> stream open\n", channel)

	// --- 2. publish exactly one message via the Pub/Sub SDK ------------------
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        []byte(body),
		OrderingKey: orderingKey,
		Attributes:  map[string]string{attrKey: attrVal}, // an ordinary attribute — rides along as a plain tag
	})
	messageID, err := result.Get(ctx) // blocks until the publish is acked by the connector.
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Printf("Publisher.Publish(%q) -> message_id=%s\n", topicID, messageID)

	// --- 3. await exactly one event natively on pubsub.{topic} ---------------
	var event *kubemq.EventStoreReceive
	select {
	case event = <-received:
	case e := <-errs:
		check(false, fmt.Sprintf("native subscribe stream error: %v", e))
	case <-time.After(15 * time.Second):
		check(false, fmt.Sprintf("native consumer received the published message on %q", channel))
	}
	check(true, fmt.Sprintf("native consumer received the published message on %q", channel))

	eventBody := string(event.Body)
	fmt.Printf("native receive(%q) -> body=%q\n", channel, eventBody)
	for _, key := range reservedTags {
		fmt.Printf("    %s = %q\n", key, event.Tags[key])
	}
	fmt.Printf("    %s (attribute) = %q\n", attrKey, event.Tags[attrKey])

	// --- 4. assert the cross-protocol invariants -----------------------------
	check(eventBody == body, "native side sees the Pub/Sub-published body")
	check(hasAll(event.Tags, reservedTags), "all three reserved tags are present (_pubsub_message_id/publish_time/ordering_key)")
	check(event.Tags["_pubsub_message_id"] == messageID, "_pubsub_message_id matches the Pub/Sub-returned message id")
	check(event.Tags["_pubsub_ordering_key"] == orderingKey, "_pubsub_ordering_key carries the publish ordering key")
	check(event.Tags[attrKey] == attrVal, "the message attribute rides along as an ordinary tag")

	// only one message was published — nothing more must arrive
	select {
	case extra := <-received:
		check(false, fmt.Sprintf("exactly one message was consumed (no cross-talk); got extra %q", string(extra.Body)))
	default:
		check(true, "exactly one message was consumed (no cross-talk)")
	}
	select {
	case e := <-errs:
		check(false, fmt.Sprintf("no subscription errors (%v)", e))
	default:
		check(true, "no subscription errors ([])")
	}

	fmt.Println("\nPASS — Pub/Sub publish consumed natively on the Events Store topic log.")
}

// check asserts an expected outcome; it exits non-zero on failure. These examples
// are runnable PROOFS, not demos — a missing message, a missing reserved tag, or a
// mismatched payload must fail the process.
func check(condition bool, message string) {
	if condition {
		fmt.Printf("  [OK] %s\n", message)
		return
	}
	fmt.Fprintf(os.Stderr, "  [FAIL] %s\n", message)
	os.Exit(1)
}

// hasAll reports whether every key in keys is present in tags.
func hasAll(tags map[string]string, keys []string) bool {
	for _, k := range keys {
		if _, ok := tags[k]; !ok {
			return false
		}
	}
	return true
}

// splitHostPort parses "host:port" into the (host string, port int) pair that
// kubemq.WithAddress expects.
func splitHostPort(addr string) (string, int, error) {
	host, portStr, ok := strings.Cut(addr, ":")
	if !ok || host == "" || portStr == "" {
		return "", 0, fmt.Errorf("expected host:port, got %q", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	return host, port, nil
}
