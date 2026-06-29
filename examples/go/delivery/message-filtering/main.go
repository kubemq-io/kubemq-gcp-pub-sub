// Command message-filtering is master-table variant 7: attributes-only CEL-subset
// filtering against the KubeMQ Pub/Sub connector.
//
//	CreateTopic
//	-> CreateSubscription(Filter='attributes.region = "eu"')   (filtered)
//	-> CreateSubscription(no filter)                            (unfiltered)
//	-> Publish a mixed batch with a `region` attribute
//	-> Pull both subs; assert the filtered sub receives ONLY the matching subset
//	   while the unfiltered sub receives everything
//	-> negative path: a malformed filter is rejected at CreateSubscription
//
// A subscription's CEL-subset Filter is attributes-only, compiled once at
// create-time (immutable), <= 256 chars, and applied at PUBLISH FAN-OUT — a
// non-matching message is never enqueued for that subscription. The topic log
// `pubsub.{t}` is unfiltered (it is the authoritative, replayable source); the
// filter only shapes the per-sub queue `pubsub.sub.{s}` (spec S3.3 / S3.5). A
// malformed filter is rejected at create-time with INVALID_ARGUMENT.
//
// Resource ids are uuid-suffixed per run so concurrent runs use distinct channels
// and never cross-talk; everything is deleted on exit (best-effort). Any mismatch
// exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./delivery/message-filtering
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// euFilter is a CEL-subset, attributes-only expression (<= 256 chars, immutable).
const euFilter = `attributes.region = "eu"`

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the project segment is parsed but ignored.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] delivery/message-filtering | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)
	fmt.Printf("filter: %s\n", euFilter)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical names: filtered / sub-filtered.
	suffix := uuid.NewString()[:8]
	topicID := "filtered-" + suffix
	subFilteredID := "sub-filtered-" + suffix
	subAllID := "sub-all-" + suffix

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// Filtered subscription: only region="eu" is enqueued for it.
	subFiltered, err := client.CreateSubscription(ctx, subFilteredID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
		Filter:      euFilter,
	})
	if err != nil {
		log.Fatalf("CreateSubscription (filtered) %q: %v", subFilteredID, err)
	}
	fmt.Printf("CreateSubscription: %s (filter=%s)\n", subFiltered.ID(), euFilter)

	// Unfiltered subscription: receives the full batch.
	subAll, err := client.CreateSubscription(ctx, subAllID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("CreateSubscription (all) %q: %v", subAllID, err)
	}
	fmt.Printf("CreateSubscription: %s (no filter)\n", subAll.ID())

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, s := range []*pubsub.Subscription{subFiltered, subAll} {
			if err := s.Delete(cleanupCtx); err != nil {
				log.Printf("warning: DeleteSubscription %s: %v", s.ID(), err)
			} else {
				fmt.Printf("DeleteSubscription: %s ok\n", s.ID())
			}
		}
		if err := topic.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteTopic: %v", err)
		} else {
			fmt.Println("DeleteTopic: ok")
		}
	}()

	// Mixed batch: the `region` attribute drives the filter.
	msgs := []struct {
		body, region string
	}{
		{"order-1", "eu"},
		{"order-2", "us"},
		{"order-3", "eu"},
		{"order-4", "ap"},
		{"order-5", "eu"},
	}
	var euBodies, allBodies []string
	fmt.Println("Publish mixed batch (one Publish per message; region attribute drives the filter):")
	for _, m := range msgs {
		id, err := topic.Publish(ctx, &pubsub.Message{
			Data:       []byte(m.body),
			Attributes: map[string]string{"region": m.region, "type": "order"},
		}).Get(ctx)
		if err != nil {
			log.Fatalf("Publish %q: %v", m.body, err)
		}
		fmt.Printf("  publish body=%-9q region=%-3s -> id=%s\n", m.body, m.region, id)
		allBodies = append(allBodies, m.body)
		if m.region == "eu" {
			euBodies = append(euBodies, m.body)
		}
	}

	// Drain both subscriptions.
	fmt.Println("Pull filtered subscription (expects region=eu only):")
	euGot := drain(ctx, subFiltered, len(euBodies))
	fmt.Printf("  filtered received: %v\n", euGot)
	fmt.Println("Pull unfiltered subscription (expects everything):")
	allGot := drain(ctx, subAll, len(allBodies))
	fmt.Printf("  unfiltered received: %v\n", allGot)

	// Assertions.
	if !equalSorted(euGot, euBodies) {
		log.Fatalf("FAIL: filtered sub got %v, want only region=eu %v", euGot, euBodies)
	}
	if !equalSorted(allGot, allBodies) {
		log.Fatalf("FAIL: unfiltered sub got %v, want full batch %v", allGot, allBodies)
	}
	suppressed := len(allBodies) - len(euBodies)
	fmt.Printf("non-matching attribute -> suppressed (%d not enqueued for filtered sub)\n", suppressed)

	// Negative path: a malformed filter is rejected at CreateSubscription with INVALID_ARGUMENT.
	fmt.Println("Negative path: a malformed filter is rejected at CreateSubscription:")
	badSub, badErr := client.CreateSubscription(ctx, "sub-bad-"+suffix, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
		Filter:      "this is not a filter",
	})
	if badErr == nil {
		_ = badSub.Delete(ctx)
		log.Fatalf("FAIL: malformed filter was accepted; expected INVALID_ARGUMENT")
	}
	code := status.Code(badErr)
	fmt.Printf("  malformed filter -> %s (%v)\n", code, badErr)
	if code != codes.InvalidArgument {
		log.Fatalf("FAIL: malformed filter rejected with %s, want INVALID_ARGUMENT", code)
	}

	fmt.Println("PASS: attributes-only CEL-subset filter applied at publish fan-out; malformed filter rejected")
}

// drain pulls messages via a short StreamingPull Receive loop, acking each, and
// returns the DISTINCT bodies seen. It stops once `want` distinct bodies have
// arrived or the bounded receive context expires (so a filtered sub that
// legitimately gets fewer messages does not hang).
//
// Delivery is at-least-once: a message can be redelivered before its ack is
// durably recorded, so the same body may arrive more than once. We therefore key
// on a stable identity (the body) and count DISTINCT bodies — never the raw
// arrival count — mirroring subscriptions/streaming-pull. Counting raw arrivals
// would let a duplicate satisfy `want` early and drop a not-yet-seen message.
func drain(ctx context.Context, sub *pubsub.Subscription, want int) []string {
	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Bound outstanding work and use a single goroutine so the distinct-count
	// stop condition is observed promptly (same settings as streaming-pull).
	if want > 0 {
		sub.ReceiveSettings.MaxOutstandingMessages = want
	}
	sub.ReceiveSettings.NumGoroutines = 1

	var (
		mu   sync.Mutex
		seen = make(map[string]struct{}, want) // distinct bodies (dedups redeliveries).
	)
	err := sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()
		mu.Lock()
		seen[string(m.Data)] = struct{}{}
		done := len(seen) >= want
		mu.Unlock()
		if done {
			cancel() // all expected distinct bodies drained; stop cleanly.
		}
	})
	if err != nil && recvCtx.Err() == nil {
		log.Fatalf("Receive on %s: %v", sub.ID(), err)
	}

	mu.Lock()
	defer mu.Unlock()
	bodies := make([]string, 0, len(seen))
	for b := range seen {
		bodies = append(bodies, b)
	}
	return bodies
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	return strings.Join(as, ",") == strings.Join(bs, ",")
}
