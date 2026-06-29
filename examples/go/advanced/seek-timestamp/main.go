// Command seek-timestamp is master-table variant 11: rewinding a subscription to
// a wall-clock instant and replaying the topic log from there.
//
//	CreateTopic -> CreateSubscription(retain_acked) -> Publish(N) -> Pull/Ack(all)
//	            -> SeekToTime(checkpoint) -> Pull (replays the in-window tail)
//	            -> SeekToTime(pre-window)  -> Pull (clamps to earliest retained).
//
// The topic is backed by a durable, replayable Events Store log (`pubsub.{t}`), so a
// Seek to a timestamp rewinds the subscription's cursor and replays the topic log from
// the first message published at/after that instant (spec S3.5 seek/snapshots). The
// connector resolves the start seq from the topic log, purges the sub queue and drops
// in-flight leases, then replays via Array.RangeEventsStore re-applying the filter,
// bounded by MaxSeekReplay. A seek to a timestamp older than the earliest retained
// message CLAMPS to the earliest retained message — it is NOT an error.
//
// Uses ONLY the official cloud.google.com/go/pubsub client: the high-level client for
// CreateTopic/CreateSubscription/Publish/SeekToTime, and its apiv1 SubscriberClient for
// the deterministic unary Pull/Acknowledge drain. Resource ids are uuid-suffixed per run
// so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never
// cross-talk; both are deleted on exit (best-effort). Any failed assertion exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./advanced/seek-timestamp
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const messageCount = 6

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] advanced/seek-timestamp | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// PUBSUB_EMULATOR_HOST routes the high-level client at the connector over insecure
	// gRPC with credentials cleared — no code change. The apiv1 SubscriberClient does NOT
	// auto-detect the env var, so we pass the same emulator options explicitly (emulatorOpts).
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	subClient, err := vkit.NewSubscriberClient(ctx, emulatorOpts()...)
	if err != nil {
		log.Fatalf("vkit.NewSubscriberClient: %v", err)
	}
	defer func() { _ = subClient.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical names: seekable / sub-seekable.
	suffix := uuid.NewString()[:8]
	topicID := "seekable-" + suffix
	subID := "sub-seekable-" + suffix

	// CreateTopic -> channel pubsub.{topicID}.
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// CreateSubscription with RetainAckedMessages so the log stays replayable after ack;
	// a Seek can then rewind past already-acknowledged messages (spec S3.5).
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:               topic,
		AckDeadline:         10 * time.Second,
		RetainAckedMessages: true,
		RetentionDuration:   10 * time.Minute,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (channel pubsub.sub.%s, retain_acked=true)\n", sub.ID(), subID)
	subName := sub.String()

	defer cleanup(sub, topic)

	// Publish order-1 and order-2, capture a checkpoint timestamp strictly between
	// order-2 and order-3, then publish the rest. Server publish-time drives the seek
	// resolution, so we space the publishes around the recorded instant.
	publish(ctx, topic, "order-1")
	publish(ctx, topic, "order-2")
	time.Sleep(1200 * time.Millisecond)
	checkpoint := time.Now().UTC()
	fmt.Printf("checkpoint timestamp recorded -> %s\n", checkpoint.Format(time.RFC3339))
	time.Sleep(1200 * time.Millisecond)
	publish(ctx, topic, "order-3")
	publish(ctx, topic, "order-4")
	publish(ctx, topic, "order-5")
	publish(ctx, topic, "order-6")

	// 1) Initial drain: all messageCount delivered once, in publish order.
	initial := drain(ctx, subClient, subName)
	fmt.Printf("Pull (initial) -> %v\n", initial)
	check(equal(initial, []string{"order-1", "order-2", "order-3", "order-4", "order-5", "order-6"}),
		fmt.Sprintf("initial drain delivered all %d in order", messageCount))

	// 2) Seek to the checkpoint: replays only the tail published at/after it.
	if err := sub.SeekToTime(ctx, checkpoint); err != nil {
		log.Fatalf("SeekToTime(checkpoint): %v", err)
	}
	fmt.Println("Seek(time=checkpoint) -> cursor rewound")
	replay := drain(ctx, subClient, subName)
	fmt.Printf("Pull (after checkpoint seek) -> %v\n", replay)
	check(equal(replay, []string{"order-3", "order-4", "order-5", "order-6"}),
		"seek-to-checkpoint replayed only order-3..order-6 (4 at/after the boundary)")

	// 3) Seek to a pre-window timestamp: clamps to the earliest retained message.
	preWindow := time.Now().UTC().Add(-365 * 24 * time.Hour)
	if err := sub.SeekToTime(ctx, preWindow); err != nil {
		log.Fatalf("SeekToTime(pre-window): %v", err)
	}
	fmt.Println("Seek(time=now-365d) -> pre-window (clamps to earliest retained, not an error)")
	clamped := drain(ctx, subClient, subName)
	fmt.Printf("Pull (after pre-window seek) -> %v\n", clamped)
	check(equal(clamped, []string{"order-1", "order-2", "order-3", "order-4", "order-5", "order-6"}),
		"pre-window seek clamped to earliest retained (all 6 replayed)")

	fmt.Println("PASS: timestamp seek replayed the in-window tail and clamped a pre-window seek")
}

// publish sends one message and blocks for the server-assigned id.
func publish(ctx context.Context, topic *pubsub.Topic, body string) {
	id, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(body)}).Get(ctx)
	if err != nil {
		log.Fatalf("Publish %q: %v", body, err)
	}
	fmt.Printf("Publish: %s -> messageId=%s\n", body, id)
}

// drain unary-Pulls + Acknowledges everything currently available and returns the
// message bodies in delivery order. It polls until two consecutive empty Pulls so a
// seek-then-replay is fully drained without a fixed sleep.
func drain(ctx context.Context, subClient *vkit.SubscriberClient, subName string) []string {
	var bodies []string
	emptyStreak := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := subClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: subName,
			MaxMessages:  10,
		})
		if err != nil {
			log.Fatalf("Pull: %v", err)
		}
		if len(resp.GetReceivedMessages()) == 0 {
			emptyStreak++
			if emptyStreak >= 2 {
				break // two empty pulls in a row: nothing left to drain.
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		emptyStreak = 0
		ackIDs := make([]string, 0, len(resp.GetReceivedMessages()))
		for _, rm := range resp.GetReceivedMessages() {
			bodies = append(bodies, string(rm.GetMessage().GetData()))
			ackIDs = append(ackIDs, rm.GetAckId())
		}
		if err := subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
			Subscription: subName,
			AckIds:       ackIDs,
		}); err != nil {
			log.Fatalf("Acknowledge: %v", err)
		}
	}
	return bodies
}

// emulatorOpts mirrors the high-level client's PUBSUB_EMULATOR_HOST handling for the
// apiv1 SubscriberClient, which does not auto-detect the emulator from the env var:
// point it at the connector over insecure gRPC with no credentials.
func emulatorOpts() []option.ClientOption {
	addr := os.Getenv("PUBSUB_EMULATOR_HOST")
	if addr == "" {
		return nil
	}
	return []option.ClientOption{
		option.WithEndpoint(addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		option.WithTelemetryDisabled(),
	}
}

// cleanup deletes the subscription then the topic (best-effort) so re-runs start clean.
func cleanup(sub *pubsub.Subscription, topic *pubsub.Topic) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sub.Delete(ctx); err != nil {
		log.Printf("warning: DeleteSubscription: %v", err)
	} else {
		fmt.Println("DeleteSubscription: ok")
	}
	if err := topic.Delete(ctx); err != nil {
		log.Printf("warning: DeleteTopic: %v", err)
	} else {
		fmt.Println("DeleteTopic: ok")
	}
}

// check asserts an expected outcome, printing an [OK] line or exiting non-zero.
func check(condition bool, message string) {
	if condition {
		fmt.Printf("[OK] %s\n", message)
		return
	}
	fmt.Printf("[FAIL] %s\n", message)
	os.Exit(1)
}

// equal reports whether two string slices have identical contents and order.
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
