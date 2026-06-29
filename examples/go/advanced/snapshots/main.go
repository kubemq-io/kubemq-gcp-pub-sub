// Command snapshots is master-table variant 12: capturing a subscription cursor
// as a snapshot and rewinding to it later without knowing an exact timestamp.
//
//	CreateTopic -> CreateSubscription -> Publish(1..3) -> Pull/Ack
//	            -> CreateSnapshot (capture cursor) -> Publish(4..6) -> Pull/Ack
//	            -> SeekToSnapshot -> Pull (replays from the captured cursor onward)
//	            -> CreateSnapshot on a detached subscription -> FAILED_PRECONDITION.
//
// A snapshot captures a subscription's current cursor as a registry record (7-day
// default expiry, swept hourly) so a later SeekToSnapshot rewinds to that position and
// replays the topic log `pubsub.{t}` from there via Array.RangeEventsStore (spec S3.5).
// The connector anchors the snapshot at the topic log's last retained sequence at
// capture time (a conservative anchor), so the replay re-drives the log forward from the
// boundary message order-3 inclusive — never re-delivering the pre-snapshot order-1/2.
// Snapshotting a detached subscription (its topic deleted) returns FAILED_PRECONDITION.
//
// Uses ONLY the official cloud.google.com/go/pubsub client: the high-level client for
// CreateTopic/CreateSubscription/Publish/CreateSnapshot/SeekToSnapshot, and its apiv1
// SubscriberClient for the deterministic unary Pull/Acknowledge drain. Resource ids are
// uuid-suffixed per run so concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}`
// channels and never cross-talk; resources are deleted on exit (best-effort). Any failed
// assertion exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./advanced/snapshots
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] advanced/snapshots | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// The apiv1 SubscriberClient does not auto-detect PUBSUB_EMULATOR_HOST; pass the
	// emulator options explicitly so the unary Pull/Acknowledge drain hits the connector.
	subClient, err := vkit.NewSubscriberClient(ctx, emulatorOpts()...)
	if err != nil {
		log.Fatalf("vkit.NewSubscriberClient: %v", err)
	}
	defer func() { _ = subClient.Close() }()

	// Unique-per-run resource ids (channel isolation).
	suffix := uuid.NewString()[:8]
	topicID := "snapped-" + suffix
	subID := "sub-snapped-" + suffix
	snapshotID := "snap-cursor-" + suffix
	detachedSubID := "sub-detached-" + suffix
	detachedSnapID := "snap-detached-" + suffix

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// RetainAckedMessages keeps the topic log replayable after ack so a seek can rewind.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:               topic,
		AckDeadline:         10 * time.Second,
		RetainAckedMessages: true,
		RetentionDuration:   10 * time.Minute,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (channel pubsub.sub.%s)\n", sub.ID(), subID)
	subName := sub.String()

	snapshot := client.Snapshot(snapshotID)
	// topicDeleted is set once the detach step below deletes the topic, so the deferred
	// cleanup does not re-delete it (which would log a spurious not-found warning).
	topicDeleted := false
	defer cleanup(snapshot, sub, topic, &topicDeleted)

	// Publish and drain order-1..order-3; the cursor advances past them.
	for i := 1; i <= 3; i++ {
		publish(ctx, topic, fmt.Sprintf("order-%d", i))
	}
	first := drain(ctx, subClient, subName)
	fmt.Printf("Pull (initial) -> %v\n", first)
	check(equal(first, []string{"order-1", "order-2", "order-3"}), "initial drain delivered order-1..order-3")

	// CreateSnapshot: capture the cursor at the current position (after order-3).
	snapCfg, err := sub.CreateSnapshot(ctx, snapshotID)
	if err != nil {
		log.Fatalf("CreateSnapshot %q: %v", snapshotID, err)
	}
	fmt.Printf("CreateSnapshot: %s (cursor captured, expires %s)\n",
		snapCfg.ID(), snapCfg.Expiration.Format(time.RFC3339))

	// Publish and drain order-4..order-6; the cursor advances again.
	for i := 4; i <= 6; i++ {
		publish(ctx, topic, fmt.Sprintf("order-%d", i))
	}
	second := drain(ctx, subClient, subName)
	fmt.Printf("Pull (after snapshot) -> %v\n", second)
	check(equal(second, []string{"order-4", "order-5", "order-6"}), "drained order-4..order-6 after the snapshot")

	// SeekToSnapshot: the cursor rewinds to the captured position and replays the log
	// from the boundary message order-3 inclusive (conservative anchor); pre-snapshot
	// order-1/order-2 are excluded.
	if err := sub.SeekToSnapshot(ctx, snapshot); err != nil {
		log.Fatalf("SeekToSnapshot %q: %v", snapshotID, err)
	}
	fmt.Printf("Seek(snapshot=%s) -> cursor rewound to captured position\n", snapshotID)
	replay := drain(ctx, subClient, subName)
	fmt.Printf("Pull (after seek-to-snapshot) -> %v\n", replay)
	check(equal(replay, []string{"order-3", "order-4", "order-5", "order-6"}),
		"seek-to-snapshot replayed order-3..order-6 from the captured cursor (pre-snapshot excluded)")

	// Negative path: snapshotting a detached subscription -> FAILED_PRECONDITION.
	detachedSub, err := client.CreateSubscription(ctx, detachedSubID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", detachedSubID, err)
	}
	fmt.Printf("CreateSubscription: %s (will be detached)\n", detachedSub.ID())
	if err := topic.Delete(ctx); err != nil {
		log.Fatalf("DeleteTopic (to detach): %v", err)
	}
	topicDeleted = true
	fmt.Printf("DeleteTopic: %s (detaches its subscriptions)\n", topicID)

	_, err = detachedSub.CreateSnapshot(ctx, detachedSnapID)
	if status.Code(err) != codes.FailedPrecondition {
		log.Fatalf("FAIL: snapshot of detached subscription returned %v, expected FAILED_PRECONDITION", err)
	}
	fmt.Println("CreateSnapshot(detached sub) -> FAILED_PRECONDITION")
	check(true, "snapshot of a detached subscription rejected FAILED_PRECONDITION")

	// Clean up the extra detached subscription explicitly (the deferred cleanup handles the rest).
	if err := detachedSub.Delete(context.Background()); err != nil {
		log.Printf("warning: DeleteSubscription (detached): %v", err)
	}

	fmt.Println("PASS: snapshot captured the cursor, seek-to-snapshot replayed, detached rejected")
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
// message bodies in delivery order, stopping after two consecutive empty Pulls.
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
				break
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
// apiv1 SubscriberClient, which does not auto-detect the emulator from the env var.
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

// cleanup deletes the snapshot, subscription, and topic (best-effort) so re-runs start
// clean. The topic is skipped when the detach step already deleted it.
func cleanup(snapshot *pubsub.Snapshot, sub *pubsub.Subscription, topic *pubsub.Topic, topicDeleted *bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := snapshot.Delete(ctx); err != nil {
		log.Printf("warning: DeleteSnapshot: %v", err)
	} else {
		fmt.Println("DeleteSnapshot: ok")
	}
	if err := sub.Delete(ctx); err != nil {
		log.Printf("warning: DeleteSubscription: %v", err)
	} else {
		fmt.Println("DeleteSubscription: ok")
	}
	if !*topicDeleted {
		if err := topic.Delete(ctx); err != nil {
			log.Printf("warning: DeleteTopic: %v", err)
		} else {
			fmt.Println("DeleteTopic: ok")
		}
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
