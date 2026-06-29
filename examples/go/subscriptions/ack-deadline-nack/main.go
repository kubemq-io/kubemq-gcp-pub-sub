// Command ack-deadline-nack is master-table variant 5: the subscription lease
// lifecycle, driven with the unary Pull + ModifyAckDeadline RPCs.
//
//	Pull -> ModifyAckDeadline(>0) extend -> ModifyAckDeadline(0) nack -> redeliver -> Acknowledge
//
// Every delivered message carries an opaque `ack_id` held under an ack-deadline
// lease. ModifyAckDeadline(ack_id, n>0) extends the lease (clamped to 10..600 s) so a
// slow consumer keeps ownership; ModifyAckDeadline(ack_id, 0) is an immediate nack
// that redelivers the message now. The connector's 250 ms sweeper also redelivers any
// lease that expires un-acked. This example proves the nack path: a pulled-but-nacked
// message comes back on the next Pull, and a final Acknowledge drains it for good.
//
// Uses ONLY the official cloud.google.com/go/pubsub client. The high-level client
// (pubsub.NewClient) handles topic/subscription lifecycle and publish; the unary lease
// RPCs (Pull / ModifyAckDeadline / Acknowledge) are not on the high-level Subscription
// type, so they go through the official low-level cloud.google.com/go/pubsub/apiv1
// SubscriberClient. Both honour PUBSUB_EMULATOR_HOST — the high-level client
// auto-detects it; the apiv1 client gets the same insecure/no-auth emulator options
// explicitly (the env var only auto-wires the high-level constructor).
//
// The topic and subscription are uuid-suffixed per run so concurrent runs use distinct
// `pubsub.{t}` / `pubsub.sub.{s}` channels and never cross-talk; both are deleted on
// exit (best-effort) so re-runs start clean. A Publish writes once to the topic log
// `pubsub.orders-<uuid8>`, then fans out one queue copy into the per-sub queue
// `pubsub.sub.sub-orders-<uuid8>` (spec S3.3); ack/nack act on the broker sequence held
// under the subscription-owned lease. Any failed assertion exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./subscriptions/ack-deadline-nack
package main

import (
	"bytes"
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

// extendSeconds is the lease extension applied before the nack — within 10..600 s.
const extendSeconds = 30

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	emulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	fmt.Printf("[kubemq-gcp-pub-sub] subscriptions/ack-deadline-nack | emulator=%s project=%s\n",
		emulatorHost, projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// High-level client for topic/subscription lifecycle + publish. PUBSUB_EMULATOR_HOST
	// routes it at the connector over insecure gRPC with credentials cleared (no code change).
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Low-level SubscriberClient for the unary lease RPCs (Pull / ModifyAckDeadline /
	// Acknowledge). The emulator env var only auto-wires the high-level constructor, so
	// apply the same insecure/no-auth emulator options here when PUBSUB_EMULATOR_HOST is set.
	var subOpts []option.ClientOption
	if emulatorHost != "" {
		subOpts = []option.ClientOption{
			option.WithEndpoint(emulatorHost),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		}
	}
	subClient, err := vkit.NewSubscriberClient(ctx, subOpts...)
	if err != nil {
		log.Fatalf("apiv1.NewSubscriberClient: %v", err)
	}
	defer func() { _ = subClient.Close() }()

	// Unique-per-run resource ids (channel isolation): the uuid8 suffix gives this run a
	// private channel namespace so parallel runs never collide on the shared channels.
	suffix := uuid.NewString()[:8]
	topicID := "orders-" + suffix
	subID := "sub-orders-" + suffix
	subPath := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)

	// 1. CreateTopic -> channel pubsub.{topicID}.
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic        -> %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// 2. CreateSubscription -> per-sub queue pubsub.sub.{subID}, ack deadline 10 s.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second, // connector default; valid range is 10..600s.
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription -> %s (ack deadline 10 s, channel pubsub.sub.%s)\n", sub.ID(), subID)

	// Best-effort teardown so re-runs start clean (DeleteSubscription -> DeleteTopic).
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := sub.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteSubscription: %v", err)
		} else {
			fmt.Println("DeleteSubscription -> ok")
		}
		if err := topic.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteTopic: %v", err)
		} else {
			fmt.Println("DeleteTopic        -> ok")
		}
	}()

	// 3. Publish one message and block for the server-assigned id.
	payload := []byte("lease-me")
	if _, err := topic.Publish(ctx, &pubsub.Message{Data: payload, Attributes: map[string]string{"seq": "0"}}).Get(ctx); err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Println("Publish            -> 1 message to the topic log")

	// 4. First delivery via unary Pull — the message is now leased under the ack deadline.
	first := pullOne(ctx, subClient, subPath)
	if first == nil {
		log.Fatalf("FAIL: first Pull returned no message")
	}
	fmt.Println("  [OK] first Pull returns the message")
	fmt.Printf("Pull #1            -> body=%q ack_id=%s...\n", first.GetMessage().GetData(), truncate(first.GetAckId(), 16))

	// 5. Extend the lease. ModifyAckDeadline(>0) pushes the deadline out (clamped to
	//    10..600 s) so a slow consumer keeps ownership instead of letting the sweeper redeliver.
	if err := subClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       subPath,
		AckIds:             []string{first.GetAckId()},
		AckDeadlineSeconds: extendSeconds,
	}); err != nil {
		log.Fatalf("ModifyAckDeadline(extend): %v", err)
	}
	fmt.Printf("ModifyAckDeadline  -> +%ds (lease extended, still owned)\n", extendSeconds)

	// 6. Nack via ModifyAckDeadline(0): immediate redelivery instead of waiting out the lease.
	if err := subClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       subPath,
		AckIds:             []string{first.GetAckId()},
		AckDeadlineSeconds: 0,
	}); err != nil {
		log.Fatalf("ModifyAckDeadline(nack): %v", err)
	}
	fmt.Println("ModifyAckDeadline  -> 0 (nack: redeliver now)")

	// 7. The nacked message comes back on a later Pull.
	second := pullWithRetry(ctx, subClient, subPath, 10, 300*time.Millisecond)
	if second == nil {
		log.Fatalf("FAIL: the nacked message was not redelivered")
	}
	fmt.Println("  [OK] the nacked message is redelivered on a later Pull")
	if !bytes.Equal(second.GetMessage().GetData(), payload) {
		log.Fatalf("FAIL: redelivered body %q != original %q", second.GetMessage().GetData(), payload)
	}
	fmt.Println("  [OK] redelivered body matches the original")
	fmt.Printf("Pull #2            -> body=%q ack_id=%s...\n", second.GetMessage().GetData(), truncate(second.GetAckId(), 16))

	// 8. Acknowledge for good — the lease is released and the message is gone.
	if err := subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: subPath,
		AckIds:       []string{second.GetAckId()},
	}); err != nil {
		log.Fatalf("Acknowledge: %v", err)
	}
	fmt.Println("Acknowledge        -> message acked")

	// 9. Prove the subscription is now empty (the ack removed it; no further redelivery).
	if leftover := pullWithRetry(ctx, subClient, subPath, 3, 300*time.Millisecond); leftover != nil {
		log.Fatalf("FAIL: subscription not empty after ack (got ack_id=%s)", truncate(leftover.GetAckId(), 16))
	}
	fmt.Println("  [OK] subscription is empty after the ack (no further redelivery)")

	fmt.Println("\nPASS: ack-deadline / nack lifecycle complete — nack redelivered, ack drained.")
}

// pullOne issues a single unary Pull and returns the first ReceivedMessage, or nil if empty.
func pullOne(ctx context.Context, c *vkit.SubscriberClient, subPath string) *pubsubpb.ReceivedMessage {
	pullCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := c.Pull(pullCtx, &pubsubpb.PullRequest{Subscription: subPath, MaxMessages: 1})
	if err != nil {
		log.Fatalf("Pull: %v", err)
	}
	if len(resp.GetReceivedMessages()) == 0 {
		return nil
	}
	return resp.GetReceivedMessages()[0]
}

// pullWithRetry polls for a (re)delivered message; the nacked copy reappears after a short backoff.
func pullWithRetry(ctx context.Context, c *vkit.SubscriberClient, subPath string, attempts int, delay time.Duration) *pubsubpb.ReceivedMessage {
	for i := 0; i < attempts; i++ {
		if msg := pullOne(ctx, c, subPath); msg != nil {
			return msg
		}
		time.Sleep(delay)
	}
	return nil
}

// truncate shortens an opaque ack_id for display (the full value is non-deterministic per run).
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
