// Command basic-publish-subscribe is master-table variant 1: the full Pub/Sub
// round-trip against the KubeMQ Pub/Sub connector.
//
//	CreateTopic -> CreateSubscription -> Publish -> Pull (Receive) -> Acknowledge
//
// It publishes one message to the topic, pulls exactly that message back through
// the subscription, asserts the body and the user attribute round-trip, and acks
// it. The topic and subscription are uuid-suffixed per run so concurrent runs use
// distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never cross-talk; both are
// deleted on exit (best-effort) so re-runs start clean. Any mismatch exits non-zero.
//
// A Publish writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative,
// replayable, cross-protocol source), then fans out one queue copy to the per-sub
// queue `pubsub.sub.sub-orders-<uuid8>` (spec S3.3).
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./topics/basic-publish-subscribe
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] topics/basic-publish-subscribe | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// PUBSUB_EMULATOR_HOST routes the official client at the connector over insecure
	// gRPC with credentials cleared — no code change, no option.WithoutAuthentication().
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). The canonical master-table
	// names are orders / sub-orders; the uuid8 suffix gives this run a private
	// channel namespace so parallel runs never collide.
	suffix := uuid.NewString()[:8]
	topicID := "orders-" + suffix
	subID := "sub-orders-" + suffix

	// 1. CreateTopic -> channel pubsub.{topicID}.
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// 2. CreateSubscription -> per-sub queue pubsub.sub.{subID}.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second, // connector default; valid range is 10..600s.
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (channel pubsub.sub.%s)\n", sub.ID(), subID)

	// Best-effort teardown so re-runs start clean (DeleteSubscription -> DeleteTopic).
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := sub.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteSubscription: %v", err)
		} else {
			fmt.Println("DeleteSubscription: ok")
		}
		if err := topic.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteTopic: %v", err)
		} else {
			fmt.Println("DeleteTopic: ok")
		}
	}()

	// 3. Publish one message and block for the server-assigned id.
	body := []byte("order #4242 — 3x widget, ship express")
	const attrKey, attrVal = "priority", "express"
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       body,
		Attributes: map[string]string{attrKey: attrVal},
	})
	msgID, err := result.Get(ctx) // blocks until the publish is acked by the connector.
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Printf("Publish: messageId=%s\n", msgID)

	// 4. Pull exactly one message via Receive (StreamingPull), then cancel the loop.
	recvCtx, recvCancel := context.WithTimeout(ctx, 15*time.Second)
	defer recvCancel()

	var (
		once     sync.Once
		got      *pubsub.Message
		received int
	)
	err = sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
		received++
		// 5. Acknowledge the broker seq under its ack-deadline lease.
		m.Ack()
		once.Do(func() {
			got = m
			recvCancel() // one message is enough; stop StreamingPull cleanly.
		})
	})
	if err != nil && recvCtx.Err() == nil {
		log.Fatalf("Receive: %v", err)
	}

	// Assertions: exactly one message, byte-for-byte body, attribute, reserved id.
	if got == nil {
		log.Fatalf("FAIL: expected 1 message, received %d", received)
	}
	fmt.Printf("Receive: messageId=%s body=%q attr[%s]=%q publishTime=%s\n",
		got.ID, string(got.Data), attrKey, got.Attributes[attrKey], got.PublishTime.Format(time.RFC3339))
	if string(got.Data) != string(body) {
		log.Fatalf("FAIL: received body %q != published %q", string(got.Data), string(body))
	}
	if got.Attributes[attrKey] != attrVal {
		log.Fatalf("FAIL: received attr %q=%q != published %q", attrKey, got.Attributes[attrKey], attrVal)
	}
	if got.ID != msgID {
		log.Fatalf("FAIL: received messageId %q != published %q", got.ID, msgID)
	}
	fmt.Println("Acknowledge: ok (acked by ack_id under its lease)")
	fmt.Println("PASS: full Pub/Sub round-trip verified (publish -> pull -> ack)")
}
