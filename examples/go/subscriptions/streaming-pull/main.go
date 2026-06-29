// Command streaming-pull is master-table variant 4: the high-throughput consume
// path most client libraries use by default.
//
//	CreateTopic -> CreateSubscription -> Publish(N) -> Receive (StreamingPull) -> Acknowledge
//
// It publishes N messages, opens a long-lived bidirectional StreamingPull stream via
// the high-level Subscription.Receive loop with an explicit flow-control ceiling
// (ReceiveSettings.MaxOutstandingMessages), acks each delivered message in the
// callback, and asserts every published message was delivered exactly once. The
// connector periodically closes each StreamingPull stream
// (CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS, default 1800 s) to force a transparent
// SDK reconnect — no messages are lost across it.
//
// Uses ONLY the official cloud.google.com/go/pubsub client. With PUBSUB_EMULATOR_HOST
// set, pubsub.NewClient clears credentials, skips Google auth, and dials insecure
// gRPC — no code change. The topic and subscription are uuid-suffixed per run so
// concurrent runs use distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never
// cross-talk; both are deleted on exit (best-effort) so re-runs start clean.
//
// A Publish writes once to the topic log `pubsub.orders-<uuid8>` (the authoritative,
// replayable, cross-protocol source), then fans out one queue copy to the per-sub
// queue `pubsub.sub.sub-orders-<uuid8>` (spec S3.3); the StreamingPull stream drains
// that subscription's queue. Any failed assertion exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./subscriptions/streaming-pull
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

// messageCount is the number of messages published and then drained over the stream.
const messageCount = 8

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] subscriptions/streaming-pull | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// PUBSUB_EMULATOR_HOST routes the official client at the connector over insecure
	// gRPC with credentials cleared — no code change, no option.WithoutAuthentication().
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). The canonical master-table
	// names are orders / sub-orders; the uuid8 suffix gives this run a private channel
	// namespace so parallel runs never collide on the shared `pubsub.{t}` channels.
	suffix := uuid.NewString()[:8]
	topicID := "orders-" + suffix
	subID := "sub-orders-" + suffix

	// 1. CreateTopic -> channel pubsub.{topicID}.
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic        -> %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// 2. CreateSubscription -> per-sub queue pubsub.sub.{subID}.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second, // connector default; valid range is 10..600s.
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription -> %s (channel pubsub.sub.%s)\n", sub.ID(), subID)

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

	// 3. Publish a known set; remember each by its seq attribute and block for the id.
	published := make(map[string]struct{}, messageCount)
	for i := 0; i < messageCount; i++ {
		seq := fmt.Sprintf("%d", i)
		result := topic.Publish(ctx, &pubsub.Message{
			Data:       []byte("event-" + seq),
			Attributes: map[string]string{"seq": seq},
		})
		if _, err := result.Get(ctx); err != nil { // blocks until the connector acks the publish.
			log.Fatalf("Publish #%d: %v", i, err)
		}
		published[seq] = struct{}{}
	}
	fmt.Printf("Publish            -> %d messages to the topic log\n", messageCount)

	// 4. Open a long-lived StreamingPull stream via Receive. ReceiveSettings.MaxOutstandingMessages
	//    is the client-side flow-control ceiling on outstanding (un-acked) messages — the connector
	//    also enforces server-side safety nets (MaxOutstandingMessages 1000, MaxInflightPerSubscription
	//    20000). One StreamingPull stream (NumGoroutines=1) keeps the proof deterministic.
	sub.ReceiveSettings.MaxOutstandingMessages = messageCount
	sub.ReceiveSettings.NumGoroutines = 1

	recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recvCancel()

	var (
		mu       sync.Mutex
		received = make(map[string]int) // seq -> delivery count (detects duplicates).
	)
	fmt.Println("StreamingPull      -> stream open (Receive loop); acking each via callback")
	err = sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
		seq := m.Attributes["seq"]
		m.Ack() // ack the broker seq under its ack-deadline lease.
		mu.Lock()
		received[seq]++
		done := len(received) == messageCount
		mu.Unlock()
		if done {
			recvCancel() // all distinct messages drained; stop StreamingPull cleanly.
		}
	})
	if err != nil && recvCtx.Err() == nil {
		log.Fatalf("Receive: %v", err)
	}

	// Assertions: exactly the published set, each delivered exactly once.
	fmt.Printf("Received           -> %d messages over the StreamingPull stream\n", len(received))
	if len(received) != messageCount {
		log.Fatalf("FAIL: received %d distinct messages, expected %d", len(received), messageCount)
	}
	fmt.Printf("  [OK] received all %d published messages\n", messageCount)
	for seq := range published {
		if received[seq] == 0 {
			log.Fatalf("FAIL: published message seq=%q was never delivered", seq)
		}
	}
	fmt.Println("  [OK] every published message was delivered exactly once")
	for seq, n := range received {
		if n != 1 {
			log.Fatalf("FAIL: message seq=%q delivered %d times (expected 1)", seq, n)
		}
	}
	fmt.Println("  [OK] no duplicate deliveries (each acked once)")
	fmt.Println("\nPASS: StreamingPull round-trip complete — all messages drained and acked.")
}
