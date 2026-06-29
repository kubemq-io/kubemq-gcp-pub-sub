// Command exactly-once is master-table variant 10: exactly-once delivery against
// the KubeMQ Pub/Sub connector.
//
//	CreateTopic
//	-> CreateSubscription(EnableExactlyOnceDelivery=true)
//	-> Publish one message
//	-> StreamingPull (Receive); ack via Message.AckWithResult() and BLOCK on the
//	   server's AcknowledgeConfirmation
//	-> assert the confirmation is AcknowledgeStatusSuccess (the broker durably
//	   recorded the ack; a subsequent redelivery is suppressed)
//
// With enable_exactly_once_delivery, StreamingPull returns an
// AcknowledgeConfirmation for each ack/nack (spec S3.5): the high-level client
// surfaces it through Message.AckWithResult(), whose AckResult.Get(ctx) blocks
// until the broker confirms (AcknowledgeStatusSuccess) or fails
// (AcknowledgeStatusInvalidAckID / FailedPrecondition). A bare Message.Ack() is
// fire-and-forget and gives no confirmation, so exactly-once code MUST use
// AckWithResult to observe the guarantee.
//
// NODE-LOCAL CAVEAT (spec S3.5, gotcha #1): an ack_id is minted on, and valid only
// on, the node that delivered the message — an ack_id from one node is invalid on
// another (unary invalid-ack -> FAILED_PRECONDITION + ErrorInfo). In a cluster,
// pin a subscription's StreamingPull to one node (sticky LB) or accept
// at-least-once across nodes.
//
// Resource ids are uuid-suffixed per run (channel isolation); everything is
// deleted on exit (best-effort). Any failure exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./delivery/exactly-once
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

// ackStatusLabel renders the AcknowledgeStatus enum (an int with no Stringer) as
// a human-readable label for the proof output.
func ackStatusLabel(s pubsub.AcknowledgeStatus) string {
	switch s {
	case pubsub.AcknowledgeStatusSuccess:
		return "Success"
	case pubsub.AcknowledgeStatusPermissionDenied:
		return "PermissionDenied"
	case pubsub.AcknowledgeStatusFailedPrecondition:
		return "FailedPrecondition"
	case pubsub.AcknowledgeStatusInvalidAckID:
		return "InvalidAckID"
	default:
		return "Other"
	}
}

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the project segment is parsed but ignored.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] delivery/exactly-once | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical: eos / sub-eos.
	suffix := uuid.NewString()[:8]
	topicID := "eos-" + suffix
	subID := "sub-eos-" + suffix

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:                     topic,
		AckDeadline:               10 * time.Second,
		EnableExactlyOnceDelivery: true,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (EnableExactlyOnceDelivery=true)\n", sub.ID())

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

	// Publish one message.
	const body = "exactly-once payload"
	msgID, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(body)}).Get(ctx)
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Printf("Publish: messageId=%s body=%q\n", msgID, body)

	// StreamingPull; ack with confirmation. AckWithResult().Get(ctx) blocks on the
	// server's AcknowledgeConfirmation — the heart of exactly-once.
	recvCtx, recvCancel := context.WithTimeout(ctx, 20*time.Second)
	defer recvCancel()

	var (
		once         sync.Once
		gotID, gotBy string
		ackStatus    pubsub.AcknowledgeStatus
		ackErr       error
		received     int
	)
	fmt.Println("StreamingPull (Receive): ack with confirmation via AckWithResult()")
	err = sub.Receive(recvCtx, func(rctx context.Context, m *pubsub.Message) {
		received++
		// Ack and BLOCK on the broker's confirmation (not fire-and-forget).
		res := m.AckWithResult()
		status, statusErr := res.Get(rctx)
		once.Do(func() {
			gotID = m.ID
			gotBy = string(m.Data)
			ackStatus = status
			ackErr = statusErr
			recvCancel() // one confirmed message is enough; stop StreamingPull.
		})
	})
	if err != nil && recvCtx.Err() == nil {
		log.Fatalf("Receive: %v", err)
	}

	// Assertions: exactly one message, body/id round-trip, ack confirmed by broker.
	if received == 0 {
		log.Fatalf("FAIL: expected 1 message, received 0")
	}
	if ackErr != nil {
		log.Fatalf("FAIL: AckWithResult confirmation error: %v", ackErr)
	}
	fmt.Printf("Receive: messageId=%s body=%q ackConfirmation=%s\n", gotID, gotBy, ackStatusLabel(ackStatus))
	if gotBy != body {
		log.Fatalf("FAIL: received body %q != published %q", gotBy, body)
	}
	if gotID != msgID {
		log.Fatalf("FAIL: received messageId %q != published %q", gotID, msgID)
	}
	if ackStatus != pubsub.AcknowledgeStatusSuccess {
		log.Fatalf("FAIL: ack confirmation %v != AcknowledgeStatusSuccess", ackStatus)
	}
	fmt.Println("PASS: exactly-once ack confirmed by the broker (AcknowledgeStatusSuccess)")
}
