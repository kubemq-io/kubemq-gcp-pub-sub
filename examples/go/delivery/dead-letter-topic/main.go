// Command dead-letter-topic is master-table variant 8: connector-level
// dead-lettering against the KubeMQ Pub/Sub connector.
//
//	CreateTopic (source) + CreateTopic (dead-letter)
//	-> CreateSubscription(source, DeadLetterPolicy{dlt, max_delivery_attempts=5})
//	-> CreateSubscription(dead-letter)
//	-> Publish one "poison" message to the source
//	-> repeatedly Pull the source WITHOUT acking (let the lease expire) so the
//	   sweeper raises the receive count; once it exceeds max_delivery_attempts the
//	   sweeper republishes to the dead-letter topic and acks the original
//	-> Pull + Acknowledge the message off the dead-letter subscription
//
// Dead-lettering is driven by NATURAL ack-deadline EXPIRY, not by an explicit
// nack (spec S3.4/S3.5): the connector's 250 ms lease sweeper raises the receive
// count on every expiry of an unacked lease; once it exceeds max_delivery_attempts
// (valid 5..100; 0 = unset) and the sub has a dead_letter_topic, the sweeper
// republishes to that topic and acks the original off the source. An explicit
// ModifyAckDeadline(0) nack redelivers but does NOT raise the count toward the DLQ
// threshold. With the minimum ack deadline (10 s) and max_delivery_attempts = 5,
// the message dead-letters after ~5 expiry cycles (≈50-65 s); the loop is bounded
// at 90 s.
//
// This uses the low-level apiv1 client for the no-ack source pulls: the high-level
// Receive loop auto-extends leases (lease management), which would prevent the
// natural expiry this variant depends on. The bare apiv1 client does NOT
// auto-detect PUBSUB_EMULATOR_HOST, so the insecure/no-auth options are wired
// explicitly.
//
// Resource ids are uuid-suffixed per run (channel isolation); everything is
// deleted on exit (best-effort). Any failure exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./delivery/dead-letter-topic
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	vkit "cloud.google.com/go/pubsub/apiv1"
)

const maxDeliveryAttempts = 5 // valid 5..100 (Google minimum); 0 = unset.

func emulatorOptions() []option.ClientOption {
	host := os.Getenv("PUBSUB_EMULATOR_HOST")
	if host == "" {
		return nil
	}
	return []option.ClientOption{
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the project segment is parsed but ignored.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] delivery/dead-letter-topic | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical: work / work-dlq / sub-work.
	suffix := uuid.NewString()[:8]
	srcTopicID := "work-" + suffix
	dltTopicID := "work-dlq-" + suffix
	workSubID := "sub-work-" + suffix
	dltSubID := "sub-dlq-" + suffix

	srcTopic, err := client.CreateTopic(ctx, srcTopicID)
	if err != nil {
		log.Fatalf("CreateTopic (source) %q: %v", srcTopicID, err)
	}
	defer srcTopic.Stop()
	dltTopic, err := client.CreateTopic(ctx, dltTopicID)
	if err != nil {
		log.Fatalf("CreateTopic (dead-letter) %q: %v", dltTopicID, err)
	}
	defer dltTopic.Stop()
	fmt.Printf("CreateTopic: %s (source) + %s (dead-letter)\n", srcTopic.ID(), dltTopic.ID())

	// Source subscription with a DeadLetterPolicy + tight RetryPolicy so each swept
	// (expired) lease re-arms and redelivers almost immediately — bounding each
	// expiry cycle by the 10 s minimum ack deadline.
	workSub, err := client.CreateSubscription(ctx, workSubID, pubsub.SubscriptionConfig{
		Topic:       srcTopic,
		AckDeadline: 10 * time.Second, // minimum; the unacked lease expires after this.
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     fmt.Sprintf("projects/%s/topics/%s", projectID, dltTopicID),
			MaxDeliveryAttempts: maxDeliveryAttempts,
		},
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: 10 * time.Millisecond,
			MaximumBackoff: 50 * time.Millisecond,
		},
	})
	if err != nil {
		log.Fatalf("CreateSubscription (source) %q: %v", workSubID, err)
	}
	fmt.Printf("CreateSubscription: %s (dead_letter_topic=%s, max_delivery_attempts=%d)\n",
		workSub.ID(), dltTopicID, maxDeliveryAttempts)

	dltSub, err := client.CreateSubscription(ctx, dltSubID, pubsub.SubscriptionConfig{
		Topic:       dltTopic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("CreateSubscription (dead-letter) %q: %v", dltSubID, err)
	}
	fmt.Printf("CreateSubscription: %s (dead-letter consumer)\n", dltSub.ID())

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, s := range []*pubsub.Subscription{dltSub, workSub} {
			if err := s.Delete(cleanupCtx); err != nil {
				log.Printf("warning: DeleteSubscription %s: %v", s.ID(), err)
			}
		}
		for _, t := range []*pubsub.Topic{dltTopic, srcTopic} {
			if err := t.Delete(cleanupCtx); err != nil {
				log.Printf("warning: DeleteTopic %s: %v", t.ID(), err)
			}
		}
		fmt.Println("cleanup: subscriptions + topics deleted")
	}()

	// Publish one poison message to the source topic.
	const poison = "poison"
	srcMsgID, err := srcTopic.Publish(ctx, &pubsub.Message{Data: []byte(poison)}).Get(ctx)
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Printf("Publish: messageId=%s body=%q (to source topic)\n", srcMsgID, poison)

	// Low-level client for the no-ack source pulls and the DLQ pull/ack.
	subClient, err := vkit.NewSubscriberClient(ctx, emulatorOptions()...)
	if err != nil {
		log.Fatalf("apiv1.NewSubscriberClient: %v", err)
	}
	defer func() { _ = subClient.Close() }()
	workPath := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, workSubID)
	dltPath := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, dltSubID)

	fmt.Println("Drive: pull the source WITHOUT acking (let leases expire) and poll the dead-letter sub:")
	start := time.Now()
	srcDeliveries := 0
	var dead string
	deadline := start.Add(90 * time.Second)
	for dead == "" && time.Now().Before(deadline) {
		// (1) Re-lease the source: a successful pull means the prior lease expired
		// and the sweeper re-armed it, raising the receive count toward the budget.
		src, err := subClient.Pull(ctx, &pubsubpb.PullRequest{Subscription: workPath, MaxMessages: 1})
		if err != nil {
			log.Fatalf("Pull (source): %v", err)
		}
		if len(src.GetReceivedMessages()) > 0 {
			srcDeliveries++
			da := src.GetReceivedMessages()[0].GetDeliveryAttempt()
			fmt.Printf("  source delivery #%d (DeliveryAttempt=%d) @ %.0fs — NOT acking, letting the lease expire\n",
				srcDeliveries, da, time.Since(start).Seconds())
			// Deliberately no Ack / no Nack — natural expiry drives the count.
		}

		// (2) Has it surfaced on the dead-letter subscription yet?
		dlt, err := subClient.Pull(ctx, &pubsubpb.PullRequest{Subscription: dltPath, MaxMessages: 1})
		if err != nil {
			log.Fatalf("Pull (dead-letter): %v", err)
		}
		if len(dlt.GetReceivedMessages()) > 0 {
			rm := dlt.GetReceivedMessages()[0]
			dead = string(rm.GetMessage().GetData())
			if err := subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: dltPath, AckIds: []string{rm.GetAckId()},
			}); err != nil {
				log.Fatalf("Acknowledge (dead-letter): %v", err)
			}
			break
		}
		time.Sleep(1 * time.Second)
	}

	if dead == "" {
		log.Fatalf("FAIL: poison message not dead-lettered within 90s (source deliveries=%d)", srcDeliveries)
	}
	fmt.Printf("Dead-letter: received %q after %d source deliveries (~%.0fs)\n",
		dead, srcDeliveries, time.Since(start).Seconds())
	if dead != poison {
		log.Fatalf("FAIL: dead-letter body %q != published %q", dead, poison)
	}
	fmt.Printf("PASS: poison dead-lettered to %q once receive count exceeded max_delivery_attempts=%d\n",
		dltTopicID, maxDeliveryAttempts)
}
