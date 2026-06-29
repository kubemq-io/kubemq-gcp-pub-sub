// Command ordering-keys is master-table variant 6: per-key in-order delivery
// against the KubeMQ Pub/Sub connector.
//
//	CreateTopic(EnableMessageOrdering) -> CreateSubscription(EnableMessageOrdering)
//	-> Publish three messages per ordering key (interleaved) + one keyless
//	-> unary Pull (one in flight per key) -> Acknowledge -> assert per-key order
//
// With enable_message_ordering, messages that share an ordering_key are delivered
// in publish order with AT MOST ONE in flight per key (spec S3.5); keyless
// messages are unordered. The key rides across the wire as the reserved tag
// _pubsub_ordering_key, which the connector strips from `attributes` and surfaces
// as Message.OrderingKey for Pub/Sub clients.
//
// Why unary Pull (apiv1.SubscriberClient) instead of the high-level Receive loop:
// pulling one message at a time and acking it before the next pull makes the
// "head-of-key blocks until acked" guarantee directly observable; the high-level
// streaming subscriber buffers and acks asynchronously, obscuring the strict
// per-key sequencing. The low-level apiv1 client does NOT auto-detect
// PUBSUB_EMULATOR_HOST, so this program wires the insecure/no-auth options
// explicitly (the high-level pubsub.NewClient auto-detects it).
//
// Resource ids are uuid-suffixed per run so concurrent runs use distinct
// `pubsub.{t}` / `pubsub.sub.{s}` channels and never cross-talk; both are deleted
// on exit (best-effort). Any out-of-order delivery exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./delivery/ordering-keys
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	vkit "cloud.google.com/go/pubsub/apiv1"
)

// emulatorOptions wires the low-level apiv1 client at the connector exactly the
// way the high-level pubsub.NewClient does when PUBSUB_EMULATOR_HOST is set:
// insecure gRPC, credentials cleared, no Google auth. Without this the bare
// apiv1 client would attempt real Google auth and fail.
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
	fmt.Printf("[kubemq-gcp-pub-sub] delivery/ordering-keys | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// High-level client (auto-detects PUBSUB_EMULATOR_HOST) for admin + publish.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical names: ordered / sub-ordered.
	suffix := uuid.NewString()[:8]
	topicID := "ordered-" + suffix
	subID := "sub-ordered-" + suffix

	// CreateTopic, then enable ordering on the publisher HANDLE. Ordering is a
	// publisher-side flag (it serializes publishes per key); the subscription must
	// also enable it (below) — enabling it on only one side is not enough.
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	topic.EnableMessageOrdering = true // publisher serializes per-key publishes.
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s, EnableMessageOrdering=true)\n", topic.ID(), topicID)

	// CreateSubscription with ordering enabled.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:                 topic,
		AckDeadline:           10 * time.Second,
		EnableMessageOrdering: true,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (channel pubsub.sub.%s, EnableMessageOrdering=true)\n", sub.ID(), subID)

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

	// Two independent ordering keys, interleaved, plus one keyless message.
	const keyA, keyB = "cust-7", "cust-9"
	plan := []struct{ key, body string }{
		{keyA, "A1"},
		{keyB, "B1"},
		{keyA, "A2"},
		{"", "keyless"},
		{keyB, "B2"},
		{keyA, "A3"},
		{keyB, "B3"},
	}

	// Publish: on the same ordering key the client serializes, so we await each
	// result in turn to preserve the interleaved publish order across keys.
	fmt.Println("Publish (publisher ordering enabled; interleaved across two keys):")
	for _, p := range plan {
		m := &pubsub.Message{Data: []byte(p.body)}
		if p.key != "" {
			m.OrderingKey = p.key
		}
		id, err := topic.Publish(ctx, m).Get(ctx)
		if err != nil {
			log.Fatalf("Publish body=%q key=%q: %v", p.body, p.key, err)
		}
		shown := p.key
		if shown == "" {
			shown = "(keyless)"
		}
		fmt.Printf("  publish body=%-9q ordering_key=%-10s -> id=%s\n", p.body, shown, id)
	}

	// Pull via the low-level apiv1 client: one message at a time, ack before the
	// next pull, so the connector releases the next per-key message in order.
	subClient, err := vkit.NewSubscriberClient(ctx, emulatorOptions()...)
	if err != nil {
		log.Fatalf("apiv1.NewSubscriberClient: %v", err)
	}
	defer func() { _ = subClient.Close() }()
	subPath := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)

	perKey := map[string][]string{keyA: {}, keyB: {}, "": {}}
	want := len(plan)
	received := 0

	fmt.Println("Pull (one in flight per key; ack drives the next per-key message):")
	deadline := time.Now().Add(30 * time.Second)
	for received < want && time.Now().Before(deadline) {
		resp, err := subClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: subPath,
			MaxMessages:  1,
		})
		if err != nil {
			log.Fatalf("Pull: %v", err)
		}
		if len(resp.GetReceivedMessages()) == 0 {
			continue
		}
		rm := resp.GetReceivedMessages()[0]
		key := rm.GetMessage().GetOrderingKey() // reserved _pubsub_ordering_key, surfaced here.
		body := string(rm.GetMessage().GetData())
		perKey[key] = append(perKey[key], body)
		received++
		shown := key
		if shown == "" {
			shown = "(keyless)"
		}
		fmt.Printf("  received body=%-9q ordering_key=%s\n", body, shown)
		if err := subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
			Subscription: subPath,
			AckIds:       []string{rm.GetAckId()},
		}); err != nil {
			log.Fatalf("Acknowledge: %v", err)
		}
	}

	fmt.Println("Per-key delivery order:")
	fmt.Printf("  %s: %v\n", keyA, perKey[keyA])
	fmt.Printf("  %s: %v\n", keyB, perKey[keyB])
	fmt.Printf("  keyless: %v\n", perKey[""])

	// Assertions: each key delivered in publish order; every message exactly once.
	if !reflect.DeepEqual(perKey[keyA], []string{"A1", "A2", "A3"}) {
		log.Fatalf("FAIL: key %q out of order: got %v, want [A1 A2 A3]", keyA, perKey[keyA])
	}
	if !reflect.DeepEqual(perKey[keyB], []string{"B1", "B2", "B3"}) {
		log.Fatalf("FAIL: key %q out of order: got %v, want [B1 B2 B3]", keyB, perKey[keyB])
	}
	if received != want {
		log.Fatalf("FAIL: expected %d messages, received %d", want, received)
	}
	fmt.Println("PASS: per-key in-order delivery, one in flight per key (A1->A2->A3, B1->B2->B3)")
}
