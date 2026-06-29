// Command topic-management is master-table variant 3: the topic administrative
// surface against the KubeMQ Pub/Sub connector.
//
//	CreateTopic -> ListTopics (find ours) -> GetTopic (Config) -> UpdateTopic (labels)
//	-> CreateSubscription -> ListTopicSubscriptions -> DeleteTopic (tombstone)
//
// It creates a topic and a subscription on it, confirms the topic appears in the
// project-wide topic list, reads its config, mutates its labels via UpdateTopic
// and asserts the mutation stuck, lists the subscriptions attached to the topic,
// then deletes the topic and asserts the delete is a tombstone (Exists -> false).
// All ids are uuid-suffixed per run and cleaned up on exit. Any mismatch exits
// non-zero.
//
// Note (spec S3.6): topic ids may not start with "sub." (reserved broker
// namespace), and the requested RetentionDuration is clamped to the broker's
// Store.MaxRetention — GetTopic returns the requested value while fan-out/seek use
// the clamped value.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./topics/topic-management
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the project segment is parsed but ignored.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] topics/topic-management | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical names: managed / sub-managed.
	suffix := uuid.NewString()[:8]
	topicID := "managed-" + suffix
	subID := "sub-managed-" + suffix

	// CreateTopic with a retention duration (10 min .. 31 days valid range).
	topic, err := client.CreateTopicWithConfig(ctx, topicID, &pubsub.TopicConfig{
		Labels:            map[string]string{"team": "orders", "stage": "demo"},
		RetentionDuration: 30 * time.Minute,
	})
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// CreateSubscription so ListTopicSubscriptions has something to return.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (channel pubsub.sub.%s)\n", sub.ID(), subID)

	// topicDeleted records whether the happy-path DeleteTopic below already ran, so the
	// best-effort cleanup does not re-delete it (which would log a spurious not-found).
	topicDeleted := false
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		// The sub may outlive the topic (detached), so always clean it up here.
		if err := sub.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteSubscription: %v", err)
		}
		if !topicDeleted {
			if err := topic.Delete(cleanupCtx); err != nil {
				log.Printf("warning: DeleteTopic: %v", err)
			}
		}
	}()

	// ListTopics — confirm our topic is in the project-wide list.
	found := false
	tit := client.Topics(ctx)
	for {
		t, err := tit.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Fatalf("ListTopics: %v", err)
		}
		if t.ID() == topicID {
			found = true
		}
	}
	if !found {
		log.Fatalf("FAIL: created topic %q not found in ListTopics", topicID)
	}
	fmt.Printf("ListTopics: found %s\n", topicID)

	// GetTopic — read the config back.
	cfg, err := topic.Config(ctx)
	if err != nil {
		log.Fatalf("GetTopic (Config): %v", err)
	}
	fmt.Printf("GetTopic: labels=%v retention=%s\n", cfg.Labels, durationOf(cfg.RetentionDuration))
	if cfg.Labels["team"] != "orders" {
		log.Fatalf("FAIL: GetTopic label team=%q, expected orders", cfg.Labels["team"])
	}

	// UpdateTopic — replace the label set, assert the mutation stuck.
	updated, err := topic.Update(ctx, pubsub.TopicConfigToUpdate{
		Labels: map[string]string{"team": "orders", "stage": "production"},
	})
	if err != nil {
		log.Fatalf("UpdateTopic: %v", err)
	}
	fmt.Printf("UpdateTopic: labels=%v\n", updated.Labels)
	if updated.Labels["stage"] != "production" {
		log.Fatalf("FAIL: UpdateTopic stage=%q, expected production", updated.Labels["stage"])
	}

	// ListTopicSubscriptions — the sub we created must be attached to this topic.
	subCount := 0
	subFound := false
	sit := topic.Subscriptions(ctx)
	for {
		s, err := sit.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Fatalf("ListTopicSubscriptions: %v", err)
		}
		subCount++
		if s.ID() == subID {
			subFound = true
		}
	}
	if !subFound {
		log.Fatalf("FAIL: subscription %q not found among the topic's %d subscriptions", subID, subCount)
	}
	fmt.Printf("ListTopicSubscriptions: found %s (%d total on topic)\n", subID, subCount)

	// DeleteTopic — tombstone: the topic no longer Exists.
	if err := topic.Delete(ctx); err != nil {
		log.Fatalf("DeleteTopic: %v", err)
	}
	topicDeleted = true
	exists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatalf("Exists after DeleteTopic: %v", err)
	}
	if exists {
		log.Fatalf("FAIL: topic %q still exists after DeleteTopic (expected tombstone)", topicID)
	}
	fmt.Printf("DeleteTopic: ok (tombstone — Exists=false)\n")

	fmt.Println("PASS: topic management verified (list/get/update/list-subs/delete-tombstone)")
}

// durationOf renders the optional.Duration (interface{}) RetentionDuration field.
func durationOf(d any) string {
	if dur, ok := d.(time.Duration); ok {
		return dur.String()
	}
	return "unset"
}
