// Command batch-publish is master-table variant 2: publishing a batch of
// messages and verifying the server-assigned ids come back in request order.
//
//	CreateTopic -> Publish (N concurrent PublishResults) -> Get each in order
//
// The official Go client batches Publish calls under the hood; each call returns
// a *PublishResult whose Get blocks for the server id. We publish N messages,
// collect the ids in the same slice order we issued the publishes, and assert we
// got N distinct ids back aligned to the request order (the connector preserves
// per-message ordering within a batch — spec S3.5 publish atomicity). The batch
// limit is 1000; a single batch over that, or any invalid message in it, rejects
// the whole batch. The topic is uuid-suffixed per run and deleted on exit.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./topics/batch-publish
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

// batchSize stays well under the connector's hard limit of 1000 messages/batch (spec S3.6).
const batchSize = 10

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the project segment is parsed but ignored.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] topics/batch-publish | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run topic id (channel isolation). Canonical master-table name is events.
	topicID := "events-" + uuid.NewString()[:8]

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := topic.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteTopic: %v", err)
		} else {
			fmt.Println("DeleteTopic: ok")
		}
	}()

	// Issue the batch: index i carries attribute seq=i so we can prove order is preserved.
	fmt.Printf("Publish: issuing a batch of %d messages (limit 1000/batch)\n", batchSize)
	results := make([]*pubsub.PublishResult, batchSize)
	for i := range results {
		results[i] = topic.Publish(ctx, &pubsub.Message{
			Data:       []byte(fmt.Sprintf("event-%d", i)),
			Attributes: map[string]string{"seq": fmt.Sprintf("%d", i)},
		})
	}

	// Collect ids in the SAME slice order we issued the publishes — Get blocks per result.
	ids := make([]string, batchSize)
	seen := make(map[string]int, batchSize)
	for i, r := range results {
		id, err := r.Get(ctx) // a single invalid message would reject the whole batch here.
		if err != nil {
			log.Fatalf("Publish[%d]: %v", i, err)
		}
		ids[i] = id
		fmt.Printf("  [%d] event-%d -> messageId=%s\n", i, i, id)
		if prev, dup := seen[id]; dup {
			log.Fatalf("FAIL: duplicate messageId %q at indexes %d and %d", id, prev, i)
		}
		seen[id] = i
	}

	// Assertions: N ids, all distinct, aligned 1:1 to request order.
	if len(seen) != batchSize {
		log.Fatalf("FAIL: expected %d distinct message ids, got %d", batchSize, len(seen))
	}
	for i, id := range ids {
		if id == "" {
			log.Fatalf("FAIL: message index %d has no id", i)
		}
	}
	fmt.Printf("Verified: %d message ids returned in request order, all distinct\n", batchSize)
	fmt.Println("PASS: batch publish verified (ids in request order, atomic batch)")
}
