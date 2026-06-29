// Command schema-protobuf is master-table variant 14: registering a Protobuf
// schema, binding a topic to it, and proving enforce-on-publish.
//
//	CreateSchema(Protobuf) -> CreateTopic(schema_settings, JSON) -> CreateSubscription
//	                       -> Publish(conforming)        -> ok
//	                       -> Publish(batch w/ 1 invalid) -> whole batch rejected INVALID_ARGUMENT
//	                       -> Pull (only the conforming message is in the log).
//
// When a topic references a schema the connector enforces it on publish: validation is
// part of the atomic batch check, so the WHOLE batch is rejected (INVALID_ARGUMENT) on
// the first non-conforming message and nothing is enqueued (spec S3.5 schema, S3.5
// publish atomicity). Schemas are registry records with a server-assigned revision id
// and full revision history; definitions are <= 300 KB. The connector parses the proto3
// definition (via jhump/protoreflect) and validates each message's JSON-encoded data.
//
// Uses ONLY the official cloud.google.com/go/pubsub client. NOTE: pubsub.NewSchemaClient
// does NOT auto-detect PUBSUB_EMULATOR_HOST (unlike pubsub.NewClient), so this example
// passes the same emulator options to it explicitly (emulatorOpts) — the Go analog of
// the C# "SchemaServiceClientBuilder has no EmulatorDetection setter" gotcha. Resource
// ids are uuid-suffixed per run so concurrent runs use distinct `pubsub.{t}` /
// `pubsub.sub.{s}` channels and the schema registry records never collide; resources are
// deleted on exit (best-effort). Any failed assertion exits non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./advanced/schema-protobuf
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

// protobufDefinition is a proto3 message the connector validates each message against:
// a string order_id and a double amount.
const protobufDefinition = `syntax = "proto3";
message Order {
  string order_id = 1;
  double amount = 2;
}
`

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the connector parses but ignores the project segment.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] advanced/schema-protobuf | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// pubsub.NewSchemaClient does not honour PUBSUB_EMULATOR_HOST on its own; pass the
	// emulator options explicitly so it dials the connector insecurely with no auth.
	schemaClient, err := pubsub.NewSchemaClient(ctx, projectID, emulatorOpts()...)
	if err != nil {
		log.Fatalf("pubsub.NewSchemaClient: %v", err)
	}
	// NOTE: pubsub.SchemaClient exposes no Close(); the process exit reclaims its conn.

	subClient, err := vkit.NewSubscriberClient(ctx, emulatorOpts()...)
	if err != nil {
		log.Fatalf("vkit.NewSubscriberClient: %v", err)
	}
	defer func() { _ = subClient.Close() }()

	// Unique-per-run resource ids (channel isolation).
	suffix := uuid.NewString()[:8]
	schemaID := "proto-schema-" + suffix
	topicID := "proto-topic-" + suffix
	subID := "sub-proto-" + suffix

	// CreateSchema(Protobuf) -> registry record with a server-assigned revision id.
	created, err := schemaClient.CreateSchema(ctx, schemaID, pubsub.SchemaConfig{
		Type:       pubsub.SchemaProtocolBuffer,
		Definition: protobufDefinition,
	})
	if err != nil {
		log.Fatalf("CreateSchema(Protobuf) %q: %v", schemaID, err)
	}
	fmt.Printf("CreateSchema(Protobuf): %s revision=%s\n", schemaID, created.RevisionID)
	check(created.RevisionID != "", "schema created with a server-assigned revision id")

	// CreateTopic with schema_settings referencing the schema; JSON encoding means each
	// message's data is validated as JSON against the proto3 message.
	topic, err := client.CreateTopicWithConfig(ctx, topicID, &pubsub.TopicConfig{
		SchemaSettings: &pubsub.SchemaSettings{
			Schema:   fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
			Encoding: pubsub.EncodingJSON,
		},
	})
	if err != nil {
		log.Fatalf("CreateTopic %q (schema_settings): %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s, schema_settings -> %s, encoding=JSON)\n",
		topic.ID(), topicID, schemaID)

	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("CreateSubscription %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (channel pubsub.sub.%s)\n", sub.ID(), subID)
	subName := sub.String()

	defer cleanup(schemaClient, schemaID, sub, topic)

	// Conforming publish: matches the proto3 message (string order_id, double amount).
	conforming := []byte(`{"order_id":"P-1","amount":99.0}`)
	msgID, err := topic.Publish(ctx, &pubsub.Message{Data: conforming}).Get(ctx)
	if err != nil {
		log.Fatalf("Publish(conforming): %v", err)
	}
	fmt.Printf("Publish(conforming): messageId=%s\n", msgID)
	check(msgID != "", "conforming message accepted (message-id returned)")

	// Non-conforming batch: a valid message followed by one whose amount is a string,
	// not a double. Publish is atomic, so the WHOLE batch is rejected and nothing in it
	// is enqueued. The two PublishResults share one underlying batch; both Get the error.
	good := topic.Publish(ctx, &pubsub.Message{Data: []byte(`{"order_id":"P-2","amount":5.0}`)})
	bad := topic.Publish(ctx, &pubsub.Message{Data: []byte(`{"order_id":"P-3","amount":"not-a-number"}`)})
	topic.Flush() // force the two messages out as one server-side batch.
	_, goodErr := good.Get(ctx)
	_, badErr := bad.Get(ctx)
	fmt.Println("Publish(batch w/ 1 invalid) -> schema-invalid msg → batch rejected INVALID_ARGUMENT")
	check(status.Code(badErr) == codes.InvalidArgument,
		"non-conforming message rejected with INVALID_ARGUMENT")
	check(goodErr != nil,
		"the conforming sibling in the rejected batch was NOT enqueued (atomic batch)")

	// Only the single conforming message published earlier reached the topic log.
	delivered := drain(ctx, subClient, subName)
	fmt.Printf("Pull -> %v\n", delivered)
	check(equal(delivered, []string{`{"order_id":"P-1","amount":99.0}`}),
		"only the conforming message was enqueued (rejected batch enqueues nothing)")

	fmt.Println("PASS: Protobuf schema enforced on publish — conforming ok, non-conforming batch rejected")
}

// drain unary-Pulls + Acknowledges everything currently available and returns the
// message bodies in delivery order, stopping after two consecutive empty Pulls.
func drain(ctx context.Context, subClient *vkit.SubscriberClient, subName string) []string {
	var bodies []string
	emptyStreak := 0
	deadline := time.Now().Add(12 * time.Second)
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
// schema and subscriber clients, neither of which auto-detects the emulator env var.
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

// cleanup deletes the subscription, topic, and schema (best-effort) so re-runs start clean.
func cleanup(schemaClient *pubsub.SchemaClient, schemaID string, sub *pubsub.Subscription, topic *pubsub.Topic) {
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
	if err := schemaClient.DeleteSchema(ctx, schemaID); err != nil {
		log.Printf("warning: DeleteSchema: %v", err)
	} else {
		fmt.Println("DeleteSchema: ok")
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
