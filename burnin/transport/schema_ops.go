package transport

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// schema_ops.go holds the schema-validation transport operations (spec S6.1;
// connector SchemaService per kubemq-server docs/20-google-pubsub.md "Schema
// Validation (M3)" — Avro via hamba/avro and Protobuf via jhump/protoreflect).
// A topic may reference a schema via schema_settings; when it does, publish is
// ENFORCED — every message in a batch is validated and the whole batch is
// rejected with INVALID_ARGUMENT on the first non-conforming message. These
// helpers build the SchemaClient against the connector (same emulator dial path
// as NewPubSubClient), register Avro/Protobuf schemas, and provision a topic
// bound to a schema. They are the burn-in transport surface for the schema
// concept (the per-language examples exercise the same RPCs idiomatically).

// NewSchemaClient builds a cloud.google.com/go/pubsub SchemaClient pointed at
// the connector (insecure gRPC, no auth — identical dial contract to
// NewPubSubClient). The SchemaService backs CreateSchema/ValidateMessage/etc.
// The caller owns Close().
func NewSchemaClient(ctx context.Context, cfg PubSubConfig) (*pubsub.SchemaClient, error) {
	host := EmulatorHost(cfg.EmulatorHost, "")
	projectID := cfg.ProjectID
	if projectID == "" {
		projectID = "burnin"
	}
	opts := []option.ClientOption{
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	client, err := pubsub.NewSchemaClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("create schema client (emulator %s): %w", host, err)
	}
	return client, nil
}

// CreateAvroSchema registers an Avro schema definition under schemaID
// (idempotent: AlreadyExists is tolerated so a re-run reuses the registered
// schema). definition is the full Avro schema JSON. It returns the registered
// schema config (nil on the AlreadyExists path).
func CreateAvroSchema(ctx context.Context, client *pubsub.SchemaClient, schemaID, definition string) (*pubsub.SchemaConfig, error) {
	return createSchema(ctx, client, schemaID, pubsub.SchemaAvro, definition)
}

// CreateProtobufSchema registers a Protobuf schema definition under schemaID
// (idempotent: AlreadyExists is tolerated). definition is the full .proto
// source. It returns the registered schema config (nil on the AlreadyExists
// path).
func CreateProtobufSchema(ctx context.Context, client *pubsub.SchemaClient, schemaID, definition string) (*pubsub.SchemaConfig, error) {
	return createSchema(ctx, client, schemaID, pubsub.SchemaProtocolBuffer, definition)
}

func createSchema(ctx context.Context, client *pubsub.SchemaClient, schemaID string, t pubsub.SchemaType, definition string) (*pubsub.SchemaConfig, error) {
	sc, err := client.CreateSchema(ctx, schemaID, pubsub.SchemaConfig{
		Type:       t,
		Definition: definition,
	})
	if err != nil {
		if IsAlreadyExists(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("create schema %q: %w", schemaID, err)
	}
	return sc, nil
}

// DeleteSchema removes a registered schema (cleanup utility; idempotent — a
// missing schema is not an error).
func DeleteSchema(ctx context.Context, client *pubsub.SchemaClient, schemaID string) error {
	if err := client.DeleteSchema(ctx, schemaID); err != nil {
		if IsNotFound(err) || IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("delete schema %q: %w", schemaID, err)
	}
	return nil
}

// SchemaSettings builds the topic schema_settings binding a topic to a
// registered schema with the given message encoding (JSON or BINARY). projectID
// must match the SchemaClient's project so the connector resolves the schema
// resource name. Use BuildSchemaSettings with EncodingJSON for the default
// burn-in (human-debuggable) path.
func SchemaSettings(projectID, schemaID string, encoding pubsub.SchemaEncoding) *pubsub.SchemaSettings {
	if projectID == "" {
		projectID = "burnin"
	}
	return &pubsub.SchemaSettings{
		Schema:   fmt.Sprintf("projects/%s/schemas/%s", projectID, schemaID),
		Encoding: encoding,
	}
}

// EnsureTopicWithSchema returns a publisher handle for topicID bound to
// schemaSettings, creating it if it does not already exist (idempotent:
// AlreadyExists reuses the existing topic). When the topic has schema settings
// the connector enforces validation on every publish. The caller owns
// topic.Stop().
func EnsureTopicWithSchema(ctx context.Context, client *pubsub.Client, topicID string, schemaSettings *pubsub.SchemaSettings) (*pubsub.Topic, error) {
	topic, err := client.CreateTopicWithConfig(ctx, topicID, &pubsub.TopicConfig{
		SchemaSettings: schemaSettings,
	})
	if err != nil {
		if !IsAlreadyExists(err) {
			return nil, fmt.Errorf("create topic %q with schema: %w", topicID, err)
		}
		topic = client.Topic(topicID)
	}
	return topic, nil
}

// ValidateMessage validates a candidate message body against a registered
// schema id without publishing (the connector's ValidateMessage RPC). It
// returns nil if the message conforms; a non-nil error (INVALID_ARGUMENT) if it
// does not. Useful for asserting the connector's enforce-on-publish contract in
// a controlled way before driving load.
func ValidateMessage(ctx context.Context, client *pubsub.SchemaClient, schemaID string, encoding pubsub.SchemaEncoding, msg []byte) error {
	if _, err := client.ValidateMessageWithID(ctx, msg, encoding, schemaID); err != nil {
		return fmt.Errorf("validate message against schema %q: %w", schemaID, err)
	}
	return nil
}
