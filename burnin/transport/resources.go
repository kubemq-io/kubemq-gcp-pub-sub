// Package transport wraps the Google Cloud Pub/Sub client (connecting via the
// standard PUBSUB_EMULATOR_HOST env var, zero-auth insecure gRPC) and the native
// kubemq-go/v2 Events Store client (interop worker only) for the burn-in harness.
// resources.go owns the naming grammar: topic/subscription names and the KubeMQ
// channel they map to (topic {t} -> Events Store log pubsub.{t}; subscription {s}
// -> Queue pubsub.sub.{s}).
package transport

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BrokerEnv is the burn-in broker address env var (the Pub/Sub emulator host).
// It is mirrored to PUBSUB_EMULATOR_HOST so the Google SDK connects insecurely.
const BrokerEnv = "KUBEMQ_BROKER_ADDRESS"

// EmulatorEnv is the standard Google env var the Pub/Sub SDK honours directly.
const EmulatorEnv = "PUBSUB_EMULATOR_HOST"

// GRPCEnv is the native gRPC address env var used by the interop worker.
const GRPCEnv = "KUBEMQ_GRPC_ADDRESS"

// DefaultBroker is the default host:port for the Pub/Sub connector gRPC listener
// (the emulator convention port 8085).
const DefaultBroker = "localhost:8085"

// DefaultGRPC is the default host:port for the native KubeMQ gRPC listener.
const DefaultGRPC = "localhost:50000"

// EmulatorHost resolves the Pub/Sub emulator host the SDK should connect to:
// the supplied emulatorHost (gcp.emulator_host), then the supplied broker
// address (broker.address), then PUBSUB_EMULATOR_HOST / KUBEMQ_BROKER_ADDRESS,
// then DefaultBroker. The connector speaks plain insecure gRPC, so there is no
// scheme — this is a bare host:port, exactly what PUBSUB_EMULATOR_HOST expects.
func EmulatorHost(emulatorHost, address string) string {
	if emulatorHost != "" {
		return emulatorHost
	}
	if address != "" {
		return address
	}
	if v := os.Getenv(EmulatorEnv); v != "" {
		return v
	}
	if v := os.Getenv(BrokerEnv); v != "" {
		return v
	}
	return DefaultBroker
}

// GRPCAddress resolves the native gRPC address: the supplied value, then
// KUBEMQ_GRPC_ADDRESS, then DefaultGRPC.
func GRPCAddress(address string) string {
	if address != "" {
		return address
	}
	if v := os.Getenv(GRPCEnv); v != "" {
		return v
	}
	return DefaultGRPC
}

// ResourcePrefixEnv overrides the resource-name prefix so concurrent burn-in
// agents (one per language) sharing the SAME stateful connector do not collide
// on the global canonical channels (the project segment is ignored, so resource
// ids are global). Default "gcp_burnin"; an agent sets it to e.g.
// "gcp_burnin_go". The prefix flows through every topic/subscription name and
// thus the mapped KubeMQ channel (pubsub.{t} / pubsub.sub.{s}), so the interop
// worker's native side stays consistent automatically.
const ResourcePrefixEnv = "BURNIN_RESOURCE_PREFIX"

func resourcePrefix() string {
	if v := os.Getenv(ResourcePrefixEnv); v != "" {
		return v
	}
	return "gcp_burnin"
}

// TopicName builds the Pub/Sub topic id for a burn-in worker channel. The
// connector maps topic {t} to the KubeMQ Events Store channel pubsub.{t}; the
// id follows the {prefix}_{worker}_{idx:04d} grammar so the mapped channel
// becomes pubsub.{prefix}_{worker}_{idx:04d}. Topic ids must satisfy the
// resource-id rules (3..255 chars, start with a letter, no "goog" prefix, and
// must NOT start with "sub.").
func TopicName(worker string, idx int) string {
	return fmt.Sprintf("%s_%s_%04d", resourcePrefix(), worker, idx)
}

// SubscriptionName builds the primary subscription id for a worker channel. The
// connector maps subscription {s} to the KubeMQ Queue channel pubsub.sub.{s}.
func SubscriptionName(worker string, idx int) string {
	return fmt.Sprintf("%s_%s_%04d_sub", resourcePrefix(), worker, idx)
}

// FanoutSubscriptionName builds a per-subscriber subscription id for the fan_out
// worker (one topic, N subscriptions).
func FanoutSubscriptionName(worker string, idx, sub int) string {
	return fmt.Sprintf("%s_%s_%04d_s%02d", resourcePrefix(), worker, idx, sub)
}

// SnapshotName builds the snapshot id for a worker channel (seek_snapshot
// worker). It is resourcePrefix()-scoped so snapshots are prefix-namespaced and
// uniquely cleaned up per run, avoiding the shared-channel false-fail (spec S5.5).
func SnapshotName(worker string, idx int) string {
	return fmt.Sprintf("%s_%s_%04d_snap", resourcePrefix(), worker, idx)
}

// SchemaName builds the schema id for a worker channel (schema_enforce worker).
// It is resourcePrefix()-scoped so schemas are prefix-namespaced and uniquely
// cleaned up per run, avoiding the shared-channel false-fail (spec S5.4).
func SchemaName(worker string, idx int) string {
	return fmt.Sprintf("%s_%s_%04d_schema", resourcePrefix(), worker, idx)
}

// DeadLetterTopicName builds the dead-letter topic id for a worker channel.
func DeadLetterTopicName(worker string, idx int) string {
	return fmt.Sprintf("%s_%s_%04d_dlt", resourcePrefix(), worker, idx)
}

// DeadLetterSubscriptionName builds the subscription that drains the DLQ topic.
func DeadLetterSubscriptionName(worker string, idx int) string {
	return fmt.Sprintf("%s_%s_%04d_dlt_sub", resourcePrefix(), worker, idx)
}

// MappedTopicChannel returns the KubeMQ Events Store channel a topic resolves to
// (for logging and the interop worker's native side): pubsub.{t}.
func MappedTopicChannel(topic string) string {
	return fmt.Sprintf("pubsub.%s", topic)
}

// MappedSubscriptionChannel returns the KubeMQ Queue channel a subscription
// resolves to: pubsub.sub.{s}.
func MappedSubscriptionChannel(sub string) string {
	return fmt.Sprintf("pubsub.sub.%s", sub)
}

// IsAlreadyExists reports whether err is a gRPC AlreadyExists status. Burn-in
// resource ids are prefix-namespaced and STABLE across re-runs (not
// uuid-suffixed), so a re-run / a competing channel may find the topic or
// subscription already present; provisioning is idempotent and tolerates this.
func IsAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

// IsNotFound reports whether err is a gRPC NotFound status. Cleanup helpers
// tolerate this so deleting an absent resource (the connector returns NotFound
// when a snapshot/schema is missing — subscriber.go DeleteSnapshot / schema.go
// DeleteSchema map registry.ErrNotFound to codes.NotFound) is an idempotent
// no-op.
func IsNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

// EnsureTopic returns a publisher handle for topicID, creating the topic if it
// does not already exist (idempotent: AlreadyExists is not an error). The caller
// owns topic.Stop().
func EnsureTopic(ctx context.Context, client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		if !IsAlreadyExists(err) {
			return nil, fmt.Errorf("create topic %q: %w", topicID, err)
		}
		topic = client.Topic(topicID)
	}
	return topic, nil
}

// FilteredSubscriptionConfig builds a SubscriptionConfig for topic with a
// CEL-subset attribute Filter (spec REQ-003 / S3.3). The connector compiles the
// filter once at CreateSubscription (immutable) and applies it per-subscription
// at publish fan-out: non-matching messages are auto-acked / never enqueued. The
// filter expression must be <= 256 chars; an empty filter matches everything; a
// malformed filter is rejected with codes.InvalidArgument at create time.
func FilteredSubscriptionConfig(topic *pubsub.Topic, ackDeadlineSeconds int, filter string) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: AckDeadline(ackDeadlineSeconds),
		Filter:      filter,
	}
}

// EnsureSubscription returns a subscription handle for subID, creating it with
// cfg if it does not already exist (idempotent: AlreadyExists is not an error).
func EnsureSubscription(ctx context.Context, client *pubsub.Client, subID string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	sub, err := client.CreateSubscription(ctx, subID, cfg)
	if err != nil {
		if !IsAlreadyExists(err) {
			return nil, fmt.Errorf("create subscription %q: %w", subID, err)
		}
		sub = client.Subscription(subID)
	}
	return sub, nil
}
