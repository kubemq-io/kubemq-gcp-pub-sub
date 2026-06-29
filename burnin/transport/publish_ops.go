package transport

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

// publish_ops.go holds the topic-publish transport operations the burn-in
// workers share (mirrors the AWS sibling's transport/sns_ops.go Publish split,
// spec S6.1). The cloud.google.com/go/pubsub publisher is asynchronous —
// Topic.Publish enqueues into a batching buffer and returns a PublishResult; the
// publish is only durable once Result.Get resolves the server-assigned id. These
// helpers wrap that enqueue→await round trip so every worker performs an
// identical synchronous publish (the burn-in tracker counts a "sent" only after
// the server acknowledges the message id).

// PublishSync publishes a single message and blocks until the connector assigns
// (and returns) its server-side id, or the context is cancelled. body is the
// message payload and attrs the instrumentation attributes carried on every
// burn-in publish. It returns the assigned message id on success.
func PublishSync(ctx context.Context, topic *pubsub.Topic, body []byte, attrs map[string]string) (string, error) {
	return PublishMessageSync(ctx, topic, &pubsub.Message{
		Data:       body,
		Attributes: attrs,
	})
}

// PublishMessageSync publishes a pre-built message and blocks until the
// connector assigns its id, or ctx is cancelled. It is used directly when the
// caller needs to set fields beyond Data/Attributes (e.g. an OrderingKey — see
// ordering_ops.go). It returns the assigned message id on success.
func PublishMessageSync(ctx context.Context, topic *pubsub.Topic, msg *pubsub.Message) (string, error) {
	result := topic.Publish(ctx, msg)
	id, err := result.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("publish: %w", err)
	}
	return id, nil
}

// PublishBatch publishes a batch of messages atomically and blocks until every
// per-message PublishResult resolves (spec REQ-002 / S3.2). The connector
// validates the entire batch before any enqueue and rejects the WHOLE batch with
// codes.InvalidArgument on the first failing message (zero partial enqueue), so a
// batch-level failure surfaces as a non-nil error from at least one Result.Get.
//
// All messages are enqueued first (Topic.Publish buffers them), then each result
// is awaited; on the first error the assigned ids gathered so far are discarded
// and the error is returned. When len(msgs) == 1 the behavior is identical to a
// single PublishMessageSync round-trip (backward compatible). The returned slice
// holds the server-assigned ids in msgs order on success.
func PublishBatch(ctx context.Context, topic *pubsub.Topic, msgs []*pubsub.Message) ([]string, error) {
	results := make([]*pubsub.PublishResult, len(msgs))
	for i, m := range msgs {
		results[i] = topic.Publish(ctx, m)
	}
	ids := make([]string, len(msgs))
	for i, r := range results {
		id, err := r.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("publish batch (msg %d/%d): %w", i+1, len(msgs), err)
		}
		ids[i] = id
	}
	return ids, nil
}
