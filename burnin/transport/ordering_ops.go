package transport

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// ordering_ops.go holds the ordering-key publish/consume transport operations
// (spec S6.1; connector ordering semantics per kubemq-server
// docs/20-google-pubsub.md "ordering.go"). Ordering is a two-sided contract:
// EnableMessageOrdering must be set on BOTH the publisher (Topic) and the
// subscription, otherwise per-key in-order delivery is not guaranteed. With it
// enabled the client serializes publishes per ordering key (one in flight per
// key) and the connector hands messages back per key in publish order. These
// helpers centralise enabling ordering and the ordered publish (which, on a
// publish error, must call ResumePublish to un-pause the key — the SDK pauses a
// key after a failed ordered publish so subsequent sends are rejected until
// explicitly resumed).

// EnableTopicOrdering marks a publisher topic so the client serializes per-key
// publishes (one in flight per ordering key, in publish order).
func EnableTopicOrdering(topic *pubsub.Topic) {
	topic.EnableMessageOrdering = true
}

// OrderingSubscriptionConfig returns a SubscriptionConfig for an
// ordering-enabled subscription bound to topic with the given ack deadline. The
// subscription side must also enable ordering — enabling it on only the
// publisher is not enough.
func OrderingSubscriptionConfig(topic *pubsub.Topic, ackDeadlineSeconds int) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{
		Topic:                 topic,
		AckDeadline:           AckDeadline(ackDeadlineSeconds),
		EnableMessageOrdering: true,
	}
}

// PublishOrdered publishes a message on a specific ordering key and blocks until
// the connector assigns its id, or ctx is cancelled. On error it calls
// ResumePublish for the key so a later publish on the same key is accepted
// (the SDK pauses an ordering key after a failed ordered publish; without the
// resume every subsequent send on that key fails). It returns the assigned
// message id on success.
func PublishOrdered(ctx context.Context, topic *pubsub.Topic, orderingKey string, body []byte, attrs map[string]string) (string, error) {
	id, err := PublishMessageSync(ctx, topic, &pubsub.Message{
		Data:        body,
		Attributes:  attrs,
		OrderingKey: orderingKey,
	})
	if err != nil {
		// Un-pause ordered delivery on the key so the producer can resume after a
		// transient failure (mirrors the SDK's ordered-publish contract).
		topic.ResumePublish(orderingKey)
		return "", err
	}
	return id, nil
}
