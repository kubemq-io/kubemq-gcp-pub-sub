package worker

import (
	"context"

	"cloud.google.com/go/pubsub"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// resources.go bridges to the transport-package provisioning helpers
// (transport.EnsureTopic / transport.EnsureSubscription, spec S6.1). Topic and
// subscription lifecycle lives in transport/resources.go alongside the naming
// grammar; the worker keeps these thin local aliases so each worker's Start
// reads against a stable name.

// ensureTopic returns a publisher handle for topicID, creating the topic if it
// does not already exist (idempotent). The caller owns topic.Stop().
func ensureTopic(ctx context.Context, client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	return transport.EnsureTopic(ctx, client, topicID)
}

// ensureSubscription returns a subscription handle for subID, creating it with
// cfg if it does not already exist (idempotent).
func ensureSubscription(ctx context.Context, client *pubsub.Client, subID string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	return transport.EnsureSubscription(ctx, client, subID, cfg)
}
