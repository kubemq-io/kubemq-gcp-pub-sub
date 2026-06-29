package transport

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
)

// pull_ops.go holds the pull / StreamingPull / ack transport operations the
// burn-in workers share (mirrors the AWS sibling's transport/sqs_ops.go
// ReceiveMessages/DeleteMessage split, spec S6.1). The cloud.google.com/go/pubsub
// consumer is a high-level StreamingPull loop: Subscription.Receive opens the
// bidirectional stream and invokes a callback per message until its context is
// cancelled. These helpers wrap the receive-settings + Receive-loop + ack so
// every worker pulls and acknowledges identically.

// DefaultMaxOutstanding is the flow-control cap on unacked messages a single
// subscription keeps in flight. The connector enforces its own
// MaxInflightPerSubscription safety net; this client-side cap keeps the burn-in
// receive buffer bounded.
const DefaultMaxOutstanding = 1000

// ConfigureReceive sets the StreamingPull flow-control settings on a
// subscription before the receive loop opens it. numStreams is the number of
// concurrent StreamingPull streams (NumGoroutines): use 1 to keep the
// ordered-receive signal deterministic (ordering / exactly-once / streaming
// workers) and N for competing receivers (publish-pull round trip). A value < 1
// is clamped to 1.
func ConfigureReceive(sub *pubsub.Subscription, numStreams int) {
	ConfigureReceiveWithBytes(sub, numStreams, 0)
}

// ConfigureReceiveWithBytes is ConfigureReceive plus an explicit
// MaxOutstandingBytes byte-ceiling (spec REQ-009 / S5.8). A value <= 0 leaves
// the high-level SDK's byte flow control unlimited (the default). NOTE (M-2):
// with the default gcp.max_outstanding_bytes: 0 this is a no-op / forward-looking
// plumbing only and is NOT part of any asserted gate — the actual server-side
// clamp assertion is the low-level FlowControlClampProbe. It exists so a non-zero
// byte ceiling can be configured later without a code change.
func ConfigureReceiveWithBytes(sub *pubsub.Subscription, numStreams, maxOutstandingBytes int) {
	if numStreams < 1 {
		numStreams = 1
	}
	sub.ReceiveSettings.NumGoroutines = numStreams
	sub.ReceiveSettings.MaxOutstandingMessages = DefaultMaxOutstanding
	if maxOutstandingBytes > 0 {
		sub.ReceiveSettings.MaxOutstandingBytes = maxOutstandingBytes
	}
}

// Receive runs the StreamingPull receive loop until ctx is cancelled, invoking
// handler for each delivered message. It is a thin pass-through over
// Subscription.Receive so the receive call site is uniform across workers; the
// caller owns reconnect/backoff on a returned (non-cancellation) error.
func Receive(ctx context.Context, sub *pubsub.Subscription, handler func(context.Context, *pubsub.Message)) error {
	return sub.Receive(ctx, handler)
}

// Ack acknowledges a received message (at-least-once subscriptions). It is the
// round-trip completion: the connector releases the message's lease on
// pubsub.sub.{s}. Ack does not return a status; on an exactly-once subscription
// use AckWithResult instead (see AckExactlyOnce).
func Ack(m *pubsub.Message) {
	m.Ack()
}

// Nack negatively-acknowledges a message so the connector redelivers it (drives
// the dead_letter worker's max-delivery-attempts path).
func Nack(m *pubsub.Message) {
	m.Nack()
}

// AckExactlyOnce acknowledges a message on an exactly-once subscription and
// resolves the AcknowledgeStatus the connector returns. Success confirms the
// ack; InvalidAckID / FailedPrecondition / Other mean the ack id expired or the
// lease was lost (the caller reconciles / retries — spec S6.6). It blocks until
// the result resolves or ctx is cancelled.
func AckExactlyOnce(ctx context.Context, m *pubsub.Message) (pubsub.AcknowledgeStatus, error) {
	return m.AckWithResult().Get(ctx)
}

// AckDeadline builds the SubscriptionConfig AckDeadline duration from a
// configured seconds value (centralising the seconds→Duration conversion every
// worker repeats).
func AckDeadline(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}
