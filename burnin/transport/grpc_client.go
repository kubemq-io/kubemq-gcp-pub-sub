package transport

import (
	"context"
	"fmt"
	"net"
	"strconv"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
)

// NativeConfig captures the native gRPC connection knobs for the interop worker.
type NativeConfig struct {
	Address  string // host:port; defaults via GRPCAddress()
	ClientID string
}

// NativeClient wraps a kubemq-go/v2 client used by the cross_protocol_interop
// worker to drive the NATIVE side of the Events Store log a topic maps to. A
// Pub/Sub topic {t} maps to the Events Store channel pubsub.{t}: a GCP Publish
// writes once there, and the native side consumes via an Events Store SUBSCRIBE
// stream (not a one-shot read). This is the ONLY place kubemq-go appears (spec
// S6.3). There is no RPC responder — Pub/Sub is pub/sub, not request/reply.
type NativeClient struct {
	client *kubemq.Client
}

// DialNative connects a native kubemq-go/v2 client to the gRPC listener.
func DialNative(ctx context.Context, cfg NativeConfig) (*NativeClient, error) {
	host, port, err := splitHostPort(GRPCAddress(cfg.Address))
	if err != nil {
		return nil, fmt.Errorf("parse grpc address: %w", err)
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "burnin-gcp-interop"
	}
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId(clientID),
		kubemq.WithCheckConnection(true),
	)
	if err != nil {
		return nil, fmt.Errorf("dial native kubemq: %w", err)
	}
	return &NativeClient{client: client}, nil
}

// Close shuts down the native client.
func (n *NativeClient) Close() error {
	if n.client == nil {
		return nil
	}
	return n.client.Close()
}

// NativeEvent is a received native Events Store message normalized to the fields
// the interop worker needs (body + instrumentation tags + broker sequence).
type NativeEvent struct {
	Body     []byte
	Tags     map[string]string
	Sequence uint64
}

// SubscribeEventsStoreFromNew opens an Events Store subscribe stream on channel
// (pubsub.{topic}) with the deterministic "new only" start policy
// (StartFromNewEvents, startAt = "new") and invokes onEvent for every event that
// arrives after the subscription is established. The connector confirms the
// subscribe stream is open before returning, so the caller can guarantee
// subscribe-before-publish ordering: establish this FIRST, then begin the GCP
// publisher, and every published message is in-window for the native consumer
// (no startup race). The returned Subscription is closed via Unsubscribe.
func (n *NativeClient) SubscribeEventsStoreFromNew(
	ctx context.Context,
	channel string,
	onEvent func(NativeEvent),
	onError func(error),
) (*kubemq.Subscription, error) {
	sub, err := n.client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			if e == nil {
				return
			}
			onEvent(NativeEvent{Body: e.Body, Tags: e.Tags, Sequence: e.Sequence})
		}),
		kubemq.WithOnError(func(err error) {
			if onError != nil && err != nil {
				onError(err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("subscribe events store %s: %w", channel, err)
	}
	return sub, nil
}

// SendEventStore writes one event to the Events Store channel (pubsub.{topic})
// with the supplied body and tags (worker-id/sequence/contenthash
// instrumentation). Used when the native side is the producer half of an interop
// check; the GCP-publish path is driven by the worker through the Pub/Sub client.
func (n *NativeClient) SendEventStore(ctx context.Context, channel string, body []byte, tags map[string]string) error {
	es := kubemq.NewEventStore().
		SetChannel(channel).
		SetBody(body).
		SetTags(tags)
	res, err := n.client.SendEventStore(ctx, es)
	if err != nil {
		return fmt.Errorf("native send event store: %w", err)
	}
	if res != nil && res.Err != nil {
		return fmt.Errorf("native send event store error: %w", res.Err)
	}
	return nil
}

func splitHostPort(address string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	return host, port, nil
}
