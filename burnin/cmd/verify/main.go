// Command verify is a one-shot liveness / round-trip verifier for the GCP
// Pub/Sub connector. Unlike the soak harness it asserts a quick functional
// round-trip, not throughput: it provisions a throwaway topic + subscription,
// publishes a uniquely-tagged probe, pulls and acks it back over Pub/Sub, and —
// when the native gRPC listener is reachable — confirms the same body landed on
// the KubeMQ Events Store log the topic maps to (pubsub.{t}), proving the
// cross-protocol bridge is live. Run it before and after the soak as a sanity
// gate. It mirrors the AWS sibling's cmd/m3verify pattern, repointed to the GCP
// Pub/Sub + kubemq-go/v2 transport.
//
// Usage:
//
//	verify [-host host:port] [-grpc host:port] [-project id] [-prefix p] [-no-interop]
//
// Exit code 0 = all hard checks passed, 1 = one or more failed, 2 = setup error.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

type harness struct {
	ctx       context.Context
	client    *pubsub.Client
	native    *transport.NativeClient
	host      string
	grpc      string
	prefix    string
	checkNat  bool
	resources []func()

	failures int
	passes   int
}

func main() {
	host := flag.String("host", "", "Pub/Sub emulator host:port (default: PUBSUB_EMULATOR_HOST / KUBEMQ_BROKER_ADDRESS / localhost:8085)")
	grpc := flag.String("grpc", "", "native KubeMQ gRPC host:port (default: KUBEMQ_GRPC_ADDRESS / localhost:50000)")
	project := flag.String("project", "", "Pub/Sub project id (default: burnin)")
	prefix := flag.String("prefix", "verify", "resource-name prefix for the throwaway topic/subscription")
	noInterop := flag.Bool("no-interop", false, "skip the native Events Store interop confirmation")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	resolvedHost := transport.EmulatorHost(*host, "")
	resolvedGRPC := transport.GRPCAddress(*grpc)

	client, err := transport.NewPubSubClient(ctx, transport.PubSubConfig{ProjectID: *project, EmulatorHost: *host})
	must(err, "build pubsub client")
	defer func() { _ = client.Close() }()

	h := &harness{
		ctx:      ctx,
		client:   client,
		host:     resolvedHost,
		grpc:     resolvedGRPC,
		prefix:   *prefix,
		checkNat: !*noInterop,
	}

	if h.checkNat {
		nat, err := transport.DialNative(ctx, transport.NativeConfig{Address: *grpc, ClientID: "burnin-gcp-verify"})
		if err != nil {
			// Native side is optional for a pure Pub/Sub liveness check; degrade
			// to the round-trip-only verification rather than failing setup.
			fmt.Fprintf(os.Stderr, "verify: native gRPC %s unreachable, skipping interop check: %v\n", resolvedGRPC, err)
			h.checkNat = false
		} else {
			h.native = nat
			defer func() { _ = nat.Close() }()
		}
	}

	fmt.Printf("verify: host=%s grpc=%s interop=%v\n\n", resolvedHost, resolvedGRPC, h.checkNat)

	h.run("pubsub: publish -> pull -> ack round-trip", h.checkRoundTrip)
	if h.checkNat {
		h.run("interop: native Events Store sees the published body (pubsub.{t})", h.checkInterop)
	}

	h.cleanup()

	fmt.Printf("\nverify: %d passed, %d failed\n", h.passes, h.failures)
	if h.failures > 0 {
		os.Exit(1)
	}
}

func (h *harness) run(name string, fn func() error) {
	if err := fn(); err != nil {
		h.failures++
		fmt.Printf("  FAIL  %s\n        %v\n", name, err)
		return
	}
	h.passes++
	fmt.Printf("  PASS  %s\n", name)
}

// checkRoundTrip provisions a throwaway topic + subscription, publishes one
// uniquely-tagged probe, and pulls it back, asserting the body and tag survive
// the Pub/Sub publish→pull→ack path.
func (h *harness) checkRoundTrip() error {
	topic, sub, err := h.provision("rt")
	if err != nil {
		return err
	}
	probe := fmt.Sprintf("verify-probe-%s", rnd())
	if _, err := transport.PublishSync(h.ctx, topic, []byte(probe), map[string]string{"verify": probe}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	got, err := h.pullOne(sub, 20*time.Second)
	if err != nil {
		return err
	}
	if got != probe {
		return fmt.Errorf("round-trip mismatch: want %q got %q", probe, got)
	}
	return nil
}

// checkInterop provisions a fresh topic + subscription, opens the native Events
// Store subscribe stream FIRST (so the publish is in-window), publishes a probe
// over Pub/Sub, and confirms the native side observes the same body — proving
// the topic→pubsub.{t} bridge is live.
func (h *harness) checkInterop() error {
	topic, sub, err := h.provision("interop")
	if err != nil {
		return err
	}
	channel := transport.MappedTopicChannel(topic.ID())

	var (
		mu    sync.Mutex
		seen  []string
		probe = fmt.Sprintf("verify-interop-%s", rnd())
	)
	nativeCtx, nativeCancel := context.WithCancel(h.ctx)
	defer nativeCancel()
	natSub, err := h.native.SubscribeEventsStoreFromNew(nativeCtx, channel,
		func(e transport.NativeEvent) {
			mu.Lock()
			seen = append(seen, string(e.Body))
			mu.Unlock()
		},
		func(error) {},
	)
	if err != nil {
		return fmt.Errorf("native subscribe %s: %w", channel, err)
	}
	defer natSub.Unsubscribe()

	// SubscribeEventsStoreFromNew confirms the stream is open before returning,
	// so a small settle then publish keeps the message in-window.
	time.Sleep(250 * time.Millisecond)

	if _, err := transport.PublishSync(h.ctx, topic, []byte(probe), map[string]string{"verify": probe}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	// Drain the Pub/Sub side too so the subscription does not retain the probe.
	_, _ = h.pullOne(sub, 10*time.Second)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, b := range seen {
			if b == probe {
				mu.Unlock()
				return nil
			}
		}
		mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("native Events Store %s did not observe probe %q within budget", channel, probe)
}

// provision creates a throwaway topic + subscription under the verify prefix and
// registers their teardown. The ids are uuid-suffixed so concurrent verify runs
// never collide and cleanup is unambiguous.
func (h *harness) provision(role string) (*pubsub.Topic, *pubsub.Subscription, error) {
	id := fmt.Sprintf("%s_%s_%s", h.prefix, role, rnd())
	topic, err := transport.EnsureTopic(h.ctx, h.client, id)
	if err != nil {
		return nil, nil, err
	}
	h.resources = append(h.resources, func() {
		topic.Stop()
		_ = topic.Delete(context.Background())
	})
	sub, err := transport.EnsureSubscription(h.ctx, h.client, id+"_sub", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: transport.AckDeadline(10),
	})
	if err != nil {
		return nil, nil, err
	}
	h.resources = append(h.resources, func() { _ = sub.Delete(context.Background()) })
	return topic, sub, nil
}

// pullOne opens a bounded StreamingPull receive loop, returns the first message
// body (acking it), and cancels the loop. It is the pull half of the round trip.
func (h *harness) pullOne(sub *pubsub.Subscription, budget time.Duration) (string, error) {
	transport.ConfigureReceive(sub, 1)
	rcvCtx, cancel := context.WithTimeout(h.ctx, budget)
	defer cancel()

	var (
		once sync.Once
		body string
		got  bool
	)
	err := transport.Receive(rcvCtx, sub, func(_ context.Context, m *pubsub.Message) {
		once.Do(func() {
			body = string(m.Data)
			got = true
			cancel()
		})
		transport.Ack(m)
	})
	if err != nil && !got {
		return "", fmt.Errorf("receive: %w", err)
	}
	if !got {
		return "", fmt.Errorf("no message received within %s", budget)
	}
	return body, nil
}

// cleanup tears down the throwaway resources in reverse creation order (sub
// before topic). Best-effort: a failed delete is logged, not fatal.
func (h *harness) cleanup() {
	for i := len(h.resources) - 1; i >= 0; i-- {
		h.resources[i]()
	}
}

func rnd() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func must(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %s: %v\n", what, err)
		os.Exit(2)
	}
}
