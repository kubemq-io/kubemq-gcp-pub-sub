// Command push-delivery is master-table variant 9: push subscriptions against the
// KubeMQ Pub/Sub connector.
//
//	start a local HTTP receiver
//	-> CreateTopic
//	-> CreateSubscription(PushConfig{Endpoint: http://127.0.0.1:<port>/push})
//	-> Publish one message with attributes
//	-> the connector's per-sub push worker POSTs a wrapped JSON envelope to the
//	   endpoint; the receiver decodes it, asserts the body/attributes/messageId,
//	   and replies 2xx (which the worker treats as an ack)
//
// A subscription with a push_config is served by a per-sub worker that POSTs a
// wrapped JSON envelope (spec S3.5):
//
//	{"message":{"data":<base64>,"attributes":{...},"messageId":"...",
//	            "publishTime":"...","orderingKey":"..."},"subscription":"..."}
//
// A 2xx response acks the message; non-2xx/timeout retries with backoff; exhaustion
// dead-letters (if a DLQ is set) else drops. HTTPS is required EXCEPT for localhost
// endpoints, so this example uses a plain-HTTP loopback receiver. The default
// wrapper is the Pub/Sub wrapper (above); `no_wrapper` would POST the raw body.
//
// This is a pure cloud.google.com/go/pubsub program: only the push subscription's
// HTTP endpoint is local. Resource ids are uuid-suffixed per run (channel
// isolation); everything is deleted on exit (best-effort). Any failure exits
// non-zero.
//
// Run:
//
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//	export PUBSUB_PROJECT_ID=my-project
//	go run ./delivery/push-delivery
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

// pushEnvelope is the Pub/Sub-wrapper JSON the connector's push worker POSTs.
type pushEnvelope struct {
	Message struct {
		Data        string            `json:"data"` // base64-encoded body.
		Attributes  map[string]string `json:"attributes"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		OrderingKey string            `json:"orderingKey"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

type received struct {
	env  pushEnvelope
	body string
}

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		projectID = "my-project" // any id; the project segment is parsed but ignored.
	}
	fmt.Printf("[kubemq-gcp-pub-sub] delivery/push-delivery | emulator=%s project=%s\n",
		os.Getenv("PUBSUB_EMULATOR_HOST"), projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Local HTTP receiver (loopback). HTTP is allowed for localhost endpoints;
	// non-localhost push endpoints must be HTTPS.
	got := make(chan received, 4)
	ln, err := net.Listen("tcp", "127.0.0.1:0") // ephemeral free port.
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	endpoint := fmt.Sprintf("http://%s/push", ln.Addr().String())
	mux := http.NewServeMux()
	mux.HandleFunc("/push", func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var env pushEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			http.Error(w, "bad envelope", http.StatusBadRequest)
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(env.Message.Data)
		if err != nil {
			http.Error(w, "bad base64 data", http.StatusBadRequest)
			return
		}
		got <- received{env: env, body: string(decoded)}
		w.WriteHeader(http.StatusNoContent) // 2xx -> the push worker acks the message.
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	}()
	fmt.Printf("Push receiver listening at %s (HTTP allowed for localhost)\n", endpoint)

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Unique-per-run resource ids (channel isolation). Canonical: pushed / sub-pushed.
	suffix := uuid.NewString()[:8]
	topicID := "pushed-" + suffix
	subID := "sub-pushed-" + suffix

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("CreateTopic %q: %v", topicID, err)
	}
	defer topic.Stop()
	fmt.Printf("CreateTopic: %s (channel pubsub.%s)\n", topic.ID(), topicID)

	// Push subscription pointing at the local receiver. Wrapper defaults to the
	// Pub/Sub wrapper (the wrapped envelope) when PushConfig.Wrapper is unset.
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
		PushConfig:  pubsub.PushConfig{Endpoint: endpoint},
	})
	if err != nil {
		log.Fatalf("CreateSubscription (push) %q: %v", subID, err)
	}
	fmt.Printf("CreateSubscription: %s (push_config endpoint=%s)\n", sub.ID(), endpoint)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := sub.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteSubscription: %v", err)
		} else {
			fmt.Println("DeleteSubscription: ok")
		}
		if err := topic.Delete(cleanupCtx); err != nil {
			log.Printf("warning: DeleteTopic: %v", err)
		} else {
			fmt.Println("DeleteTopic: ok")
		}
	}()

	// Publish one message; the connector's push worker delivers it to the endpoint.
	const body = "push me"
	const attrKey, attrVal = "priority", "high"
	msgID, err := topic.Publish(ctx, &pubsub.Message{
		Data:       []byte(body),
		Attributes: map[string]string{attrKey: attrVal},
	}).Get(ctx)
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Printf("Publish: messageId=%s body=%q attr[%s]=%q\n", msgID, body, attrKey, attrVal)

	// Wait for the push worker to POST the wrapped envelope to our receiver.
	fmt.Println("Waiting for the push worker to POST the wrapped envelope...")
	var r received
	select {
	case r = <-got:
	case <-time.After(30 * time.Second):
		log.Fatalf("FAIL: no push delivery received within 30s")
	}

	fmt.Printf("Push received: messageId=%s body=%q attr[%s]=%q subscription=%q publishTime=%s\n",
		r.env.Message.MessageID, r.body, attrKey, r.env.Message.Attributes[attrKey],
		r.env.Subscription, r.env.Message.PublishTime)

	// Assertions: the wrapped envelope decodes to the published body/attribute/id.
	if r.body != body {
		log.Fatalf("FAIL: pushed body %q != published %q", r.body, body)
	}
	if r.env.Message.Attributes[attrKey] != attrVal {
		log.Fatalf("FAIL: pushed attr %q=%q != published %q", attrKey, r.env.Message.Attributes[attrKey], attrVal)
	}
	if r.env.Message.MessageID != msgID {
		log.Fatalf("FAIL: pushed messageId %q != published %q", r.env.Message.MessageID, msgID)
	}
	fmt.Println("PASS: push worker POSTed the wrapped envelope; 2xx acked it")
}
