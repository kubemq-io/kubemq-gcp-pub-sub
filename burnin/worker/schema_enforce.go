package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// SchemaEnforceWorker (NEW, spec REQ-004/S5.4, opt-in; default-ON only in the
// bounded soak profile) asserts schema enforce-on-publish (connector §3.6/§4.10b,
// docs "Schema Validation (M3)"): a topic bound to a registered Avro/Protobuf
// schema validates EVERY message in a PublishRequest before any enqueue and
// rejects the WHOLE batch with codes.InvalidArgument on the first non-conforming
// message (zero partial enqueue — consistent with §3.7 batch atomicity).
//
// Provisioning: build transport.NewSchemaClient, register a schema under the
// prefix-scoped id transport.SchemaName(WorkerSchemaEnforce, idx) via
// CreateAvroSchema / CreateProtobufSchema (per gcp.schema_type), bind a topic via
// transport.EnsureTopicWithSchema + transport.SchemaSettings(projectID, schemaID,
// encoding), and provision a plain pull subscription on it. Cleanup in
// StopConsumers: DeleteSchema + topic.Stop + client Close.
//
// Steady-state traffic: the producer publishes schema-CONFORMING bodies at Rate.
// The instrumentation (worker_id / seq / content_hash / ts_ns) moves OUT of the
// Pub/Sub attributes and INTO the schema-valid body, because the body itself must
// conform to pass enforcement. The consumer unmarshals the body, verifies the
// CRC over the pad, recordTracked(worker_id, seq), and acks — so the conforming
// stream is reconciled by the tracker with no loss (the LOSS gate always applies).
//
// Negative-path probe (one-shot at Start, before the steady loop): publish a
// single NON-conforming body (a wrong-typed field — seq as a JSON string where
// the schema declares a long/int64) to the schema-bound topic and assert the
// connector rejects it with codes.InvalidArgument. The probe uses a SEPARATE,
// non-tracked producer namespace so it is never reconciled as a conforming send.
//   - expected codes.InvalidArgument rejection  -> recordSchemaEnforceConfirmed.
//   - a non-conforming publish that SUCCEEDS     -> recordSchemaRejectViolation
//     ([HARD-FAIL] the schema_reject gate, deterministic per S5.10).
//
// When the probe executes (schema-bound topic provisioned AND the non-conforming
// publish attempted) it sets SchemaEnforceProbeRan = true. The verdict then fails
// if SchemaRejectViolations > 0 (deterministic HARD-FAIL) and additionally if
// SchemaEnforceProbeRan && SchemaEnforceConfirmed == 0 (probe ran but enforcement
// never observed — guarded by the probe-ran flag so a disabled/zero-traffic
// worker cannot false-fail, I-1).
type SchemaEnforceWorker struct {
	*BaseWorker
	topicID  string
	subID    string
	schemaID string

	schemaType     string
	encoding       pubsub.SchemaEncoding
	encodingString string

	schemaClient *pubsub.SchemaClient
	client       *pubsub.Client
	topic        *pubsub.Topic
	sub          *pubsub.Subscription

	seq atomic.Uint64
}

// NewSchemaEnforceWorker creates a schema_enforce worker.
func NewSchemaEnforceWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerSchemaEnforce, idx)
	subID := transport.SubscriptionName(config.WorkerSchemaEnforce, idx)
	schemaID := transport.SchemaName(config.WorkerSchemaEnforce, idx)
	return &SchemaEnforceWorker{
		BaseWorker: NewBaseWorker(config.WorkerSchemaEnforce,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID:        topicID,
		subID:          subID,
		schemaID:       schemaID,
		schemaType:     cfg.GCP.SchemaType,
		encoding:       schemaEncodingOf(cfg.GCP.SchemaEncoding),
		encodingString: cfg.GCP.SchemaEncoding,
	}
}

// schemaEncodingOf maps the gcp.schema_encoding config string to the SDK
// SchemaEncoding enum. The burn-in default is JSON (human-debuggable); BINARY is
// honoured when configured. An empty/unknown value defaults to JSON so the topic
// binding always carries an explicit encoding the connector can enforce against.
func schemaEncodingOf(enc string) pubsub.SchemaEncoding {
	if enc == "binary" {
		return pubsub.EncodingBinary
	}
	return pubsub.EncodingJSON
}

// schemaBody is the instrumentation envelope encoded INTO the schema-conforming
// message body (the body must conform, so the seq/CRC instrumentation cannot ride
// in Pub/Sub attributes here). The JSON field names match the registered schema's
// declared fields exactly; seq/ts_ns are JSON integers (the schema's long/int64
// fields), worker_id/content_hash/pad are strings.
type schemaBody struct {
	WorkerID    string `json:"worker_id"`
	Seq         uint64 `json:"seq"`
	ContentHash string `json:"content_hash"`
	TsNS        int64  `json:"ts_ns"`
	Pad         string `json:"pad"`
}

// avroSchemaDefinition is the Avro record the topic is bound to. It carries the
// burn-in instrumentation as typed fields: worker_id/content_hash/pad are
// "string", seq/ts_ns are "long". A JSON-encoded body whose seq/ts_ns is a string
// (not an integer) fails the connector's Avro coercion deterministically with
// INVALID_ARGUMENT — the basis of the negative-path probe (S5.4; connector
// schemaval/avro.go coercePrimitive→jsonInt).
const avroSchemaDefinition = `{
  "type": "record",
  "name": "BurninSchemaEnforce",
  "namespace": "kubemq.burnin",
  "fields": [
    {"name": "worker_id", "type": "string"},
    {"name": "seq", "type": "long"},
    {"name": "content_hash", "type": "string"},
    {"name": "ts_ns", "type": "long"},
    {"name": "pad", "type": "string"}
  ]
}`

// protoSchemaDefinition is the Protocol Buffers message the topic is bound to
// when gcp.schema_type is "protobuf". The field types mirror the Avro record
// (string / int64); a JSON body whose seq is a string fails proto3 JSON
// unmarshalling deterministically with INVALID_ARGUMENT (connector
// schemaval/proto.go ValidateProtoMessage).
const protoSchemaDefinition = `syntax = "proto3";
package kubemq.burnin;
message BurninSchemaEnforce {
  string worker_id = 1;
  int64 seq = 2;
  string content_hash = 3;
  int64 ts_ns = 4;
  string pad = 5;
}`

// Start builds the schema + Pub/Sub clients, registers the schema, provisions the
// schema-bound topic + a plain pull subscription, runs the one-shot
// non-conforming probe, and brings up the steady-state receiver.
func (w *SchemaEnforceWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	schemaClient, err := transport.NewSchemaClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build schema client: %w", err)
	}
	w.schemaClient = schemaClient

	definition := avroSchemaDefinition
	if w.schemaType == "protobuf" {
		definition = protoSchemaDefinition
	}
	if w.schemaType == "protobuf" {
		_, err = transport.CreateProtobufSchema(ctx, schemaClient, w.schemaID, definition)
	} else {
		_, err = transport.CreateAvroSchema(ctx, schemaClient, w.schemaID, definition)
	}
	if err != nil {
		return fmt.Errorf("register schema %q: %w", w.schemaID, err)
	}

	client, err := transport.NewPubSubClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build pubsub client: %w", err)
	}
	w.client = client

	// Bind the topic to the registered schema: from here every publish is
	// enforced by the connector (§4.10b). A conforming body passes; a
	// non-conforming body rejects the whole PublishRequest with INVALID_ARGUMENT.
	w.topic, err = transport.EnsureTopicWithSchema(ctx, client, w.topicID,
		transport.SchemaSettings(w.psCfg.ProjectID, w.schemaID, w.encoding))
	if err != nil {
		return err
	}

	w.sub, err = ensureSubscription(ctx, client, w.subID, pubsub.SubscriptionConfig{
		Topic:       w.topic,
		AckDeadline: transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
	})
	if err != nil {
		return err
	}

	// NOTE on probe placement: the negative-path probe is NOT run here. The engine
	// lifecycle is Start -> StartProducers(warmup) -> StopProducers -> Reset-
	// AfterWarmup (which clears SchemaEnforceConfirmed AND the probe-ran flag) ->
	// StartProducers(measurement). A probe fired in Start would be erased by the
	// warmup reset and never reach the verdict snapshot. The one-shot probe is
	// therefore fired at the head of StartProducers (S5.4 "one-shot at Start" of the
	// producer phase): the warmup-phase fire is reset, the measurement-phase fire
	// sets the probe-ran flag + confirmation that the verdict observes (S6.3, I-1).

	// Steady-state receiver: drain conforming messages and reconcile them.
	transport.ConfigureReceive(w.sub, w.workerCfg.ConsumersPerChannel)
	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()
	return nil
}

// runNonConformingProbe publishes a single deliberately NON-conforming body to
// the schema-bound topic and asserts the connector rejects it with
// codes.InvalidArgument. The probe body lives in a separate, NON-tracked producer
// namespace ("probe-<channel>") so it is never reconciled as a conforming send
// (S5.4 tracker interaction). It marks SchemaEnforceProbeRan once the publish has
// been attempted, and resolves the outcome:
//   - codes.InvalidArgument  -> recordSchemaEnforceConfirmed (the expected reject).
//   - publish SUCCEEDS       -> recordSchemaRejectViolation ([HARD-FAIL] deterministic).
//   - context cancelled / non-InvalidArgument transport error -> neither counter
//     advances (the probe-ran flag is still set; the verdict's probe-ran-guarded
//     "never observed" sub-gate then surfaces a missing confirmation as a fail,
//     which is correct: a schema-bound topic that does not reject is a failure).
func (w *SchemaEnforceWorker) runNonConformingProbe(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	body := w.buildNonConformingBody()

	// Optional pre-publish controlled assertion: ValidateMessage must itself
	// reject the bad body. A non-nil error here is the expected outcome and is not
	// a failure; it only strengthens the confidence that the probe body is truly
	// non-conforming before we drive it through Publish.
	if err := transport.ValidateMessage(ctx, w.schemaClient, w.schemaID, w.encoding, body); err != nil {
		w.logger.Debug("schema-enforce probe: ValidateMessage rejected the non-conforming body (expected)",
			"err", errString(err))
	} else {
		w.logger.Warn("schema-enforce probe: ValidateMessage ACCEPTED a non-conforming body (unexpected)")
	}

	// The probe has executed (topic provisioned, non-conforming publish attempted).
	w.markSchemaEnforceProbeRan()

	_, err := transport.PublishSync(ctx, w.topic, body, nil)
	if err == nil {
		// A non-conforming publish that SUCCEEDS is a deterministic enforcement
		// failure: the connector accepted a body that violates the bound schema.
		w.recordSchemaRejectViolation()
		w.logger.Error("schema-enforce probe: non-conforming publish was ACCEPTED (expected INVALID_ARGUMENT)",
			"schema", w.schemaID, "topic", w.topicID)
		return
	}
	if ctx.Err() != nil {
		return
	}
	if status.Code(err) == codes.InvalidArgument {
		w.recordSchemaEnforceConfirmed()
		w.logger.Info("schema-enforce probe: non-conforming publish rejected with INVALID_ARGUMENT (expected)",
			"schema", w.schemaID, "topic", w.topicID)
		return
	}
	// Any other transport error: not the expected reject and not an acceptance.
	// Record it as an error; the probe-ran-guarded verdict sub-gate will surface a
	// missing confirmation, which correctly fails a schema-bound topic that did
	// not enforce.
	w.recordError("schema_probe_failure")
	w.logger.Warn("schema-enforce probe: non-conforming publish failed with an unexpected code",
		"schema", w.schemaID, "code", status.Code(err).String(), "err", errString(err))
}

// buildNonConformingBody returns a body that the connector's schema validator
// rejects DETERMINISTICALLY. The seq field (declared long/int64) is encoded as a
// JSON STRING, which fails the connector's Avro json-int coercion
// (schemaval/avro.go coercePrimitive→jsonInt) and proto3 JSON int64 parsing
// alike. A wrong-typed field is used rather than an extra/unknown field because
// the connector's Avro coercer ignores undeclared fields (it only walks declared
// record fields), so an extra field would NOT reliably reject — a type mismatch
// always does (matching the connector truth in schemaval/avro.go and proto.go).
func (w *SchemaEnforceWorker) buildNonConformingBody() []byte {
	// seq is intentionally a quoted string ("not-a-number") instead of an integer.
	return []byte(`{"worker_id":"probe-` + w.channelName + `","seq":"not-a-number","content_hash":"00000000","ts_ns":0,"pad":"x"}`)
}

// receiveLoop runs the StreamingPull receive loop over the schema-bound
// subscription, re-opening it after a forced disconnect or a transient error.
func (w *SchemaEnforceWorker) receiveLoop(ctx context.Context) {
	gen := w.disconnectGeneration()
	first := true
	for {
		if ctx.Err() != nil {
			return
		}
		if !first {
			w.recordReconnection()
		}
		first = false

		w.signalReady()

		recvCtx, cancel := context.WithCancel(ctx)
		go watchDisconnect(recvCtx, cancel, gen, w.disconnectGeneration)

		err := transport.Receive(recvCtx, w.sub, func(_ context.Context, m *pubsub.Message) {
			w.handleMessage(m, "c-"+w.channelName)
		})
		cancel()
		gen = w.disconnectGeneration()

		if ctx.Err() != nil {
			return
		}
		if err != nil {
			w.recordError("receive_failure")
			if !sleepCtx(ctx, reconnectBackoff(w.cfg)) {
				return
			}
		}
	}
}

// handleMessage reconciles a conforming message: unmarshal the schema body,
// verify the CRC over the pad, recordTracked(worker_id, seq), and ack. A body
// that fails to unmarshal or carries no worker_id is counted as corrupted (it
// passed schema enforcement but the harness instrumentation is unreadable) and is
// still acked so it does not redeliver forever.
func (w *SchemaEnforceWorker) handleMessage(m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

	var sb schemaBody
	if err := json.Unmarshal(m.Data, &sb); err != nil || sb.WorkerID == "" {
		w.recordCorrupted()
		transport.Ack(m)
		w.recordAcked()
		return
	}
	if sb.ContentHash != "" && !payload.VerifyCRC([]byte(sb.Pad), sb.ContentHash) {
		w.recordCorrupted()
	}
	if sb.TsNS > 0 {
		w.recordLatency(time.Since(time.Unix(0, sb.TsNS)))
	}
	w.recordTracked(sb.WorkerID, sb.Seq)

	transport.Ack(m)
	w.recordAcked()
}

// StartProducers launches the conforming-publish loop(s) (measurement window).
// It also re-runs the one-shot non-conforming probe once at the START of the
// measurement window so the SchemaEnforceProbeRan flag + SchemaEnforceConfirmed
// counter are observed by the engine snapshot AFTER ResetAfterWarmup has cleared
// the warmup-phase probe (the verdict's probe-ran-guarded sub-gate must see a
// confirmation recorded in the measured window — S6.3, I-1).
func (w *SchemaEnforceWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())

	// Re-assert enforcement at the top of the measurement window so the
	// confirmation persists past the warmup reset. The probe is one-shot and uses
	// the non-tracked probe namespace, so it never perturbs the conforming
	// loss/throughput accounting.
	w.runNonConformingProbe(w.producerCtx)

	n := w.workerCfg.ProducersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		w.producerWG.Add(1)
		go func(pi int) {
			defer w.producerWG.Done()
			w.produceLoop(w.producerCtx, pi)
		}(i)
	}
}

// produceLoop publishes schema-CONFORMING bodies at Rate. Each body carries the
// instrumentation (worker_id / seq / content_hash / ts_ns / pad) inside the
// schema-valid JSON envelope; the CRC is taken over the pad bytes so the consumer
// can verify integrity from the body alone (attributes are not used for
// instrumentation under enforcement). recordSent registers the tracked send AFTER
// the connector acknowledges the message id, so the tracker reconciles every
// accepted conforming message and the LOSS gate is meaningful.
func (w *SchemaEnforceWorker) produceLoop(ctx context.Context, producerIdx int) {
	producerID := fmt.Sprintf("p-%s-%d", w.channelName, producerIdx)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		seq := w.seq.Add(1)
		body, ok := w.buildConformingBody(producerID, seq)
		if !ok {
			w.recordError("encode_failure")
			continue
		}

		start := time.Now()
		if _, err := transport.PublishSync(ctx, w.topic, body, nil); err != nil {
			if ctx.Err() != nil {
				return
			}
			// A conforming body MUST be accepted; a rejection here is a transport
			// failure (or an unexpected enforcement of a conforming body) — count it
			// as an error, not a tracked send, so the loss gate is not poisoned.
			w.recordError("publish_failure")
			continue
		}
		metricObserveSend(w.name, time.Since(start))
		w.recordSent(len(body), producerID)
	}
}

// buildConformingBody marshals a schema-conforming JSON body for (producerID,
// seq). The pad fills the configured message size with high-entropy hex and the
// content_hash is its CRC32 so the consumer can verify integrity from the body.
func (w *SchemaEnforceWorker) buildConformingBody(producerID string, seq uint64) ([]byte, bool) {
	pad, crcHex := payload.Build(w.selectMessageSize())
	sb := schemaBody{
		WorkerID:    producerID,
		Seq:         seq,
		ContentHash: crcHex,
		TsNS:        time.Now().UnixNano(),
		Pad:         string(pad),
	}
	body, err := json.Marshal(sb)
	if err != nil {
		return nil, false
	}
	return body, true
}

// StopConsumers stops the receiver, cleans up the registered schema, stops the
// topic publisher, and closes both clients (the schema + Pub/Sub clients). The
// schema id and topic are prefix-scoped so cleanup is per-run and idempotent.
func (w *SchemaEnforceWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()

	if w.schemaClient != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := transport.DeleteSchema(cleanupCtx, w.schemaClient, w.schemaID); err != nil {
			w.logger.Warn("schema-enforce cleanup: delete schema failed", "schema", w.schemaID, "err", errString(err))
		}
		cancel()
		_ = w.schemaClient.Close()
	}
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
