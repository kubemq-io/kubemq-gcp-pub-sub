package transport

import (
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// flowControlStreamPollInterval is the base granularity for the
// FlowControlClampProbe idle window: the per-Recv idle window is
// recvBudget * flowControlStreamPollInterval, floored at
// flowControlMinIdleWindow so the probe always waits long enough after the
// clamp to be sure no further message is coming before concluding.
const flowControlStreamPollInterval = 50 * time.Millisecond

// flowControlMinIdleWindow floors the FlowControlClampProbe idle window so a
// small recvBudget cannot conclude the clamp prematurely; ~1s comfortably
// exceeds the connector's StreamingPull batch cadence.
const flowControlMinIdleWindow = 1 * time.Second

// InvalidAckIDReason is the ErrorInfo reason the connector attaches to a unary
// invalid-ack on an exactly-once subscription (spec S3.7).
const InvalidAckIDReason = "PERMANENT_FAILURE_INVALID_ACK_ID"

// AckErrorReason extracts the ErrorInfo.Reason from a gRPC error's status
// details (spec S3.7: the connector returns FAILED_PRECONDITION + ErrorInfo with
// reason=PERMANENT_FAILURE_INVALID_ACK_ID, domain=pubsub.googleapis.com). It
// returns "" when there is no ErrorInfo detail.
func AckErrorReason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// apiv1_ops.go holds the LOW-LEVEL apiv1 (vkit) transport operations the
// hardened-partial probes need (spec S4: UnaryAckInvalid, ModifyAckDeadline,
// FlowControlClampProbe, CrossStreamAckProbe). The high-level pubsub.Subscription
// Receive path applies its own client-side flow control and auto-extends leases,
// so these assertions about CONNECTOR behaviour must go through the bare
// SubscriberClient (the same low-level transport dead_letter.go already uses).
// All low-level clients dial the connector insecurely via EmulatorClientOptions,
// the shared wiring the bare apiv1 client needs because it does NOT auto-detect
// PUBSUB_EMULATOR_HOST.

// EmulatorClientOptions wires a low-level apiv1 client at the connector exactly
// the way the high-level client does when PUBSUB_EMULATOR_HOST is set: insecure
// gRPC, credentials cleared, no Google auth. The bare apiv1 client does NOT
// auto-detect the env var, so the options are wired explicitly. emulatorHost is
// the bare host:port (resolve via EmulatorHost first).
func EmulatorClientOptions(emulatorHost string) []option.ClientOption {
	if emulatorHost == "" {
		return nil
	}
	return []option.ClientOption{
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

// NewSubscriberClient builds a low-level apiv1 SubscriberClient pointed at the
// connector (insecure, no auth). The caller owns Close(). It is the transport
// surface for the unary-ack / lease-extend / flow-control / cross-stream probes.
func NewSubscriberClient(ctx context.Context, cfg PubSubConfig) (*vkit.SubscriberClient, error) {
	host := EmulatorHost(cfg.EmulatorHost, "")
	c, err := vkit.NewSubscriberClient(ctx, EmulatorClientOptions(host)...)
	if err != nil {
		return nil, fmt.Errorf("create apiv1 subscriber client (emulator %s): %w", host, err)
	}
	return c, nil
}

// SubscriptionPath builds the fully-qualified subscription resource name the
// low-level apiv1 RPCs require: projects/{project}/subscriptions/{sub}.
func SubscriptionPath(projectID, subID string) string {
	if projectID == "" {
		projectID = "burnin"
	}
	return fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
}

// IsInvalidArgument reports whether err carries a gRPC InvalidArgument status —
// the connector's whole-batch publish reject (S3.2) and out-of-range
// ModifyAckDeadline reject (S3.11). Use this (not a generic error check) to
// distinguish a connector reject from a client-side guard (spec S5.2/S5.8).
func IsInvalidArgument(err error) bool {
	return status.Code(err) == codes.InvalidArgument
}

// IsFailedPrecondition reports whether err carries a gRPC FailedPrecondition
// status — the connector's exactly-once invalid-ack reject (S3.7).
func IsFailedPrecondition(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

// UnaryAckInvalid acknowledges a fabricated/expired ack id via the LOW-LEVEL
// unary Acknowledge on an exactly-once subscription and resolves the connector's
// response (spec REQ-007 / S3.7). On an exactly-once subscription an invalid ack
// id returns codes.FailedPrecondition with ErrorInfo
// reason=PERMANENT_FAILURE_INVALID_ACK_ID. This helper maps the gRPC status to a
// pubsub.AcknowledgeStatus so the caller can assert AcknowledgeStatusInvalidAckID
// / AcknowledgeStatusFailedPrecondition; it returns the raw error too so the
// caller can inspect ErrorInfo via AckErrorReason.
func UnaryAckInvalid(ctx context.Context, subClient *vkit.SubscriberClient, subPath, ackID string) (pubsub.AcknowledgeStatus, error) {
	err := subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: subPath,
		AckIds:       []string{ackID},
	})
	return ackStatusFromError(err), err
}

// ModifyAckDeadline issues a LOW-LEVEL unary ModifyAckDeadline on subPath for
// ackID with the given deadline in seconds (spec REQ-011 / S3.11). seconds == 0
// is a nack (immediate redelivery); a positive deadline in [10,600] extends the
// lease; 1-9 and 601+ are rejected with codes.InvalidArgument. It returns the
// raw error (nil on success) so the caller can assert via IsInvalidArgument.
func ModifyAckDeadline(ctx context.Context, subClient *vkit.SubscriberClient, subPath, ackID string, seconds int) error {
	return subClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       subPath,
		AckIds:             []string{ackID},
		AckDeadlineSeconds: int32(seconds),
	})
}

// FlowControlClampProbe opens a LOW-LEVEL StreamingPull stream on subPath
// requesting max_outstanding_messages set ABOVE the connector ceiling (spec
// REQ-009 / S3.9) and counts the un-acked messages the connector streams BEFORE
// any ack. The connector clamps to its MaxOutstandingMessages ceiling and stops
// delivering until acks arrive, so the peak un-acked count proves the
// server-side clamp (a callback-sampled high-level gauge cannot, S3.9).
//
// The probe is SELF-BOUNDING: a clamped connector simply stops sending on the
// open stream — it does NOT emit empty StreamingPull responses — so the probe
// cannot rely on counting empty Recv()s to detect quiescence (that would block
// until ctx cancellation at run-end and starve the sequential lease-extend /
// cross-stream probes — REQ-009). Instead it runs stream.Recv() in a helper
// goroutine feeding a channel and selects against a per-Recv IDLE timer that is
// reset on each non-empty batch: once no new message arrives within the idle
// window after the clamp, the clamp is concluded held and the peak un-acked
// count is returned. The probe also returns promptly on ctx cancellation or a
// stream error.
//
// requestedMaxOutstanding is the (deliberately too-high) ceiling to request (e.g.
// 5000 > the connector default 1000); streamAckDeadlineSeconds sets the stream's
// ack deadline; recvBudget is the idle-window size in flowControlStreamPollInterval
// units (the window is floored at flowControlMinIdleWindow). The caller asserts
// peakUnacked <= connector ceiling.
func FlowControlClampProbe(ctx context.Context, subClient *vkit.SubscriberClient, subPath string, requestedMaxOutstanding, streamAckDeadlineSeconds, recvBudget int) (peakUnacked int, err error) {
	stream, err := subClient.StreamingPull(ctx)
	if err != nil {
		return 0, fmt.Errorf("open streaming pull: %w", err)
	}
	defer func() { _ = stream.CloseSend() }()

	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subPath,
		StreamAckDeadlineSeconds: int32(streamAckDeadlineSeconds),
		MaxOutstandingMessages:   int64(requestedMaxOutstanding),
	}); err != nil {
		return 0, fmt.Errorf("send initial streaming pull request: %w", err)
	}

	if recvBudget < 1 {
		recvBudget = 1
	}
	idleWindow := time.Duration(recvBudget) * flowControlStreamPollInterval
	if idleWindow < flowControlMinIdleWindow {
		idleWindow = flowControlMinIdleWindow
	}

	// recvResult carries one stream.Recv() outcome from the helper goroutine.
	type recvResult struct {
		got int
		err error
	}
	// Buffered so the goroutine never blocks delivering its final result after we
	// have returned (e.g. on idle-window or ctx); the parent ctx cancellation /
	// deferred CloseSend unblocks any in-flight Recv() so the goroutine exits.
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				recvCh <- recvResult{err: recvErr}
				return
			}
			recvCh <- recvResult{got: len(resp.GetReceivedMessages())}
		}
	}()

	unacked := 0
	idle := time.NewTimer(idleWindow)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return unacked, nil
		case <-idle.C:
			// No new message within the idle window after the clamp: conclude held.
			return unacked, nil
		case r := <-recvCh:
			if r.err != nil {
				if r.err == io.EOF || ctx.Err() != nil {
					return unacked, nil
				}
				return unacked, fmt.Errorf("streaming pull recv: %w", r.err)
			}
			if r.got == 0 {
				// An empty batch is not progress; keep waiting on the same window.
				continue
			}
			// Deliberately NOT acking: accumulate the un-acked outstanding count and
			// reset the idle window — the connector is still delivering pre-clamp.
			// We only reach this branch from the recvCh case, so the timer has not
			// fired (a fired timer is consumed by the idle.C case, which returns):
			// Stop() reliably cancels the pending timer with nothing to drain.
			unacked += r.got
			idle.Stop()
			idle.Reset(idleWindow)
		}
	}
}

// CrossStreamAckProbe asserts subscription-owned leases enable a cross-stream ack
// (spec REQ-013 / S3.13): a message delivered on one StreamingPull stream can be
// acked on a different stream (or via the unary Acknowledge) because the lease is
// owned by the subscription, not the stream. This helper acks ackID — captured
// from a message delivered on a SEPARATE stream — via the unary low-level
// Acknowledge on subPath and returns the error (nil = the cross-stream ack
// succeeded with codes.OK). The caller separately asserts the message is not
// redelivered.
func CrossStreamAckProbe(ctx context.Context, subClient *vkit.SubscriberClient, subPath, ackID string) error {
	return subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: subPath,
		AckIds:       []string{ackID},
	})
}

// ackStatusFromError maps a gRPC error from a unary Acknowledge / ModifyAckDeadline
// on an exactly-once subscription to the pubsub.AcknowledgeStatus the high-level
// SDK would surface (spec S3.7): FailedPrecondition with an invalid-ack-id
// ErrorInfo reason → InvalidAckID; bare FailedPrecondition → FailedPrecondition;
// PermissionDenied → PermissionDenied; nil → Success; otherwise → Other.
func ackStatusFromError(err error) pubsub.AcknowledgeStatus {
	if err == nil {
		return pubsub.AcknowledgeStatusSuccess
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		if AckErrorReason(err) == InvalidAckIDReason {
			return pubsub.AcknowledgeStatusInvalidAckID
		}
		return pubsub.AcknowledgeStatusFailedPrecondition
	case codes.PermissionDenied:
		return pubsub.AcknowledgeStatusPermissionDenied
	default:
		return pubsub.AcknowledgeStatusOther
	}
}
