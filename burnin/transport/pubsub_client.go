package transport

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PubSubConfig captures everything needed to build the Google Cloud Pub/Sub
// client against the KubeMQ Pub/Sub connector. EmulatorHost is the bare
// host:port the SDK connects to (the connector is no-auth, insecure gRPC by
// design, exactly like Google's local emulator). ProjectID is parsed by the SDK
// but its segment is ignored by the connector.
type PubSubConfig struct {
	ProjectID    string
	EmulatorHost string
}

// NewPubSubClient builds a cloud.google.com/go/pubsub client pointed at the
// connector. Setting PUBSUB_EMULATOR_HOST makes the SDK clear credentials, skip
// Google auth, and use insecure gRPC — the zero-code-change drop-in the
// connector is built around. We ALSO pass explicit insecure-credentials dial
// options so the client connects even if the ambient env var is unset (e.g. a
// caller that only set gcp.emulator_host in the config).
func NewPubSubClient(ctx context.Context, cfg PubSubConfig) (*pubsub.Client, error) {
	host := EmulatorHost(cfg.EmulatorHost, "")
	// Honour the standard env-var contract: ensure the SDK sees the emulator host
	// so any internal auth bypass it keys off the var also activates.
	if os.Getenv(EmulatorEnv) == "" {
		_ = os.Setenv(EmulatorEnv, host)
	}

	projectID := cfg.ProjectID
	if projectID == "" {
		projectID = "burnin"
	}

	opts := []option.ClientOption{
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("create pubsub client (emulator %s): %w", host, err)
	}
	return client, nil
}
