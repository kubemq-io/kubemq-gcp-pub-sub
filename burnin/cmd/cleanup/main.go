// Command cleanup best-effort deletes the Pub/Sub resources this Go burn-in
// agent created — topics, subscriptions, snapshots, and schemas whose ids are
// prefixed with BURNIN_RESOURCE_PREFIX (default "gcp_burnin"). The connector
// ignores the project segment, so resource ids are GLOBAL across every language
// agent sharing the stateful connector; matching strictly on the prefix means
// this only removes the resources of THIS agent and never disturbs the others.
//
// It connects via PUBSUB_EMULATOR_HOST (the connector's insecure gRPC listener,
// emulator protocol) using the same transport client factories the harness uses
// (transport/pubsub_client.go, transport/schema_ops.go). It mirrors the AWS
// sibling's cmd/cleanup pattern, repointed to the GCP Pub/Sub client.
//
// Usage:
//
//	cleanup [-host host:port] [-project id] [-prefix p]
//
// Deletion order is subscriptions → snapshots → topics → schemas: a topic with
// live subscriptions/snapshots cannot be removed, so its dependents go first.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

func main() {
	host := flag.String("host", "", "Pub/Sub emulator host:port (default: PUBSUB_EMULATOR_HOST / KUBEMQ_BROKER_ADDRESS / localhost:8085)")
	project := flag.String("project", "", "Pub/Sub project id (default: burnin)")
	prefix := flag.String("prefix", "", "resource-name prefix to match (default: BURNIN_RESOURCE_PREFIX / gcp_burnin)")
	flag.Parse()

	pfx := *prefix
	if pfx == "" {
		pfx = envOr(transport.ResourcePrefixEnv, "gcp_burnin")
	}
	resolvedHost := transport.EmulatorHost(*host, "")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := transport.PubSubConfig{ProjectID: *project, EmulatorHost: *host}
	client, err := transport.NewPubSubClient(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build pubsub client: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	subDeleted, subFailed := cleanupSubscriptions(ctx, client, pfx)
	snapDeleted, snapFailed := cleanupSnapshots(ctx, client, pfx)
	topicDeleted, topicFailed := cleanupTopics(ctx, client, pfx)
	schemaDeleted, schemaFailed := cleanupSchemas(ctx, cfg, pfx)

	fmt.Printf("cleanup prefix=%q host=%s subs_deleted=%d subs_failed=%d snapshots_deleted=%d snapshots_failed=%d topics_deleted=%d topics_failed=%d schemas_deleted=%d schemas_failed=%d\n",
		pfx, resolvedHost,
		subDeleted, subFailed,
		snapDeleted, snapFailed,
		topicDeleted, topicFailed,
		schemaDeleted, schemaFailed)
}

// cleanupSubscriptions deletes every subscription whose id starts with prefix.
// Subscriptions are removed before topics: a topic with live subscriptions
// cannot be deleted.
func cleanupSubscriptions(ctx context.Context, client *pubsub.Client, prefix string) (deleted, failed int) {
	it := client.Subscriptions(ctx)
	for {
		sub, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "list subscriptions: %v\n", err)
			break
		}
		if !strings.HasPrefix(sub.ID(), prefix) {
			continue
		}
		if err := sub.Delete(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "delete subscription %s: %v\n", sub.ID(), err)
			failed++
			continue
		}
		deleted++
	}
	return deleted, failed
}

// cleanupSnapshots deletes every snapshot whose id starts with prefix. Snapshots
// are removed before topics for the same reason as subscriptions.
func cleanupSnapshots(ctx context.Context, client *pubsub.Client, prefix string) (deleted, failed int) {
	it := client.Snapshots(ctx)
	for {
		cfg, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "list snapshots: %v\n", err)
			break
		}
		if cfg.Snapshot == nil || !strings.HasPrefix(cfg.ID(), prefix) {
			continue
		}
		if err := cfg.Delete(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "delete snapshot %s: %v\n", cfg.ID(), err)
			failed++
			continue
		}
		deleted++
	}
	return deleted, failed
}

// cleanupTopics deletes every topic whose id starts with prefix (after its
// subscriptions and snapshots are gone).
func cleanupTopics(ctx context.Context, client *pubsub.Client, prefix string) (deleted, failed int) {
	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "list topics: %v\n", err)
			break
		}
		if !strings.HasPrefix(topic.ID(), prefix) {
			continue
		}
		if err := topic.Delete(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "delete topic %s: %v\n", topic.ID(), err)
			failed++
			continue
		}
		deleted++
	}
	return deleted, failed
}

// cleanupSchemas deletes every registered schema whose id starts with prefix.
// Schemas live behind the SchemaClient; their fully-qualified Name is
// projects/{project}/schemas/{schema}, so we match on the trailing id segment
// and delete by that bare id.
func cleanupSchemas(ctx context.Context, cfg transport.PubSubConfig, prefix string) (deleted, failed int) {
	schemaCli, err := transport.NewSchemaClient(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build schema client: %v\n", err)
		return 0, 0
	}
	defer func() { _ = schemaCli.Close() }()

	it := schemaCli.Schemas(ctx, pubsub.SchemaViewBasic)
	for {
		sc, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			// The connector may not implement ListSchemas; treat as "nothing to do".
			fmt.Fprintf(os.Stderr, "list schemas: %v\n", err)
			break
		}
		id := schemaID(sc.Name)
		if !strings.HasPrefix(id, prefix) {
			continue
		}
		if err := schemaCli.DeleteSchema(ctx, id); err != nil {
			fmt.Fprintf(os.Stderr, "delete schema %s: %v\n", id, err)
			failed++
			continue
		}
		deleted++
	}
	return deleted, failed
}

// schemaID extracts the bare schema id from a fully-qualified schema name
// (projects/{project}/schemas/{schema}).
func schemaID(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
