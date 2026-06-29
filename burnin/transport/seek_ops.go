package transport

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

// seek_ops.go holds the seek / snapshot transport operations (spec S6.1;
// connector Seek & Snapshots per kubemq-server docs/20-google-pubsub.md "Seek &
// Snapshots (M3)" — seek.go). Seek rewinds a subscription to a timestamp or a
// snapshot: the connector resolves a start sequence in the topic log
// (pubsub.{t}), purges the subscription queue, and replays the log from that
// sequence back into the subscription (bounded by MaxSeekReplay). A timestamp
// before the retained window is CLAMPED to the earliest retained message — not
// an error. Snapshots capture a subscription cursor (CreateSnapshot, 7-day
// default expiry) and can be the target of a Seek. These helpers are the
// burn-in transport surface for the seek concept (the per-language examples
// exercise the same RPCs idiomatically).

// SeekToTime rewinds a subscription to a point in time: messages published
// before t are marked acknowledged, messages published after t are marked
// unacknowledged (the connector replays the topic log from the resolved start
// sequence). A timestamp before the retained window is clamped, not rejected.
func SeekToTime(ctx context.Context, sub *pubsub.Subscription, t time.Time) error {
	if err := sub.SeekToTime(ctx, t); err != nil {
		return fmt.Errorf("seek %q to time %s: %w", sub.ID(), t.Format(time.RFC3339Nano), err)
	}
	return nil
}

// CreateSnapshot captures a subscription's cursor as a named snapshot (7-day
// default expiry, swept hourly by the connector) so a later SeekToSnapshot can
// rewind to exactly this point. A snapshot of a detached subscription fails
// (FAILED_PRECONDITION). It returns the snapshot config (its .Snapshot handle is
// the SeekToSnapshot target).
func CreateSnapshot(ctx context.Context, sub *pubsub.Subscription, snapshotID string) (*pubsub.SnapshotConfig, error) {
	cfg, err := sub.CreateSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("create snapshot %q on %q: %w", snapshotID, sub.ID(), err)
	}
	return cfg, nil
}

// SeekToSnapshot rewinds a subscription to a previously captured snapshot. The
// snapshot need not be created from this subscription, but it must be for the
// same topic. The connector purges the subscription queue and replays the topic
// log from the snapshot's cursor.
func SeekToSnapshot(ctx context.Context, sub *pubsub.Subscription, snap *pubsub.Snapshot) error {
	if err := sub.SeekToSnapshot(ctx, snap); err != nil {
		return fmt.Errorf("seek %q to snapshot %q: %w", sub.ID(), snap.ID(), err)
	}
	return nil
}

// GetSnapshot resolves a snapshot's config by id (incidental control RPC the
// seek_snapshot worker MAY call — spec S4). The high-level SDK exposes no
// per-snapshot Get, so this scans ListSnapshots for the matching id. A snapshot
// that is not found returns a non-nil error.
func GetSnapshot(ctx context.Context, client *pubsub.Client, snapshotID string) (*pubsub.SnapshotConfig, error) {
	cfgs, err := ListSnapshots(ctx, client)
	if err != nil {
		return nil, err
	}
	for _, cfg := range cfgs {
		if cfg.Snapshot != nil && cfg.Snapshot.ID() == snapshotID {
			return cfg, nil
		}
	}
	return nil, fmt.Errorf("get snapshot %q: not found", snapshotID)
}

// ListSnapshots enumerates the snapshot configs visible to the client (incidental
// control RPC the seek_snapshot worker MAY call — spec S4). It drains the
// iterator and returns the configs.
func ListSnapshots(ctx context.Context, client *pubsub.Client) ([]*pubsub.SnapshotConfig, error) {
	var out []*pubsub.SnapshotConfig
	it := client.Snapshots(ctx)
	for {
		cfg, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list snapshots: %w", err)
		}
		out = append(out, cfg)
	}
	return out, nil
}

// DeleteSnapshot removes a snapshot (cleanup utility). The snapshot is
// referenced by id off the client; a missing snapshot is tolerated as a no-op.
func DeleteSnapshot(ctx context.Context, client *pubsub.Client, snapshotID string) error {
	if err := client.Snapshot(snapshotID).Delete(ctx); err != nil {
		if IsNotFound(err) || IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("delete snapshot %q: %w", snapshotID, err)
	}
	return nil
}
