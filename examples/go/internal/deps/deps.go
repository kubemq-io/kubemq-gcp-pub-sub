// Package deps is a build anchor for the kubemq-gcp-pub-sub Go examples module.
//
// It exists only so that `go build ./...` resolves and `go.sum` stays complete
// while the per-variant example programs (topics/, subscriptions/, delivery/,
// advanced/, interop/) are added by the group agents. The blank imports pin the
// official Google Cloud Pub/Sub client and the native KubeMQ SDK (interop) into
// the module graph; remove or replace nothing here when adding variants — each
// variant lives in its own `package main` directory.
//
// Pinned (spec S5.1 / S9 — latest stable, no floating range in the lockfile):
//   - cloud.google.com/go/pubsub        (official GCP Pub/Sub client)
//   - github.com/kubemq-io/kubemq-go/v2 (native KubeMQ SDK, interop variant only)
//   - github.com/google/uuid            (per-run resource id suffixes)
package deps

import (
	_ "cloud.google.com/go/pubsub"
	_ "github.com/google/uuid"
	_ "github.com/kubemq-io/kubemq-go/v2"
)
