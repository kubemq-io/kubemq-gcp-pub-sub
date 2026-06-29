# frozen_string_literal: true

require "logger"
require "securerandom"
require "google/cloud/pubsub"

# Quiet the official client's per-RPC INFO/DEBUG logging so example stdout stays
# readable; the connector traffic is still observable on the wire. This is the
# library's own supported lever (`Google::Cloud::PubSub.configure.logger`).
Google::Cloud::PubSub.configure do |config|
  warn_logger = Logger.new($stderr)
  warn_logger.level = Logger::WARN
  config.logger = warn_logger
end

# Shared client/config + resource helpers for every KubeMQ GCP Pub/Sub Ruby
# example. These examples are native first-party `google-cloud-pubsub` apps
# pointed at the KubeMQ Pub/Sub connector with ZERO code changes — the standard
# Google emulator env var does it.
#
# Connection contract (see ../../SHARED-CONVENTIONS.md and the root
# SHARED-CONVENTIONS.md §1):
#   * Endpoint — PUBSUB_EMULATOR_HOST (default localhost:8085, the connector's
#     gRPC port). When set, the official client clears credentials, skips Google
#     auth, and uses insecure gRPC — exactly as against Google's local emulator.
#     The Ruby client does not always pick the var up from the environment, so we
#     pass it explicitly as `emulator_host:` (root SHARED-CONVENTIONS.md §6).
#   * Project — PUBSUB_PROJECT_ID (any id; the connector parses but ignores the
#     project segment — resource ids are global across "projects").
#   * Auth / TLS — none (emulator posture). Do not expose 8085 to untrusted
#     networks.
#
# Channel-isolation rule (spec S9, connection-and-isolation.md §B): the canonical
# channels are shared and global (topic `t` -> `pubsub.{t}`, subscription `s` ->
# `pubsub.sub.{s}`), so concurrent runs that reuse a bare id cross-talk on the
# same channel and false-fail. Every example MUST therefore build UNIQUE per-run
# ids via `PubsubClient.unique_id` (uuid-suffixed) and best-effort delete what it
# created on exit.
module PubsubClient
  DEFAULT_EMULATOR_HOST = "localhost:8085"
  DEFAULT_PROJECT_ID = "my-project"

  module_function

  # PUBSUB_EMULATOR_HOST, falling back to the connector's default gRPC port.
  def emulator_host
    ENV.fetch("PUBSUB_EMULATOR_HOST", DEFAULT_EMULATOR_HOST)
  end

  # Any project id works; the connector parses but ignores the project segment.
  def project_id
    ENV["PUBSUB_PROJECT_ID"] || ENV["GOOGLE_CLOUD_PROJECT"] || DEFAULT_PROJECT_ID
  end

  # A ready-to-use Google::Cloud::PubSub client pointed at the connector. Passing
  # `emulator_host:` explicitly clears credentials -> insecure gRPC (no auth).
  def connect
    Google::Cloud::PubSub.new(project_id: project_id, emulator_host: emulator_host)
  end

  # Build a unique-per-run resource id from a base, e.g. "stream" ->
  # "stream-1a2b3c4d". Respects the Pub/Sub resource-id rules (spec S3.6):
  # 3..255 chars, starts with a letter, charset [A-Za-z0-9._~%+-], no `goog`
  # prefix, and (for topics) never starts with `sub.`. The uuid8 suffix gives
  # each run a private `pubsub.{id}` / `pubsub.sub.{id}` channel namespace so
  # parallel runs never collide (spec S9 / connection-and-isolation.md §B).
  def unique_id(base)
    "#{base}-#{SecureRandom.hex(4)}"
  end

  # Print the standard one-line connection banner every example opens with.
  def banner
    puts "[PUBSUB] emulator_host=#{emulator_host} project=#{project_id} (no-auth / insecure gRPC)"
  end
end
