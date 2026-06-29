# frozen_string_literal: true

# KubeMQ Google Cloud Pub/Sub — advanced: seek to a timestamp & replay
# (Ruby / google-cloud-pubsub).
#
# Demonstrates rewinding a subscription to a point in time and replaying the
# topic log from there:
#   CreateTopic -> CreateSubscription -> Publish(m1, <checkpoint>, m2, m3)
#     -> drain all 3 -> Seek(time=<checkpoint>) -> replay m2,m3 only.
#
# Uses ONLY the official `google-cloud-pubsub` client. With PUBSUB_EMULATOR_HOST
# set the client clears credentials, skips Google auth, and dials insecure gRPC
# at the connector — exactly as against Google's local emulator, zero code
# changes. Control-plane RPCs (CreateTopic / CreateSubscription / Seek) go
# through the admin clients (`pubsub.topic_admin` / `pubsub.subscription_admin`);
# the data-plane publish/pull use `pubsub.publisher` / `pubsub.subscriber`.
#
# Because every topic is backed by a durable, replayable Events Store log
# (`pubsub.{t}`), `Seek(time)` resolves the first sequence at/after the timestamp,
# purges the subscription queue + drops leases, and replays the log from there
# (re-applying the subscription filter), bounded by MaxSeekReplay. A timestamp
# OLDER than the earliest retained message clamps to the earliest — it is NOT an
# error. See ../../README.md and ../../../SHARED-CONVENTIONS.md.
#
# Resource ids are uuid-suffixed (channel-isolation rule) so concurrent runs use
# distinct pubsub.{t} / pubsub.sub.{s} channels and never cross-talk; the
# topic/subscription are deleted on exit (best-effort).

require "logger"
require "securerandom"
require "google/cloud/pubsub"

# google-cloud-* logs every RPC at INFO/DEBUG; route it to /dev/null so the
# example's own progress is the only thing on stdout.
Google::Cloud::PubSub.configure { |c| c.logger = Logger.new(File::NULL) }

# Any id works — the connector parses but ignores the project segment.
def project_id
  ENV["PUBSUB_PROJECT_ID"] || ENV["GOOGLE_CLOUD_PROJECT"] || "my-project"
end

# uuid-suffix a resource id so parallel runs occupy private channels. Topic id
# 'seekable' -> 'seekable-<uuid8>'; the connector maps it to the Events Store log
# 'pubsub.seekable-<uuid8>'. Ids stay within the connector rules (3..255 chars,
# start with a letter, charset [A-Za-z0-9._~%+-]).
def rname(base)
  "#{base}-#{SecureRandom.hex(4)}"
end

def banner(title)
  puts "=== #{title} ==="
  puts "  emulator : #{ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")}"
  puts "  project  : #{project_id} (parsed but ignored by the connector)"
  puts "  auth     : none / insecure gRPC (emulator drop-in)"
  puts
end

# Assert an expected outcome; exit non-zero on failure. Examples are runnable
# PROOFS, not demos — a missing replay or wrong window must fail the process.
def check(condition, message)
  if condition
    puts "  [OK] #{message}"
  else
    warn "  [FAIL] #{message}"
    exit 1
  end
end

# Drain up to `want` messages from the subscription, acking each batch, until
# `want` are collected or the deadline passes. Returns the ordered bodies.
def drain(subscriber, want, timeout: 10)
  bodies = []
  deadline = Time.now + timeout
  while bodies.size < want && Time.now < deadline
    received = subscriber.pull immediate: true, max: 10
    if received.empty?
      sleep 0.2
      next
    end
    bodies.concat(received.map(&:data))
    subscriber.acknowledge(*received)
  end
  bodies
end

def main
  proj = project_id
  topic_id = rname("seekable")
  sub_id = rname("sub-seekable")
  banner("advanced/seek_timestamp — topic '#{topic_id}'")

  pubsub = Google::Cloud::PubSub.new(project_id: proj, emulator_host: ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085"))
  topic_admin = pubsub.topic_admin
  subscription_admin = pubsub.subscription_admin
  topic_path = pubsub.topic_path(topic_id)
  sub_path = pubsub.subscription_path(sub_id)

  begin
    # 1) CreateTopic -> Events Store log pubsub.{topic_id} (the replay source).
    created = topic_admin.create_topic name: topic_path
    puts "CreateTopic -> #{created.name}"
    check(created.name == topic_path, "created topic name matches the requested path")

    # 2) CreateSubscription -> Queue pubsub.sub.{sub_id} (default ack deadline 10s).
    sub = subscription_admin.create_subscription name: sub_path, topic: topic_path, ack_deadline_seconds: 10
    puts "CreateSubscription -> #{sub.name}  (ack_deadline=#{sub.ack_deadline_seconds}s)"

    publisher = pubsub.publisher(topic_id)
    subscriber = pubsub.subscriber(sub_id)

    # 3) Publish m1, then record a CHECKPOINT timestamp, then publish m2, m3.
    #    Seeking back to the checkpoint must replay m2,m3 (at/after it) but NOT m1.
    m1 = publisher.publish "m1"
    sleep 1.1 # ensure m1's publish_time is strictly < checkpoint
    checkpoint = Time.now.utc
    sleep 0.2
    m2 = publisher.publish "m2"
    m3 = publisher.publish "m3"
    puts "Publish -> m1=#{m1.message_id} m2=#{m2.message_id} m3=#{m3.message_id}"
    puts "Checkpoint -> #{checkpoint.iso8601(6)} (between m1 and m2)"
    check([m1, m2, m3].all? { |m| m.message_id && !m.message_id.empty? }, "all three publishes got server ids")

    # 4) First pass: drain & ack all three.
    first = drain(subscriber, 3)
    puts "Pull (first pass) -> #{first.inspect}"
    check(first.sort == %w[m1 m2 m3], "first pass delivers m1, m2, m3 then acks them")
    check(drain(subscriber, 1, timeout: 2).empty?, "subscription drained after first pass (no redelivery)")

    # 5) Seek the subscription back to the checkpoint. The connector resolves the
    #    first sequence at/after the timestamp, purges the queue, and replays the
    #    topic log from there into pubsub.sub.{sub_id}.
    subscription_admin.seek subscription: sub_path,
                            time: Google::Protobuf::Timestamp.new(seconds: checkpoint.to_i, nanos: checkpoint.nsec)
    puts "Seek(time=checkpoint) -> replaying pubsub.#{topic_id} from the checkpoint"

    # 6) Replay pass: only m2,m3 are at/after the checkpoint, so exactly those
    #    two are redelivered — m1 (before the checkpoint) is not.
    replay = drain(subscriber, 2)
    puts "Pull (replay pass) -> #{replay.inspect}"
    check(replay.sort == %w[m2 m3], "seek replays exactly m2,m3 (at/after the checkpoint); m1 excluded")

    # 7) Seek to a pre-window timestamp (1h before m1) — this CLAMPS to the
    #    earliest retained message, it is NOT an error, and replays from m1.
    pre_window = Time.now.utc - 3600
    subscription_admin.seek subscription: sub_path,
                            time: Google::Protobuf::Timestamp.new(seconds: pre_window.to_i)
    puts "Seek(time=now-1h) -> pre-window timestamp clamps to earliest retained (not an error)"
    clamped = drain(subscriber, 3)
    puts "Pull (clamped replay) -> #{clamped.inspect}"
    check(clamped.sort == %w[m1 m2 m3], "pre-window seek clamps and replays from the earliest retained message")

    puts "\nPASS — seek-to-timestamp replay complete."
  ensure
    # Best-effort cleanup so re-runs start clean.
    begin
      subscription_admin&.delete_subscription subscription: sub_path
    rescue Google::Cloud::Error
      nil
    end
    begin
      topic_admin&.delete_topic topic: topic_path
    rescue Google::Cloud::Error
      nil
    end
  end
end

main if $PROGRAM_NAME == __FILE__
