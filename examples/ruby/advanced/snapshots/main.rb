# frozen_string_literal: true

# KubeMQ Google Cloud Pub/Sub — advanced: snapshots (CreateSnapshot → Seek-to-snapshot)
# (Ruby / google-cloud-pubsub).
#
# Demonstrates capturing a subscription's cursor as a snapshot and seeking back
# to it later without knowing an exact timestamp:
#   CreateTopic -> CreateSubscription -> CreateSnapshot (empty backlog)
#     -> Publish(m1,m2,m3) -> drain & ack all 3 -> Seek(snapshot) -> replay all 3.
#
# Uses ONLY the official `google-cloud-pubsub` client. With PUBSUB_EMULATOR_HOST
# set the client clears credentials, skips Google auth, and dials insecure gRPC
# at the connector — zero code changes. Control-plane RPCs (CreateTopic /
# CreateSubscription / CreateSnapshot / Seek / DeleteSnapshot) go through the
# admin clients; publish/pull use the data-plane publisher/subscriber.
#
# Connector cursor model (grounded in the connector, NOT Google's acked-state
# model): CreateSnapshot records the topic Events Store log's CURRENT last
# retained sequence as a conservative replay anchor; a later Seek(snapshot)
# re-drives the topic log FORWARD from that anchor. So the messages to replay
# are the ones published AFTER the snapshot — snapshot the (empty/drained)
# subscription FIRST, then publish, and seek-to-snapshot replays that whole
# batch even though it was already acked. Snapshots have a 7-day default expiry
# and are swept hourly. A snapshot of a detached subscription (its topic
# deleted) returns FAILED_PRECONDITION.
# See ../../README.md and ../../../SHARED-CONVENTIONS.md.
#
# Resource ids are uuid-suffixed (channel-isolation rule) so concurrent runs use
# distinct pubsub.{t} / pubsub.sub.{s} channels and never cross-talk; the
# topic/subscription/snapshot are deleted on exit (best-effort).

require "logger"
require "securerandom"
require "google/cloud/pubsub"

# google-cloud-* logs every RPC; route it to /dev/null so the example's own
# progress is the only thing on stdout.
Google::Cloud::PubSub.configure { |c| c.logger = Logger.new(File::NULL) }

# Any id works — the connector parses but ignores the project segment.
def project_id
  ENV["PUBSUB_PROJECT_ID"] || ENV["GOOGLE_CLOUD_PROJECT"] || "my-project"
end

# uuid-suffix a resource id so parallel runs occupy private channels. Topic id
# 'snapped' -> 'snapped-<uuid8>' (-> Events Store log 'pubsub.snapped-<uuid8>').
# Ids stay within the connector rules (3..255 chars, start with a letter).
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
  topic_id = rname("snapped")
  sub_id = rname("sub-snapped")
  snapshot_id = rname("snap")
  banner("advanced/snapshots — topic '#{topic_id}'")

  pubsub = Google::Cloud::PubSub.new(project_id: proj, emulator_host: ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085"))
  topic_admin = pubsub.topic_admin
  subscription_admin = pubsub.subscription_admin
  topic_path = pubsub.topic_path(topic_id)
  sub_path = pubsub.subscription_path(sub_id)
  snapshot_path = pubsub.snapshot_path(snapshot_id)

  snapshot_created = false

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

    # 3) CreateSnapshot on the FRESH (empty-backlog) subscription. The connector
    #    records the topic log's current last retained sequence as the replay
    #    anchor; a later Seek(snapshot) re-drives the log FORWARD from here, so
    #    everything published after this point is replayable.
    snap = subscription_admin.create_snapshot name: snapshot_path, subscription: sub_path
    snapshot_created = true
    puts "CreateSnapshot -> #{snap.name}  (expire_time=#{Time.at(snap.expire_time.seconds).utc.iso8601})"
    check(snap.name == snapshot_path, "snapshot anchored at the current cursor (default 7-day expiry)")

    # 4) Publish m1,m2,m3 AFTER the snapshot, then drain & ack all three.
    m1 = publisher.publish "m1"
    m2 = publisher.publish "m2"
    m3 = publisher.publish "m3"
    puts "Publish -> m1=#{m1.message_id} m2=#{m2.message_id} m3=#{m3.message_id}"
    first = drain(subscriber, 3)
    puts "Pull (live) -> #{first.inspect}"
    check(first.sort == %w[m1 m2 m3], "m1,m2,m3 delivered live then acked")
    check(drain(subscriber, 1, timeout: 2).empty?, "subscription drained (no redelivery before seek)")

    # 5) Seek BACK to the snapshot cursor. The connector purges the queue, drops
    #    leases, and replays the topic log from the captured anchor into
    #    pubsub.sub.{sub_id} — re-delivering the whole post-snapshot batch even
    #    though it was already acked.
    subscription_admin.seek subscription: sub_path, snapshot: snapshot_path
    puts "Seek(snapshot) -> rewind to the captured cursor; replay pubsub.#{topic_id}"
    replay = drain(subscriber, 3)
    puts "Pull (replay after seek-to-snapshot) -> #{replay.inspect}"
    check(replay.sort == %w[m1 m2 m3],
          "seek-to-snapshot replays the whole post-snapshot batch (m1,m2,m3) despite prior acks")

    puts "\nPASS — snapshot capture & seek-to-snapshot replay complete."
  ensure
    # Best-effort cleanup so re-runs start clean (snapshot → subscription → topic).
    if snapshot_created
      begin
        subscription_admin&.delete_snapshot snapshot: snapshot_path
      rescue Google::Cloud::Error
        nil
      end
    end
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
