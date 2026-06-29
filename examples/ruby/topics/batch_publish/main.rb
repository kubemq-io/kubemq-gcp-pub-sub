# frozen_string_literal: true

# KubeMQ Google Cloud Pub/Sub — topics: batch publish
# (Ruby / google-cloud-pubsub).
#
# One Publish RPC carrying many messages (1..1000). The connector validates the
# WHOLE batch before enqueuing anything (publish atomicity: ≤1000 msgs, ≤10 MiB/
# msg, ≤100 attrs; any single invalid message rejects the whole batch with
# INVALID_ARGUMENT and enqueues nothing), then returns one server message_id per
# message IN REQUEST ORDER. We publish a 5-message batch via the synchronous
# block form of `publisher.publish`, assert the returned ids line up with the
# request order, then pull all five back and confirm every body/id round-trips.
#
# Uses ONLY the official `google-cloud-pubsub` client (v3.x), pointed at the
# connector by `PUBSUB_EMULATOR_HOST` (insecure gRPC, no Google auth). Resource
# ids are uuid-suffixed (channel-isolation rule) so concurrent runs use distinct
# `pubsub.{t}` / `pubsub.sub.{s}` channels and never cross-talk; the topic and
# subscription are deleted on exit (best-effort).
# See ../../README.md and ../../../SHARED-CONVENTIONS.md.

# Silence the google-cloud SDK's structured request/response logger so the
# example's own progress output is clean. This is the documented gapic switch
# (gapic-common logging_concerns); `||=` keeps any explicit user override.
ENV["GOOGLE_SDK_RUBY_LOGGING_GEMS"] ||= "none"

require "securerandom"
require "google/cloud/pubsub"

# ─── Demo helpers (inlined per example — the examples carry no shared lib) ─────
module Demo
  module_function

  # The connector parses but ignores the project segment; any id works.
  def project_id
    ENV["PUBSUB_PROJECT_ID"] || ENV["GOOGLE_CLOUD_PROJECT"] || "my-project"
  end

  # The standard Google env var IS the contract: when set, the client clears
  # credentials, skips auth, and dials insecure gRPC at this host.
  def emulator_host
    ENV["PUBSUB_EMULATOR_HOST"] || "localhost:8085"
  end

  # uuid-suffix a resource id so parallel runs occupy private channels (channel
  # pubsub.events-<uuid8>); stays within the connector id rules (3..255 chars,
  # starts with a letter, charset [A-Za-z0-9._~%+-]); topic ids must NOT start
  # with "sub.".
  def rname(base)
    "#{base}-#{SecureRandom.hex(4)}"
  end

  def banner(title)
    puts "=== #{title} ==="
    puts "  emulator : #{emulator_host}"
    puts "  project  : #{project_id} (parsed but ignored by the connector)"
    puts "  auth     : none / insecure gRPC (emulator drop-in)"
    puts
  end

  def step(msg) = puts "[*] #{msg}"
  def sent(msg) = puts "[x] #{msg}"
  def got(msg)  = puts "[v] #{msg}"

  # Assert an expected outcome; exit non-zero on failure. Examples are runnable
  # PROOFS, not demos — a wrong id ordering or a missing message must fail.
  def check(condition, msg)
    if condition
      puts "  [OK] #{msg}"
    else
      warn "  [FAIL] #{msg}"
      raise Failure, msg
    end
  end

  # Best-effort cleanup that never fails the run if the resource is already gone.
  def best_effort(label)
    yield
    step("Cleaned up #{label}")
  rescue StandardError => e
    warn "  [warn] cleanup skipped (#{label}): #{e.class}: #{e.message}"
  end

  class Failure < StandardError; end
end

BATCH_SIZE = 5

def main
  proj      = Demo.project_id
  topic_id  = Demo.rname("events")
  sub_id    = Demo.rname("sub-events")

  Demo.banner("topics/batch_publish — topic '#{topic_id}' (#{BATCH_SIZE} messages, one RPC)")

  pubsub      = Google::Cloud::PubSub.new(project_id: proj, emulator_host: Demo.emulator_host)
  topic_admin = pubsub.topic_admin
  sub_admin   = pubsub.subscription_admin
  topic_path  = pubsub.topic_path(topic_id)
  sub_path    = pubsub.subscription_path(sub_id)

  begin
    topic = topic_admin.create_topic(name: topic_path)
    Demo.step("CreateTopic -> #{topic.name}  (channel pubsub.#{topic_id})")

    sub = sub_admin.create_subscription(name: sub_path, topic: topic_path, ack_deadline_seconds: 10)
    Demo.step("CreateSubscription -> #{sub.name}  (channel pubsub.sub.#{sub_id})")

    # Build the bodies up front; each carries a 1-based `seq` attribute so we can
    # verify request-order id correspondence.
    bodies = (1..BATCH_SIZE).map { |i| "order ##{1000 + i}" }

    # ONE Publish RPC for the whole batch (synchronous block form). The block
    # collects messages; `publish` returns an Array<Message> with the ids
    # assigned by the connector IN REQUEST ORDER (ids[i] is the id of bodies[i]).
    publisher = pubsub.publisher(topic.name)
    published = publisher.publish do |batch|
      bodies.each_with_index { |b, i| batch.publish(b, seq: (i + 1).to_s) }
    end
    Demo.sent("Publish -> #{published.length} messages in one batch RPC")
    Demo.check(published.length == BATCH_SIZE, "got #{BATCH_SIZE} message ids back")

    # ids come back in request order: build a body->id map and assert each id is
    # non-empty and lines up with the body at the same index.
    id_by_body = {}
    published.each_with_index do |m, i|
      Demo.check(!m.message_id.to_s.empty?, "message #{i} got a server-assigned id")
      Demo.check(m.data == bodies[i], "published[#{i}] body is in request order")
      id_by_body[m.data] = m.message_id
      Demo.step("  ids[#{i}] = #{m.message_id}  <->  #{m.data.inspect}")
    end

    # Pull all five back (one Pull may return fewer than requested, so loop until
    # we have the whole batch or the deadline passes). Ack as we go.
    subscriber = pubsub.subscriber(sub.name)
    pulled = []
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
    while pulled.length < BATCH_SIZE && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      batch = subscriber.pull(immediate: false, max: BATCH_SIZE)
      next if batch.empty?

      pulled.concat(batch)
      subscriber.acknowledge(*batch)
    end

    Demo.got("Pulled and acked #{pulled.length} messages")
    Demo.check(pulled.length == BATCH_SIZE, "pulled all #{BATCH_SIZE} messages")

    # Every published body must come back with the matching server id assigned in
    # the Publish response (id <-> body correspondence; queue order is not
    # guaranteed, so match on body).
    pulled.each do |rcv|
      Demo.check(id_by_body.key?(rcv.data), "pulled a known body #{rcv.data.inspect}")
      Demo.check(rcv.message_id == id_by_body[rcv.data], "message_id matches for #{rcv.data.inspect}")
    end

    puts "\nPASS — batch publish of #{BATCH_SIZE} messages complete (ids returned in request order)."
  ensure
    Demo.best_effort("subscription '#{sub_id}'") { sub_admin.delete_subscription(subscription: sub_path) }
    Demo.best_effort("topic '#{topic_id}'") { topic_admin.delete_topic(topic: topic_path) }
  end
end

begin
  main
  exit 0
rescue Demo::Failure
  exit 1
rescue StandardError => e
  warn "[ERROR] #{e.class}: #{e.message}"
  exit 1
end
