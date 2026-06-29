# frozen_string_literal: true

# KubeMQ Google Cloud Pub/Sub — topics: basic publish & subscribe
# (Ruby / google-cloud-pubsub).
#
# The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
#   CreateTopic -> CreateSubscription -> Publish -> Pull -> Acknowledge.
#
# Uses ONLY the official `google-cloud-pubsub` client (v3.x). The client is
# pointed at the connector by `PUBSUB_EMULATOR_HOST` (insecure gRPC, no Google
# auth) — zero code changes. The connector lives inside kubemq-server (default
# gRPC port 8085). See ../../README.md and ../../../SHARED-CONVENTIONS.md.
#
# Resource ids are uuid-suffixed (channel-isolation rule) so concurrent runs use
# distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never cross-talk; the
# topic/subscription are deleted on exit (best-effort).

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

  # uuid-suffix a resource id so parallel runs occupy private channels. Topic id
  # "orders" -> "orders-<uuid8>" (channel pubsub.orders-<uuid8>); stays within
  # the connector id rules (3..255 chars, starts with a letter, charset
  # [A-Za-z0-9._~%+-]); topic ids must NOT start with "sub.".
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
  # PROOFS, not demos — a missing message or a wrong body must fail the process.
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

def main
  proj       = Demo.project_id
  topic_id   = Demo.rname("orders")
  sub_id     = Demo.rname("sub-orders")
  body       = "hello from google-cloud-pubsub"

  Demo.banner("topics/basic_publish_subscribe — topic '#{topic_id}'")

  # v3.x: one client; admin plane via topic_admin/subscription_admin, data plane
  # via publisher/subscriber. The emulator host is passed explicitly.
  pubsub = Google::Cloud::PubSub.new(project_id: proj, emulator_host: Demo.emulator_host)

  topic_admin = pubsub.topic_admin
  sub_admin   = pubsub.subscription_admin
  topic_path  = pubsub.topic_path(topic_id)
  sub_path    = pubsub.subscription_path(sub_id)

  begin
    # 1) CreateTopic -> Events Store log pubsub.{topic_id}
    topic = topic_admin.create_topic(name: topic_path)
    Demo.step("CreateTopic -> #{topic.name}  (channel pubsub.#{topic_id})")
    Demo.check(topic.name == topic_path, "created topic name matches the requested path")

    # 2) CreateSubscription -> Queue pubsub.sub.{sub_id}, default ack deadline 10s
    sub = sub_admin.create_subscription(name: sub_path, topic: topic_path, ack_deadline_seconds: 10)
    Demo.step("CreateSubscription -> #{sub.name}  (channel pubsub.sub.#{sub_id})")
    Demo.step("                     ack_deadline=#{sub.ack_deadline_seconds}s")
    Demo.check(sub.ack_deadline_seconds == 10, "default ack deadline is the connector floor (10s)")

    # 3) Publish one message with a user attribute. The connector writes it once
    #    to the topic log, assigns a server message_id, then fans out one queue
    #    copy per subscription.
    publisher = pubsub.publisher(topic.name)
    msg = publisher.publish(body, source: "basic_publish_subscribe")
    message_id = msg.message_id
    Demo.sent("Publish -> message_id=#{message_id}")
    Demo.check(!message_id.to_s.empty?, "publish returned a server-assigned message id")

    # 4) Pull the message back from the per-subscription queue. immediate:false
    #    lets the connector wait briefly for the fan-out to land.
    subscriber = pubsub.subscriber(sub.name)
    received = subscriber.pull(immediate: false, max: 10)
    Demo.check(received.length == 1, "pulled exactly 1 message")

    rcv = received.first
    Demo.got("Pull -> data=#{rcv.data.inspect}  message_id=#{rcv.message_id}")
    puts "        published_at=#{rcv.published_at&.iso8601}  attributes=#{rcv.attributes.to_h.inspect}"

    # Assert the body, server id, attribute, and the server-stamped publish time.
    Demo.check(rcv.data == body, "pulled body equals the published body")
    Demo.check(rcv.message_id == message_id, "pulled message_id matches the publish id")
    Demo.check(!rcv.published_at.nil?, "server stamped a publish_time")
    # Reserved tags (_pubsub_*) are stripped for Pub/Sub clients; only the
    # caller's own attribute survives. Native Events Store consumers still see them.
    Demo.check(rcv.attributes.to_h == { "source" => "basic_publish_subscribe" },
               "attributes round-trip; reserved _pubsub_* tags are not exposed")

    # 5) Acknowledge by ack_id; the message leaves the subscription queue.
    rcv.acknowledge!
    Demo.step("Acknowledge -> 1 ack_id")

    drain = subscriber.pull(immediate: true, max: 10)
    Demo.check(drain.empty?, "subscription empty after ack (no redelivery)")

    puts "\nPASS — basic publish/subscribe round-trip complete."
  ensure
    # Best-effort cleanup: delete subscription then topic so re-runs start clean.
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
