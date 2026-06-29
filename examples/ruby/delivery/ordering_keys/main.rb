# frozen_string_literal: true

# delivery/ordering_keys — per-key in-order delivery via `enable_message_ordering`.
#
# Flow:
#   CreateTopic -> CreateSubscription(enable_message_ordering:true)
#   -> Publish N messages on the SAME ordering_key
#   -> Pull + Acknowledge, asserting they arrive in publish order
#   -> also publish a second key to show keys are independent
#   -> DeleteSubscription -> DeleteTopic (best-effort cleanup)
#
# Connector behavior (spec S3.5 / docs 190-191): with `enable_message_ordering`,
# messages sharing an `ordering_key` are delivered in publish order with AT MOST
# ONE in flight per key; the head of a key blocks until acked, and redelivery is
# in order. Keyless messages are unordered. The ordering key rides the wire as the
# reserved tag `_pubsub_ordering_key` and is surfaced back as Message#ordering_key.
#
# Uses ONLY the official google-cloud-pubsub client. Run via bundler; see README.

require "logger"
require "securerandom"
require "google/cloud/pubsub"

# The gapic client logs every RPC at INFO/DEBUG to stderr by default; silence it
# so the example's own progress lines are the only output.
Google::Cloud::PubSub.configure { |config| config.logger = Logger.new(IO::NULL) }

# Unique-per-run resource names (connection-and-isolation.md §B): each run occupies
# its own `pubsub.{t}` / `pubsub.sub.{s}` channels so concurrent runs never collide.
SUFFIX = SecureRandom.hex(4)
TOPIC   = "ordering-keys-#{SUFFIX}".freeze
SUB     = "sub-ordering-keys-#{SUFFIX}".freeze
KEY_A   = "tenant-A"
KEY_B   = "tenant-B"
COUNT   = 5

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

# PUBSUB_EMULATOR_HOST makes the Google client skip auth and dial insecure gRPC.
pubsub = Google::Cloud::PubSub.new(
  project_id: ENV.fetch("PUBSUB_PROJECT_ID", "kubemq"),
  emulator_host: ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")
)
topic_admin = pubsub.topic_admin
sub_admin   = pubsub.subscription_admin

puts "[pubsub] emulator=#{ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")} " \
     "project=#{pubsub.project_id} (insecure / no-auth)"

begin
  topic_admin.create_topic name: pubsub.topic_path(TOPIC)
  puts "CreateTopic            -> #{TOPIC}"

  # enable_message_ordering on the SUBSCRIPTION turns on per-key ordering.
  sub_admin.create_subscription(
    name: pubsub.subscription_path(SUB),
    topic: pubsub.topic_path(TOPIC),
    enable_message_ordering: true,
    ack_deadline_seconds: 30
  )
  puts "CreateSubscription     -> #{SUB} (enable_message_ordering=true)"

  publisher = pubsub.publisher TOPIC
  # Required before publishing with an ordering_key on the publisher side.
  publisher.enable_message_ordering!

  # Publish COUNT messages on KEY_A in order, interleaved with KEY_B, to prove the
  # two keys are independent yet each key is internally ordered.
  expected_a = []
  expected_b = []
  COUNT.times do |i|
    a = "A-step-#{i}"
    b = "B-step-#{i}"
    publisher.publish a, ordering_key: KEY_A
    publisher.publish b, ordering_key: KEY_B
    expected_a << a
    expected_b << b
  end
  puts "Publish                -> #{COUNT} on #{KEY_A.inspect} + #{COUNT} on #{KEY_B.inspect} (interleaved)"

  subscriber = pubsub.subscriber SUB
  by_key = Hash.new { |h, k| h[k] = [] }
  deadline = Time.now + 20
  total = COUNT * 2
  while by_key.values.sum(&:size) < total && Time.now < deadline
    subscriber.pull(immediate: false, max: 100).each do |rm|
      by_key[rm.message.ordering_key] << rm.data
      rm.acknowledge!
    end
  end

  got_a = by_key[KEY_A]
  got_b = by_key[KEY_B]
  puts "Pull+Ack (#{KEY_A})   -> #{got_a.inspect}"
  puts "Pull+Ack (#{KEY_B})   -> #{got_b.inspect}"

  fail!("#{KEY_A} not delivered in publish order: #{got_a.inspect}") unless got_a == expected_a
  fail!("#{KEY_B} not delivered in publish order: #{got_b.inspect}") unless got_b == expected_b
  puts "Assert                 -> each key delivered in publish order (0 ordering violations)"
ensure
  # Best-effort cleanup so re-runs start clean; never fail the run if already gone.
  begin
    sub_admin.delete_subscription subscription: pubsub.subscription_path(SUB)
  rescue Google::Cloud::Error
    nil
  end
  begin
    topic_admin.delete_topic topic: pubsub.topic_path(TOPIC)
  rescue Google::Cloud::Error
    nil
  end
  puts "Cleanup                -> deleted subscription + topic"
end

puts "PASS: per-key in-order delivery; keys independent; zero ordering violations"
exit 0
