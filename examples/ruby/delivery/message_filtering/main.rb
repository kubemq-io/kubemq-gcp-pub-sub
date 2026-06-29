# frozen_string_literal: true

# delivery/message_filtering — a CEL-subset attribute filter set at subscription
# create time; non-matching messages are suppressed at publish fan-out.
#
# Flow:
#   CreateTopic
#   -> CreateSubscription(filter: 'attributes.region = "eu" AND attributes:tier')
#   -> Publish 3 messages: one matches, two do NOT
#   -> Pull + Acknowledge: only the matching message is delivered
#   -> show a malformed filter is rejected with INVALID_ARGUMENT
#   -> DeleteSubscription -> DeleteTopic (best-effort cleanup)
#
# Connector behavior (spec S3.5 / docs 203-213): the filter is a hand-written
# CEL-subset — attributes-only, <= 256 chars, compiled once at create (immutable),
# applied at publish fan-out. A filtered-out message is never enqueued (~ auto-
# acked); it does NOT count against delivery attempts. Supported forms:
#   attributes:KEY                       -- KEY exists
#   attributes.KEY = "v"  /  != "v"      -- equality / inequality
#   hasPrefix(attributes.KEY, "p")       -- prefix match
#   AND  OR  NOT  + parentheses
# A malformed filter -> INVALID_ARGUMENT.
#
# Uses ONLY the official google-cloud-pubsub client. Run via bundler; see README.

require "logger"
require "securerandom"
require "google/cloud/pubsub"

Google::Cloud::PubSub.configure { |config| config.logger = Logger.new(IO::NULL) }

SUFFIX = SecureRandom.hex(4)
TOPIC  = "message-filtering-#{SUFFIX}".freeze
SUB    = "sub-message-filtering-#{SUFFIX}".freeze
FILTER = 'attributes.region = "eu" AND attributes:tier'

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

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

  sub_admin.create_subscription(
    name: pubsub.subscription_path(SUB),
    topic: pubsub.topic_path(TOPIC),
    filter: FILTER,
    ack_deadline_seconds: 30
  )
  puts "CreateSubscription     -> #{SUB}"
  puts "  filter               -> #{FILTER}"

  publisher = pubsub.publisher TOPIC

  # Exactly one message satisfies the filter (region=eu AND a tier attribute set).
  match     = { "region" => "eu", "tier" => "gold" }
  wrong_reg = { "region" => "us", "tier" => "gold" } # region mismatch
  no_tier   = { "region" => "eu" }                   # missing tier attribute

  publisher.publish "delivered", match
  publisher.publish "suppressed-region", wrong_reg
  publisher.publish "suppressed-no-tier", no_tier
  puts "Publish                -> 1 matching + 2 non-matching"

  subscriber = pubsub.subscriber SUB
  received = []
  deadline = Time.now + 12
  while received.empty? && Time.now < deadline
    subscriber.pull(immediate: false, max: 100).each do |rm|
      received << rm.data
      rm.acknowledge!
    end
  end
  puts "Pull+Ack               -> #{received.inspect}"

  # Drain once more to PROVE the suppressed messages were never enqueued.
  extra = subscriber.pull(immediate: false, max: 100)
  extra.each(&:acknowledge!)

  fail!("expected exactly 1 delivered, got #{received.inspect}") unless received == ["delivered"]
  fail!("filtered-out messages leaked: #{extra.map(&:data).inspect}") unless extra.empty?
  puts "Assert                 -> 2 non-matching messages suppressed (0 delivered)"

  # Negative path: a malformed filter must be rejected with INVALID_ARGUMENT.
  bad_sub = "sub-bad-filter-#{SUFFIX}"
  begin
    sub_admin.create_subscription(
      name: pubsub.subscription_path(bad_sub),
      topic: pubsub.topic_path(TOPIC),
      filter: "this is not a valid CEL filter"
    )
    fail!("malformed filter was NOT rejected")
  rescue Google::Cloud::InvalidArgumentError => e
    puts "Malformed filter       -> rejected INVALID_ARGUMENT (#{e.message.split("\n").first})"
  end
ensure
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

puts "PASS: only the matching message delivered; non-matching suppressed; malformed filter rejected"
exit 0
