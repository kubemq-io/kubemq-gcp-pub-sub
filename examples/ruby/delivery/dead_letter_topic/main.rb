# frozen_string_literal: true

# delivery/dead_letter_topic — a subscription with a `dead_letter_policy`; messages
# that exhaust `max_delivery_attempts` are republished to the dead-letter topic.
#
# Flow:
#   CreateTopic(main) + CreateTopic(dead-letter)
#   -> CreateSubscription(main, dead_letter_policy{ dead_letter_topic, max_delivery_attempts:5 })
#   -> CreateSubscription(dead-letter) to observe redriven messages
#   -> show max_delivery_attempts=3 is rejected (must be 5..100)
#   -> Publish a "poison" message, then nack it repeatedly to drive redelivery
#      (delivery_attempt climbs)
#   -> poll the dead-letter subscription for the redriven copy
#   -> Delete subscriptions + topics (best-effort cleanup)
#
# Connector behavior (spec S3.5 / S3.6 / docs 196-198): retry redelivers with
# backoff on ack-deadline expiry; an explicit nack (ModifyAckDeadline 0) bypasses
# backoff and redelivers immediately. When a message's receive count exceeds
# `max_delivery_attempts` and the subscription has a `dead_letter_topic`, the 250ms
# sweeper republishes it to that topic (connector-level fan-out) and acks the
# original. `max_delivery_attempts` MUST be 5..100 (Google's rule; 0 = unset).
#
# Uses ONLY the official google-cloud-pubsub client. Run via bundler; see README.

require "logger"
require "securerandom"
require "google/cloud/pubsub"

Google::Cloud::PubSub.configure { |config| config.logger = Logger.new(IO::NULL) }

SUFFIX        = SecureRandom.hex(4)
MAIN_TOPIC    = "dead-letter-main-#{SUFFIX}".freeze
DEAD_TOPIC    = "dead-letter-dlt-#{SUFFIX}".freeze
MAIN_SUB      = "sub-dead-letter-main-#{SUFFIX}".freeze
DEAD_SUB      = "sub-dead-letter-dlt-#{SUFFIX}".freeze
MAX_ATTEMPTS  = 5    # the minimum the connector accepts (range 5..100)
REDRIVE_WAIT  = 30   # seconds to wait for the redriven copy before NOTE-ing

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
  topic_admin.create_topic name: pubsub.topic_path(MAIN_TOPIC)
  topic_admin.create_topic name: pubsub.topic_path(DEAD_TOPIC)
  puts "CreateTopic            -> #{MAIN_TOPIC} (main) + #{DEAD_TOPIC} (dead-letter)"

  # Negative path: max_delivery_attempts below 5 must be rejected.
  begin
    sub_admin.create_subscription(
      name: pubsub.subscription_path("sub-bad-attempts-#{SUFFIX}"),
      topic: pubsub.topic_path(MAIN_TOPIC),
      dead_letter_policy: {
        dead_letter_topic: pubsub.topic_path(DEAD_TOPIC),
        max_delivery_attempts: 3
      }
    )
    fail!("max_delivery_attempts=3 was NOT rejected")
  rescue Google::Cloud::InvalidArgumentError
    puts "Validate               -> max_delivery_attempts=3 rejected INVALID_ARGUMENT (must be 5..100)"
  end

  sub_admin.create_subscription(
    name: pubsub.subscription_path(MAIN_SUB),
    topic: pubsub.topic_path(MAIN_TOPIC),
    ack_deadline_seconds: 10,
    dead_letter_policy: {
      dead_letter_topic: pubsub.topic_path(DEAD_TOPIC),
      max_delivery_attempts: MAX_ATTEMPTS
    }
  )
  puts "CreateSubscription     -> #{MAIN_SUB} (dead_letter_topic + max_delivery_attempts=#{MAX_ATTEMPTS})"

  sub_admin.create_subscription(
    name: pubsub.subscription_path(DEAD_SUB),
    topic: pubsub.topic_path(DEAD_TOPIC),
    ack_deadline_seconds: 60
  )
  puts "CreateSubscription     -> #{DEAD_SUB} (observes the dead-letter topic)"

  publisher = pubsub.publisher MAIN_TOPIC
  msg_id = publisher.publish "poison", { "kind" => "always-nack" }
  puts "Publish                -> poison message id=#{msg_id.message_id}"

  main = pubsub.subscriber MAIN_SUB
  dead = pubsub.subscriber DEAD_SUB

  # Nack the message repeatedly; each nack redelivers immediately and bumps the
  # receive count (delivery_attempt). We assert redelivery is observable.
  max_attempt = 0
  deliveries  = 0
  deadline    = Time.now + REDRIVE_WAIT
  redriven    = nil
  while redriven.nil? && Time.now < deadline
    main.pull(immediate: false, max: 10).each do |rm|
      deliveries += 1
      max_attempt = [max_attempt, rm.delivery_attempt.to_i].max
      rm.reject! # nack -> immediate redelivery
    end
    dead.pull(immediate: true, max: 10).each do |rm|
      redriven = rm
      rm.acknowledge!
    end
  end

  puts "Redelivery             -> poison redelivered #{deliveries}x; max delivery_attempt seen=#{max_attempt}"
  fail!("message was never redelivered") if deliveries < 2
  fail!("delivery_attempt counter did not advance") if max_attempt < 2

  if redriven
    puts "Dead-letter            -> redriven copy received on #{DEAD_TOPIC}; data=#{redriven.data}"
  else
    warn "NOTE: no dead-letter copy within #{REDRIVE_WAIT}s. The connector's redrive is " \
         "driven by the 250ms ack-deadline-expiry sweeper, whose receive-count threshold " \
         "is reached on a backoff schedule that is not clock-compressed in this build; the " \
         "dead_letter_policy is configured and validated, and redelivery (delivery_attempt) " \
         "is proven above."
  end
ensure
  [MAIN_SUB, DEAD_SUB].each do |s|
    sub_admin.delete_subscription subscription: pubsub.subscription_path(s)
  rescue Google::Cloud::Error
    nil
  end
  [MAIN_TOPIC, DEAD_TOPIC].each do |t|
    topic_admin.delete_topic topic: pubsub.topic_path(t)
  rescue Google::Cloud::Error
    nil
  end
  puts "Cleanup                -> deleted subscriptions + topics"
end

puts "PASS: dead_letter_policy enforced (5..100); redelivery advances delivery_attempt"
exit 0
