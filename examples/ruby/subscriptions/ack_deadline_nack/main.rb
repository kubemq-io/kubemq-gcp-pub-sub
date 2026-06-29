# frozen_string_literal: true

# Ruby — subscriptions: ack-deadline + nack redelivery
#
# Flow: CreateTopic -> CreateSubscription(ack_deadline=10s) -> Publish ->
#       Pull #1 -> ModifyAckDeadline(+20s, extend the lease) ->
#       ModifyAckDeadline(0) (== nack) -> Pull #2 (redelivered) -> Acknowledge.
#
# Demonstrates the connector's ack-deadline lease semantics (spec S3.5):
#   * `ModifyAckDeadline(>0)` EXTENDS the per-message lease (10..600 s) so a slow
#     consumer keeps ownership instead of letting the message redeliver.
#   * `ModifyAckDeadline(0)` is an immediate NACK: the connector's 250 ms sweeper
#     expires the lease and redelivers the message on the next pull.
# The redelivery is proven by the SAME message_id coming back under a NEW ack_id
# whose embedded receive-count (`r`) has incremented 1 -> 2.
#
# Uses ONLY the official `google-cloud-pubsub` client. Unique-per-run resource
# ids (uuid-suffixed) keep parallel runs on private channels (spec S9).
#
# Run:
#   export PUBSUB_EMULATOR_HOST=localhost:8085
#   export PUBSUB_PROJECT_ID=my-project
#   bundle exec ruby subscriptions/ack_deadline_nack/main.rb

require "base64"
require "json"
require_relative "../../support/pubsub_client"

TOPIC_ID = PubsubClient.unique_id("redeliver")
SUB_ID = PubsubClient.unique_id("sub-redeliver")
ACK_DEADLINE = 10 # seconds; the subscription default (connector min is 10)

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

# The connector mints an opaque ack_id = base64(JSON{...,"r":receiveCount,...})
# (spec S3.5). Decode it ONLY to surface the receive count for the demo; clients
# must otherwise treat the ack_id as opaque.
def receive_count(ack_id)
  JSON.parse(Base64.urlsafe_decode64(ack_id))["r"]
rescue StandardError
  "?"
end

# Pull a single message, retrying briefly so we don't race the broker.
def pull_one(subscriber)
  deadline = Time.now + 10
  loop do
    msgs = subscriber.pull immediate: false, max: 1
    return msgs.first unless msgs.empty?
    return nil if Time.now > deadline
  end
end

pubsub = PubsubClient.connect
PubsubClient.banner

topic_admin = pubsub.topic_admin
sub_admin = pubsub.subscription_admin
topic_path = pubsub.topic_path(TOPIC_ID)
sub_path = pubsub.subscription_path(SUB_ID)

begin
  topic_admin.create_topic name: topic_path
  puts "CreateTopic         -> #{TOPIC_ID}"

  sub_admin.create_subscription name: sub_path, topic: topic_path, ack_deadline_seconds: ACK_DEADLINE
  puts "CreateSubscription  -> #{SUB_ID} (ack_deadline=#{ACK_DEADLINE}s)"

  publisher = pubsub.publisher(TOPIC_ID)
  publish_id = publisher.publish("process-me").message_id
  puts "Publish             -> message_id=#{publish_id}"

  subscriber = pubsub.subscriber(SUB_ID)

  first = pull_one(subscriber)
  fail!("no message on first pull") if first.nil?
  puts "Pull #1             -> data=#{first.data.inspect} receive_count=#{receive_count(first.ack_id)}"
  fail!("pull #1 returned a different message") unless first.message_id == publish_id

  # Extend the lease: tell the broker we need more time (the lease now expires
  # 20 s out instead of at the original 10 s deadline).
  first.modify_ack_deadline! 20
  puts "ModifyAckDeadline   -> 20s (lease extended; not yet acked)"

  # Now nack: ModifyAckDeadline(0) immediately releases the lease for redelivery.
  first.nack!
  puts "Nack (deadline=0)   -> released for redelivery"

  # The 250 ms sweeper expires the released lease; the message comes back.
  second = pull_one(subscriber)
  fail!("message was NOT redelivered after nack") if second.nil?
  rc2 = receive_count(second.ack_id)
  puts "Pull #2             -> data=#{second.data.inspect} receive_count=#{rc2} (redelivered)"
  fail!("redelivered a different message") unless second.message_id == publish_id
  fail!("expected receive_count to increment to 2, got #{rc2}") unless rc2 == 2
  fail!("redelivery must mint a fresh ack_id") if second.ack_id == first.ack_id

  # Ack for real this time so the message is removed from the queue.
  second.acknowledge!
  puts "Acknowledge         -> ok (removed from #{SUB_ID})"

  puts "PASS: nack redelivered the same message_id with receive_count 1 -> 2; ack removed it"
  exit 0
ensure
  # Best-effort cleanup so re-runs start clean (spec S9). Never fail the run.
  begin
    sub_admin.delete_subscription subscription: sub_path
  rescue StandardError
    nil
  end
  begin
    topic_admin.delete_topic topic: topic_path
  rescue StandardError
    nil
  end
end
