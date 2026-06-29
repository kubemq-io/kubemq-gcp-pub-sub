# frozen_string_literal: true

# delivery/exactly_once — `enable_exactly_once_delivery`: acknowledge returns a
# confirmation (AcknowledgeResult), and a stale/invalid ack_id is rejected with
# FAILED_PRECONDITION + ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID).
#
# Flow:
#   CreateTopic -> CreateSubscription(enable_exactly_once_delivery:true)
#   -> confirm the subscription reports exactly-once enabled
#   -> Publish -> Pull -> Acknowledge with a result block (assert status SUCCESS)
#   -> re-ack the SAME (now-acked) ack_id (assert FAILED_PRECONDITION raised)
#   -> DeleteSubscription -> DeleteTopic (best-effort cleanup)
#
# Connector behavior (spec S3.5 / docs 178-185, 320-321): with
# `enable_exactly_once_delivery`, a successful unary Acknowledge confirms the ack;
# an unparseable/expired/unknown ack_id returns a FAILED_PRECONDITION status
# carrying ErrorInfo{reason: PERMANENT_FAILURE_INVALID_ACK_ID} (the REAL Google SDK
# contract, NOT a literal INVALID_ARGUMENT). The google-cloud-pubsub client maps
# the success to an AcknowledgeResult(status: SUCCESS) yielded to the ack block,
# and surfaces the invalid-ack as a Google::Cloud::FailedPreconditionError.
#
# GOTCHA — exactly-once is NODE-LOCAL: an ack_id minted on one node is invalid on
# another (the token carries the minting node's id). In a cluster, pin a
# subscription's StreamingPull/ack traffic to one node (sticky LB), or accept
# at-least-once across nodes. This example runs against a single connector instance.
#
# Uses ONLY the official google-cloud-pubsub client. Run via bundler; see README.

require "logger"
require "securerandom"
require "google/cloud/pubsub"

Google::Cloud::PubSub.configure { |config| config.logger = Logger.new(IO::NULL) }

SUFFIX = SecureRandom.hex(4)
TOPIC  = "exactly-once-#{SUFFIX}".freeze
SUB    = "sub-exactly-once-#{SUFFIX}".freeze

# Human-readable name for an AcknowledgeResult status constant.
def status_name(status)
  Google::Cloud::PubSub::AcknowledgeResult.constants.find do |c|
    Google::Cloud::PubSub::AcknowledgeResult.const_get(c) == status
  end
end

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

  created = sub_admin.create_subscription(
    name: pubsub.subscription_path(SUB),
    topic: pubsub.topic_path(TOPIC),
    enable_exactly_once_delivery: true,
    ack_deadline_seconds: 10
  )
  puts "CreateSubscription     -> #{SUB} (enable_exactly_once_delivery=#{created.enable_exactly_once_delivery})"
  fail!("exactly-once was not enabled on the subscription") unless created.enable_exactly_once_delivery

  publisher = pubsub.publisher TOPIC
  msg_id = publisher.publish "exactly-once-payload", { "n" => "1" }
  puts "Publish                -> id=#{msg_id.message_id}"

  subscriber = pubsub.subscriber SUB
  received = []
  deadline = Time.now + 12
  received = subscriber.pull(immediate: false, max: 10) while received.empty? && Time.now < deadline
  fail!("no message pulled") if received.empty?
  message = received.first
  puts "Pull                   -> data=#{message.data.inspect} (1 message)"

  # Successful ack on an EOS subscription -> AcknowledgeResult(status: SUCCESS).
  ack_status = nil
  message.acknowledge! { |result| ack_status = result.status }
  puts "Acknowledge            -> confirmation status=#{status_name(ack_status)}"
  fail!("ack confirmation was not SUCCESS: #{status_name(ack_status)}") unless
    ack_status == Google::Cloud::PubSub::AcknowledgeResult::SUCCESS

  # Re-ack the SAME ack_id: it is now stale -> FAILED_PRECONDITION + ErrorInfo
  # (PERMANENT_FAILURE_INVALID_ACK_ID), surfaced as FailedPreconditionError.
  stale_rejected = false
  begin
    message.acknowledge!
    fail!("re-acking a stale ack_id was NOT rejected")
  rescue Google::Cloud::FailedPreconditionError => e
    stale_rejected = true
    puts "Re-ack (stale id)      -> rejected FAILED_PRECONDITION " \
         "(ErrorInfo: PERMANENT_FAILURE_INVALID_ACK_ID) [#{e.class}]"
  end
  fail!("stale ack was not rejected with FAILED_PRECONDITION") unless stale_rejected
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

puts "PASS: exactly-once ack confirmed SUCCESS; stale ack_id rejected FAILED_PRECONDITION"
exit 0
