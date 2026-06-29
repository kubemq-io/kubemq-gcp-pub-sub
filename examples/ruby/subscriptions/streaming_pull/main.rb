# frozen_string_literal: true

# Ruby — subscriptions: streaming pull with flow control
#
# Flow: CreateTopic -> CreateSubscription -> Publish N -> listen (StreamingPull) ->
#       drain all N via the async callback (ack each) -> stop! gracefully.
#
# Demonstrates the connector's bidirectional StreamingPull delivery path
# (spec S3, capability matrix): `Subscriber#listen` opens a long-lived
# StreamingPull stream and pushes messages to a callback as they arrive, bounded
# by client-side FLOW CONTROL (`inventory: { max_outstanding_messages: ... }`) so
# the consumer is never overwhelmed. The connector periodically closes the stream
# (`StreamCloseSeconds`, default 1800 s) to force a transparent SDK reconnect —
# the official client re-establishes the stream automatically, so a long-running
# listener survives it with no message loss.
#
# Uses ONLY the official `google-cloud-pubsub` client. Unique-per-run resource
# ids (uuid-suffixed) keep parallel runs on private channels (spec S9).
#
# Run:
#   export PUBSUB_EMULATOR_HOST=localhost:8085
#   export PUBSUB_PROJECT_ID=my-project
#   bundle exec ruby subscriptions/streaming_pull/main.rb

require "monitor"
require_relative "../../support/pubsub_client"

TOPIC_ID = PubsubClient.unique_id("stream")
SUB_ID = PubsubClient.unique_id("sub-stream")
MESSAGE_COUNT = 5
# Flow-control ceiling on un-acked messages held by this client at once. Kept
# above MESSAGE_COUNT here so the whole batch drains in one pass; lower it below
# the batch size to watch the client throttle intake to the window.
FLOW_CONTROL_MAX = 10

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
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

  sub_admin.create_subscription name: sub_path, topic: topic_path
  puts "CreateSubscription  -> #{SUB_ID}"

  publisher = pubsub.publisher(TOPIC_ID)
  expected = (1..MESSAGE_COUNT).map { |i| "stream-msg-#{i}" }
  expected.each { |body| publisher.publish(body) }
  puts "Publish             -> #{MESSAGE_COUNT} messages"

  # Thread-safe collection: the listen callback runs on background threads.
  received = []
  collector = Monitor.new
  done = collector.new_cond

  subscriber = pubsub.subscriber(SUB_ID)
  listener = subscriber.listen(inventory: { max_outstanding_messages: FLOW_CONTROL_MAX }) do |message|
    message.acknowledge!
    collector.synchronize do
      received << message.data
      done.signal if received.size >= MESSAGE_COUNT
    end
  end

  # Surface stream errors instead of swallowing them (official on_error hook).
  listener.on_error { |error| warn "stream error: #{error.class}: #{error.message}" }

  puts "StreamingPull       -> listening (flow control: max #{FLOW_CONTROL_MAX} outstanding)"
  listener.start

  # Wait (bounded) for all N to arrive over the stream, then stop gracefully.
  collector.synchronize do
    done.wait(30) if received.size < MESSAGE_COUNT
  end
  listener.stop!

  drained = collector.synchronize { received.dup }
  puts "Drained             -> #{drained.size} messages acked over the stream"
  fail!("expected #{MESSAGE_COUNT} messages, drained #{drained.size}") unless drained.size == MESSAGE_COUNT
  fail!("stream delivered an unexpected payload set") unless drained.sort == expected.sort

  puts "Stop                -> listener stopped cleanly"
  puts "PASS: StreamingPull delivered all #{MESSAGE_COUNT} messages under flow control and acked them"
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
