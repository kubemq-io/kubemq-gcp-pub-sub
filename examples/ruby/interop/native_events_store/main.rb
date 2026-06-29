# frozen_string_literal: true

# ─── interop 15: native_events_store ───────────────────────────────────────────
#
# The ONLY example in this repo that imports a KubeMQ SDK. It demonstrates the
# cross-protocol bridge: a message published with the official Google Cloud
# Pub/Sub client (`google-cloud-pubsub`) to topic `orders` is consumed NATIVELY
# from the Events Store log `pubsub.orders` via the native KubeMQ gRPC SDK (gem
# `kubemq`) — with the three reserved tags (_pubsub_message_id,
# _pubsub_publish_time, _pubsub_ordering_key) visible.
#
# Two endpoints are used:
#   - Pub/Sub half: PUBSUB_EMULATOR_HOST (default localhost:8085) — the connector.
#     The Google client honours the env var, skips Google auth, and uses insecure
#     gRPC. Zero code changes — passed explicitly here via `emulator_host:` so the
#     intent is obvious in the example.
#   - Native half:  KUBEMQ_GRPC_ADDRESS  (default localhost:50000) — native broker.
#
# Deterministic ordering (subscribe-before-publish, startAt = "new"): we open the
# native Events Store subscribe stream with START_NEW_ONLY and confirm it is open
# BEFORE issuing the Pub/Sub publish, so the published message is guaranteed
# in-window for the native consumer (no startup race). See spec S5.7 +
# docs/concepts/cross-protocol-interop.md.

# Keep the demonstration's stdout focused: the Google client's GAPIC layer logs
# every RPC at INFO/DEBUG when this env var opts in. Default it to "none" so the
# example output is clean; set it to "all"/"pubsub" before running to debug RPCs.
ENV["GOOGLE_SDK_RUBY_LOGGING_GEMS"] ||= "none"

require "securerandom"
require "timeout"
require "google/cloud/pubsub"
require "kubemq"

# Reserved tags the connector stamps on every message (stripped from `attributes`
# for Pub/Sub clients, visible to native Events Store consumers). See spec S3.4.
TAG_MESSAGE_ID = "_pubsub_message_id"
TAG_PUBLISH_TIME = "_pubsub_publish_time"
TAG_ORDERING_KEY = "_pubsub_ordering_key"

@exit_code = 0

def fail!(why)
  warn "[FAIL] #{why}"
  @exit_code = 1
end

# Channel isolation: uuid-suffix the topic id so concurrent runs (other languages,
# or /examples + /burnin) occupy distinct channels and never cross-talk. The native
# side derives its `pubsub.{t}` channel name from the SAME generated topic id.
suffix = SecureRandom.hex(4)
project_id = ENV.fetch("PUBSUB_PROJECT_ID", "my-project")
topic_id = "orders-#{suffix}"            # Pub/Sub topic id (canonical demo name: orders)
native_channel = "pubsub.#{topic_id}"    # Events Store log it maps to
emulator_host = ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")
grpc_address = ENV.fetch("KUBEMQ_GRPC_ADDRESS", "localhost:50000")

# The Pub/Sub payload + the attributes we expect to surface natively as tags.
payload = "order #4242 — cross-protocol via Pub/Sub"
ordering_key = "customer-42"
attributes = { "region" => "eu-west-1", "priority" => "high" }

puts "[*] topic id            : #{topic_id}  (Events Store channel '#{native_channel}')"
puts "[*] PUBSUB_EMULATOR_HOST: #{emulator_host}"
puts "[*] native gRPC broker  : #{grpc_address}"

# ── Pub/Sub half: build the project client (emulator drop-in) ──────────────────
# The Google client honours PUBSUB_EMULATOR_HOST automatically; we pass it
# explicitly so the example is self-documenting. With an emulator host set the
# client clears credentials and uses an insecure channel. The project id is parsed
# but ignored by the connector (single-tenant).
pubsub = Google::Cloud::PubSub.new(project_id: project_id, emulator_host: emulator_host)
topic_path = pubsub.topic_path(topic_id)

pubsub.topic_admin.create_topic name: topic_path
puts "[*] CreateTopic '#{topic_id}' (project '#{project_id}' parsed but ignored)"

# ── Native half: connect the KubeMQ gRPC client ────────────────────────────────
# Constructing PubSubClient eagerly opens the gRPC channel to localhost:50000.
kube = KubeMQ::PubSubClient.new(
  address: grpc_address,
  client_id: "gcp-pubsub-interop-ruby-#{suffix}"
)
cancel = KubeMQ::CancellationToken.new
received = Queue.new # thread-safe hand-off from the background subscribe thread

begin
  puts "[*] Connected native KubeMQ client to #{grpc_address}"

  # Subscribe-before-publish with startAt = "new": establish the Events Store
  # subscribe stream FIRST. START_NEW_ONLY (enum value 1) receives only events
  # published after the subscription is established. The subscribe runs on a
  # background thread and hands the first event to `received`.
  subscription = KubeMQ::PubSub::EventsStoreSubscription.new(
    channel: native_channel,
    start_position: KubeMQ::PubSub::EventStoreStartPosition::START_NEW_ONLY
  )
  handle = kube.subscribe_to_events_store(
    subscription,
    cancellation_token: cancel,
    on_error: ->(e) { warn "[!] subscribe error: #{e.message}" }
  ) do |event|
    received.push(event)
  end

  # Confirm the subscribe stream is open before publishing. The stream is
  # established on the background thread's first gRPC read; give it a brief,
  # bounded readiness window so the published message lands in-window.
  sleep 0.75
  fail!("native Events Store subscribe thread is not active") unless handle.active?
  puts "[*] Events Store subscribe stream open on '#{native_channel}' (startAt=new)"

  # ── Pub/Sub publish (the producer never speaks KubeMQ) ───────────────────────
  publisher = pubsub.publisher(topic_id)
  publisher.enable_message_ordering! # required before publishing with an ordering_key
  message = publisher.publish payload, attributes, ordering_key: ordering_key
  message_id = message.message_id
  puts "[>] Pub/Sub Publish to '#{topic_id}' → messageId=#{message_id}"

  # ── Await exactly one native event and verify the bridge ─────────────────────
  event = nil
  begin
    event = Timeout.timeout(15) { received.pop }
  rescue Timeout::Error
    fail!("native Events Store consumer received no event on '#{native_channel}' within 15s")
  end

  if event
    # The native body arrives as raw bytes (ASCII-8BIT); the connector stores the
    # Pub/Sub `data` verbatim. Force UTF-8 so the round-tripped string (incl. the
    # em dash) compares and prints cleanly.
    body = event.body.to_s.dup.force_encoding(Encoding::UTF_8)
    puts "[<] native Events Store received on '#{event.channel}' (seq #{event.sequence}) → '#{body}'"

    fail!("native body mismatch: expected '#{payload}', got '#{body}'") unless body == payload

    tags = event.tags || {}

    # The three reserved tags must be present for the native consumer.
    [TAG_MESSAGE_ID, TAG_PUBLISH_TIME, TAG_ORDERING_KEY].each do |reserved|
      fail!("reserved tag '#{reserved}' missing from native message") unless tags.key?(reserved)
    end

    if tags.key?(TAG_MESSAGE_ID)
      native_msg_id = tags[TAG_MESSAGE_ID]
      puts "[v] reserved tag #{TAG_MESSAGE_ID}   = #{native_msg_id}"
      fail!("#{TAG_MESSAGE_ID} '#{native_msg_id}' != Pub/Sub messageId '#{message_id}'") if native_msg_id != message_id
    end

    puts "[v] reserved tag #{TAG_PUBLISH_TIME} = #{tags[TAG_PUBLISH_TIME]}" if tags.key?(TAG_PUBLISH_TIME)

    if tags.key?(TAG_ORDERING_KEY)
      native_ordering_key = tags[TAG_ORDERING_KEY]
      puts "[v] reserved tag #{TAG_ORDERING_KEY} = #{native_ordering_key}"
      if native_ordering_key != ordering_key
        fail!("#{TAG_ORDERING_KEY} '#{native_ordering_key}' != published ordering key '#{ordering_key}'")
      end
    end

    # The Pub/Sub attributes ride along as ordinary tags.
    attributes.each do |k, v|
      got = tags[k]
      if got == v
        puts "[v] attribute #{k} = #{got}"
      else
        fail!("attribute '#{k}' expected '#{v}', got '#{got || "<missing>"}'")
      end
    end
  end
ensure
  # Best-effort cleanup: cancel the native subscribe + delete the topic so re-runs
  # start clean. Must not fail the run if a resource is already gone.
  begin
    cancel.cancel
    handle&.wait(5)
  rescue StandardError
    # ignore — already stopping
  end
  begin
    kube.close
  rescue StandardError
    # ignore
  end
  begin
    pubsub.topic_admin.delete_topic topic: topic_path
    puts "[*] Cleaned up topic '#{topic_id}'"
  rescue StandardError => e
    puts "[*] Cleanup: topic '#{topic_id}' already gone (#{e.class.name})"
  end
end

puts "[PASS] Pub/Sub publish consumed natively on Events Store with reserved tags present" if @exit_code.zero?
exit @exit_code
