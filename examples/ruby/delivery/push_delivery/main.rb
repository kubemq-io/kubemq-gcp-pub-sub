# frozen_string_literal: true

# delivery/push_delivery — a subscription with a `push_config`; the connector's
# per-subscription delivery worker POSTs a wrapped JSON envelope to a local HTTP
# endpoint (no pull loop on the client side).
#
# Flow:
#   start a tiny stdlib HTTP endpoint on 127.0.0.1
#   -> CreateTopic
#   -> CreateSubscription(push_config{ push_endpoint: http://127.0.0.1:<port>/push })
#   -> Publish a message
#   -> the connector POSTs the wrapped envelope to the endpoint (we capture it)
#   -> assert the envelope shape + base64 data + attributes
#   -> DeleteSubscription -> DeleteTopic (best-effort cleanup)
#
# Connector behavior (spec S3.5 / docs 218-225): a push subscription is served by
# a per-sub worker that POSTs the wrapped envelope
#   { "message": { "data": "<base64>", "attributes": {..}, "messageId": "..",
#                  "publishTime": "..", "orderingKey": ".." }, "subscription": ".." }
# (or the raw body when `no_wrapper` is set). 2xx -> ack; non-2xx/timeout -> retry
# with backoff; exhaustion -> dead-letter topic if set, else drop. Endpoints must be
# HTTPS, EXCEPT HTTP is allowed for localhost (used here so the example needs no
# TLS cert). Optional OIDC Bearer when `oidc_token` is configured.
#
# Uses ONLY the official google-cloud-pubsub client (plus Ruby stdlib for the
# webhook). Run via bundler; see README.

require "logger"
require "securerandom"
require "socket"
require "base64"
require "json"
require "google/cloud/pubsub"

Google::Cloud::PubSub.configure { |config| config.logger = Logger.new(IO::NULL) }

SUFFIX = SecureRandom.hex(4)
TOPIC  = "push-delivery-#{SUFFIX}".freeze
SUB    = "sub-push-delivery-#{SUFFIX}".freeze

# 127.0.0.1 literal matches the 0.0.0.0 IPv4 bind below and avoids the dual-stack
# localhost->::1 pitfall (a local connector reaches us over IPv4 loopback). HTTP is
# permitted because the endpoint is localhost.
WEBHOOK_HOST = ENV.fetch("WEBHOOK_HOST", "127.0.0.1")

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

# Minimal HTTP endpoint that captures the POSTed envelope and replies 200.
def start_webhook
  server   = TCPServer.new("0.0.0.0", 0)
  port     = server.addr[1]
  captured = []
  thread = Thread.new do
    loop do
      client = server.accept
      request_line = client.gets
      headers = {}
      while (line = client.gets) && line != "\r\n"
        key, value = line.split(":", 2)
        headers[key.strip.downcase] = value.strip if value
      end
      body = +""
      if (len = headers["content-length"]&.to_i) && len.positive?
        body << client.read(len)
      end
      captured << { request_line: request_line, headers: headers, body: body }
      client.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
      client.close
    rescue IOError, Errno::ECONNRESET
      next
    end
  end
  [server, port, captured, thread]
end

pubsub = Google::Cloud::PubSub.new(
  project_id: ENV.fetch("PUBSUB_PROJECT_ID", "kubemq"),
  emulator_host: ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")
)
topic_admin = pubsub.topic_admin
sub_admin   = pubsub.subscription_admin

puts "[pubsub] emulator=#{ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")} " \
     "project=#{pubsub.project_id} (insecure / no-auth)"

server, port, captured, thread = start_webhook
endpoint = "http://#{WEBHOOK_HOST}:#{port}/push"
puts "Webhook listening      -> #{endpoint}"

begin
  topic_admin.create_topic name: pubsub.topic_path(TOPIC)
  puts "CreateTopic            -> #{TOPIC}"

  sub_admin.create_subscription(
    name: pubsub.subscription_path(SUB),
    topic: pubsub.topic_path(TOPIC),
    push_config: { push_endpoint: endpoint }
  )
  puts "CreateSubscription     -> #{SUB} (push_config.push_endpoint=#{endpoint})"

  publisher = pubsub.publisher TOPIC
  msg_id = publisher.publish "hello-push", { "color" => "blue" }
  puts "Publish                -> id=#{msg_id.message_id}"

  delivered = nil
  deadline = Time.now + 15
  while delivered.nil? && Time.now < deadline
    delivered = captured.first
    sleep 0.2 if delivered.nil?
  end
  fail!("connector never POSTed to the push endpoint") if delivered.nil?

  fail!("expected POST, got #{delivered[:request_line]}") unless delivered[:request_line].start_with?("POST ")
  envelope = JSON.parse(delivered[:body])
  message  = envelope["message"]
  data     = Base64.decode64(message["data"])
  puts "Push received          -> POST envelope keys=#{envelope.keys.inspect}"
  puts "  message.data         -> #{data.inspect} (base64-decoded)"
  puts "  message.attributes   -> #{message["attributes"].inspect}"
  puts "  message.messageId    -> #{message["messageId"]}"
  puts "  subscription         -> #{envelope["subscription"]}"

  fail!("envelope missing 'message'") unless envelope.key?("message")
  fail!("envelope missing 'subscription'") unless envelope.key?("subscription")
  fail!("decoded data mismatch: #{data.inspect}") unless data == "hello-push"
  fail!("attributes mismatch: #{message["attributes"].inspect}") unless message["attributes"] == { "color" => "blue" }
  puts "Assert                 -> wrapped envelope shape + base64 data + attributes verified"
ensure
  thread&.kill
  server&.close
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

puts "PASS: push subscription delivered the wrapped JSON envelope to the local endpoint"
exit 0
