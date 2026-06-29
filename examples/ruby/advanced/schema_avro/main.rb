# frozen_string_literal: true

# KubeMQ Google Cloud Pub/Sub — advanced: schema validation (Avro)
# (Ruby / google-cloud-pubsub).
#
# Demonstrates enforce-on-publish schema validation with an Avro schema:
#   CreateSchema(Avro) -> CreateTopic(schema_settings → schema, JSON encoding)
#     -> conforming Publish OK -> non-conforming Publish rejected INVALID_ARGUMENT
#     -> ValidateMessage echoes the same decision.
#
# Uses ONLY the official `google-cloud-pubsub` client. With PUBSUB_EMULATOR_HOST
# set the client clears credentials, skips Google auth, and dials insecure gRPC
# at the connector — zero code changes. Schema RPCs go through the schema admin
# client (`pubsub.schemas`); topic create through `pubsub.topic_admin`; publish
# through the data-plane `pubsub.publisher`.
#
# When a topic references a schema the connector ENFORCES it on publish: every
# message in the batch is validated and the WHOLE batch is rejected with
# INVALID_ARGUMENT on the first non-conforming message — nothing is enqueued.
# Schema definitions are capped at 300 KB. The connector validates Avro via its
# Avro engine; JSON encoding lets us publish a JSON object body with no codegen.
# See ../../README.md and ../../../SHARED-CONVENTIONS.md.
#
# Resource ids are uuid-suffixed (channel-isolation rule) so concurrent runs use
# distinct pubsub.{t} channels and schema ids; topic/schema are deleted on exit
# (best-effort).

require "json"
require "logger"
require "securerandom"
require "google/cloud/pubsub"

# google-cloud-* logs every RPC; route it to /dev/null so the example's own
# progress is the only thing on stdout.
Google::Cloud::PubSub.configure { |c| c.logger = Logger.new(File::NULL) }

# Any id works — the connector parses but ignores the project segment.
def project_id
  ENV["PUBSUB_PROJECT_ID"] || ENV["GOOGLE_CLOUD_PROJECT"] || "my-project"
end

# uuid-suffix a resource id so parallel runs occupy private channels/ids. Ids
# stay within the connector rules (3..255 chars, start with a letter).
def rname(base)
  "#{base}-#{SecureRandom.hex(4)}"
end

def banner(title)
  puts "=== #{title} ==="
  puts "  emulator : #{ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085")}"
  puts "  project  : #{project_id} (parsed but ignored by the connector)"
  puts "  auth     : none / insecure gRPC (emulator drop-in)"
  puts
end

# Assert an expected outcome; exit non-zero on failure. Examples are runnable
# PROOFS, not demos — a missing rejection or wrong code must fail the process.
def check(condition, message)
  if condition
    puts "  [OK] #{message}"
  else
    warn "  [FAIL] #{message}"
    exit 1
  end
end

# An Avro record schema: an order with a required string id and int quantity.
AVRO_DEFINITION = {
  type: "record",
  name: "Order",
  fields: [
    { name: "id", type: "string" },
    { name: "quantity", type: "int" }
  ]
}.to_json

def main
  proj = project_id
  topic_id = rname("avro-topic")
  schema_id = rname("avro-order")
  banner("advanced/schema_avro — topic '#{topic_id}'")

  pubsub = Google::Cloud::PubSub.new(project_id: proj, emulator_host: ENV.fetch("PUBSUB_EMULATOR_HOST", "localhost:8085"))
  schemas = pubsub.schemas
  topic_admin = pubsub.topic_admin
  topic_path = pubsub.topic_path(topic_id)
  schema_path = pubsub.schema_path(schema_id)

  begin
    # 1) CreateSchema registers the Avro definition (≤ 300 KB) with a revision id.
    schema = schemas.create_schema parent: pubsub.project_path,
                                   schema_id: schema_id,
                                   schema: { type: :AVRO, definition: AVRO_DEFINITION }
    puts "CreateSchema -> #{schema.name}  type=#{schema.type}  revision=#{schema.revision_id}"
    check(schema.type == :AVRO, "schema registered as AVRO")
    check(!schema.revision_id.empty?, "schema got a server-assigned revision id (full revision history)")

    # 2) CreateTopic bound to the schema (JSON encoding) -> enforce-on-publish.
    created = topic_admin.create_topic name: topic_path,
                                       schema_settings: { schema: schema_path, encoding: :JSON }
    puts "CreateTopic -> #{created.name}  (schema_settings → #{schema_id}, encoding=JSON)"
    check(created.schema_settings.schema == schema_path, "topic bound to the schema")
    check(created.schema_settings.encoding == :JSON, "topic encoding is JSON")

    publisher = pubsub.publisher(topic_id)

    # 3) Conforming publish: a JSON body matching the Avro record is accepted and
    #    written once to the topic log pubsub.{topic_id}.
    conforming = { id: "order-1001", quantity: 3 }.to_json
    ok = publisher.publish conforming
    puts "Publish (conforming) -> message_id=#{ok.message_id}"
    check(!ok.message_id.empty?, "conforming JSON publish accepted (matches the Avro schema)")

    # 4) Non-conforming publish: 'quantity' is the wrong Avro type (a string, not
    #    an int). The connector validates and rejects the WHOLE batch with
    #    INVALID_ARGUMENT — nothing is enqueued.
    non_conforming = { id: "order-1002", quantity: "three" }.to_json
    rejected = false
    begin
      publisher.publish non_conforming
    rescue Google::Cloud::InvalidArgumentError => e
      rejected = true
      puts "Publish (non-conforming) -> rejected #{e.class} (INVALID_ARGUMENT)"
    end
    check(rejected, "non-conforming publish → batch rejected INVALID_ARGUMENT (enforce-on-publish)")

    # 5) ValidateMessage echoes the same decision without publishing.
    schemas.validate_message parent: pubsub.project_path, name: schema_path,
                             message: conforming, encoding: :JSON
    puts "ValidateMessage (conforming) -> OK"
    validate_rejected = false
    begin
      schemas.validate_message parent: pubsub.project_path, name: schema_path,
                               message: non_conforming, encoding: :JSON
    rescue Google::Cloud::InvalidArgumentError
      validate_rejected = true
    end
    check(validate_rejected, "ValidateMessage rejects the non-conforming body (INVALID_ARGUMENT)")

    puts "\nPASS — Avro schema enforce-on-publish complete."
  ensure
    # Best-effort cleanup so re-runs start clean (topic → schema).
    begin
      topic_admin&.delete_topic topic: topic_path
    rescue Google::Cloud::Error
      nil
    end
    begin
      schemas&.delete_schema name: schema_path
    rescue Google::Cloud::Error
      nil
    end
  end
end

main if $PROGRAM_NAME == __FILE__
