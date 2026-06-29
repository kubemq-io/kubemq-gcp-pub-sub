# frozen_string_literal: true

# KubeMQ Google Cloud Pub/Sub — topics: topic management
# (Ruby / google-cloud-pubsub).
#
# The topic admin surface against the KubeMQ Pub/Sub connector:
#   CreateTopic -> ListTopics -> GetTopic -> UpdateTopic
#               -> CreateSubscription -> ListTopicSubscriptions -> DeleteTopic.
#
# Demonstrates the connector's topic-admin behaviour (spec S3 / docs
# 20-google-pubsub.md):
#   * GetTopic returns the requested (un-clamped) retention.
#   * UpdateTopic takes a FieldMask over message_retention_duration (10 min..31
#     days; the effective value is clamped to the broker's Store.MaxRetention).
#   * ListTopicSubscriptions lists the subscriptions attached to a topic.
#   * DeleteTopic is a TOMBSTONE — the record is retained so existing
#     subscriptions survive; GetTopic afterwards raises NotFound.
#   * Topic ids may NOT start with "sub." (reserved broker namespace) ->
#     INVALID_ARGUMENT.
#
# Uses ONLY the official `google-cloud-pubsub` client (v3.x), pointed at the
# connector by `PUBSUB_EMULATOR_HOST` (insecure gRPC, no Google auth). Resource
# ids are uuid-suffixed (channel-isolation rule) so concurrent runs use distinct
# channels and never cross-talk; everything created is deleted on exit
# (best-effort). See ../../README.md and ../../../SHARED-CONVENTIONS.md.

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

  # uuid-suffix a resource id so parallel runs occupy private channels; stays
  # within the connector id rules (3..255 chars, starts with a letter, charset
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

  # Assert an expected outcome; exit non-zero on failure. Examples are runnable
  # PROOFS, not demos — a wrong admin response must fail the process.
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

RETAIN_SECONDS = 30 * 60 # 30 minutes — within the 10 min..31 day band.

def main
  proj         = Demo.project_id
  topic_id     = Demo.rname("managed")
  sub_id       = Demo.rname("sub-managed")

  Demo.banner("topics/topic_management — topic '#{topic_id}'")

  pubsub       = Google::Cloud::PubSub.new(project_id: proj, emulator_host: Demo.emulator_host)
  topic_admin  = pubsub.topic_admin
  sub_admin    = pubsub.subscription_admin
  project_path = pubsub.project_path
  topic_path   = pubsub.topic_path(topic_id)
  sub_path     = pubsub.subscription_path(sub_id)
  topic_deleted = false

  begin
    # 1) CreateTopic -> Events Store log pubsub.{topic_id}
    topic = topic_admin.create_topic(name: topic_path)
    Demo.step("CreateTopic -> #{topic.name}  (channel pubsub.#{topic_id})")

    # 2) ListTopics — page over the project's topics; our new topic is present.
    #    (Iterating the paged enumerable transparently follows page tokens.)
    listed = topic_admin.list_topics(project: project_path).map(&:name)
    Demo.step("ListTopics -> #{listed.length} topic(s) in #{project_path}")
    Demo.check(listed.include?(topic_path), "ListTopics includes the topic we created")

    # 3) GetTopic — fetch the single topic by name.
    got = topic_admin.get_topic(topic: topic_path)
    Demo.step("GetTopic -> #{got.name}")
    Demo.check(got.name == topic_path, "GetTopic returns the requested topic")

    # 4) UpdateTopic via FieldMask over message_retention_duration. GetTopic
    #    returns the requested (un-clamped) retention; the EFFECTIVE retention
    #    is clamped to the broker's Store.MaxRetention (a connector gotcha).
    updated = topic_admin.update_topic(
      topic: { name: topic_path, message_retention_duration: { seconds: RETAIN_SECONDS } },
      update_mask: { paths: ["message_retention_duration"] }
    )
    requested = updated.message_retention_duration&.seconds
    Demo.step("UpdateTopic -> message_retention_duration=#{requested}s (requested)")
    Demo.step("               effective value clamped to the broker's Store.MaxRetention")
    Demo.check(requested == RETAIN_SECONDS, "UpdateTopic echoes the requested retention (un-clamped)")

    # 5) CreateSubscription -> Queue pubsub.sub.{sub_id}, attached to the topic.
    sub = sub_admin.create_subscription(name: sub_path, topic: topic_path, ack_deadline_seconds: 10)
    Demo.step("CreateSubscription -> #{sub.name}  (channel pubsub.sub.#{sub_id})")

    # 6) ListTopicSubscriptions — the attached subscription is listed by name.
    #    The response carries a repeated-string `subscriptions` field.
    attached = topic_admin.list_topic_subscriptions(topic: topic_path).subscriptions.to_a
    Demo.step("ListTopicSubscriptions -> #{attached.length} subscription(s): #{attached.inspect}")
    Demo.check(attached.include?(sub_path), "the subscription is attached to the topic")

    # 7) Negative path: a topic id starting with "sub." is reserved -> the
    #    connector rejects CreateTopic with INVALID_ARGUMENT.
    reserved = pubsub.topic_path("sub.#{topic_id}")
    rejected = false
    begin
      topic_admin.create_topic(name: reserved)
    rescue Google::Cloud::InvalidArgumentError => e
      rejected = true
      Demo.step("CreateTopic('sub.…') -> rejected: #{e.class.name.split("::").last}")
    end
    Demo.check(rejected, "topic id starting with 'sub.' is rejected (INVALID_ARGUMENT)")

    # 8) DeleteTopic — tombstone. The record is retained so existing
    #    subscriptions survive, but GetTopic now raises NotFound.
    topic_admin.delete_topic(topic: topic_path)
    topic_deleted = true
    Demo.step("DeleteTopic -> tombstoned (existing subscriptions survive)")
    gone = false
    begin
      topic_admin.get_topic(topic: topic_path)
    rescue Google::Cloud::NotFoundError
      gone = true
    end
    Demo.check(gone, "GetTopic after DeleteTopic raises NotFound")

    puts "\nPASS — topic management surface exercised."
  ensure
    # Best-effort cleanup. The subscription is always removed here. The topic is
    # only deleted if the demo did not already delete it in step 8 (an early
    # assertion failure can leave it behind) — this avoids a redundant
    # NotFound on the happy path.
    Demo.best_effort("subscription '#{sub_id}'") { sub_admin.delete_subscription(subscription: sub_path) }
    Demo.best_effort("topic '#{topic_id}'") { topic_admin.delete_topic(topic: topic_path) } unless topic_deleted
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
