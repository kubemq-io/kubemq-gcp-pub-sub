// ─── topics 1: Basic publish / subscribe ─────────────────────────────────────
//
// The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
//   CreateTopic → CreateSubscription → Publish → Pull → Acknowledge.
//
// The official @google-cloud/pubsub client is pointed at the connector purely by
// PUBSUB_EMULATOR_HOST (no Google auth, insecure gRPC) — the Node client
// auto-detects the emulator from the env var, so there are zero code changes
// versus production. We use the low-level v1 admin clients (PublisherClient /
// SubscriberClient) so the RPCs map 1:1 to the connector surface
// (CreateTopic / CreateSubscription / Publish / Pull / Acknowledge).
//
// Topic "t" maps to Events Store log "pubsub.{t}" (the authoritative, replayable,
// cross-protocol source); subscription "s" maps to queue "pubsub.sub.{s}".
// Resource ids are uuid-suffixed per run so concurrent runs occupy distinct
// channels and never cross-talk.

import { randomUUID } from "node:crypto";
import { PubSub, v1 } from "@google-cloud/pubsub";

// Declared before the top-level `await runDemo(...)` so it is initialized when the
// demo body runs (class declarations are not hoisted like functions).
class DemoFailure extends Error {}

await runDemo(async () => {
  const projectId = projectIdEnv();

  // Unique-per-run resource names (channel-isolation rule): distinct
  // pubsub.{t} / pubsub.sub.{s} channels per run, no collisions across
  // languages or with the burn-in harness.
  const run = randomUUID().replace(/-/g, "").slice(0, 8);
  const topicId = `orders-${run}`;
  const subId = `sub-orders-${run}`;

  // The high-level PubSub client reads PUBSUB_EMULATOR_HOST and resolves the
  // emulator transport (servicePath/port + insecure gRPC, no Google auth). We
  // reuse those resolved options to build the low-level v1 admin clients, whose
  // RPCs map 1:1 to the connector surface.
  const clientOptions = emulatorClientOptions(projectId);
  const publisher = new v1.PublisherClient(clientOptions);
  const subscriber = new v1.SubscriberClient(clientOptions);

  const topicName = publisher.projectTopicsPath(projectId, topicId);
  const subName = subscriber.subscriptionPath(projectId, subId);

  await publisher.createTopic({ name: topicName });
  step(`Created topic '${topicId}' (→ channel pubsub.${topicId})`);

  try {
    // CreateSubscription wires the sub's queue (pubsub.sub.{s}) to the topic log.
    await subscriber.createSubscription({
      name: subName,
      topic: topicName,
      ackDeadlineSeconds: 10,
    });
    step(`Created subscription '${subId}' (→ channel pubsub.sub.${subId})`);

    try {
      // Publish a single message with a user attribute. The connector writes it
      // once to the topic log, then fans out one queue copy per subscription.
      const [publishResponse] = await publisher.publish({
        topic: topicName,
        messages: [
          { data: Buffer.from("order #1001"), attributes: { priority: "high" } },
        ],
      });
      const messageId = publishResponse.messageIds?.[0] ?? "";
      sent(`Published MessageId=${messageId}`);
      require(messageId.length > 0, "server did not assign a MessageId");

      // Pull the message back, retrying briefly while the publish fan-out lands
      // in the sub queue (pubsub.sub.{s}).
      let received: import("@google-cloud/pubsub").protos.google.pubsub.v1.IReceivedMessage | undefined;
      const deadline = Date.now() + 15_000;
      while (Date.now() < deadline) {
        const [pull] = await subscriber.pull({ subscription: subName, maxMessages: 1 });
        if (pull.receivedMessages && pull.receivedMessages.length > 0) {
          received = pull.receivedMessages[0];
          break;
        }
      }
      require(received !== undefined, "expected 1 message, got 0");

      const message = received!.message!;
      const body = bodyToString(message.data);
      got(`Pulled '${body}' MessageId=${message.messageId}`);

      // Assert the body, server id, attribute, and the server-stamped publish time.
      requireEqual("order #1001", body, "message body");
      requireEqual(messageId, message.messageId ?? "", "MessageId");
      requireEqual("high", message.attributes?.priority ?? "", "priority attribute");
      require(message.publishTime != null, "missing server publish_time");

      // The three reserved pubsub tags are stripped from `attributes` for Pub/Sub
      // clients (native consumers still see them) — assert no leak.
      require(
        !(message.attributes && "_pubsub_message_id" in message.attributes),
        "reserved tag _pubsub_message_id leaked into attributes",
      );

      // Acknowledge by ack_id (subscription-owned lease).
      await subscriber.acknowledge({ subscription: subName, ackIds: [received!.ackId!] });
      step("Acknowledged the message by ack_id");

      ok("Basic publish/subscribe round-trip complete");
    } finally {
      await bestEffort(
        () => subscriber.deleteSubscription({ subscription: subName }),
        `Cleaned up subscription '${subId}'`,
      );
    }
  } finally {
    await bestEffort(
      () => publisher.deleteTopic({ topic: topicName }),
      `Cleaned up topic '${topicId}'`,
    );
  }
});

// ─── Console-output + assertion helpers ──────────────────────────────────────
// Inlined per example (the examples carry no shared module). Examples are
// runnable proofs: each prints clear progress and exits non-zero on any failed
// assertion or unexpected error.

function step(m: string): void {
  console.log(`[*] ${m}`);
}
function sent(m: string): void {
  console.log(`[x] ${m}`);
}
function got(m: string): void {
  console.log(`[v] ${m}`);
}
function ok(m: string): void {
  console.log(`[ok] ${m}`);
}

/** The project id is parsed but ignored by the connector; any id works. */
function projectIdEnv(): string {
  return process.env.PUBSUB_PROJECT_ID ?? process.env.GOOGLE_CLOUD_PROJECT ?? "my-project";
}

/**
 * Resolve the emulator transport via the high-level PubSub client (which reads
 * PUBSUB_EMULATOR_HOST → insecure gRPC, no Google auth) and hand the options to
 * the low-level v1 admin clients. `port` is normalized to a number (the v1
 * ClientOptions require a numeric port).
 */
function emulatorClientOptions(projectId: string): ConstructorParameters<typeof v1.PublisherClient>[0] {
  const opts = new PubSub({ projectId }).options;
  return { ...opts, port: opts.port === undefined ? undefined : Number(opts.port) };
}

/** Decode a Pub/Sub message body type-agnostically (string | Buffer | Uint8Array). */
function bodyToString(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof Uint8Array) return Buffer.from(data).toString("utf8");
  return Buffer.from(data as ArrayLike<number>).toString("utf8");
}

function require(condition: boolean, message: string): void {
  if (!condition) throw new DemoFailure(message);
}

function requireEqual<T>(expected: T, actual: T, what: string): void {
  if (expected !== actual) {
    throw new DemoFailure(`${what}: expected '${expected}', got '${actual}'`);
  }
}

/** Best-effort cleanup that never fails the run if the resource is already gone. */
async function bestEffort(action: () => Promise<unknown>, okMessage: string): Promise<void> {
  try {
    await action();
    step(okMessage);
  } catch (err) {
    const e = err as Error;
    console.error(`[warn] cleanup skipped: ${e.name}: ${e.message}`);
  }
}

async function runDemo(body: () => Promise<void>): Promise<void> {
  try {
    await body();
    process.exit(0);
  } catch (err) {
    if (err instanceof DemoFailure) {
      console.error(`[FAIL] ${err.message}`);
    } else {
      const e = err as Error;
      console.error(`[ERROR] ${e.name}: ${e.message}`);
    }
    process.exit(1);
  }
}
