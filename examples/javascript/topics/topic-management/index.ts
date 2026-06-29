// ─── topics 3: Topic management ──────────────────────────────────────────────
//
// The topic admin surface: CreateTopic → GetTopic → ListTopics →
// ListTopicSubscriptions → UpdateTopic (FieldMask) → DeleteTopic. DeleteTopic is
// a TOMBSTONE: the record is retained so existing subscriptions survive (they
// detach rather than disappear), which we prove by reading the subscription back
// after the topic is deleted. We also prove the reserved-namespace rule — a topic
// id starting with `sub.` is rejected with INVALID_ARGUMENT.
//
// The official @google-cloud/pubsub client is pointed at the connector purely by
// PUBSUB_EMULATOR_HOST; the Node client auto-detects the emulator from the env
// var. We use the low-level v1 admin clients so each admin RPC maps 1:1 to the
// connector surface. Resource ids are uuid-suffixed per run so concurrent runs
// never collide.

import { randomUUID } from "node:crypto";
import { PubSub, v1 } from "@google-cloud/pubsub";

// Declared before the top-level `await runDemo(...)` so it is initialized when the
// demo body runs (class declarations are not hoisted like functions).
class DemoFailure extends Error {}

await runDemo(async () => {
  const projectId = projectIdEnv();

  const run = randomUUID().replace(/-/g, "").slice(0, 8);
  const topicId = `managed-${run}`;
  const subId = `sub-managed-${run}`;

  // The high-level PubSub client reads PUBSUB_EMULATOR_HOST and resolves the
  // emulator transport (servicePath/port + insecure gRPC, no Google auth). We
  // reuse those resolved options to build the low-level v1 admin clients.
  const clientOptions = emulatorClientOptions(projectId);
  const publisher = new v1.PublisherClient(clientOptions);
  const subscriber = new v1.SubscriberClient(clientOptions);

  const topicName = publisher.projectTopicsPath(projectId, topicId);
  const subName = subscriber.subscriptionPath(projectId, subId);
  const projectName = publisher.projectPath(projectId);

  let subDeleted = false;
  let topicDeleted = false;

  // CreateTopic with an initial label.
  await publisher.createTopic({ name: topicName, labels: { env: "dev" } });
  step(`Created topic '${topicId}' (→ channel pubsub.${topicId})`);

  try {
    // GetTopic echoes the created topic and its labels.
    const [got_] = await publisher.getTopic({ topic: topicName });
    got(`GetTopic name=${parseTopicId(got_.name ?? "")} labels.env=${got_.labels?.env ?? ""}`);
    requireEqual(topicName, got_.name ?? "", "GetTopic topic name");
    requireEqual("dev", got_.labels?.env ?? "", "env label");

    // ListTopics must include the topic we just created.
    const [topics] = await publisher.listTopics({ project: projectName });
    const found = topics.some((t) => t.name === topicName);
    step(`ListTopics contains '${topicId}' → ${found}`);
    require(found, "freshly created topic missing from ListTopics");

    // Attach a subscription, then ListTopicSubscriptions must report it.
    await subscriber.createSubscription({ name: subName, topic: topicName, ackDeadlineSeconds: 10 });
    step(`Created subscription '${subId}' on the topic`);

    const [subs] = await publisher.listTopicSubscriptions({ topic: topicName });
    step(`ListTopicSubscriptions → [${subs.map(parseSubId).join(", ")}]`);
    require(subs.includes(subName), "subscription missing from ListTopicSubscriptions");

    // UpdateTopic via FieldMask: change only `labels`, leave everything else untouched.
    const [updated] = await publisher.updateTopic({
      topic: { name: topicName, labels: { env: "staging", team: "orders" } },
      updateMask: { paths: ["labels"] },
    });
    step(`UpdateTopic labels → env=${updated.labels?.env ?? ""}, team=${updated.labels?.team ?? ""}`);
    requireEqual("staging", updated.labels?.env ?? "", "updated env label");
    requireEqual("orders", updated.labels?.team ?? "", "added team label");

    // Confirm the label change is durable via a fresh GetTopic.
    const [reread] = await publisher.getTopic({ topic: topicName });
    requireEqual("staging", reread.labels?.env ?? "", "re-read env label");

    // The reserved-namespace rule: a topic id beginning with `sub.` collides with
    // the broker's subscription-queue namespace (`pubsub.sub.*`) and is rejected
    // with INVALID_ARGUMENT (gotcha #7).
    let rejected = false;
    try {
      await publisher.createTopic({ name: publisher.projectTopicsPath(projectId, `sub.bad-${run}`) });
    } catch {
      rejected = true;
    }
    step(`CreateTopic 'sub.bad-${run}' rejected → ${rejected}`);
    require(rejected, "topic id starting with 'sub.' should be rejected (reserved namespace)");

    // DeleteTopic is a TOMBSTONE: the topic record is retained so the subscription
    // survives (detaches) rather than being destroyed.
    await publisher.deleteTopic({ topic: topicName });
    topicDeleted = true;
    step(`DeleteTopic '${topicId}' (tombstone — subscriptions survive)`);

    // Prove the subscription still exists after the topic delete.
    const [survivor] = await subscriber.getSubscription({ subscription: subName });
    got(`Subscription '${subId}' survived the topic tombstone (topic field='${survivor.topic}')`);
    requireEqual(subName, survivor.name ?? "", "subscription did not survive the topic tombstone");

    // Clean the surviving subscription up ourselves.
    await subscriber.deleteSubscription({ subscription: subName });
    subDeleted = true;
    step(`Cleaned up subscription '${subId}'`);

    ok("Topic management lifecycle complete (create/get/list/update/tombstone)");
  } finally {
    // The topic was deleted in the happy path; on an early failure best-effort
    // remove whatever is still around so re-runs start clean.
    if (!subDeleted) {
      await bestEffort(
        () => subscriber.deleteSubscription({ subscription: subName }),
        `Cleaned up subscription '${subId}'`,
      );
    }
    if (!topicDeleted) {
      await bestEffort(
        () => publisher.deleteTopic({ topic: topicName }),
        `Cleaned up topic '${topicId}'`,
      );
    }
  }
});

// ─── Console-output + assertion helpers ──────────────────────────────────────
// Inlined per example (the examples carry no shared module). Examples are
// runnable proofs: each prints clear progress and exits non-zero on any failed
// assertion or unexpected error.

function step(m: string): void {
  console.log(`[*] ${m}`);
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

/** projects/{p}/topics/{t} → {t} */
function parseTopicId(name: string): string {
  return name.split("/").pop() ?? name;
}

/** projects/{p}/subscriptions/{s} → {s} */
function parseSubId(name: string): string {
  return name.split("/").pop() ?? name;
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
