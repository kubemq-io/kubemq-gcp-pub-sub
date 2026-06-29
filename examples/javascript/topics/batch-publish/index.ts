// ─── topics 2: Batch publish ─────────────────────────────────────────────────
//
// One Publish RPC carrying many messages (1..1000). The connector validates the
// WHOLE batch before enqueuing anything (publish atomicity), then returns one
// server message id per message IN REQUEST ORDER. We publish a 5-message batch,
// assert the returned ids line up with the request order, then pull all five
// back and confirm every body/id round-trips.
//
// The official @google-cloud/pubsub client is pointed at the connector purely by
// PUBSUB_EMULATOR_HOST; the Node client auto-detects the emulator from the env
// var. We use the low-level v1 admin clients so the single Publish RPC maps 1:1
// to the connector surface. Resource ids are uuid-suffixed per run so concurrent
// runs occupy distinct pubsub.{t} / pubsub.sub.{s} channels and never cross-talk.

import { randomUUID } from "node:crypto";
import { PubSub, v1 } from "@google-cloud/pubsub";

const BATCH_SIZE = 5;

// Declared before the top-level `await runDemo(...)` so it is initialized when the
// demo body runs (class declarations are not hoisted like functions).
class DemoFailure extends Error {}

await runDemo(async () => {
  const projectId = projectIdEnv();

  const run = randomUUID().replace(/-/g, "").slice(0, 8);
  const topicId = `events-${run}`;
  const subId = `sub-events-${run}`;

  // The high-level PubSub client reads PUBSUB_EMULATOR_HOST and resolves the
  // emulator transport (servicePath/port + insecure gRPC, no Google auth). We
  // reuse those resolved options to build the low-level v1 admin clients.
  const clientOptions = emulatorClientOptions(projectId);
  const publisher = new v1.PublisherClient(clientOptions);
  const subscriber = new v1.SubscriberClient(clientOptions);

  const topicName = publisher.projectTopicsPath(projectId, topicId);
  const subName = subscriber.subscriptionPath(projectId, subId);

  await publisher.createTopic({ name: topicName });
  step(`Created topic '${topicId}' (→ channel pubsub.${topicId})`);

  try {
    await subscriber.createSubscription({
      name: subName,
      topic: topicName,
      ackDeadlineSeconds: 10,
    });
    step(`Created subscription '${subId}' (→ channel pubsub.sub.${subId})`);

    try {
      // Build a batch of distinct messages. Each carries a 1-based `seq`
      // attribute so we can verify request-order id correspondence.
      const bodies: string[] = [];
      const messages = [];
      for (let i = 1; i <= BATCH_SIZE; i++) {
        const body = `order #${1000 + i}`;
        bodies.push(body);
        messages.push({ data: Buffer.from(body), attributes: { seq: String(i) } });
      }

      // ONE Publish RPC for the whole batch. The connector validates the entire
      // batch atomically (≤1000 messages, ≤10 MiB/msg, ≤100 attrs); any single
      // invalid message rejects the whole batch with INVALID_ARGUMENT and
      // enqueues nothing.
      const [response] = await publisher.publish({ topic: topicName, messages });
      const ids = response.messageIds ?? [];
      sent(`Published ${ids.length} messages in one Publish RPC`);
      require(ids.length === BATCH_SIZE, `expected ${BATCH_SIZE} message ids, got ${ids.length}`);

      // The ids come back in request order: ids[i] is the id of messages[i].
      const idByBody = new Map<string, string>();
      for (let i = 0; i < BATCH_SIZE; i++) {
        const id = ids[i] ?? "";
        require(id.length > 0, `message ${i} got an empty id`);
        idByBody.set(bodies[i]!, id);
        step(`  ids[${i}] = ${id}  ↔  '${bodies[i]}'`);
      }

      // Pull all five back (one Pull may return fewer than requested, so loop).
      const pulled: import("@google-cloud/pubsub").protos.google.pubsub.v1.IReceivedMessage[] = [];
      const deadline = Date.now() + 20_000;
      while (pulled.length < BATCH_SIZE && Date.now() < deadline) {
        const [pull] = await subscriber.pull({ subscription: subName, maxMessages: BATCH_SIZE });
        const batch = pull.receivedMessages ?? [];
        pulled.push(...batch);
        if (batch.length > 0) {
          await subscriber.acknowledge({
            subscription: subName,
            ackIds: batch.map((r) => r.ackId!),
          });
        }
      }

      got(`Pulled and acked ${pulled.length} messages`);
      require(pulled.length === BATCH_SIZE, `expected to pull ${BATCH_SIZE} messages, got ${pulled.length}`);

      // Every published body must come back with the matching server id assigned
      // in the Publish response (id ↔ body correspondence).
      for (const r of pulled) {
        const body = bodyToString(r.message!.data);
        const expectedId = idByBody.get(body);
        require(expectedId !== undefined, `pulled an unexpected body '${body}'`);
        requireEqual(expectedId!, r.message!.messageId ?? "", `MessageId for '${body}'`);
      }

      ok(`Batch publish of ${BATCH_SIZE} messages complete (ids returned in request order)`);
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
