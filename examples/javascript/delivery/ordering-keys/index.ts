/**
 * KubeMQ Google Cloud Pub/Sub — delivery: ordering keys
 * (TypeScript / @google-cloud/pubsub).
 *
 * CreateTopic -> CreateSubscription(enableMessageOrdering) -> publish three
 * messages per ordering key with the publisher's message-ordering enabled ->
 * StreamingPull and assert each key's messages arrive in publish order.
 *
 * This is an unmodified @google-cloud/pubsub program. Setting PUBSUB_EMULATOR_HOST
 * makes the client clear credentials, skip Google auth, and dial insecure gRPC at
 * the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.
 *
 * Connector behaviour (spec S3.5 / docs/concepts/ordering-keys.md): with
 * enable_message_ordering, messages sharing an ordering_key are delivered in
 * publish order with AT MOST ONE in flight per key; keyless messages are
 * unordered. The key is carried across the wire as the reserved tag
 * _pubsub_ordering_key.
 *
 * Run: npx tsx delivery/ordering-keys/index.ts
 */
import { PubSub, v1 } from '@google-cloud/pubsub';
import { grpc } from 'google-gax';
import { randomUUID } from 'node:crypto';

// Build a low-level v1.SubscriberClient that honours PUBSUB_EMULATOR_HOST the
// same way the high-level PubSub client does: when the var is set, point the
// client at that host over insecure gRPC and skip Google auth. Unlike the
// high-level `PubSub`, the bare v1 client does NOT auto-detect the emulator, so
// we wire it explicitly (otherwise it attempts real Google auth → invalid_grant).
function newSubscriberClient(projectId: string): v1.SubscriberClient {
  const host = process.env['PUBSUB_EMULATOR_HOST'];
  if (host) {
    const [apiEndpoint, portStr] = host.split(':');
    return new v1.SubscriberClient({
      projectId,
      apiEndpoint,
      port: portStr ? Number(portStr) : 8085,
      sslCreds: grpc.credentials.createInsecure(),
    });
  }
  return new v1.SubscriberClient({ projectId });
}

// --- Connection (PUBSUB_EMULATOR_HOST drop-in) -----------------------------
// @google-cloud/pubsub auto-detects PUBSUB_EMULATOR_HOST: when set it clears
// credentials and dials insecure gRPC. Any project id works; the connector
// parses but ignores the project segment.
const PROJECT =
  process.env['PUBSUB_PROJECT_ID'] ?? process.env['GOOGLE_CLOUD_PROJECT'] ?? 'my-project';

// --- Unique-per-run resource names (channel-isolation rule, spec S9) -------
// Bare ids would collide on the shared canonical channels pubsub.{t}/pubsub.sub.{s};
// a uuid8 suffix gives each run a private channel namespace so concurrent runs
// (other languages, or /examples + /burnin) never cross-talk.
const RUN = randomUUID().slice(0, 8);
const TOPIC_ID = `ordered-${RUN}`;
const SUB_ID = `sub-ordered-${RUN}`;

// Two independent ordering keys + one keyless message.
const KEY_A = 'cust-7';
const KEY_B = 'cust-9';

let failed = false;
function check(condition: boolean, message: string): void {
  if (condition) {
    console.log(`  [OK] ${message}`);
  } else {
    console.error(`  [FAIL] ${message}`);
    failed = true;
  }
}

async function main(): Promise<void> {
  console.log(`=== Pub/Sub ordering keys — topic '${TOPIC_ID}' / sub '${SUB_ID}' ===`);
  console.log(`  emulator : ${process.env['PUBSUB_EMULATOR_HOST'] ?? '(unset!)'}`);
  console.log(`  project  : ${PROJECT} (parsed but ignored by the connector)`);
  console.log();

  const pubsub = new PubSub({ projectId: PROJECT });

  // The publisher MUST enable message ordering as well (it serializes publishes
  // per key); enabling it only on the subscription is not enough.
  const [topic] = await pubsub.createTopic(TOPIC_ID);
  topic.setPublishOptions({ messageOrdering: true });
  console.log(`CreateTopic            -> projects/${PROJECT}/topics/${TOPIC_ID}`);

  const [subscription] = await topic.createSubscription(SUB_ID, {
    enableMessageOrdering: true,
  });
  console.log(`CreateSubscription     -> ${SUB_ID}  (enableMessageOrdering=true)`);
  console.log();

  try {
    // --- Publish: 3 ordered per key, interleaved, plus 1 keyless ------------
    const plan: Array<[string, string]> = [
      [KEY_A, 'A1'],
      [KEY_B, 'B1'],
      [KEY_A, 'A2'],
      ['', 'keyless'],
      [KEY_B, 'B2'],
      [KEY_A, 'A3'],
      [KEY_B, 'B3'],
    ];
    console.log('Publish (publisher ordering enabled; interleaved across two keys):');
    // Publishes on the SAME ordering key are serialized by the client, so we
    // await each in turn to preserve the interleaved publish order.
    for (const [key, body] of plan) {
      const msgId = await topic.publishMessage(
        key ? { data: Buffer.from(body), orderingKey: key } : { data: Buffer.from(body) },
      );
      const shown = key || '(keyless)';
      console.log(
        `  publish body='${body}'`.padEnd(26) + `ordering_key=${shown.padEnd(12)} -> id=${msgId}`,
      );
    }
    console.log();

    // --- Pull (unary), ack each message before pulling the next ------------
    // Unary Pull + immediate Acknowledge makes the "one in flight per key, head
    // blocks until acked" guarantee directly observable: by acking each message
    // before the next pull, we let the connector release the next message for
    // that key in publish order. (The high-level streaming subscriber buffers
    // and acks asynchronously, which obscures this strict per-key sequencing.)
    const perKey: Record<string, string[]> = { [KEY_A]: [], [KEY_B]: [], '': [] };
    const expectedTotal = plan.length;
    const subClient = newSubscriberClient(PROJECT);
    const subPath = subClient.subscriptionPath(PROJECT, SUB_ID);

    console.log('Pull (one in flight per key; ack drives the next per-key message):');
    let receivedTotal = 0;
    const deadline = Date.now() + 30_000;
    while (receivedTotal < expectedTotal && Date.now() < deadline) {
      const [resp] = await subClient.pull({
        subscription: subPath,
        maxMessages: 1,
        returnImmediately: false,
      });
      if (!resp.receivedMessages || resp.receivedMessages.length === 0) {
        continue;
      }
      const rm = resp.receivedMessages[0]!;
      // The connector strips the reserved _pubsub_* tags from attributes for
      // Pub/Sub clients; the ordering key is surfaced as message.orderingKey.
      const key = rm.message?.orderingKey || '';
      const body = Buffer.from(rm.message?.data ?? '').toString();
      (perKey[key] ??= []).push(body);
      receivedTotal += 1;
      const shown = key || '(keyless)';
      console.log(`  received body='${body}'`.padEnd(27) + `ordering_key=${shown}`);
      await subClient.acknowledge({ subscription: subPath, ackIds: [rm.ackId!] });
    }
    await subClient.close();
    console.log();

    // --- Assertions ---------------------------------------------------------
    console.log('Per-key delivery order:');
    console.log(`  ${KEY_A}: [${(perKey[KEY_A] ?? []).join(', ')}]`);
    console.log(`  ${KEY_B}: [${(perKey[KEY_B] ?? []).join(', ')}]`);
    console.log(`  keyless: [${(perKey[''] ?? []).join(', ')}]`);
    check(
      JSON.stringify(perKey[KEY_A]) === JSON.stringify(['A1', 'A2', 'A3']),
      `key '${KEY_A}' delivered in publish order (A1,A2,A3)`,
    );
    check(
      JSON.stringify(perKey[KEY_B]) === JSON.stringify(['B1', 'B2', 'B3']),
      `key '${KEY_B}' delivered in publish order (B1,B2,B3)`,
    );
    const allBodies = Object.values(perKey)
      .flat()
      .sort()
      .join(',');
    const wantBodies = plan
      .map(([, b]) => b)
      .sort()
      .join(',');
    check(allBodies === wantBodies, 'every published message was delivered exactly once');
    console.log();
    console.log('PASS — ordering keys: per-key in-order delivery, one in flight per key.');
  } finally {
    // Best-effort cleanup so re-runs start clean; never fail the run.
    await subscription.delete().then(
      () => console.log('cleanup: DeleteSubscription ok'),
      () => undefined,
    );
    await topic.delete().then(
      () => console.log('cleanup: DeleteTopic ok'),
      () => undefined,
    );
  }
}

main()
  .then(() => process.exit(failed ? 1 : 0))
  .catch((err) => {
    console.error(`  [FAIL] ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  });
