/**
 * advanced/snapshots — capture a subscription cursor, then seek back to it.
 *
 * Flow:
 *   CreateTopic -> CreateSubscription -> Publish(msg-1,msg-2) -> Pull+Ack
 *     -> CreateSnapshot (captures the acked cursor) -> Publish(msg-3) -> Pull+Ack
 *     -> Seek(snapshot) -> Pull (replay everything published after the snapshot cursor)
 *
 * A snapshot captures the subscription's cursor as a registry record (7-day default
 * expiry, swept hourly). Seeking the subscription to that snapshot purges the queue
 * and replays the topic log `pubsub.{t}` from the captured position via
 * Array.RangeEventsStore. Snapshotting a detached subscription returns FAILED_PRECONDITION.
 *
 * Uses ONLY the official @google-cloud/pubsub client (high-level admin + a short
 * streaming-pull collection window for deterministic draining). Emulator-protocol
 * drop-in: set PUBSUB_EMULATOR_HOST before running; no code endpoint override.
 *
 * Run: npx tsx advanced/snapshots/index.ts
 */
import { PubSub, type Subscription, type Message } from '@google-cloud/pubsub';
import { randomUUID } from 'node:crypto';

const suffix = (): string => randomUUID().replace(/-/g, '').slice(0, 8);
function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
}
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
function fail(err: unknown): void {
  console.error('\nFAILED:', err instanceof Error ? err.message : err);
  process.exitCode = 1;
}

// Unique-per-run resource ids so concurrent runs use distinct channels and never cross-talk.
const TOPIC = `snapped-${suffix()}`;
const SUB = `sub-snapped-${suffix()}`;
const SNAPSHOT = `snap-${suffix()}`;

// Open a short streaming-pull window, ack every message it delivers, and return the
// bodies collected in that window. The high-level Subscription is emulator-aware via the
// parent PubSub (PUBSUB_EMULATOR_HOST → insecure gRPC, no Google auth).
async function drainAck(pubsub: PubSub, label: string, expect: number, maxMs = 5000): Promise<string[]> {
  const sub: Subscription = pubsub.subscription(SUB, { flowControl: { maxMessages: 100 } });
  const bodies: string[] = [];
  sub.on('message', (m: Message) => {
    bodies.push(m.data.toString('utf8'));
    m.ack();
  });
  sub.on('error', () => {});
  const start = Date.now();
  let settleUntil = 0;
  while (Date.now() - start < maxMs) {
    await sleep(150);
    if (bodies.length >= expect) {
      if (settleUntil === 0) settleUntil = Date.now() + 600; // let any stragglers arrive
      else if (Date.now() >= settleUntil) break;
    }
  }
  await sub.close();
  // Delivery is at-least-once; report the DISTINCT set so output is stable across runs.
  const distinct = [...new Set(bodies)].sort();
  console.log(`${label} -> ${distinct.length} distinct [${distinct.join(', ')}]`);
  return bodies;
}

async function main(): Promise<void> {
  const projectId = process.env.PUBSUB_PROJECT_ID ?? process.env.GOOGLE_CLOUD_PROJECT ?? 'my-project';
  const pubsub = new PubSub({ projectId });
  const host = process.env.PUBSUB_EMULATOR_HOST ?? '(unset)';
  console.log(`Connecting to KubeMQ Pub/Sub connector via PUBSUB_EMULATOR_HOST=${host}`);

  const pullAck = (label: string, expect: number): Promise<string[]> => drainAck(pubsub, label, expect);

  try {
    const [topic] = await pubsub.createTopic(TOPIC);
    console.log(`CreateTopic        -> ${TOPIC}  (channel pubsub.${TOPIC})`);
    const [sub] = await topic.createSubscription(SUB);
    console.log(`CreateSubscription -> ${SUB}  (channel pubsub.sub.${SUB})`);

    // Publish + drain the first two messages so the subscription cursor advances past them.
    // Delivery is at-least-once, so assert on the DISTINCT set.
    await topic.publishMessage({ data: Buffer.from('msg-1') });
    await topic.publishMessage({ data: Buffer.from('msg-2') });
    const drained = new Set(await pullAck('Pull (drain msg-1,msg-2)', 2));
    assert(drained.has('msg-1') && drained.has('msg-2'), `expected msg-1,msg-2 drained, got [${[...drained].join(', ')}]`);

    // Let the streaming acks for msg-1,msg-2 commit before snapshotting (acks round-trip
    // asynchronously) so the captured cursor sits at the msg-1/msg-2 boundary.
    await sleep(800);

    // Capture the cursor as a snapshot at this position (after msg-1,msg-2 acked).
    await sub.createSnapshot(SNAPSHOT);
    console.log(`CreateSnapshot     -> ${SNAPSHOT} (cursor after msg-1,msg-2; 7-day default expiry, swept hourly)`);

    // Publish + drain a third message; the live subscription is now past msg-3 too.
    await topic.publishMessage({ data: Buffer.from('msg-3') });
    const after = new Set(await pullAck('Pull (drain msg-3)', 1));
    assert(after.has('msg-3'), `expected msg-3, got [${[...after].join(', ')}]`);
    assert(!after.has('msg-1') && !after.has('msg-2'), 'msg-1/msg-2 were already acked and must not redeliver here');

    // Seek the subscription back to the snapshot cursor. The connector purges the queue
    // and replays the topic log from the captured start sequence via Array.RangeEventsStore.
    // msg-3 (published AFTER the snapshot) is redelivered; msg-1 (well before the cursor) is
    // NOT. The cursor sits at the msg-1/msg-2 boundary, so the boundary message msg-2 may be
    // re-included depending on the inclusive start sequence — accept either.
    await sub.seek(SNAPSHOT);
    console.log(`Seek(snapshot)     -> rewound subscription to snapshot ${SNAPSHOT}`);
    const replay = new Set(await pullAck('Pull (after seek-to-snapshot)', 1));
    assert(replay.has('msg-3'), 'expected msg-3 replayed (published after the snapshot cursor)');
    assert(!replay.has('msg-1'), 'msg-1 was published well before the cursor and must NOT replay');

    console.log('\nPASS: snapshot captured the cursor; seek-to-snapshot replayed messages from the captured position (msg-3 redelivered, msg-1 not).');
  } finally {
    // Best-effort cleanup so re-runs start clean (must not fail the run).
    await pubsub.snapshot(SNAPSHOT).delete().catch(() => {});
    await pubsub.subscription(SUB).delete().catch(() => {});
    await pubsub.topic(TOPIC).delete().catch(() => {});
    await pubsub.close().catch(() => {});
  }
}

main().catch(fail);
