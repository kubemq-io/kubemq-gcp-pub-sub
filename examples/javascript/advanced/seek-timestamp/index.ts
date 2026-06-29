/**
 * advanced/seek-timestamp — rewind a subscription to a point in time and replay.
 *
 * Flow:
 *   CreateTopic -> CreateSubscription -> Publish(3 msgs) -> Pull+Ack(drain)
 *     -> Seek(timestamp between msg-1 and msg-2) -> Pull (replay of msg-2,msg-3)
 *     -> Seek(epoch, a pre-window timestamp) -> Pull (clamps to earliest retained, replays all)
 *
 * Against the KubeMQ Pub/Sub connector a Seek resolves the start sequence from the
 * topic log `pubsub.{t}`, purges the subscription queue + drops leases, then replays
 * the topic log via Array.RangeEventsStore re-applying the subscription filter,
 * bounded by MaxSeekReplay. A seek to a pre-window timestamp CLAMPS to the earliest
 * retained message (it is NOT an error).
 *
 * Uses ONLY the official @google-cloud/pubsub client (high-level admin + a short
 * streaming-pull collection window for deterministic draining). Emulator-protocol
 * drop-in: set PUBSUB_EMULATOR_HOST before running; no code endpoint override.
 *
 * Run: npx tsx advanced/seek-timestamp/index.ts
 */
import { PubSub, type Subscription, type Message } from '@google-cloud/pubsub';
import { randomUUID } from 'node:crypto';

// ---- minimal self-contained helpers (kept local so this example is standalone) ----
const suffix = (): string => randomUUID().replace(/-/g, '').slice(0, 8);
function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
}
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
function fail(err: unknown): void {
  console.error('\nFAILED:', err instanceof Error ? err.message : err);
  process.exitCode = 1;
}

// Unique-per-run resource ids so concurrent runs use distinct pubsub.{t} / pubsub.sub.{s}
// channels and never cross-talk (see connection-and-isolation.md).
const TOPIC = `seekable-${suffix()}`;
const SUB = `sub-seekable-${suffix()}`;

// Open a streaming-pull window, ack every message it delivers, and return the bodies
// collected. Waits up to `maxMs` for the stream to establish and deliver, returning early
// once `expect` messages have arrived AND a short settle period passes (to catch any
// extra/unexpected delivery). The high-level Subscription is emulator-aware via the parent
// PubSub (PUBSUB_EMULATOR_HOST → insecure gRPC, no Google auth).
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
  // Delivery is at-least-once; report the DISTINCT set so output is stable across runs
  // (a redelivered duplicate would otherwise vary the raw list).
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

    // Publish three messages with a recorded marker timestamp clearly between #1 and #2.
    // The gaps are generous (>1s) so the seek boundary is unambiguous at publish-time
    // resolution: msg-1 is strictly before the marker, msg-2/msg-3 strictly after.
    const id1 = await topic.publishMessage({ data: Buffer.from('msg-1') });
    await sleep(1200);
    const marker = new Date(); // seek target: after msg-1, before msg-2/msg-3
    await sleep(1200);
    const id2 = await topic.publishMessage({ data: Buffer.from('msg-2') });
    const id3 = await topic.publishMessage({ data: Buffer.from('msg-3') });
    console.log(`Publish            -> 3 messages (ids ${id1}, ${id2}, ${id3})`);
    console.log(`Marker timestamp   -> ${marker.toISOString()} (between msg-1 and msg-2)`);

    // Drain the subscription queue (all three fanned-out copies). Delivery is at-least-once,
    // so assert on the DISTINCT set (a redelivered duplicate is valid Pub/Sub semantics).
    const first = new Set(await pullAck('Pull (initial)', 3));
    assert(first.size === 3, `expected 3 distinct messages on first drain, got ${first.size}`);

    // Seek to the marker timestamp: connector purges the queue and replays the topic
    // log from that point, re-applying the filter. We expect msg-2 and msg-3 back.
    await sub.seek(marker);
    console.log(`Seek(timestamp)    -> rewound to ${marker.toISOString()}`);
    const replay = new Set(await pullAck('Pull (after seek-to-timestamp)', 2));
    assert(replay.has('msg-2') && replay.has('msg-3'), 'expected msg-2 and msg-3 in replay');
    assert(!replay.has('msg-1'), 'msg-1 must NOT be replayed (published before the marker)');

    // Seek to a pre-window timestamp (epoch). The connector CLAMPS to the earliest
    // retained message rather than erroring, so the full topic log replays.
    const epoch = new Date(0);
    await sub.seek(epoch);
    console.log(`Seek(epoch)        -> ${epoch.toISOString()} (pre-window: clamps to earliest retained, not an error)`);
    const clamped = new Set(await pullAck('Pull (after seek-to-epoch / clamp)', 3));
    assert(clamped.has('msg-1') && clamped.has('msg-2') && clamped.has('msg-3'), 'clamp replay missing a message');

    console.log('\nPASS: seek-to-timestamp replayed from the marker; pre-window seek clamped to earliest retained.');
  } finally {
    // Best-effort cleanup so re-runs start clean (must not fail the run).
    await pubsub.subscription(SUB).delete().catch(() => {});
    await pubsub.topic(TOPIC).delete().catch(() => {});
    await pubsub.close().catch(() => {});
  }
}

main().catch(fail);
