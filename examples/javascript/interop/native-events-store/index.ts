// ─── interop: native-events-store ─────────────────────────────────────────────
//
// The ONLY example in this repo that imports a KubeMQ SDK. It demonstrates the
// connector's cross-protocol bridge end to end, in ONE process:
//
//   1. A native `kubemq-js` (kubemq-js@3.0.1) Events Store subscriber attaches to
//      channel `pubsub.{topic}` with start position StartFromNew ("new only").
//   2. We block until the subscribe stream is confirmed open (subscribe-BEFORE-publish).
//   3. The official `@google-cloud/pubsub` client publishes one message to `{topic}`
//      via PUBSUB_EMULATOR_HOST (zero code changes — the SDK clears credentials and
//      dials insecure gRPC when that env var is set).
//   4. The native subscriber receives exactly that message on `pubsub.{topic}` and
//      sees the three reserved tags the connector stamps on every message —
//      `_pubsub_message_id`, `_pubsub_publish_time`, `_pubsub_ordering_key` — which
//      are stripped from `attributes` when delivered to a Pub/Sub client.
//
// Two endpoints are used:
//   - Pub/Sub half: PUBSUB_EMULATOR_HOST (default localhost:8085) — the connector.
//   - Native half:  KUBEMQ_GRPC_ADDRESS  (default localhost:50000) — native broker.
//
// Why subscribe-before-publish + StartFromNew: the native side attaches to a
// STREAM, not a fixed offset. A naive "publish then subscribe" races — the publish
// could land before the subscriber attaches and be missed. Establishing the
// "new only" subscription first guarantees the published message is in-window (no
// startup race). See spec S5.7 and docs/concepts/cross-protocol-interop.md.

import { PubSub } from '@google-cloud/pubsub';
import { KubeMQClient, EventStoreStartPosition } from 'kubemq-js';
import type { EventStoreReceived, KubeMQError } from 'kubemq-js';

// The two halves of the bridge (defaults match the emulator/native conventions).
const projectId = process.env.PUBSUB_PROJECT_ID ?? 'my-project'; // parsed but ignored
const grpcAddress = process.env.KUBEMQ_GRPC_ADDRESS ?? 'localhost:50000';
const emulatorHost = process.env.PUBSUB_EMULATOR_HOST ?? '<unset>';

// Per-run isolation (parallel-safe; spec S9 / connection-and-isolation.md §B).
// The canonical interop name is `orders`, but the channel `pubsub.orders` is global
// and shared, so a fixed name would cross-talk with a concurrent run (other
// languages, or /examples + /burnin) and false-fail the single-message assertion.
// uuid8-suffix the topic so each run reads its own private Events Store channel.
const suffix = Math.random().toString(16).slice(2, 10).padEnd(8, '0');
const topicId = `orders-${suffix}`; // Pub/Sub topic id (starts with a letter; not `sub.`)
const nativeChannel = `pubsub.${topicId}`; // the Events Store channel it maps to

// The Pub/Sub payload + the attributes we expect to surface natively as tags.
const payload = 'order #4242 — cross-protocol via Pub/Sub';
const orderingKey = 'customer-42';
const attributes: Record<string, string> = {
  region: 'eu-west-1',
  priority: 'high',
};

// The three reserved tags the connector stamps on every Pub/Sub message and that a
// native consumer sees (stripped from `attributes` for Pub/Sub clients; spec S3.4).
const RESERVED = ['_pubsub_message_id', '_pubsub_publish_time', '_pubsub_ordering_key'] as const;

let exitCode = 0;
function fail(why: string): void {
  console.error(`  [FAIL] ${why}`);
  exitCode = 1;
}
function ok(message: string): void {
  console.log(`  [OK] ${message}`);
}

async function main(): Promise<void> {
  console.log(
    `=== interop: Pub/Sub publish -> native Events Store consume — ` +
      `topic '${topicId}' / channel '${nativeChannel}' ===`,
  );
  console.log(`  pubsub : ${emulatorHost} (PUBSUB_EMULATOR_HOST; insecure, no Google auth)`);
  console.log(`  grpc   : ${grpcAddress} (native KubeMQ broker)`);
  console.log();

  // ── Pub/Sub half: the high-level client auto-detects PUBSUB_EMULATOR_HOST ──────
  // When the env var is set, @google-cloud/pubsub clears credentials and uses
  // insecure gRPC — no apiEndpoint override needed (Node auto-detects, unlike C#).
  const pubsub = new PubSub({ projectId });
  // enableMessageOrdering is required to publish with an ordering key — it makes
  // the reserved tag _pubsub_ordering_key observable on the native side.
  const [topic] = await pubsub.createTopic(topicId);
  topic.setPublishOptions({ messageOrdering: true });
  console.log(`CreateTopic -> ${topic.name} (project '${projectId}' parsed but ignored)`);

  // ── Native half: connect the KubeMQ gRPC client ────────────────────────────────
  // KubeMQClient.create() builds, validates, and connects before returning a ready
  // client (there is no separate connect() step in kubemq-js).
  const kube = await KubeMQClient.create({
    address: grpcAddress,
    clientId: `gcp-pubsub-interop-js-${suffix}`,
  });
  console.log(`Connected native KubeMQ client to ${grpcAddress}`);

  // A promise resolved by the first native event (or rejected on subscribe error).
  let resolveEvent!: (event: EventStoreReceived) => void;
  let rejectEvent!: (err: Error) => void;
  const received = new Promise<EventStoreReceived>((resolve, reject) => {
    resolveEvent = resolve;
    rejectEvent = reject;
  });

  // ── 1. Subscribe FIRST, with start position "new only" ─────────────────────────
  // StartFromNew (gRPC StartNewOnly = 1) receives only events published AFTER the
  // subscription is established. subscribeToEventsStore opens the gRPC stream.
  const subscription = kube.subscribeToEventsStore({
    channel: nativeChannel,
    startFrom: EventStoreStartPosition.StartFromNew,
    onEvent: (event: EventStoreReceived) => resolveEvent(event),
    onError: (err: KubeMQError) => rejectEvent(err),
  });

  try {
    // The subscribe stream opens asynchronously; give it a brief, bounded readiness
    // window so the publish below is guaranteed in-window for the "new only"
    // subscription (subscribe-before-publish — avoids the startup race).
    await delay(1500);
    if (!subscription.isActive) {
      fail(`native Events Store subscribe stream did not open on '${nativeChannel}'`);
      return;
    }
    console.log(`native SubscribeToEventsStore('${nativeChannel}', startAt='new') -> stream open`);

    // ── 2. Publish exactly one message via the Pub/Sub SDK ───────────────────────
    const messageId = await topic.publishMessage({
      data: Buffer.from(payload, 'utf-8'),
      orderingKey,
      attributes,
    });
    console.log(`topic.publishMessage('${topicId}') -> messageId=${messageId}`);

    // ── 3. Await exactly one native event on pubsub.{topic} ──────────────────────
    const event = await withTimeout(
      received,
      15_000,
      `native consumer received no event on '${nativeChannel}' within 15s`,
    );
    ok(`native consumer received the published message on '${nativeChannel}'`);

    const body = Buffer.from(event.body).toString('utf-8');
    console.log(`native receive('${event.channel}') (seq ${event.sequence}) -> body=${JSON.stringify(body)}`);
    for (const key of RESERVED) {
      console.log(`    ${key} = ${JSON.stringify(event.tags[key])}`);
    }
    for (const key of Object.keys(attributes)) {
      console.log(`    ${key} (attribute) = ${JSON.stringify(event.tags[key])}`);
    }

    // ── 4. Assert the cross-protocol invariants ──────────────────────────────────
    body === payload
      ? ok('native side sees the Pub/Sub-published body')
      : fail(`native body mismatch: expected ${JSON.stringify(payload)}, got ${JSON.stringify(body)}`);

    RESERVED.every((tag) => tag in event.tags)
      ? ok('all three reserved tags are present (_pubsub_message_id/publish_time/ordering_key)')
      : fail(`a reserved tag is missing: have [${Object.keys(event.tags).join(', ')}]`);

    event.tags['_pubsub_message_id'] === messageId
      ? ok('_pubsub_message_id matches the Pub/Sub-returned message id')
      : fail(`_pubsub_message_id '${event.tags['_pubsub_message_id']}' != messageId '${messageId}'`);

    event.tags['_pubsub_ordering_key'] === orderingKey
      ? ok('_pubsub_ordering_key carries the publish ordering key')
      : fail(`_pubsub_ordering_key '${event.tags['_pubsub_ordering_key']}' != '${orderingKey}'`);

    for (const [key, want] of Object.entries(attributes)) {
      event.tags[key] === want
        ? ok(`attribute '${key}' rides along as an ordinary tag`)
        : fail(`attribute '${key}' expected '${want}', got '${event.tags[key] ?? '<missing>'}'`);
    }
  } finally {
    // ── Best-effort cleanup (must not fail the run) ──────────────────────────────
    subscription.cancel();
    await kube.close().catch(() => {});
    try {
      await topic.delete();
      console.log(`DeleteTopic('${topicId}') -> done`);
    } catch (err) {
      console.log(`DeleteTopic('${topicId}') -> skipped (${(err as Error).name})`);
    }
  }

  if (exitCode === 0) {
    console.log('\nPASS — Pub/Sub publish consumed natively on the Events Store topic log.');
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_resolve, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}

main()
  .catch((err: unknown) => {
    console.error(`  [FAIL] ${err instanceof Error ? err.message : String(err)}`);
    exitCode = 1;
  })
  .finally(() => {
    process.exit(exitCode);
  });
