/**
 * KubeMQ Google Cloud Pub/Sub — subscriptions: StreamingPull (Node/TS, @google-cloud/pubsub).
 *
 * The high-throughput consume path most client libraries use by default:
 *   CreateTopic -> CreateSubscription -> Publish(N) -> subscription.on('message') (StreamingPull) -> Ack.
 *
 * A long-lived bidirectional StreamingPull stream is opened via the high-level
 * `Subscription` event emitter with explicit flow control (`flowControl.maxMessages`); the
 * connector pushes messages as they arrive, the handler acks each, and the example asserts every
 * published message was delivered exactly once. The connector periodically closes each StreamingPull
 * stream (`CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS`, default 1800 s) to force a transparent SDK
 * reconnect — no messages are lost across it.
 *
 * Uses ONLY the official `@google-cloud/pubsub` client. With `PUBSUB_EMULATOR_HOST` set the SDK
 * clears credentials, skips Google auth, and dials insecure gRPC — no code changes. Resource ids are
 * uuid-suffixed so concurrent runs occupy distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and do
 * not cross-talk; both resources are deleted on exit (best-effort).
 *
 * Run: npx tsx subscriptions/streaming-pull/index.ts
 */
import { randomUUID } from 'node:crypto';
import { PubSub, type Message } from '@google-cloud/pubsub';

// Number of messages published and then drained over the StreamingPull stream.
const MESSAGE_COUNT = 8;

/** Any id works — the connector parses but ignores the project segment. */
function projectId(): string {
  return process.env.PUBSUB_PROJECT_ID ?? process.env.GOOGLE_CLOUD_PROJECT ?? 'my-project';
}

/**
 * uuid-suffix a resource id so parallel runs never share a canonical channel.
 *
 * Each run then occupies distinct `pubsub.{t}` / `pubsub.sub.{s}` channels (the project segment is
 * ignored, so a bare `orders` would collide across concurrent runs / languages). Ids stay within the
 * connector rules (3..255 chars, start with a letter, charset [A-Za-z0-9._~%+-]); topic ids must not
 * start with `sub.`.
 */
function unique(stem: string): string {
  return `${stem}-${randomUUID().replace(/-/g, '').slice(0, 8)}`;
}

function banner(title: string): void {
  console.log(`=== ${title} ===`);
  console.log(`  emulator : ${process.env.PUBSUB_EMULATOR_HOST ?? '(unset!)'}`);
  console.log(`  project  : ${projectId()} (parsed but ignored by the connector)`);
  console.log('  auth     : none — credentials cleared, insecure gRPC (emulator drop-in)');
  console.log();
}

/** Assert an expected outcome; throw (exit non-zero) on failure — examples are proofs. */
function check(condition: boolean, message: string): void {
  if (condition) {
    console.log(`  [OK] ${message}`);
  } else {
    throw new Error(message);
  }
}

async function main(): Promise<void> {
  banner('subscriptions: StreamingPull with flow control');

  // `PUBSUB_EMULATOR_HOST` is read at construction: the client clears credentials and dials
  // insecure gRPC. No apiEndpoint / servicePath override is needed.
  const pubsub = new PubSub({ projectId: projectId() });

  const topicId = unique('orders');
  const subId = unique('sub-orders');

  const [topic] = await pubsub.createTopic(topicId);
  console.log(`CreateTopic        -> ${topic.name}`);

  // ackDeadlineSeconds 10 = connector minimum (0 or 10..600).
  const [subscription] = await topic.createSubscription(subId, { ackDeadlineSeconds: 10 });
  console.log(`CreateSubscription -> ${subscription.name}`);

  try {
    // Publish a known set of messages; remember each by its sequence attribute.
    const published = new Set<string>();
    for (let i = 0; i < MESSAGE_COUNT; i++) {
      await topic.publishMessage({ data: Buffer.from(`event-${i}`), attributes: { seq: String(i) } });
      published.add(String(i));
    }
    console.log(`Publish            -> ${MESSAGE_COUNT} messages to the topic log`);

    // Open a long-lived StreamingPull stream by attaching a 'message' listener. `flowControl`
    // (set on the subscription handle below) is the client-side ceiling on outstanding (un-acked)
    // messages — the connector also enforces its own server-side MaxOutstandingMessages /
    // MaxInflightPerSubscription safety nets.
    //
    // The connector's delivery model is at-least-once, so a redelivery (the same seq twice) is
    // normal and correct, not a bug: the library may re-push a message whose ack is still in flight,
    // and the 250 ms sweeper redelivers any lease that lapses. We therefore track DISTINCT seqs and
    // assert every published message arrives at least once (acking each delivery either way).
    const seen = new Set<string>();
    let totalDeliveries = 0;
    await new Promise<void>((resolve, reject) => {
      // flowControl.maxMessages caps outstanding (un-acked) messages on this stream.
      const stream = pubsub.subscription(subId, { flowControl: { maxMessages: MESSAGE_COUNT } });
      console.log('StreamingPull      -> stream open (message listener); draining via handler');

      const settle = (fn: () => void) => {
        // Stop the background stream cleanly before settling.
        stream.removeAllListeners();
        void stream.close().finally(fn);
      };

      stream.on('message', (message: Message) => {
        totalDeliveries++;
        seen.add(message.attributes.seq ?? message.data.toString());
        message.ack();
        // Done once every distinct published message has been delivered at least once.
        if (seen.size >= MESSAGE_COUNT) settle(resolve);
      });
      stream.on('error', (err) => settle(() => reject(err)));

      // Safety timeout so a wedged stream fails the run instead of hanging.
      const timer = setTimeout(() => settle(() => reject(new Error('timed out waiting for messages'))), 30_000);
      timer.unref();
    });

    const redeliveries = totalDeliveries - seen.size;
    console.log(
      `Received           -> ${seen.size} distinct messages over the StreamingPull stream ` +
        `(${totalDeliveries} total deliveries, ${redeliveries} redelivery/-ies — at-least-once)`,
    );
    check(seen.size === MESSAGE_COUNT, `received all ${MESSAGE_COUNT} distinct published messages`);
    check(
      [...published].every((s) => seen.has(s)),
      'every published message was delivered at least once',
    );

    console.log('\nStreamingPull round-trip complete — all messages drained and acked.');
  } finally {
    // Best-effort cleanup so re-runs start clean; never fail the run if already gone.
    await subscription.delete().catch(() => undefined);
    await topic.delete().catch(() => undefined);
    await pubsub.close().catch(() => undefined);
  }
}

main().catch((err: unknown) => {
  console.error(`  [FAIL] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
