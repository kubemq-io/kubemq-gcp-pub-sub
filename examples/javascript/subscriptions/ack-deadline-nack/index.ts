/**
 * KubeMQ Google Cloud Pub/Sub — subscriptions: ack deadline & nack (Node/TS, @google-cloud/pubsub).
 *
 * Lease lifecycle on a subscription, driven with unary `Pull` + `ModifyAckDeadline`:
 *   Pull -> ModifyAckDeadline(>0) extend -> ModifyAckDeadline(0) nack -> redeliver -> Acknowledge.
 *
 * Every delivered message carries an opaque `ackId` held under an ack-deadline lease.
 * `modifyAckDeadline(ackId, n>0)` extends the lease (clamped to 10..600 s);
 * `modifyAckDeadline(ackId, 0)` is an immediate nack that redelivers the message now; the connector's
 * 250 ms sweeper also redelivers any lease that expires un-acked. This example proves the nack path:
 * a pulled-but-nacked message comes back on the next pull, and a final `acknowledge` drains it.
 *
 * The unary RPCs are issued through the low-level `v1.SubscriberClient` (the high-level `Subscription`
 * only exposes StreamingPull); publishing uses the high-level `PubSub` client. Both honour
 * `PUBSUB_EMULATOR_HOST`: the SDK clears credentials, skips Google auth, and dials insecure gRPC — no
 * code changes. Resource ids are uuid-suffixed so concurrent runs occupy distinct `pubsub.{t}` /
 * `pubsub.sub.{s}` channels and do not cross-talk; both resources are deleted on exit (best-effort).
 *
 * Run: npx tsx subscriptions/ack-deadline-nack/index.ts
 */
import { randomUUID } from 'node:crypto';
import { PubSub, v1, type protos } from '@google-cloud/pubsub';
import { grpc } from 'google-gax';

// Lease extension applied before the nack — must be within the valid 10..600 s window.
const EXTEND_SECONDS = 30;

/** Any id works — the connector parses but ignores the project segment. */
function projectId(): string {
  return process.env.PUBSUB_PROJECT_ID ?? process.env.GOOGLE_CLOUD_PROJECT ?? 'my-project';
}

/**
 * uuid-suffix a resource id so parallel runs never share a canonical channel.
 *
 * Each run then occupies distinct `pubsub.{t}` / `pubsub.sub.{s}` channels. Ids stay within the
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

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Build emulator-aware options for the low-level `v1.SubscriberClient`.
 *
 * The high-level `PubSub` client auto-detects `PUBSUB_EMULATOR_HOST` and configures its internal
 * GAPIC clients (insecure gRPC, no Google auth). A standalone `v1.SubscriberClient` does NOT — left
 * to defaults it tries to fetch Google credentials and fails with `invalid_grant`. So when the
 * emulator var is set we point it at that host with insecure credentials, exactly as the high-level
 * client does internally (`pubsub.determineBaseUrl_`).
 */
function subscriberClientOptions(): ConstructorParameters<typeof v1.SubscriberClient>[0] {
  const emulator = process.env.PUBSUB_EMULATOR_HOST;
  if (!emulator) return { projectId: projectId() };
  // Split host:port and pass them as servicePath + port (gax builds `host:port`); passing the whole
  // `host:port` as apiEndpoint makes gax append a default :443. This mirrors how the high-level
  // PubSub client configures its internal GAPIC clients in emulator mode.
  const [host, port] = emulator.replace(/^https?:\/\//, '').split(':');
  return {
    servicePath: host,
    port: port ? Number(port) : 80,
    sslCreds: grpc.credentials.createInsecure(),
    projectId: projectId(),
  };
}

type ReceivedMessage = protos.google.pubsub.v1.IReceivedMessage;

// Per-call deadline for a unary Pull (ms). Bounds the connector's blocking long-poll so an empty
// queue returns control to our retry loop instead of holding the RPC open (mirrors the Python
// reference's client-side `timeout=10`). gax surfaces a hit deadline as a DEADLINE_EXCEEDED error,
// which we treat as "empty for now" and retry.
const PULL_TIMEOUT_MS = 8_000;

/** Unary Pull of a single message; returns the ReceivedMessage or null if the queue is empty. */
async function pullOne(
  subscriber: v1.SubscriberClient,
  subPath: string,
): Promise<ReceivedMessage | null> {
  try {
    const [response] = await subscriber.pull(
      { subscription: subPath, maxMessages: 1 },
      { timeout: PULL_TIMEOUT_MS, retry: null },
    );
    const msgs = response.receivedMessages ?? [];
    return msgs.length > 0 ? msgs[0] : null;
  } catch (err) {
    // A blocking long-poll that times out on an empty queue is not a failure — retry.
    if (err instanceof Error && /DEADLINE_EXCEEDED|deadline/i.test(err.message)) return null;
    throw err;
  }
}

/** Poll for a (re)delivered message; the nacked copy reappears after a short backoff. */
async function pullWithRetry(
  subscriber: v1.SubscriberClient,
  subPath: string,
  attempts = 10,
  delayMs = 300,
): Promise<ReceivedMessage | null> {
  for (let i = 0; i < attempts; i++) {
    const msg = await pullOne(subscriber, subPath);
    if (msg !== null) return msg;
    await sleep(delayMs);
  }
  return null;
}

async function main(): Promise<void> {
  banner('subscriptions: ack deadline extend + nack redelivery');

  const project = projectId();
  // High-level client (publish + topic/subscription admin) and the low-level v1 SubscriberClient
  // (unary Pull / ModifyAckDeadline / Acknowledge). Both read PUBSUB_EMULATOR_HOST at construction.
  const pubsub = new PubSub({ projectId: project });
  const subscriber = new v1.SubscriberClient(subscriberClientOptions());

  const topicId = unique('orders');
  const subId = unique('sub-orders');
  const subPath = subscriber.subscriptionPath(project, subId);

  const [topic] = await pubsub.createTopic(topicId);
  console.log(`CreateTopic        -> ${topic.name}`);
  const [subscription] = await topic.createSubscription(subId, { ackDeadlineSeconds: 10 });
  console.log(`CreateSubscription -> ${subscription.name} (ack deadline 10 s)`);

  try {
    const payload = Buffer.from('lease-me');
    await topic.publishMessage({ data: payload, attributes: { seq: '0' } });
    console.log('Publish            -> 1 message to the topic log');

    // 1) First delivery — message is now leased under the ack deadline.
    const first = await pullWithRetry(subscriber, subPath);
    check(first !== null, 'first Pull returns the message');
    const firstAckId = first!.ackId!;
    console.log(`Pull #1            -> body=${JSON.stringify(first!.message?.data?.toString())} ackId=${firstAckId.slice(0, 16)}...`);

    // 2) Extend the lease. ModifyAckDeadline(>0) pushes the deadline out (clamped to 10..600 s) so a
    //    slow consumer keeps ownership instead of letting the sweeper redeliver.
    await subscriber.modifyAckDeadline({ subscription: subPath, ackIds: [firstAckId], ackDeadlineSeconds: EXTEND_SECONDS });
    console.log(`ModifyAckDeadline  -> +${EXTEND_SECONDS}s (lease extended, still owned)`);

    // 3) Nack via ModifyAckDeadline(0): immediate redelivery instead of waiting out the lease.
    await subscriber.modifyAckDeadline({ subscription: subPath, ackIds: [firstAckId], ackDeadlineSeconds: 0 });
    console.log('ModifyAckDeadline  -> 0 (nack: redeliver now)');

    // 4) The nacked message comes back on the next Pull.
    const second = await pullWithRetry(subscriber, subPath);
    check(second !== null, 'the nacked message is redelivered on a later Pull');
    const secondBody = Buffer.from(second!.message?.data ?? []);
    check(secondBody.equals(payload), 'redelivered body matches the original');
    const secondAckId = second!.ackId!;
    console.log(`Pull #2            -> body=${JSON.stringify(secondBody.toString())} ackId=${secondAckId.slice(0, 16)}...`);

    // 5) Acknowledge for good — the lease is released and the message is gone.
    await subscriber.acknowledge({ subscription: subPath, ackIds: [secondAckId] });
    console.log('Acknowledge        -> message acked');

    // 6) Prove the subscription is now empty (the ack removed it; no further redelivery).
    const leftover = await pullOne(subscriber, subPath);
    check(leftover === null, 'subscription is empty after the ack (no further redelivery)');

    console.log('\nAck-deadline / nack lifecycle complete — nack redelivered, ack drained.');
  } finally {
    // Best-effort cleanup so re-runs start clean; never fail the run if already gone.
    await subscription.delete().catch(() => undefined);
    await topic.delete().catch(() => undefined);
    await subscriber.close().catch(() => undefined);
    await pubsub.close().catch(() => undefined);
  }
}

main().catch((err: unknown) => {
  console.error(`  [FAIL] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
