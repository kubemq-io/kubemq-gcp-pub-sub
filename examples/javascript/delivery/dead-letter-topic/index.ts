/**
 * KubeMQ Google Cloud Pub/Sub — delivery: dead-letter topic
 * (TypeScript / @google-cloud/pubsub).
 *
 * CreateTopic(work) + CreateTopic(dead-letters) -> CreateSubscription on the DLQ
 * topic -> CreateSubscription on work with a deadLetterPolicy
 * (deadLetterTopic + maxDeliveryAttempts=5) and a tight retryPolicy -> publish one
 * poison message -> pull-and-HOLD (never ack) so each ack-deadline expiry raises
 * the receive count -> once it exceeds maxDeliveryAttempts the connector
 * republishes the message to the dead-letter topic and acks the original.
 *
 * This is an unmodified @google-cloud/pubsub program. Setting PUBSUB_EMULATOR_HOST
 * makes the client clear credentials, skip Google auth, and dial insecure gRPC at
 * the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.
 *
 * Connector behaviour (spec S3.5 / docs/concepts/dead-letter-and-retry.md): the
 * dead-letter decision lives ONLY in the 250ms lease sweeper — each ACK-DEADLINE
 * EXPIRY increments the broker receive count; an explicit nack does NOT (Google
 * semantics: only expiry raises the count). When the count EXCEEDS
 * maxDeliveryAttempts and the sub names a deadLetterTopic, the sweeper republishes
 * the message (connector-level fan-out via pubsub.{dlt}) and acks the original.
 * maxDeliveryAttempts must be 5..100.
 *
 * Run: npx tsx delivery/dead-letter-topic/index.ts
 */
import { PubSub, v1 } from '@google-cloud/pubsub';
import { grpc } from 'google-gax';
import { randomUUID } from 'node:crypto';

const PROJECT =
  process.env['PUBSUB_PROJECT_ID'] ?? process.env['GOOGLE_CLOUD_PROJECT'] ?? 'my-project';

// Unique-per-run resource names (channel-isolation rule, spec S9).
const RUN = randomUUID().slice(0, 8);
const WORK_TOPIC = `work-${RUN}`;
const DLQ_TOPIC = `work-dlq-${RUN}`;
const WORK_SUB = `sub-work-${RUN}`;
const DLQ_SUB = `sub-dlq-${RUN}`;

// Must be in 5..100 (connector + Google floor); 0 would mean "unset".
const MAX_DELIVERY_ATTEMPTS = 5;
// Connector ack-deadline floor is 10s; each unacked lease expires after this.
const ACK_DEADLINE_SECONDS = 10;
const POISON = 'poison-pill';

let failed = false;
function check(condition: boolean, message: string): void {
  if (condition) {
    console.log(`  [OK] ${message}`);
  } else {
    console.error(`  [FAIL] ${message}`);
    failed = true;
  }
}

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

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  console.log(`=== Pub/Sub dead-letter topic — work '${WORK_TOPIC}' -> dlq '${DLQ_TOPIC}' ===`);
  console.log(`  emulator : ${process.env['PUBSUB_EMULATOR_HOST'] ?? '(unset!)'}`);
  console.log(`  project  : ${PROJECT} (parsed but ignored by the connector)`);
  console.log(`  max_delivery_attempts : ${MAX_DELIVERY_ATTEMPTS}`);
  console.log('  note     : dead-lettering is driven by ack-deadline EXPIRY, not nack —');
  console.log(`             each expiry waits ~${ACK_DEADLINE_SECONDS}s, so this runs ~60s.`);
  console.log();

  const pubsub = new PubSub({ projectId: PROJECT });
  const subClient = newSubscriberClient(PROJECT);

  const [workTopic] = await pubsub.createTopic(WORK_TOPIC);
  console.log(`CreateTopic(work)      -> projects/${PROJECT}/topics/${WORK_TOPIC}`);
  const [dlqTopic] = await pubsub.createTopic(DLQ_TOPIC);
  console.log(`CreateTopic(dlq)       -> projects/${PROJECT}/topics/${DLQ_TOPIC}`);

  // A subscription on the DLQ topic so we can observe the redriven message.
  await dlqTopic.createSubscription(DLQ_SUB);
  console.log(`CreateSubscription(dlq)-> ${DLQ_SUB}`);

  // The work subscription dead-letters to the DLQ topic after max attempts.
  // A tight retryPolicy makes a swept lease re-arm and redeliver almost
  // immediately, so the loop is bounded by the ack deadline, not backoff.
  await workTopic.createSubscription(WORK_SUB, {
    ackDeadlineSeconds: ACK_DEADLINE_SECONDS,
    deadLetterPolicy: {
      deadLetterTopic: dlqTopic.name,
      maxDeliveryAttempts: MAX_DELIVERY_ATTEMPTS,
    },
    retryPolicy: {
      minimumBackoff: { seconds: 0, nanos: 10_000_000 }, // 10ms
      maximumBackoff: { seconds: 0, nanos: 50_000_000 }, // 50ms
    },
  });
  console.log(
    `CreateSubscription(work)-> ${WORK_SUB}  ` +
      `(dead_letter_topic=${DLQ_TOPIC}, max_delivery_attempts=${MAX_DELIVERY_ATTEMPTS})`,
  );
  console.log();

  const workPath = subClient.subscriptionPath(PROJECT, WORK_SUB);
  const dlqPath = subClient.subscriptionPath(PROJECT, DLQ_SUB);

  try {
    await workTopic.publishMessage({ data: Buffer.from(POISON) });
    console.log(`Publish -> '${POISON}'`);
    console.log();

    // Drive redelivery via natural ack-deadline expiry: Pull leases the message
    // under the ack deadline; we deliberately NEVER ack it, so the connector's
    // lease sweeper expires the lease and raises the receive count. Once the
    // count exceeds max_delivery_attempts the sweeper moves it to the DLQ topic.
    // (An explicit nack would not raise the count.)
    console.log('Driving redelivery (Pull then hold unacked; each expiry raises the receive count):');
    let dlqBody: string | null = null;
    // Generous bound: the message dead-letters once the receive count EXCEEDS
    // max_delivery_attempts (=5), i.e. on the ~6th ack-deadline expiry. Each
    // expiry is ~ACK_DEADLINE_SECONDS apart, so the happy path completes in
    // ~50-60s. The connector's own integration test allows a 90s+ budget for
    // this (kubemq-server connectors/pubsub/integration_m2_test.go), so we set
    // a (max+6)-cycle ceiling (~110s) to keep the failure path comfortably
    // above the connector's verified latency and avoid flaky runs.
    const deadline = Date.now() + (MAX_DELIVERY_ATTEMPTS + 6) * ACK_DEADLINE_SECONDS * 1000;
    let attempt = 0;
    while (dlqBody === null && Date.now() < deadline) {
      // Pull and HOLD: lease the head of the work sub but never ack it, so the
      // 250ms sweeper expires the lease after ACK_DEADLINE_SECONDS and raises
      // the broker receive count for the next cycle.
      const [resp] = await subClient.pull({
        subscription: workPath,
        maxMessages: 1,
        returnImmediately: true,
      });
      if (resp.receivedMessages && resp.receivedMessages.length > 0) {
        attempt += 1;
        const rm = resp.receivedMessages[0]!;
        console.log(
          `  pull ${attempt}: leased (delivery_attempt=${rm.deliveryAttempt ?? 0}) ` +
            `-> hold unacked, let the lease expire`,
        );
      }
      // Let the held lease expire (one full ack-deadline cycle) before the next
      // pull, so each iteration advances the receive count by exactly one.
      await sleep((ACK_DEADLINE_SECONDS + 1) * 1000);
      // After the sweep, check whether the message has been redriven to the DLQ.
      const [dresp] = await subClient.pull({
        subscription: dlqPath,
        maxMessages: 1,
        returnImmediately: true,
      });
      if (dresp.receivedMessages && dresp.receivedMessages.length > 0) {
        const drm = dresp.receivedMessages[0]!;
        dlqBody = Buffer.from(drm.message?.data ?? '').toString();
        await subClient.acknowledge({ subscription: dlqPath, ackIds: [drm.ackId!] });
      }
    }
    console.log();

    check(dlqBody !== null, 'poison message landed on the dead-letter topic');
    console.log(`  DLQ message body='${dlqBody}'`);
    check(dlqBody === POISON, 'DLQ holds the original body');
    check(attempt >= 1, 'the source subscription delivered the message before dead-lettering');

    // The source subscription is now drained (the original was acked off it).
    const [drained] = await subClient.pull({
      subscription: workPath,
      maxMessages: 1,
      returnImmediately: true,
    });
    check(
      !drained.receivedMessages || drained.receivedMessages.length === 0,
      'work subscription drained (original acked off the source)',
    );

    console.log();
    console.log('PASS — dead-letter topic: receive count > max_delivery_attempts redrives to the DLQ.');
  } finally {
    await subClient.close();
    for (const name of [WORK_SUB, DLQ_SUB]) {
      await pubsub
        .subscription(name)
        .delete()
        .then(
          () => console.log(`cleanup: DeleteSubscription(${name}) ok`),
          () => undefined,
        );
    }
    for (const [label, t] of [
      ['work', workTopic],
      ['dlq', dlqTopic],
    ] as const) {
      await t.delete().then(
        () => console.log(`cleanup: DeleteTopic(${label}) ok`),
        () => undefined,
      );
    }
  }
}

main()
  .then(() => process.exit(failed ? 1 : 0))
  .catch((err) => {
    console.error(`  [FAIL] ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  });
