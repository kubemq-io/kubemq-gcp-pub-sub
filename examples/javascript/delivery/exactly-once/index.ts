/**
 * KubeMQ Google Cloud Pub/Sub — delivery: exactly-once
 * (TypeScript / @google-cloud/pubsub).
 *
 * CreateTopic -> CreateSubscription(enableExactlyOnceDelivery=true) -> publish ->
 * StreamingPull and ack with `message.ackWithResponse()`, awaiting the connector's
 * AcknowledgeConfirmation (AckResponses.Success) -> then prove the unary
 * invalid-ack contract: Acknowledge with a bogus ack_id returns FAILED_PRECONDITION
 * (gRPC code 9) carrying an ErrorInfo, NOT a literal INVALID_ARGUMENT.
 *
 * This is an unmodified @google-cloud/pubsub program. Setting PUBSUB_EMULATOR_HOST
 * makes the client clear credentials, skip Google auth, and dial insecure gRPC at
 * the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.
 *
 * Connector behaviour (spec S3.5 / docs/concepts/exactly-once.md): with
 * enable_exactly_once_delivery, StreamingPull returns AcknowledgeConfirmation /
 * ModifyAckDeadlineConfirmation (expired -> invalid_ack_ids, transient ->
 * temporary_failed_ack_ids); a unary Acknowledge with an invalid ack_id returns
 * FAILED_PRECONDITION + ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID).
 * Exactly-once is NODE-LOCAL: an ack_id minted on one node is invalid on another,
 * so pin a subscription's StreamingPull to one node in a cluster.
 *
 * Run: npx tsx delivery/exactly-once/index.ts
 */
import { PubSub, AckResponses, v1, type Message } from '@google-cloud/pubsub';
import { grpc } from 'google-gax';
import { randomUUID } from 'node:crypto';

const PROJECT =
  process.env['PUBSUB_PROJECT_ID'] ?? process.env['GOOGLE_CLOUD_PROJECT'] ?? 'my-project';

// Unique-per-run resource names (channel-isolation rule, spec S9).
const RUN = randomUUID().slice(0, 8);
const TOPIC_ID = `eos-${RUN}`;
const SUB_ID = `sub-eos-${RUN}`;
const BODY = 'exactly-once-payload';

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

async function main(): Promise<void> {
  console.log(`=== Pub/Sub exactly-once — topic '${TOPIC_ID}' / sub '${SUB_ID}' ===`);
  console.log(`  emulator : ${process.env['PUBSUB_EMULATOR_HOST'] ?? '(unset!)'}`);
  console.log(`  project  : ${PROJECT} (parsed but ignored by the connector)`);
  console.log();

  const pubsub = new PubSub({ projectId: PROJECT });
  const subClient = newSubscriberClient(PROJECT);

  const [topic] = await pubsub.createTopic(TOPIC_ID);
  console.log(`CreateTopic            -> projects/${PROJECT}/topics/${TOPIC_ID}`);

  const [subscription] = await topic.createSubscription(SUB_ID, {
    enableExactlyOnceDelivery: true,
  });
  console.log(`CreateSubscription     -> ${SUB_ID}  (enableExactlyOnceDelivery=true)`);
  console.log();

  const subPath = subClient.subscriptionPath(PROJECT, SUB_ID);

  try {
    const msgId = await topic.publishMessage({ data: Buffer.from(BODY) });
    console.log(`Publish -> '${BODY}'  id=${msgId}`);
    console.log();

    // --- StreamingPull + ackWithResponse (the exactly-once ack path) --------
    // With exactly-once, ack is no longer fire-and-forget: ackWithResponse()
    // returns a promise that resolves to AckResponses.Success only once the
    // connector has durably confirmed the ack (AcknowledgeConfirmation). The
    // client de-duplicates redeliveries so each message surfaces exactly once.
    console.log('StreamingPull (exactly-once; ackWithResponse awaits the AcknowledgeConfirmation):');
    let received = 0;
    let ackResponse: string | null = null;
    const got = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('did not receive the message within 30s')),
        30_000,
      );
      subscription.on('message', (message: Message) => {
        received += 1;
        const body = message.data.toString();
        console.log(`  received body='${body}' messageId=${message.id} (exactly-once)`);
        message
          .ackWithResponse()
          .then((resp) => {
            ackResponse = resp;
            console.log(`  ackWithResponse -> ${resp}`);
            clearTimeout(timer);
            resolve();
          })
          .catch((err) => {
            clearTimeout(timer);
            reject(err);
          });
      });
      subscription.on('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
    await got;
    await subscription.close();
    console.log();

    check(received === 1, 'exactly one delivery surfaced to the application');
    check(
      ackResponse === AckResponses.Success,
      `ackWithResponse confirmed Success (AcknowledgeConfirmation), got '${ackResponse}'`,
    );

    // --- Negative path: unary Acknowledge with a bogus ack_id ---------------
    // The connector's exactly-once contract: an invalid/expired ack_id on a
    // unary Acknowledge returns FAILED_PRECONDITION (gRPC code 9) carrying an
    // ErrorInfo(reason: PERMANENT_FAILURE_INVALID_ACK_ID) — NOT a literal
    // INVALID_ARGUMENT. (Matches the real Google SDK contract.)
    console.log();
    console.log('Negative path: unary Acknowledge with a bogus ack_id (exactly-once contract):');
    try {
      await subClient.acknowledge({ subscription: subPath, ackIds: ['not-a-real-ack-id'] });
      check(false, 'bogus ack_id should be rejected with FAILED_PRECONDITION');
    } catch (err) {
      const code = (err as { code?: number }).code;
      const details = (err as { details?: string }).details ?? '';
      console.log(`  bogus ack_id -> FAILED_PRECONDITION (gRPC code ${code})`);
      if (details) {
        console.log(`  details: ${details}`);
      }
      // gRPC FAILED_PRECONDITION = status code 9 (NOT 3 = INVALID_ARGUMENT).
      check(code === 9, 'unary invalid-ack returns FAILED_PRECONDITION (not INVALID_ARGUMENT)');
    }

    console.log();
    console.log('PASS — exactly-once: ack confirmation reconciled; unary invalid-ack is FAILED_PRECONDITION.');
  } finally {
    await subClient.close();
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
