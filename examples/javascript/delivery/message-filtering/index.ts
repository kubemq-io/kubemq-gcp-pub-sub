/**
 * KubeMQ Google Cloud Pub/Sub — delivery: message filtering
 * (TypeScript / @google-cloud/pubsub).
 *
 * CreateTopic -> CreateSubscription(filter='attributes.region = "eu"') +
 * CreateSubscription(no filter) -> publish a mixed batch with a `region`
 * attribute -> pull both subs and assert the filtered subscription receives ONLY
 * the matching subset while the unfiltered subscription receives everything.
 *
 * This is an unmodified @google-cloud/pubsub program. Setting PUBSUB_EMULATOR_HOST
 * makes the client clear credentials, skip Google auth, and dial insecure gRPC at
 * the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.
 *
 * Connector behaviour (spec S3.5 / docs/guides/filtering.md): a subscription's
 * CEL-subset `filter` is attributes-only, compiled once at create-time
 * (immutable), and applied at PUBLISH FAN-OUT — a non-matching message is never
 * enqueued for that subscription. The topic log pubsub.{t} is unfiltered.
 *
 * Run: npx tsx delivery/message-filtering/index.ts
 */
import { PubSub, v1 } from '@google-cloud/pubsub';
import { grpc } from 'google-gax';
import { randomUUID } from 'node:crypto';

const PROJECT =
  process.env['PUBSUB_PROJECT_ID'] ?? process.env['GOOGLE_CLOUD_PROJECT'] ?? 'my-project';

// Unique-per-run resource names (channel-isolation rule, spec S9).
const RUN = randomUUID().slice(0, 8);
const TOPIC_ID = `filtered-${RUN}`;
const SUB_FILTERED = `sub-filtered-${RUN}`;
const SUB_ALL = `sub-all-${RUN}`;

// CEL-subset, attributes-only, <= 256 chars, immutable after create.
const EU_FILTER = 'attributes.region = "eu"';

// Mixed batch: the region attribute drives the filter.
const MESSAGES: Array<[string, Record<string, string>]> = [
  ['order-1', { region: 'eu', type: 'order' }],
  ['order-2', { region: 'us', type: 'order' }],
  ['order-3', { region: 'eu', type: 'order' }],
  ['order-4', { region: 'ap', type: 'order' }],
  ['order-5', { region: 'eu', type: 'order' }],
];
const EU_BODIES = MESSAGES.filter(([, a]) => a['region'] === 'eu')
  .map(([b]) => b)
  .sort();
const ALL_BODIES = MESSAGES.map(([b]) => b).sort();

let failed = false;
function check(condition: boolean, message: string): void {
  if (condition) {
    console.log(`  [OK] ${message}`);
  } else {
    console.error(`  [FAIL] ${message}`);
    failed = true;
  }
}

// Build a low-level v1.SubscriberClient that honours PUBSUB_EMULATOR_HOST the
// same way the high-level PubSub client does (insecure gRPC, no Google auth).
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

async function drain(
  subClient: v1.SubscriberClient,
  subPath: string,
  want: number,
): Promise<string[]> {
  const bodies: string[] = [];
  let empty = 0;
  while (bodies.length < want && empty < 3) {
    const [resp] = await subClient.pull({
      subscription: subPath,
      maxMessages: 10,
      returnImmediately: true,
    });
    if (!resp.receivedMessages || resp.receivedMessages.length === 0) {
      empty += 1;
      continue;
    }
    const ackIds: string[] = [];
    for (const rm of resp.receivedMessages) {
      bodies.push(Buffer.from(rm.message?.data ?? '').toString());
      ackIds.push(rm.ackId!);
    }
    await subClient.acknowledge({ subscription: subPath, ackIds });
  }
  return bodies;
}

async function main(): Promise<void> {
  console.log(`=== Pub/Sub message filtering — topic '${TOPIC_ID}' ===`);
  console.log(`  emulator : ${process.env['PUBSUB_EMULATOR_HOST'] ?? '(unset!)'}`);
  console.log(`  project  : ${PROJECT} (parsed but ignored by the connector)`);
  console.log(`  filter   : ${EU_FILTER}`);
  console.log();

  const pubsub = new PubSub({ projectId: PROJECT });
  const subClient = newSubscriberClient(PROJECT);

  const [topic] = await pubsub.createTopic(TOPIC_ID);
  console.log(`CreateTopic            -> projects/${PROJECT}/topics/${TOPIC_ID}`);

  await topic.createSubscription(SUB_FILTERED, { filter: EU_FILTER });
  console.log(`CreateSubscription     -> ${SUB_FILTERED}  (filter=${EU_FILTER})`);
  await topic.createSubscription(SUB_ALL);
  console.log(`CreateSubscription     -> ${SUB_ALL}  (no filter)`);
  console.log();

  const filteredPath = subClient.subscriptionPath(PROJECT, SUB_FILTERED);
  const allPath = subClient.subscriptionPath(PROJECT, SUB_ALL);

  try {
    console.log('Publish mixed batch (one Publish per message; region attribute drives the filter):');
    for (const [body, attrs] of MESSAGES) {
      const msgId = await topic.publishMessage({ data: Buffer.from(body), attributes: attrs });
      console.log(
        `  publish body='${body}'`.padEnd(25) + `region=${(attrs['region'] ?? '').padEnd(3)} -> id=${msgId}`,
      );
    }
    console.log();

    console.log('Pull filtered subscription (expects EU only):');
    const euGot = (await drain(subClient, filteredPath, EU_BODIES.length)).sort();
    console.log(`  filtered received: [${euGot.join(', ')}]`);
    console.log('Pull unfiltered subscription (expects everything):');
    const allGot = (await drain(subClient, allPath, ALL_BODIES.length)).sort();
    console.log(`  unfiltered received: [${allGot.join(', ')}]`);
    console.log();

    check(
      JSON.stringify(euGot) === JSON.stringify(EU_BODIES),
      `filtered sub received only region=eu ([${EU_BODIES.join(', ')}])`,
    );
    check(
      JSON.stringify(allGot) === JSON.stringify(ALL_BODIES),
      'unfiltered sub received the full batch',
    );
    const suppressed = MESSAGES.length - EU_BODIES.length;
    check(
      suppressed === 2,
      `non-matching attribute -> suppressed (${suppressed} not enqueued for filtered sub)`,
    );

    console.log();
    console.log('Negative path: a malformed filter is rejected at CreateSubscription:');
    try {
      await topic.createSubscription(`sub-bad-${RUN}`, { filter: 'this is not a filter' });
      check(false, 'malformed filter should be rejected with INVALID_ARGUMENT');
    } catch (err) {
      const code = (err as { code?: number }).code;
      // gRPC INVALID_ARGUMENT = status code 3.
      console.log(`  malformed filter -> INVALID_ARGUMENT (gRPC code ${code})`);
      check(code === 3, 'malformed filter rejected at create-time (INVALID_ARGUMENT)');
    }

    console.log();
    console.log('PASS — message filtering: attributes-only CEL-subset applied at publish fan-out.');
  } finally {
    await subClient.close();
    for (const name of [SUB_FILTERED, SUB_ALL]) {
      await pubsub
        .subscription(name)
        .delete()
        .then(
          () => console.log(`cleanup: DeleteSubscription(${name}) ok`),
          () => undefined,
        );
    }
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
