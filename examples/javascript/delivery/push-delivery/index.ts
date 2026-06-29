/**
 * KubeMQ Google Cloud Pub/Sub — delivery: push delivery
 * (TypeScript / @google-cloud/pubsub).
 *
 * Bind a tiny local HTTP endpoint -> CreateTopic -> CreateSubscription with a
 * pushConfig.pushEndpoint pointing at that endpoint -> publish one message ->
 * the connector's per-sub push worker POSTs a wrapped JSON envelope to the
 * endpoint; the handler asserts the envelope shape and returns 200 (ack).
 *
 * This is an unmodified @google-cloud/pubsub program. Setting PUBSUB_EMULATOR_HOST
 * makes the client clear credentials, skip Google auth, and dial insecure gRPC at
 * the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.
 *
 * Connector behaviour (spec S3.5 / docs/guides/push-delivery.md): a subscription
 * with a push_config is served by a per-sub worker that POSTs a wrapped JSON
 * envelope:
 *   { "message": { "data": "<base64>", "attributes": {...}, "messageId": "...",
 *                  "publishTime": "...", "orderingKey": "..." },
 *     "subscription": "..." }
 * A 2xx response ACKs the message; non-2xx/timeout retries with backoff;
 * exhaustion dead-letters (if a DLQ is set) else drops. OIDC Bearer is optional.
 * HTTPS is required EXCEPT for localhost, where plain HTTP is allowed (used here).
 *
 * Run: npx tsx delivery/push-delivery/index.ts
 */
import { PubSub } from '@google-cloud/pubsub';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';

const PROJECT =
  process.env['PUBSUB_PROJECT_ID'] ?? process.env['GOOGLE_CLOUD_PROJECT'] ?? 'my-project';

// Unique-per-run resource names (channel-isolation rule, spec S9).
const RUN = randomUUID().slice(0, 8);
const TOPIC_ID = `pushed-${RUN}`;
const SUB_ID = `sub-pushed-${RUN}`;

const BODY = 'push-payload';
const ATTRS = { region: 'eu', source: 'push-example' };

// The host the CONNECTOR uses to reach this process. The connector runs in
// kubemq-server (default localhost), so it dials back to localhost; HTTP is
// allowed because the endpoint host is localhost (HTTPS is required otherwise).
const PUSH_HOST = process.env['PUSH_ENDPOINT_HOST'] ?? '127.0.0.1';

interface PushEnvelope {
  message?: {
    data?: string;
    attributes?: Record<string, string>;
    messageId?: string;
    message_id?: string;
    publishTime?: string;
    publish_time?: string;
    orderingKey?: string;
  };
  subscription?: string;
}

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
  console.log(`=== Pub/Sub push delivery — topic '${TOPIC_ID}' / sub '${SUB_ID}' ===`);
  console.log(`  emulator : ${process.env['PUBSUB_EMULATOR_HOST'] ?? '(unset!)'}`);
  console.log(`  project  : ${PROJECT} (parsed but ignored by the connector)`);
  console.log();

  // --- Bind a local push receiver (HTTP; localhost exemption) --------------
  let resolveEnvelope: (e: PushEnvelope) => void;
  const envelopePromise = new Promise<PushEnvelope>((resolve) => {
    resolveEnvelope = resolve;
  });

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString();
      let parsed: PushEnvelope = {};
      try {
        parsed = JSON.parse(raw) as PushEnvelope;
      } catch {
        // Leave parsed empty; the assertions below will surface the failure.
      }
      // 2xx ACKs the message so the connector does not retry.
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
      resolveEnvelope(parsed);
    });
  });

  server.listen(0, PUSH_HOST);
  await once(server, 'listening');
  const port = (server.address() as AddressInfo).port;
  const pushEndpoint = `http://${PUSH_HOST}:${port}/push`;
  console.log(`Local push receiver listening -> ${pushEndpoint}`);

  const pubsub = new PubSub({ projectId: PROJECT });
  const [topic] = await pubsub.createTopic(TOPIC_ID);
  console.log(`CreateTopic            -> projects/${PROJECT}/topics/${TOPIC_ID}`);

  const [subscription] = await topic.createSubscription(SUB_ID, {
    pushConfig: { pushEndpoint },
  });
  console.log(`CreateSubscription     -> ${SUB_ID}  (push_config -> ${pushEndpoint})`);
  console.log();

  try {
    const msgId = await topic.publishMessage({
      data: Buffer.from(BODY),
      attributes: ATTRS,
    });
    console.log(`Publish -> '${BODY}'  id=${msgId}  attributes=${JSON.stringify(ATTRS)}`);
    console.log();

    console.log('Awaiting the connector push POST (wrapped JSON envelope):');
    const envelope = await Promise.race([
      envelopePromise,
      new Promise<PushEnvelope>((_, reject) =>
        setTimeout(() => reject(new Error('no push received within 30s')), 30_000),
      ),
    ]);

    const wrapped = envelope.message ?? {};
    const decoded = Buffer.from(wrapped.data ?? '', 'base64').toString();
    const envMsgId = wrapped.messageId ?? wrapped.message_id;
    console.log(`  envelope.message.data (base64-decoded) = '${decoded}'`);
    console.log(`  envelope.message.messageId             = ${envMsgId}`);
    console.log(`  envelope.message.attributes            = ${JSON.stringify(wrapped.attributes ?? {})}`);
    console.log(`  envelope.subscription                  = ${envelope.subscription}`);
    console.log();

    check(decoded === BODY, 'pushed envelope carries the base64-encoded original body');
    check(envMsgId === msgId, 'pushed envelope messageId matches the published id');
    check(
      (wrapped.attributes ?? {})['region'] === ATTRS.region,
      'pushed envelope preserves the message attributes',
    );
    check(
      typeof envelope.subscription === 'string' && envelope.subscription.includes(SUB_ID),
      'pushed envelope names the subscription',
    );

    console.log();
    console.log('PASS — push delivery: connector POSTed the wrapped envelope; 2xx acked it.');
  } finally {
    await subscription.delete().then(
      () => console.log('cleanup: DeleteSubscription ok'),
      () => undefined,
    );
    await topic.delete().then(
      () => console.log('cleanup: DeleteTopic ok'),
      () => undefined,
    );
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

main()
  .then(() => process.exit(failed ? 1 : 0))
  .catch((err) => {
    console.error(`  [FAIL] ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  });
