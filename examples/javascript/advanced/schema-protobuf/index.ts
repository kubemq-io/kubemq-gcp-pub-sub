/**
 * advanced/schema-protobuf — attach a Protobuf schema to a topic and enforce it on publish.
 *
 * Flow:
 *   CreateSchema(ProtocolBuffer) -> CreateTopic{ schemaSettings: { schema, encoding: JSON } }
 *     -> Publish(conforming)      -> accepted, message-id returned
 *     -> Publish(non-conforming)  -> whole batch rejected INVALID_ARGUMENT
 *
 * When a topic references a schema the connector enforces it at publish time and rejects
 * the ENTIRE batch (INVALID_ARGUMENT) on the first non-conforming message. Protobuf is
 * handled connector-side via jhump/protoreflect; schema definitions are <= 300 KB and at
 * least one revision is always kept.
 *
 * Uses ONLY the official @google-cloud/pubsub client (pubsub.createSchema + Topic schemaSettings).
 * Emulator-protocol drop-in: set PUBSUB_EMULATOR_HOST before running; no code endpoint override.
 *
 * Run: npx tsx advanced/schema-protobuf/index.ts
 */
import { PubSub, SchemaTypes, Encodings } from '@google-cloud/pubsub';
import { randomUUID } from 'node:crypto';

const suffix = (): string => randomUUID().replace(/-/g, '').slice(0, 8);
function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
}
function fail(err: unknown): void {
  console.error('\nFAILED:', err instanceof Error ? err.message : err);
  process.exitCode = 1;
}

// Unique-per-run resource ids so concurrent runs use distinct channels/registry records.
const SCHEMA = `proto-schema-${suffix()}`;
const TOPIC = `proto-topic-${suffix()}`;
const SUB = `sub-proto-${suffix()}`;

// A proto3 message: Order { int64 id = 1; string name = 2; }. JSON messages must conform.
const PROTO_DEFINITION = [
  'syntax = "proto3";',
  'message Order {',
  '  int64 id = 1;',
  '  string name = 2;',
  '}',
].join('\n');

async function main(): Promise<void> {
  const projectId = process.env.PUBSUB_PROJECT_ID ?? process.env.GOOGLE_CLOUD_PROJECT ?? 'my-project';
  const pubsub = new PubSub({ projectId });
  const host = process.env.PUBSUB_EMULATOR_HOST ?? '(unset)';
  console.log(`Connecting to KubeMQ Pub/Sub connector via PUBSUB_EMULATOR_HOST=${host}`);

  try {
    const schema = await pubsub.createSchema(SCHEMA, SchemaTypes.ProtocolBuffer, PROTO_DEFINITION);
    const schemaName = await schema.getName();
    console.log(`CreateSchema(Proto) -> ${SCHEMA} (<= 300 KB; >= 1 revision kept)`);

    // Bind the schema to the topic with JSON encoding; publishes are now enforced.
    const [topic] = await pubsub.createTopic({
      name: TOPIC,
      schemaSettings: { schema: schemaName, encoding: Encodings.Json },
    });
    console.log(`CreateTopic         -> ${TOPIC}  (schema ${SCHEMA}, encoding JSON; channel pubsub.${TOPIC})`);
    await topic.createSubscription(SUB);
    console.log(`CreateSubscription  -> ${SUB}  (channel pubsub.sub.${SUB})`);

    // Conforming message: JSON matching the proto message -> accepted.
    const conforming = Buffer.from(JSON.stringify({ id: 1, name: 'widget' }));
    const id = await topic.publishMessage({ data: conforming });
    console.log(`Publish (conforming)     -> accepted, id ${id}`);
    assert(typeof id === 'string' && id.length > 0, 'conforming publish should return a message id');

    // Non-conforming message: unknown field not in the proto -> whole batch rejected.
    const bad = Buffer.from(JSON.stringify({ id: 2, name: 'widget', bogus: 'extra-field' }));
    let rejected = false;
    let detail = '';
    try {
      await topic.publishMessage({ data: bad });
    } catch (err) {
      rejected = true;
      detail = err instanceof Error ? err.message : String(err);
    }
    console.log(`Publish (non-conforming) -> ${rejected ? 'rejected (batch) INVALID_ARGUMENT' : 'UNEXPECTEDLY ACCEPTED'}`);
    assert(rejected, 'schema-invalid message must be rejected by the connector');
    assert(/INVALID_ARGUMENT|invalid|schema|unknown/i.test(detail), `expected a schema/INVALID_ARGUMENT rejection, got: ${detail}`);

    console.log('\nPASS: Protobuf schema enforced — conforming message accepted, non-conforming batch rejected INVALID_ARGUMENT.');
  } finally {
    // Best-effort cleanup so re-runs start clean (must not fail the run).
    await pubsub.subscription(SUB).delete().catch(() => {});
    await pubsub.topic(TOPIC).delete().catch(() => {});
    await pubsub.schema(SCHEMA).delete().catch(() => {});
    await pubsub.close().catch(() => {});
  }
}

main().catch(fail);
