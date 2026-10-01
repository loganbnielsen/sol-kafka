import { test } from "node:test";
import assert from "node:assert/strict";
import { Kafka, type Consumer } from "kafkajs";
import { registerTopic } from "../src/register.js";
import { publish } from "../src/publish.js";
import { provisionRelayTopics } from "../src/relay.js";
import { describeTopic } from "../src/admin.js";
import { decodeWire } from "../src/wireFormat.js";
import type { TopicContract } from "../src/contract.js";

// FEAT-117's certificate, mirroring the OCaml integration case BUG-099 added
// (framework/ocaml/kafka-eio-service/test/test_kafka_service_integration.ml,
// "same-key records keep their order across partitions"): a 3-partition topic
// is created at the DECLARED count, its relay topics inherit that count, and
// interleaved records for three keys are consumed by two members of one group
// with each key arriving exactly once, in publication order, handled entirely
// by one member.
//
// Local-only and env-gated like the rest of the broker suite: it needs a broker
// AND a schema registry, so it skips unless both are set. Run with:
//
//   KAFKA_BROKERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 npm test

const BROKERS = (process.env.KAFKA_BROKERS ?? "").split(",").filter(Boolean);
const REGISTRY = process.env.SCHEMA_REGISTRY_URL;
const it = BROKERS.length > 0 && REGISTRY ? test : test.skip;

interface OrderingEvent {
  ordering_key: string;
  seq: number;
}

const SCHEMA = JSON.stringify({
  type: "object",
  properties: {
    ordering_key: { type: "string" },
    seq: { type: "integer" },
  },
  required: ["ordering_key", "seq"],
});

const suffix = () => `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
const DECLARED_PARTITIONS = 3;
// Deliberately more keys than partitions: kafkajs's default partitioner
// round-robins an unkeyed record, and with a key count equal to the partition
// count each key would land on one partition by accident, hiding the missing
// key. Four keys make an unkeyed run spread every key across all three
// partitions, so only real keying satisfies the per-key assertions.
const KEYS = ["alpha", "beta", "gamma", "delta"];
const PER_KEY = 4;

async function waitUntil(fn: () => boolean, what: string, timeoutMs = 60000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!fn()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 150));
  }
}

it("integration: a declared multi-partition topic keeps same-key records ordered on one member", async () => {
  const s = suffix();
  const topicName = `sol-int-partition-${s}`;
  const groupId = `sol-int-partition-group-${s}`;
  const kafka = new Kafka({ clientId: `sol-int-partition-${s}`, brokers: BROKERS });

  const contract: TopicContract<OrderingEvent> = {
    name: topicName,
    schema: SCHEMA,
    partitions: DECLARED_PARTITIONS,
    key: (m) => m.ordering_key,
  };

  const topic = await registerTopic({ kafka, registryUrl: REGISTRY as string, contract });

  // The broker got the declared count, not a default.
  const live = await describeTopic(kafka, topicName);
  assert.equal(live?.partitions, DECLARED_PARTITIONS, "the topic was created at the declared count");

  // The relay topics inherit the source's count, so a transferred record keeps
  // its key -> partition mapping.
  const { retryTopic, dlqTopic } = await provisionRelayTopics({ kafka, groupId, source: topic });
  assert.equal((await describeTopic(kafka, retryTopic))?.partitions, DECLARED_PARTITIONS);
  assert.equal((await describeTopic(kafka, dlqTopic))?.partitions, DECLARED_PARTITIONS);

  const producer = kafka.producer();
  await producer.connect();
  const consumers: Consumer[] = [];
  const observed: Array<{ key: string; seq: number; member: string }> = [];
  // kafkajs 2.x has no `consumer.assignment()`, so each member's latest
  // assignment is read from its GROUP_JOIN event instead.
  const assignedPartitions = new Map<string, number>();

  try {
    const startMember = async (member: string): Promise<void> => {
      const consumer = kafka.consumer({ groupId, clientId: `sol-int-member-${member}-${s}` });
      consumer.on(consumer.events.GROUP_JOIN, (event) => {
        assignedPartitions.set(member, event.payload.memberAssignment[topicName]?.length ?? 0);
      });
      await consumer.connect();
      await consumer.subscribe({ topic: topicName, fromBeginning: true });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const { json } = decodeWire(message.value as Buffer);
          const event = json as OrderingEvent;
          observed.push({ key: event.ordering_key, seq: event.seq, member });
        },
      });
      consumers.push(consumer);
    };

    await startMember("a");
    await startMember("b");

    // Wait until both members own partitions and the group covers the topic, so
    // no key's records can straddle a rebalance mid-run.
    await waitUntil(
      () => {
        const sizes = [...assignedPartitions.values()];
        return (
          assignedPartitions.size === 2 &&
          sizes.every((n) => n >= 1) &&
          sizes.reduce((a, b) => a + b, 0) === DECLARED_PARTITIONS
        );
      },
      "both members to be assigned partitions",
    );

    // Interleave: seq 1 for every key, then seq 2, ...
    for (let seq = 1; seq <= PER_KEY; seq += 1) {
      for (const key of KEYS) {
        await publish(producer, topic, { ordering_key: key, seq });
      }
    }

    const target = KEYS.length * PER_KEY;
    await waitUntil(() => observed.length >= target, `${target} records to be consumed`);

    const arrivalOrderFor = (key: string) =>
      observed.filter((o) => o.key === key).map((o) => o.seq);
    const membersFor = (key: string) =>
      new Set(observed.filter((o) => o.key === key).map((o) => o.member));

    for (const key of KEYS) {
      assert.deepEqual(
        arrivalOrderFor(key),
        Array.from({ length: PER_KEY }, (_, i) => i + 1),
        `every record for ${key} arrived once, in order`,
      );
      assert.equal(membersFor(key).size, 1, `one member handled every record for ${key}`);
    }
  } finally {
    await Promise.all(consumers.map((c) => c.disconnect()));
    await producer.disconnect();
  }
});
