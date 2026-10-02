import { test } from "node:test";
import assert from "node:assert/strict";
import { publish } from "../src/publish.js";
import { provisionDlqTopic } from "../src/dlq.js";
import { decodeWire } from "../src/wireFormat.js";
import type { RegisteredTopic } from "../src/contract.js";
import { fakeKafka, fakeProducer } from "./fakes.js";

// FEAT-117: the declared contract's two halves. The topic is created at the
// declared count and its DLQ inherits that count; every published record is
// keyed by the declared key, which is what keeps one entity's records ordered
// on a multi-partition topic (BUG-099).

interface Order {
  id: string;
  seq: number;
}

const registered: RegisteredTopic<Order> = {
  name: "orders",
  schema: "{}",
  partitions: 3,
  key: (m) => m.id,
  schemaId: 42,
};

test("publish: keys the record with the contract's declared key and wire-encodes it", async () => {
  const { producer, sent } = fakeProducer();

  await publish(producer, registered, { id: "alpha", seq: 1 }, { headers: { traceparent: "00-t-s-01" } });

  assert.equal(sent.length, 1);
  assert.equal(sent[0]?.topic, "orders");
  const record = sent[0]?.messages[0];
  assert.equal(record?.key, "alpha", "the declared key reaches the record");
  assert.deepEqual(decodeWire(record?.value as Buffer), {
    schemaId: 42,
    json: { id: "alpha", seq: 1 },
  });
  assert.deepEqual(record?.headers, { traceparent: "00-t-s-01" });
});

test("publish: an undefined declared key publishes unkeyed", async () => {
  const { producer, sent } = fakeProducer();
  const unkeyed: RegisteredTopic<{ id?: string }> = {
    name: "orders",
    schema: "{}",
    partitions: 1,
    key: () => undefined,
    schemaId: 1,
  };

  await publish(producer, unkeyed, {});

  assert.equal("key" in (sent[0]?.messages[0] as object), false);
});

test("provisionDlqTopic: the DLQ inherits the source topic's live partition count", async () => {
  const { kafka, created } = fakeKafka({ livePartitions: 6 });

  const dlqTopic = await provisionDlqTopic({
    kafka,
    groupId: "g",
    source: { name: "orders", partitions: 3 },
  });

  assert.equal(dlqTopic, "orders.g-b2f5ff474366.dlq");
  assert.deepEqual(
    created.map((t) => t.numPartitions),
    [6],
    "the live source count wins over the declaration",
  );
});

test("provisionDlqTopic: before the source exists, the declared count is used", async () => {
  const { kafka, created } = fakeKafka();

  await provisionDlqTopic({ kafka, groupId: "g", source: { name: "orders", partitions: 4 } });

  assert.deepEqual(created.map((t) => t.numPartitions), [4]);
});

test("provisionDlqTopic: a declared count below one is rejected before any broker call", async () => {
  const { kafka, calls } = fakeKafka();

  await assert.rejects(
    () => provisionDlqTopic({ kafka, groupId: "g", source: { name: "orders", partitions: 0 } }),
    /a topic has at least one/,
  );
  assert.deepEqual(calls, []);
});
