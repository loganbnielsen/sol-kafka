import { test } from "node:test";
import assert from "node:assert/strict";
import type { EachMessagePayload } from "kafkajs";
import { wrapEachRetryableMessage, type RelayRecord, type RetryRelay } from "../src/retryable.js";
import { ACK, deadLetter, retry } from "../src/outcome.js";
import {
  HDR_ATTEMPT,
  HDR_DECODE_ERROR,
  HDR_ORIGIN_GROUP,
  HDR_RETRY_AT,
  type RetryStrategy,
} from "../src/retry.js";
import { encodeWire } from "../src/wireFormat.js";

// FEAT-081: the routing/record-shape behaviour of the retry-capable wrapper,
// exercised without a broker through an injected RetryRelay. The kafkajs
// wiring itself is covered by the opt-in broker integration test.

function payload(value: Buffer): EachMessagePayload {
  return { message: { value, headers: {}, key: Buffer.from("k") } } as unknown as EachMessagePayload;
}

function collector(): { relay: RetryRelay; records: RelayRecord[] } {
  const records: RelayRecord[] = [];
  return {
    records,
    relay: {
      publish: async (record) => {
        records.push(record);
      },
    },
  };
}

const base = { baseDelayS: 1, maxDelayS: 600, maxAttempts: 3, jitterRatio: 0 };
const retryTopics = (over: Partial<typeof base> = {}): RetryStrategy => ({
  kind: "retry-topics",
  policy: { ...base, ...over },
});
const inMemory = (over: Partial<typeof base> = {}): RetryStrategy => ({
  kind: "in-memory",
  policy: { ...base, ...over },
});

const decodeOk = (json: unknown) => json as { x: number };
function counter(): { inc(): void; readonly value: number } {
  let n = 0;
  return { inc: () => (n += 1), get value() { return n; } };
}

test("retry-topics: Retry forwards to <source>.<group>.retry with attempt/retry-at and commits", async () => {
  const { relay, records } = collector();
  let calls = 0;
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "fulfillment",
    sourceTopic: "orders",
    relay,
    nowS: () => 1000,
    handler: async () => {
      calls += 1;
      return retry("db down");
    },
  });

  await wrapped(payload(encodeWire(1, { x: 1 })));

  assert.equal(calls, 1, "source path runs the handler once, then forwards");
  assert.equal(records.length, 1);
  assert.equal(records[0].topic, "orders.fulfillment.retry");
  assert.equal(records[0].headers[HDR_ATTEMPT], "1");
  assert.equal(records[0].headers[HDR_RETRY_AT], "1001"); // now + backoff(attempt 1) = 1s
  assert.equal(records[0].key?.toString(), "k", "the source key travels with the record");
});

test("retry-topics: Dead_letter forwards straight to <source>.<group>.dlq with origin group", async () => {
  const { relay, records } = collector();
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "fulfillment",
    sourceTopic: "orders",
    relay,
    nowS: () => 1000,
    handler: async () => deadLetter("unprocessable"),
  });

  await wrapped(payload(encodeWire(1, { x: 1 })));

  assert.equal(records.length, 1);
  assert.equal(records[0].topic, "orders.fulfillment.dlq");
  assert.equal(records[0].headers[HDR_ORIGIN_GROUP], "fulfillment");
  assert.equal(records[0].headers[HDR_RETRY_AT], "1000"); // dead letters are immediate
});

test("retry-topics: maxAttempts=1 means no retry -- Retry goes straight to the DLQ", async () => {
  const { relay, records } = collector();
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics({ maxAttempts: 1 }),
    groupId: "fulfillment",
    sourceTopic: "orders",
    relay,
    nowS: () => 1000,
    handler: async () => retry("nope"),
  });

  await wrapped(payload(encodeWire(1, { x: 1 })));
  assert.equal(records.length, 1);
  assert.equal(records[0].topic, "orders.fulfillment.dlq");
  assert.equal(records[0].headers[HDR_ATTEMPT], "1");
});

test("retry-topics: a failed publish fails closed (throws, never acked)", async () => {
  const failing: RetryRelay = { publish: async () => { throw new Error("broker down"); } };
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay: failing,
    handler: async () => retry("boom"),
  });
  await assert.rejects(() => wrapped(payload(encodeWire(1, { x: 1 }))), /not acking/);
});

test("retry-topics: construction fails without a relay or with maxAttempts < 1", () => {
  const common = {
    decode: decodeOk,
    decodeErrorCounter: counter(),
    groupId: "g",
    sourceTopic: "t",
    handler: async () => ACK,
  };
  assert.throws(
    () => wrapEachRetryableMessage({ ...common, retryStrategy: retryTopics() }),
    /requires a relay/,
  );
  assert.throws(
    () => wrapEachRetryableMessage({ ...common, relay: collector().relay, retryStrategy: retryTopics({ maxAttempts: -1 }) }),
    /maxAttempts must be >= 1/,
  );
});

test("ack-and-drop: decode failure increments the counter, never runs the handler, never publishes", async () => {
  const { relay, records } = collector();
  const errors = counter();
  let handlerCalled = false;
  const wrapped = wrapEachRetryableMessage({
    decode: () => {
      throw new Error("bad shape");
    },
    decodeErrorCounter: errors,
    decodeErrorPolicy: "ack-and-drop",
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay,
    handler: async () => {
      handlerCalled = true;
      return ACK;
    },
  });

  await wrapped(payload(encodeWire(1, { x: 1 })));
  assert.equal(errors.value, 1);
  assert.equal(handlerCalled, false);
  assert.equal(records.length, 0);
});

test("in-memory: Retry sleeps the policy backoff and re-runs the handler; a later Ack commits", async () => {
  const sleeps: number[] = [];
  const outcomes = [retry("transient"), ACK];
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: inMemory(),
    groupId: "g",
    sourceTopic: "t",
    sleep: async (s) => {
      sleeps.push(s);
    },
    handler: async () => outcomes.shift()!,
  });

  await wrapped(payload(encodeWire(1, { x: 1 })));
  assert.deepEqual(sleeps, [1]); // backoff(attempt 1)
});

test("in-memory: Dead_letter and retry exhaustion fail closed (throw)", async () => {
  const dl = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: inMemory(),
    groupId: "g",
    sourceTopic: "t",
    sleep: async () => {},
    handler: async () => deadLetter("nope"),
  });
  await assert.rejects(() => dl(payload(encodeWire(1, { x: 1 }))), /not acking/);

  const exhausted = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: inMemory({ maxAttempts: 2 }),
    groupId: "g",
    sourceTopic: "t",
    sleep: async () => {},
    handler: async () => retry("always"),
  });
  await assert.rejects(() => exhausted(payload(encodeWire(1, { x: 1 }))), /not acking/);
});

test("retry-topics: retry/relay metrics are reported", async () => {
  const scheduled: Array<{ attempt: number; delayS: number }> = [];
  const published: string[] = [];
  const { relay } = collector();
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay,
    nowS: () => 0,
    metrics: {
      onSchedule: (i) => scheduled.push(i),
      onRelayPublish: (i) => published.push(i.outcome),
    },
    handler: async () => retry("x"),
  });
  await wrapped(payload(encodeWire(1, { x: 1 })));
  assert.deepEqual(scheduled, [{ attempt: 1, delayS: 1 }]);
  assert.deepEqual(published, ["published"]);
});

// FEAT-098: under retry-topics a source record that cannot be decoded goes, raw,
// to the group-scoped DLQ with the decode diagnostic -- parity with OCaml's
// Route_to_dlq (BUG-051). The offset commits only once that publish lands.
const decodeFails = () => {
  throw new Error("bad shape");
};

test("retry-topics default: an undecodable record is published raw to the DLQ before commit", async () => {
  const { relay, records } = collector();
  const errors = counter();
  let handlerCalled = false;
  const value = encodeWire(1, { x: 1 });
  const wrapped = wrapEachRetryableMessage({
    decode: decodeFails,
    decodeErrorCounter: errors,
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay,
    handler: async () => {
      handlerCalled = true;
      return ACK;
    },
  });

  await wrapped({
    message: { value, headers: { traceparent: Buffer.from("tp") }, key: Buffer.from("k") },
  } as unknown as EachMessagePayload);
  assert.equal(errors.value, 1);
  assert.equal(handlerCalled, false);
  assert.equal(records.length, 1);
  const [record] = records;
  assert.equal(record.topic, "t.g.dlq");
  assert.deepEqual(record.value, value);
  assert.deepEqual(record.key, Buffer.from("k"));
  assert.match(record.headers[HDR_DECODE_ERROR] ?? "", /bad shape/);
  assert.equal(record.headers[HDR_ORIGIN_GROUP], "g");
  assert.equal(record.headers.traceparent, "tp", "original headers are preserved");
  assert.equal(record.headers[HDR_ATTEMPT], undefined, "not another scheduled attempt");
});

test("retry-topics: a failed DLQ publish rejects, so the offset stays uncommitted", async () => {
  const outcomes: string[] = [];
  const wrapped = wrapEachRetryableMessage({
    decode: decodeFails,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay: {
      publish: async () => {
        throw new Error("broker down");
      },
    },
    metrics: { onRelayPublish: ({ outcome }) => outcomes.push(outcome) },
    handler: async () => ACK,
  });
  await assert.rejects(wrapped(payload(encodeWire(1, { x: 1 }))), /not acking/);
  assert.deepEqual(outcomes, ["failed"]);
});

test("in-memory: route-to-dlq is a construction error; the default drops", async () => {
  assert.throws(
    () =>
      wrapEachRetryableMessage({
        decode: decodeFails,
        decodeErrorCounter: counter(),
        decodeErrorPolicy: "route-to-dlq",
        retryStrategy: inMemory(),
        groupId: "g",
        sourceTopic: "t",
        handler: async () => ACK,
      }),
    /route-to-dlq needs a DLQ/,
  );
  const errors = counter();
  const wrapped = wrapEachRetryableMessage({
    decode: decodeFails,
    decodeErrorCounter: errors,
    retryStrategy: inMemory(),
    groupId: "g",
    sourceTopic: "t",
    handler: async () => ACK,
  });
  await wrapped(payload(encodeWire(1, { x: 1 })));
  assert.equal(errors.value, 1);
});

test("retry-topics: a source tombstone is dead-lettered as a tombstone, not an empty value", async () => {
  const { relay, records } = collector();
  const wrapped = wrapEachRetryableMessage({
    decode: decodeOk,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay,
    handler: async () => ACK,
  });
  await wrapped({
    message: { value: null, headers: {}, key: Buffer.from("k") },
  } as unknown as EachMessagePayload);
  assert.equal(records.length, 1);
  assert.equal(records[0].value, null);
  assert.match(records[0].headers[HDR_DECODE_ERROR] ?? "", /tombstone/);
});

test("retry-topics: a redriven record's stale diagnostic headers are replaced by the fresh ones", async () => {
  const { relay, records } = collector();
  const wrapped = wrapEachRetryableMessage({
    decode: decodeFails,
    decodeErrorCounter: counter(),
    retryStrategy: retryTopics(),
    groupId: "g",
    sourceTopic: "t",
    relay,
    handler: async () => ACK,
  });
  await wrapped({
    message: {
      value: encodeWire(1, { x: 1 }),
      headers: {
        [HDR_DECODE_ERROR]: Buffer.from("old error"),
        [HDR_ORIGIN_GROUP]: Buffer.from("old-group"),
        keep: Buffer.from("me"),
      },
      key: Buffer.from("k"),
    },
  } as unknown as EachMessagePayload);
  const [record] = records;
  assert.match(record.headers[HDR_DECODE_ERROR] ?? "", /bad shape/);
  assert.equal(record.headers[HDR_ORIGIN_GROUP], "g");
  assert.equal(record.headers.keep, "me");
});
