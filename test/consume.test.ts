import { test } from "node:test";
import assert from "node:assert/strict";
import { MessageFailError, wrapEachMessage, wireCrashListener } from "../src/consume.js";
import { encodeWire } from "../src/wireFormat.js";
import { ACK, fail } from "../src/outcome.js";
import type { DlqRecord } from "../src/dlq.js";
import type { Consumer, EachMessagePayload } from "kafkajs";

// FEAT-113/118: Sol's worker outcome contract is exactly Ack | Fail. A decode
// failure is a rejection routed to the group DLQ (never retried, handler never
// invoked); a handler Fail is fail-stop (offset uncommitted, consumer stops);
// the consumer only exits on a non-Fail crash when kafkajs itself has given up
// (payload.restart === false).

function fakePayload(value: Buffer | null, headers: Record<string, string> = {}): EachMessagePayload {
  return { message: { value, headers } } as unknown as EachMessagePayload;
}

function recordingDlq() {
  const published: DlqRecord[] = [];
  const publisher = {
    publish: async (record: DlqRecord) => {
      published.push(record);
    },
  };
  return { published, publisher };
}

test("wrapEachMessage: a decode failure is counted, never handled, and parked on the group DLQ", async () => {
  let count = 0;
  let handlerCalled = false;
  const { published, publisher } = recordingDlq();
  const wrapped = wrapEachMessage<{ x: number }>({
    decode: () => {
      throw new Error("invalid shape");
    },
    decodeErrorCounter: { inc: () => (count += 1) },
    dlq: { publisher, groupId: "orders-worker", sourceTopic: "orders" },
    handler: async () => {
      handlerCalled = true;
      return ACK;
    },
  });

  await wrapped(fakePayload(encodeWire(1, { x: 1 }), { "content-type": "application/json" }));

  assert.equal(count, 1);
  assert.equal(handlerCalled, false);
  assert.equal(published.length, 1);
  assert.equal(published[0]?.topic, "orders.orders-worker-040f797f7448.dlq");
  assert.equal(published[0]?.headers["X-Sol-Decode-Error"], "Error: invalid shape");
  assert.equal(published[0]?.headers["X-Sol-Origin-Group"], "orders-worker");
  assert.equal(published[0]?.headers["content-type"], "application/json");
});

test("wrapEachMessage: a tombstone (null value) is a decode failure whose DLQ record stays a tombstone", async () => {
  let count = 0;
  const { published, publisher } = recordingDlq();
  const wrapped = wrapEachMessage<{ x: number }>({
    decode: (json) => json as { x: number },
    decodeErrorCounter: { inc: () => (count += 1) },
    dlq: { publisher, groupId: "g", sourceTopic: "orders" },
    handler: async () => {
      throw new Error("should not be called");
    },
  });

  await wrapped(fakePayload(null));
  assert.equal(count, 1);
  assert.equal(published[0]?.value, null);
});

test("wrapEachMessage: a failed DLQ publish throws, so the offset stays uncommitted", async () => {
  const wrapped = wrapEachMessage<{ x: number }>({
    decode: () => {
      throw new Error("invalid shape");
    },
    decodeErrorCounter: { inc: () => {} },
    dlq: {
      publisher: {
        publish: async () => {
          throw new Error("broker down");
        },
      },
      groupId: "g",
      sourceTopic: "orders",
    },
    handler: async () => ACK,
  });

  await assert.rejects(() => wrapped(fakePayload(encodeWire(1, {}))), /not acking/);
});

test("wrapEachMessage: ack-and-drop counts the decode failure and publishes nothing", async () => {
  let count = 0;
  const { published, publisher } = recordingDlq();
  const wrapped = wrapEachMessage<{ x: number }>({
    decode: () => {
      throw new Error("invalid shape");
    },
    decodeErrorCounter: { inc: () => (count += 1) },
    decodeErrorPolicy: "ack-and-drop",
    dlq: { publisher, groupId: "g", sourceTopic: "orders" },
    handler: async () => ACK,
  });

  await wrapped(fakePayload(encodeWire(1, {})));
  assert.equal(count, 1);
  assert.equal(published.length, 0);
});

test("wrapEachMessage: route-to-dlq without a dlq is a construction error", () => {
  assert.throws(
    () =>
      wrapEachMessage<{ x: number }>({
        decode: (json) => json as { x: number },
        decodeErrorCounter: { inc: () => {} },
        decodeErrorPolicy: "route-to-dlq",
        handler: async () => ACK,
      }),
    /needs a dlq publisher/,
  );
});

test("wrapEachMessage: an Ack handler resolves, committing the offset (no throw)", async () => {
  const wrapped = wrapEachMessage<{ x: number }>({
    decode: (json) => json as { x: number },
    decodeErrorCounter: { inc: () => {} },
    handler: async () => ACK,
  });

  await wrapped(fakePayload(encodeWire(1, { x: 1 })));
});

test("wrapEachMessage: a Fail handler throws MessageFailError with the reason (fail-stop)", async () => {
  const wrapped = wrapEachMessage<{ x: number }>({
    decode: (json) => json as { x: number },
    decodeErrorCounter: { inc: () => {} },
    handler: async () => fail("downstream DB error"),
  });

  await assert.rejects(() => wrapped(fakePayload(encodeWire(1, { x: 1 }))), (err: unknown) => {
    assert.ok(err instanceof MessageFailError);
    assert.equal((err as MessageFailError).reason, "downstream DB error");
    return true;
  });
});

function fakeConsumer() {
  const listeners: Record<string, (arg: unknown) => void> = {};
  const state = { stopped: false };
  const consumer = {
    events: { CRASH: "consumer.crash" },
    on: (event: string, cb: (arg: unknown) => void) => {
      listeners[event] = cb;
    },
    stop: async () => {
      state.stopped = true;
    },
  } as unknown as Consumer;
  return { consumer, listeners, state };
}

async function withStubbedExit(run: (exit: { code?: number }) => Promise<void> | void) {
  const originalExit = process.exit;
  const exit: { code?: number } = {};
  // @ts-expect-error -- intentionally stubbing for the test
  process.exit = (code?: number) => {
    exit.code = code;
  };
  try {
    await run(exit);
  } finally {
    process.exit = originalExit;
  }
}

test("wireCrashListener: a MessageFailError stops the consumer and exits 0", async () => {
  const fake = fakeConsumer();
  await withStubbedExit(async (exit) => {
    wireCrashListener(fake.consumer);
    fake.listeners["consumer.crash"]({
      payload: { error: new MessageFailError("bad fact"), restart: true },
    });
    await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(fake.state.stopped, true, "the consumer must be stopped on a handler Fail");
    assert.equal(exit.code, 0);
  });
});

test("wireCrashListener: onFailStop overrides the default stop, then exits 0", async () => {
  const fake = fakeConsumer();
  const reasons: string[] = [];
  await withStubbedExit(async (exit) => {
    wireCrashListener(fake.consumer, {
      onFailStop: (reason) => {
        reasons.push(reason);
      },
    });
    fake.listeners["consumer.crash"]({
      payload: { error: new MessageFailError("bad fact"), restart: true },
    });
    await new Promise((resolve) => setTimeout(resolve, 5));
    assert.deepEqual(reasons, ["bad fact"]);
    assert.equal(fake.state.stopped, false, "the default stop must not also run");
    assert.equal(exit.code, 0);
  });
});

test("wireCrashListener: exits 1 only when kafkajs has given up (payload.restart === false)", async () => {
  const fake = fakeConsumer();
  await withStubbedExit(async (exit) => {
    wireCrashListener(fake.consumer);
    fake.listeners["consumer.crash"]({ payload: { error: new Error("retriable"), restart: true } });
    assert.equal(exit.code, undefined, "must not exit when kafkajs is restarting on its own");

    fake.listeners["consumer.crash"]({ payload: { error: new Error("fatal"), restart: false } });
    assert.equal(exit.code, 1, "must exit once kafkajs has given up");
  });
});
