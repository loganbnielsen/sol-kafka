import { test } from "node:test";
import assert from "node:assert/strict";
import { registerTopic } from "../src/register.js";
import type { TopicContract } from "../src/contract.js";
import { fakeKafka } from "./fakes.js";

// registerTopic must, in this exact order:
//   0. reject a declared partition count below 1 -- before any broker call
//   1. refuse to reduce an existing topic below its live partition count
//   2. createTopics (topic provisioning) with the DECLARED count -- BEFORE any
//      schema-registry call
//   3. registerSchema -- FATAL, must propagate on failure
//   4. setSubjectCompatibility -- NON-FATAL, must swallow failure (warn only)
// FEAT-033's hand-rolled port got the order and fatality backwards on its
// first draft (compatibility-check-then-register, both fatal).

const DECLARED = 3;

function contract(over: Partial<TopicContract<{ id: string }>> = {}): TopicContract<{ id: string }> {
  return {
    name: "orders",
    schema: "{}",
    partitions: DECLARED,
    key: (m) => m.id,
    ...over,
  };
}

test("registerTopic: provisions with the declared count before touching the schema registry", async (t) => {
  const { kafka, calls, created } = fakeKafka();
  const fetchMock = t.mock.method(global, "fetch", async (_url: string, init: RequestInit) => {
    calls.push(init.method === "POST" ? "registerSchema" : "setCompatibility");
    if ((init.method as string) === "POST") {
      return new Response(JSON.stringify({ id: 7 }), { status: 200 });
    }
    return new Response("", { status: 204 });
  });

  const declared = contract();
  const result = await registerTopic({ kafka, registryUrl: "http://registry", contract: declared });

  assert.deepEqual(calls, [
    "listTopics",
    "createTopics",
    "registerSchema",
    "setCompatibility",
  ]);
  assert.equal(created[0]?.topic, "orders");
  assert.equal(created[0]?.numPartitions, DECLARED, "the declared count reaches the broker");
  assert.equal(result.schemaId, 7);
  assert.equal(result.partitions, DECLARED);
  assert.equal(result.key, declared.key, "the registered topic carries the declared key");
  fetchMock.mock.restore();
});

test("registerTopic: registerSchema failure is FATAL and propagates", async (t) => {
  const { kafka } = fakeKafka();
  const fetchMock = t.mock.method(global, "fetch", async () => new Response("boom", { status: 500 }));

  await assert.rejects(() =>
    registerTopic({ kafka, registryUrl: "http://registry", contract: contract() })
  );
  fetchMock.mock.restore();
});

test("registerTopic: setSubjectCompatibility failure is NON-FATAL and does not throw", async (t) => {
  const { kafka } = fakeKafka();
  let call = 0;
  const fetchMock = t.mock.method(global, "fetch", async () => {
    call += 1;
    if (call === 1) return new Response(JSON.stringify({ id: 3 }), { status: 200 }); // registerSchema ok
    return new Response("registry does not support this", { status: 500 }); // setSubjectCompatibility fails
  });

  const result = await registerTopic({ kafka, registryUrl: "http://registry", contract: contract() });
  assert.equal(result.schemaId, 3); // did not throw despite compatibility failure
  fetchMock.mock.restore();
});

test("registerTopic: refuses to reduce an existing topic below its live partition count", async () => {
  const { kafka, calls, created } = fakeKafka({ livePartitions: 5 });

  await assert.rejects(
    () => registerTopic({ kafka, registryUrl: "http://registry", contract: contract() }),
    /refusing to reduce it to the declared 3/,
  );
  assert.deepEqual(calls, ["listTopics", "fetchTopicMetadata"], "the guard stops before any create");
  assert.equal(created.length, 0);
});

test("registerTopic: an existing topic at the declared count is not an error", async (t) => {
  const { kafka, created } = fakeKafka({ livePartitions: DECLARED });
  const fetchMock = t.mock.method(
    global,
    "fetch",
    async () => new Response(JSON.stringify({ id: 1 }), { status: 200 }),
  );

  await registerTopic({ kafka, registryUrl: "http://registry", contract: contract() });

  assert.equal(created[0]?.numPartitions, DECLARED);
  fetchMock.mock.restore();
});

test("registerTopic: a declared count below one is rejected before any broker call", async () => {
  const { kafka, calls } = fakeKafka();
  await assert.rejects(
    () =>
      registerTopic({ kafka, registryUrl: "http://registry", contract: contract({ partitions: 0 }) }),
    /a topic has at least one/,
  );
  assert.deepEqual(calls, [], "no broker round-trip for an invalid declaration");
});
