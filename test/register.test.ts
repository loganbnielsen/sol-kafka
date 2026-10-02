import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import {
  checkContract,
  connectTopic,
  provisionTopic,
  registerContract,
  resolveContract,
} from "../src/register.js";
import { checkCompatibility } from "../src/schemaRegistry.js";
import type { TopicContract } from "../src/contract.js";
import { fakeKafka } from "./fakes.js";

// The BUG-105 split: provisioning and the read-only runtime (`connectTopic`,
// `resolveContract`, `checkContract`) never touch the registry's write
// endpoints; only the deployment path (`registerContract`) does, and it sets
// `FULL` before registering.
//
//   provisionTopic   -> partition guard + createTopics (no registry)
//   connectTopic     -> provisionTopic + resolveContract (read-only)
//   registerContract -> PUT /config then POST /versions (the only writes)

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

interface RegistryCall {
  method: string;
  path: string;
}

function registryFetch(t: TestContext, respond: (call: RegistryCall) => Response): RegistryCall[] {
  const calls: RegistryCall[] = [];
  t.mock.method(global, "fetch", async (url: string, init: RequestInit) => {
    const call = { method: String(init?.method ?? "GET"), path: new URL(String(url)).pathname };
    calls.push(call);
    return respond(call);
  });
  return calls;
}

const compatible = (): Response =>
  new Response(JSON.stringify({ is_compatible: true }), { status: 200 });
const registered = (id: number): Response => new Response(JSON.stringify({ id }), { status: 200 });

test("provisionTopic: provisions with the declared count and makes no registry call", async (t) => {
  const { kafka, calls, created } = fakeKafka();
  const registryCalls = registryFetch(t, () => new Response(null, { status: 204 }));

  await provisionTopic({ kafka, contract: contract() });

  assert.deepEqual(calls, ["listTopics", "createTopics"]);
  assert.equal(created[0]?.topic, "orders");
  assert.equal(created[0]?.numPartitions, DECLARED, "the declared count reaches the broker");
  assert.deepEqual(registryCalls, [], "provisioning touches no registry endpoint");
});

test("provisionTopic: refuses to reduce an existing topic below its live partition count", async () => {
  const { kafka, calls, created } = fakeKafka({ livePartitions: 5 });

  await assert.rejects(
    () => provisionTopic({ kafka, contract: contract() }),
    /refusing to reduce it to the declared 3/,
  );
  assert.deepEqual(calls, ["listTopics", "fetchTopicMetadata"], "the guard stops before any create");
  assert.equal(created.length, 0);
});

test("provisionTopic: a declared count below one is rejected before any broker call", async () => {
  const { kafka, calls } = fakeKafka();
  await assert.rejects(
    () => provisionTopic({ kafka, contract: contract({ partitions: 0 }) }),
    /a topic has at least one/,
  );
  assert.deepEqual(calls, [], "no broker round-trip for an invalid declaration");
});

test("connectTopic: resolves read-only -- no version POST, no compatibility PUT", async (t) => {
  const { kafka, calls } = fakeKafka();
  const registryCalls = registryFetch(t, (call) =>
    call.path.includes("/compatibility/") ? compatible() : registered(7),
  );

  const declared = contract();
  const topic = await connectTopic({ kafka, registryUrl: "http://registry", contract: declared });

  assert.equal(topic.schemaId, 7);
  assert.equal(topic.partitions, DECLARED);
  assert.equal(topic.key, declared.key, "the connected topic carries the declared key");
  assert.deepEqual(calls, ["listTopics", "createTopics"]);
  assert.deepEqual(
    registryCalls.map((c) => `${c.method} ${c.path}`),
    ["POST /compatibility/subjects/orders-value/versions/latest", "POST /subjects/orders-value"],
    "the runtime checks compatibility and looks up the registered id, nothing else",
  );
  assert.equal(
    registryCalls.some((c) => c.path.endsWith("/versions")),
    false,
    "the runtime never registers a version",
  );
  assert.equal(
    registryCalls.some((c) => c.path.startsWith("/config/")),
    false,
    "the runtime never changes subject compatibility",
  );
});

test("connectTopic: an unregistered contract fails with a clear error", async (t) => {
  const { kafka } = fakeKafka();
  registryFetch(t, (call) =>
    call.path.includes("/compatibility/")
      ? compatible()
      : new Response(JSON.stringify({ error_code: 40401 }), { status: 404 }),
  );

  await assert.rejects(
    () => connectTopic({ kafka, registryUrl: "http://registry", contract: contract() }),
    /has no registered schema matching the declared contract/,
  );
});

test("resolveContract: an incompatible declared schema fails before any lookup", async (t) => {
  const registryCalls = registryFetch(t, () =>
    new Response(JSON.stringify({ is_compatible: false }), { status: 200 }),
  );

  await assert.rejects(
    () => resolveContract({ registryUrl: "http://registry", contract: contract() }),
    /is not compatible with the registered version/,
  );
  assert.equal(registryCalls.length, 1, "the lookup is never reached");
});

test("registerContract: sets FULL before registering", async (t) => {
  const registryCalls = registryFetch(t, (call) =>
    call.path.startsWith("/config/") ? new Response(null, { status: 204 }) : registered(9),
  );

  const result = await registerContract({ registryUrl: "http://registry", contract: contract() });

  assert.equal(result.schemaId, 9);
  assert.deepEqual(
    registryCalls.map((c) => `${c.method} ${c.path}`),
    ["PUT /config/orders-value", "POST /subjects/orders-value/versions"],
  );
});

test("registerContract: a failed compatibility PUT is fatal", async (t) => {
  registryFetch(t, () => new Response("registry does not support this", { status: 500 }));

  await assert.rejects(() =>
    registerContract({ registryUrl: "http://registry", contract: contract() }),
  );
});

test("checkContract: an incompatible reader schema is an error", async (t) => {
  registryFetch(t, () => new Response(JSON.stringify({ is_compatible: false }), { status: 200 }));

  await assert.rejects(
    () => checkContract({ registryUrl: "http://registry", contract: contract() }),
    /is not compatible with the registered version/,
  );
});

test("checkContract: an empty subject is compatible (40401 only)", async (t) => {
  registryFetch(t, () => new Response(JSON.stringify({ error_code: 40401 }), { status: 404 }));

  await checkContract({ registryUrl: "http://registry", contract: contract() });
});

test("checkCompatibility: a 404 that is not subject-not-found is an error", async (t) => {
  registryFetch(t, () => new Response("<html>not a registry</html>", { status: 404 }));

  await assert.rejects(
    () => checkCompatibility("http://registry", "orders", "{}"),
    /is http:\/\/registry the registry's base URL/,
  );
});
