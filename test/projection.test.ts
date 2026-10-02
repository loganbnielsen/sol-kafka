import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { contractProjection, runContractCli, type NamedContract } from "../src/projection.js";
import type { TopicContract } from "../src/contract.js";

// The projection object is the language-neutral contract Sol consumes (BUG-105):
// the exact JSON the OCaml `Contract.projection` emits, so `sol plan`,
// `sol up --apply` and `sol deploy`'s in-destination Job need no language-
// specific handling. `--json` is offline; `--check` is read-only; `--apply` is
// the only writer, and it sets FULL before registering.

const order: TopicContract<{ id: string }> = {
  name: "orders",
  schema: '{"type":"object"}',
  partitions: 3,
  key: (m) => m.id,
};

const events: NamedContract[] = [
  { module: "OrderPlaced", contract: order },
  { module: "OrderShipped", contract: { ...order, name: "shipments" } },
];

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

test("contractProjection: emits BUG-105's language-neutral object", () => {
  assert.deepEqual(contractProjection(events), {
    version: 1,
    events: [
      { module: "OrderPlaced", topic: "orders", partitions: 3, schema: '{"type":"object"}' },
      { module: "OrderShipped", topic: "shipments", partitions: 3, schema: '{"type":"object"}' },
    ],
  });
});

test("runContractCli --json: prints the projection offline and exits 0", async () => {
  const lines: string[] = [];
  const registryCalls = await withNoFetch(async () =>
    runContractCli(events, ["--json"], { stdout: (line) => lines.push(line) }),
  );

  assert.equal(registryCalls, "no fetch");
  assert.equal(lines.length, 1);
  assert.deepEqual(JSON.parse(lines[0] as string), contractProjection(events));
});

test("runContractCli --check: read-only, never writes the registry", async (t) => {
  const lines: string[] = [];
  const calls = registryFetch(t, () =>
    new Response(JSON.stringify({ is_compatible: true }), { status: 200 }),
  );

  const code = await runContractCli(events, ["--check"], {
    registryUrl: "http://registry",
    stdout: (line) => lines.push(line),
  });

  assert.equal(code, 0);
  assert.deepEqual(lines, ["contract OrderPlaced: compatible", "contract OrderShipped: compatible"]);
  assert.equal(
    calls.every((c) => c.method === "POST" && c.path.includes("/compatibility/")),
    true,
    "checking makes no write call",
  );
});

test("runContractCli --apply: sets FULL then registers, and exits 0", async (t) => {
  const lines: string[] = [];
  const calls = registryFetch(t, (call) =>
    call.path.startsWith("/config/")
      ? new Response(null, { status: 204 })
      : new Response(JSON.stringify({ id: 5 }), { status: 200 }),
  );

  const code = await runContractCli(events, ["--apply"], {
    registryUrl: "http://registry",
    stdout: (line) => lines.push(line),
  });

  assert.equal(code, 0);
  assert.deepEqual(lines, [
    "contract OrderPlaced: registered (schema id 5)",
    "contract OrderShipped: registered (schema id 5)",
  ]);
  assert.deepEqual(
    calls.map((c) => `${c.method} ${c.path}`),
    [
      "PUT /config/orders-value",
      "POST /subjects/orders-value/versions",
      "PUT /config/shipments-value",
      "POST /subjects/shipments-value/versions",
    ],
  );
});

test("runContractCli --apply: a failed registration is reported and exits 1", async (t) => {
  const errors: string[] = [];
  registryFetch(t, (call) =>
    call.path.startsWith("/config/") ? new Response(null, { status: 204 }) : new Response("boom", { status: 500 }),
  );

  const code = await runContractCli([events[0] as NamedContract], ["--apply"], {
    registryUrl: "http://registry",
    stderr: (line) => errors.push(line),
  });

  assert.equal(code, 1);
  assert.equal(errors.length, 1);
  assert.match(errors[0] as string, /contract OrderPlaced: schema registry/);
});

test("runContractCli: no mode prints usage and exits 2", async () => {
  const errors: string[] = [];
  const code = await runContractCli(events, [], { stderr: (line) => errors.push(line) });

  assert.equal(code, 2);
  assert.deepEqual(errors, ["usage: contract [--json | --check | --apply]"]);
});

test("runContractCli --apply: a missing registry URL is an error, not a silent skip", async () => {
  const errors: string[] = [];
  const code = await runContractCli(events, ["--apply"], {
    registryUrl: "",
    stderr: (line) => errors.push(line),
  });

  assert.equal(code, 1);
  assert.deepEqual(errors, ["SCHEMA_REGISTRY_URL is not set"]);
});

async function withNoFetch<T>(body: () => Promise<T>): Promise<"no fetch" | T> {
  const original = global.fetch;
  global.fetch = (() => {
    throw new Error("a registry call was made during an offline mode");
  }) as typeof global.fetch;
  try {
    await body();
    return "no fetch";
  } finally {
    global.fetch = original;
  }
}
