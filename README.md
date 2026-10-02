# @sol-fab/kafka

Sol's Kafka **policy** layer on top of [`kafkajs`](https://kafka.js.org/) — not a new Kafka client. `kafkajs` remains the transport; this package owns the Sol-specific conventions a TypeScript service needs to interoperate correctly with Sol's OCaml services on the same topics.

```bash
npm install @sol-fab/kafka kafkajs
```

Encodes six conventions that a hand-rolled TypeScript port of Sol's `local-demo` got wrong at least once each, evidenced by two independent adversarial review rounds:

1. **Declared partitioning and record keys** (`TopicContract`/`connectTopic`/`publish`) — an event contract declares the topic's partition count and how each message is keyed, the topic is created at that count and never reduced, and every published record is keyed by the declaration. Matches Sol's OCaml `Kafka_service.MESSAGE.partitions`/`MESSAGE.key`. See *Declared partitioning and keys* below.
2. **A read-only runtime and a deployment-owned registration** (`connectTopic` vs `registerContract`) — a producer or consumer startup only *resolves* the registry: it provisions the topic, checks reader compatibility, and looks up the registered schema id, failing when the declared contract is not registered. Registering a version or setting a subject's compatibility is the deployment lifecycle's job (Sol's `sol up`/`sol deploy` run the workspace's contract projection), so a runtime can never become the author of a producer's contract. Mirrors Sol's BUG-105 split: `Kafka_service.register` reads, `Schema.register_contract` writes.
3. **Contract projection** (`contractProjection`/`runContractCli`) — the workspace's declared contracts are emitted as the language-neutral object Sol's deployment lifecycle consumes (BUG-105): `--json`, `--check`, `--apply`, byte-compatible with the generated OCaml `contract/contract.exe`. See *The contract projection* below.
4. **Explicit topic provisioning** (`provisionTopic`, called by `connectTopic`) — provisions the topic via `admin().createTopics()` rather than relying on broker auto-create (invisible in local dev, fails hard in production with `auto.create.topics.enable=false`).
5. **Confluent wire format** (`encodeWire`/`decodeWire`) — the 5-byte header (magic byte + big-endian schema ID), byte-for-byte compatible with Sol's OCaml `Confluent_wire`.
6. **The `Ack | Fail` outcome and the decode DLQ** (`wrapEachMessage`/`wireCrashListener`) — a decode/validation failure is a rejection: counted, routed raw to the group DLQ, and committed once that publish lands. A handler `Fail` is fail-stop: the offset stays uncommitted and the consumer stops, mirroring `worker.ml`'s `| Fail -> ... Kafka.Consumer.Stop`. Matches FEAT-113, which deleted Kafka message-level retry from the OCaml framework.
7. **W3C `traceparent` propagation** (`traceparentOf`/`extractTraceparent`) — OpenTelemetry has no official Kafka carrier, so this glue has to exist somewhere; correctly preserves an unsampled trace's flags byte (`0`) instead of coercing it to "sampled" via JS falsy-zero coercion.

## Declared partitioning and keys

Sol treats an event's partition count and message key as one contract, and so does
this package. `TopicContract<T>` declares the topic name, its JSON schema, the
number of partitions it is created with, and `key: (message: T) => string | undefined`:

```ts
const orders: TopicContract<OrderPlaced> = {
  name: "orders",
  schema: ORDER_PLACED_SCHEMA,
  partitions: 3,
  key: (order) => order.order_id,
};
```

`connectTopic({ kafka, registryUrl, contract: orders })` provisions the topic at
the declared count and refuses to reduce an existing topic below its live count
(mirroring `Kafka_service.register`'s `Partition_count_reduction`, and the
`Config` error for a count below 1), then resolves the registered schema id
read-only. `publish(producer, connectedTopic, order)`
sends the record under the declared key, which is what keeps every record for one
entity on one partition and therefore in order for one consumer — the property
BUG-099 established on the OCaml side and this package now upholds for
TypeScript. `provisionDlqTopic` inherits the source topic's partition count
(read from the broker, falling back to the declaration before the source exists),
so a record keeps its key → partition mapping when the DLQ takes it over, and a
TypeScript worker can inherit the shape of a topic an OCaml service created
without restating it.

## The contract projection

Sol's deployment lifecycle owns schema registration (BUG-105). A workspace
declares each event once — `TopicContract` carries the schema, so nothing is
duplicated into a manifest — and exposes it through a projection program that Sol
runs. `contractProjection(events)` emits the same object the OCaml
`Kafka_service.Contract.projection` does:

```json
{"version":1,"events":[{"module":"OrderPlaced","topic":"orders","partitions":3,"schema":"…"}]}
```

`runContractCli(events, argv)` is the program's `main`, with the same modes and
output as the generated OCaml `contract/contract.exe`:

- `--json` prints the projection object and exits 0 (offline; `sol plan` reads it).
- `--check` is read-only: it reports each event's compatibility and never writes.
- `--apply` is the only writer: for each event it sets the subject's
  compatibility to `FULL` *then* registers the declared schema, both failures
  fatal, and exits 1 if any registration failed. `sol up` and `sol deploy` run it.
- no mode prints usage and exits 2.

`--check`/`--apply` read `SCHEMA_REGISTRY_URL`; a missing value is an error, not a
silent skip. Because the object is language-neutral, Sol drives an OCaml and a
TypeScript workspace through the same code path.

## Non-goals

- Not a general-purpose Kafka framework. `kafkajs` remains the transport — this package never wraps or hides it.
- Not a reimplementation of the Confluent Schema Registry HTTP client beyond what Sol's own policy needs.
- `traceparentOf`/`extractTraceparent` are re-exported from [`@sol-fab/obs`](https://github.com/loganbnielsen/sol-obs), which owns the tracing primitives. This package carries no second copy.
- Not a retry mechanism. FEAT-113 removed Kafka message-level retry from Sol on both sides; independently retryable work belongs in a durable Postgres job queue, not the topic.

## The `Ack | Fail` outcome and the decode DLQ

The outcome vocabulary is exactly `Ack | Fail` (`outcome.ts`), matching
`worker.ml`:

- `ACK` — the offset commits.
- `fail(reason)` — fail-stop. `wrapEachMessage` throws a `MessageFailError`, so
  the offset is never committed, and `wireCrashListener` stops the consumer and
  exits 0, mirroring `| Fail -> ... Kafka.Consumer.Stop` (OCaml's `run` then
  returns `Ok ()`). The reason is diagnostic text only; the runtime never
  inspects it to decide routing or retryability.
- Any other crash still exits only when `kafkajs` itself has given up
  (`payload.restart === false`), not on every crash it was already self-healing
  from.

`dlq.ts` owns the group-scoped dead-letter queue, byte-compatible with
`kafka_service_dlq.ml`:

- `dlqTopicName` / `canonicalGroupSegment` / `sanitizeGroupId` —
  `<source>.<canonical-group>.dlq`, the group-scoping rule (sanitize to
  `[a-zA-Z0-9-]`, truncate the readable prefix past 51 chars, and always append
  the 12-hex MD5 of the original group id — BUG-117).
- `decodeFailureHeaders` — the `X-Sol-Decode-Error` / `X-Sol-Origin-Group`
  provenance headers, with the original record's headers preserved.
- `provisionDlqTopic` — creates the DLQ topic at the source's partition count.

**Undecodable source records** (`decodeErrorPolicy`, parity with OCaml's
`decode_error_policy`). The default when a `dlq` is configured is
`"route-to-dlq"`: a record that cannot be decoded is published, raw (value, key,
original headers), with `X-Sol-Decode-Error` and `X-Sol-Origin-Group`, to
`<source>.<group>.dlq`. Its offset commits only once that publish lands, and a
failed publish throws so it stays uncommitted. `"ack-and-drop"` is the explicit
opt-in to count it and commit past it, for a consumer with no DLQ; asking for
`"route-to-dlq"` without a `dlq` is a construction error.

## Usage

```ts
import { Kafka } from "kafkajs";
import {
  ACK,
  fail,
  kafkaConfigFromEnv,
  publish,
  provisionDlqTopic,
  connectTopic,
  traceparentOf,
  wireCrashListener,
  wrapEachMessage,
} from "@sol-fab/kafka";
import type { TopicContract } from "@sol-fab/kafka";

// Reads KAFKA_BROKERS and KAFKA_SECURITY_PROTOCOL (required) plus the TLS/SASL
// variables; throws naming a missing or malformed one.
const kafka = new Kafka({ clientId: "order-svc", ...kafkaConfigFromEnv() });

const orders: TopicContract<OrderPlaced> = {
  name: "orders",
  schema: JSON.stringify({ type: "object", properties: { /* ... */ } }),
  partitions: 3,
  key: (order) => order.order_id,
};

const topic = await connectTopic({ kafka, registryUrl: "http://localhost:8081", contract: orders });

// The deployment's projection program (`sol` runs it; see the section above):
//   import { runContractCli } from "@sol-fab/kafka";
//   process.exit(await runContractCli([{ module: "OrderPlaced", contract: orders }]));

// producer: wire-encoded with the registered schema id, keyed by the contract
await publish(producer, topic, order, { headers: { traceparent: traceparentOf(span) } });

// consumer
const groupId = "fulfillment-worker";
await provisionDlqTopic({ kafka, groupId, source: orders });
const decodeErrorsTotal = /* your Prometheus counter */;
await consumer.run({
  eachMessage: wrapEachMessage({
    decode: (json) => validateOrder(json), // throw to reject
    decodeErrorCounter: decodeErrorsTotal,
    dlq: {
      publisher: { publish: (record) => producer.send({ topic: record.topic, messages: [record] }) },
      groupId,
      sourceTopic: orders.name,
    },
    handler: async ({ message, traceContext }) => {
      if (!canFulfil(message)) return fail("inventory service unavailable");
      await fulfil(message, traceContext);
      return ACK;
    },
  }),
});
wireCrashListener(consumer);
```

## Kafka security posture

`kafkaConfigFromEnv()` is the one place a TypeScript application reads the
transport posture, mirroring the OCaml `Kafka_service.config_of_env` (SEC-007):

- `KAFKA_SECURITY_PROTOCOL` is **required** — `plaintext`, `ssl`,
  `sasl_plaintext` or `sasl_ssl`. Absent or blank is an error, never a
  plaintext default.
- `KAFKA_BROKERS` is required.
- `KAFKA_SSL_CA_LOCATION` (optional, for the two TLS protocols) is read and
  passed as the kafkajs `ca`; an unreadable file fails closed.
- `KAFKA_SASL_MECHANISM` / `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` are
  required for the two SASL protocols (`PLAIN`, `SCRAM-SHA-256`,
  `SCRAM-SHA-512`).

Every error names the variable it is about. Sol renders
`KAFKA_SECURITY_PROTOCOL` into every workload's manifest; a local process sets
`KAFKA_SECURITY_PROTOCOL=plaintext`.

## Development

```bash
npm ci
npm run build
npm test          # unit tests; broker-backed tests self-skip
```

Broker-backed partitioning tests need a real broker and a schema registry:

```bash
KAFKA_BROKERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 npm test
```

## Related

- [`@sol-fab/obs`](https://github.com/loganbnielsen/sol-obs) — metric/label,
  Loki, and `traceparent` conventions this package re-exports.
- [Sol](https://github.com/loganbnielsen/sol) — the platform these conventions
  come from.

## License

Apache-2.0. See [LICENSE](./LICENSE). The "Sol" name and logo are trademarks
and are not covered by the licence.
