# @sol-fab/kafka

Sol's Kafka **policy** layer on top of [`kafkajs`](https://kafka.js.org/) — not a new Kafka client. `kafkajs` remains the transport; this package owns the Sol-specific conventions a TypeScript service needs to interoperate correctly with Sol's OCaml services on the same topics.

```bash
npm install @sol-fab/kafka kafkajs
```

Encodes six conventions that a hand-rolled TypeScript port of Sol's `local-demo` got wrong at least once each, evidenced by two independent adversarial review rounds:

1. **Declared partitioning and record keys** (`TopicContract`/`registerTopic`/`publish`) — an event contract declares the topic's partition count and how each message is keyed, the topic is created at that count and never reduced, and every published record is keyed by the declaration. Matches Sol's OCaml `Kafka_service.MESSAGE.partitions`/`MESSAGE.key`. See *Declared partitioning and keys* below.
2. **Schema registration ordering/fatality** (`registerTopic`) — register the schema first (fatal on failure), then set subject compatibility second (non-fatal, logged as a warning). Matches Sol's OCaml `kafka_service.ml` `register` exactly.
3. **Explicit topic provisioning** (`registerTopic`) — provisions the topic via `admin().createTopics()` *before* touching the schema registry, rather than relying on broker auto-create (invisible in local dev, fails hard in production with `auto.create.topics.enable=false`).
4. **Confluent wire format** (`encodeWire`/`decodeWire`) — the 5-byte header (magic byte + big-endian schema ID), byte-for-byte compatible with Sol's OCaml `Confluent_wire`.
5. **Decode/retry/crash routing** (`wrapEachMessage`/`wireCrashListener`) — a decode/validation failure is a rejection (counted, never retried, handler never runs); a downstream handler failure (e.g. a DB error) is retried by `kafkajs`; the process only exits when `kafkajs` itself has given up (`payload.restart === false`), not on every crash it was already self-healing from.
6. **W3C `traceparent` propagation** (`traceparentOf`/`extractTraceparent`) — OpenTelemetry has no official Kafka carrier, so this glue has to exist somewhere; correctly preserves an unsampled trace's flags byte (`0`) instead of coercing it to "sampled" via JS falsy-zero coercion.

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

`registerTopic({ kafka, registryUrl, contract: orders })` provisions the topic at
the declared count and refuses to reduce an existing topic below its live count
(mirroring `Kafka_service.register`'s `Partition_count_reduction`, and the
`Config` error for a count below 1). `publish(producer, registeredTopic, order)`
sends the record under the declared key, which is what keeps every record for one
entity on one partition and therefore in order for one consumer — the property
BUG-099 established on the OCaml side and this package now upholds for
TypeScript. `provisionRelayTopics` inherits the source topic's partition count
(read from the broker, falling back to the declaration before the source exists),
so a record keeps its key → partition mapping when a retry or DLQ topic takes it
over, and a TypeScript worker can inherit the shape of a topic an OCaml service
created without restating it.

## Non-goals

- Not a general-purpose Kafka framework. `kafkajs` remains the transport — this package never wraps or hides it.
- Not a reimplementation of the Confluent Schema Registry HTTP client beyond what Sol's own policy needs.
- `traceparentOf`/`extractTraceparent` are re-exported from [`@sol-fab/obs`](https://github.com/loganbnielsen/sol-obs), which owns the tracing primitives. This package carries no second copy.

## Retry / DLQ record conventions

`retry.ts` owns Sol's retry policy and on-the-wire record shape, matching the
OCaml worker side exactly rather than approximating it:

- `RetryPolicy` + `backoffS` — mirrors kafka-eio's `Kafka.Consumer.backoff_s`
  (exponential, symmetric jitter applied before the `maxDelayS` clamp; an
  injectable RNG for deterministic tests).
- `relayTopicName` / `canonicalGroupSegment` — `<source>.<canonical-group>.retry|dlq`,
  the group-scoping rule (sanitize to `[a-zA-Z0-9-]`, truncate the readable
  prefix past 51 chars, and always append the 12-hex MD5 of the original group
  id).
- `retryRecordHeaders` / `deadLetterHeaders` / `retryDecodeFailureHeaders` —
  the `X-Sol-Attempt` / `X-Sol-Retry-At` / `X-Sol-Decode-Error` /
  `X-Sol-Origin-Group` conventions.
- `decideAction` — retry-topic vs DLQ routing at the retry budget boundary.

The consumer side (`Ack`/`Retry`/`Dead_letter` outcome, in-memory vs
retry-topics strategies) is built on these primitives and has parity with the
OCaml worker's retry/DLQ semantics.

**Undecodable source records** (`decodeErrorPolicy`, parity with OCaml's
`decode_error_policy`). Under `retry-topics` the default is `"route-to-dlq"`: a
record that cannot be decoded is published, raw (value, key, original headers),
with `X-Sol-Decode-Error` and `X-Sol-Origin-Group`, to `<source>.<group>.dlq`. Its
offset commits only once that publish lands, and a failed publish throws so it
stays uncommitted. `"ack-and-drop"` is the explicit opt-in to count it and commit
past it. `in-memory` has no DLQ, so it only allows `"ack-and-drop"` (its default),
and asking it for `"route-to-dlq"` is a construction error.

## Usage

```ts
import { Kafka } from "kafkajs";
import { registerTopic, publish, wrapEachMessage, wireCrashListener, traceparentOf } from "@sol-fab/kafka";
import type { TopicContract } from "@sol-fab/kafka";

const kafka = new Kafka({ clientId: "order-svc", brokers: ["localhost:9092"] });

const orders: TopicContract<OrderPlaced> = {
  name: "orders",
  schema: JSON.stringify({ type: "object", properties: { /* ... */ } }),
  partitions: 3,
  key: (order) => order.order_id,
};

const topic = await registerTopic({ kafka, registryUrl: "http://localhost:8081", contract: orders });

// producer: wire-encoded with the registered schema id, keyed by the contract
await publish(producer, topic, order, { headers: { traceparent: traceparentOf(span) } });

// consumer
const decodeErrorsTotal = /* your Prometheus counter */;
await consumer.run({
  eachMessage: wrapEachMessage({
    decode: (json) => validateOrder(json), // throw to reject
    decodeErrorCounter: decodeErrorsTotal,
    handler: async ({ message, traceContext }) => { /* ... */ },
  }),
});
wireCrashListener(consumer);
```

## Development

```bash
npm ci
npm run build
npm test          # unit tests; broker-backed tests self-skip
```

Broker-backed retry/DLQ and partitioning tests need a real broker; the
multi-partition ordering test additionally needs a schema registry:

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
