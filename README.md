# @sol-fab/kafka

Sol's Kafka **policy** layer on top of [`kafkajs`](https://kafka.js.org/) — not a new Kafka client. `kafkajs` remains the transport; this package owns the Sol-specific conventions a TypeScript service needs to interoperate correctly with Sol's OCaml services on the same topics.

```bash
npm install @sol-fab/kafka kafkajs
```

Encodes five conventions that a hand-rolled TypeScript port of Sol's `local-demo` got wrong at least once each, evidenced by two independent adversarial review rounds:

1. **Schema registration ordering/fatality** (`registerTopic`) — register the schema first (fatal on failure), then set subject compatibility second (non-fatal, logged as a warning). Matches Sol's OCaml `kafka_service.ml` `register` exactly.
2. **Explicit topic provisioning** (`registerTopic`) — provisions the topic via `admin().createTopics()` *before* touching the schema registry, rather than relying on broker auto-create (invisible in local dev, fails hard in production with `auto.create.topics.enable=false`).
3. **Confluent wire format** (`encodeWire`/`decodeWire`) — the 5-byte header (magic byte + big-endian schema ID), byte-for-byte compatible with Sol's OCaml `Confluent_wire`.
4. **Decode/retry/crash routing** (`wrapEachMessage`/`wireCrashListener`) — a decode/validation failure is a rejection (counted, never retried, handler never runs); a downstream handler failure (e.g. a DB error) is retried by `kafkajs`; the process only exits when `kafkajs` itself has given up (`payload.restart === false`), not on every crash it was already self-healing from.
5. **W3C `traceparent` propagation** (`traceparentOf`/`extractTraceparent`) — OpenTelemetry has no official Kafka carrier, so this glue has to exist somewhere; correctly preserves an unsampled trace's flags byte (`0`) instead of coercing it to "sampled" via JS falsy-zero coercion.

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
  the group-scoping rule (sanitize to `[a-zA-Z0-9-]`, truncate + MD5 suffix
  past 64 chars).
- `retryRecordHeaders` / `deadLetterHeaders` / `retryDecodeFailureHeaders` —
  the `X-Sol-Attempt` / `X-Sol-Retry-At` / `X-Sol-Decode-Error` /
  `X-Sol-Origin-Group` conventions.
- `decideAction` — retry-topic vs DLQ routing at the retry budget boundary.

The consumer side (`Ack`/`Retry`/`Dead_letter` outcome, in-memory vs
retry-topics strategies) is built on these primitives and has parity with the
OCaml worker's retry/DLQ semantics.

## Usage

```ts
import { Kafka } from "kafkajs";
import { registerTopic, encodeWire, wrapEachMessage, wireCrashListener, traceparentOf } from "@sol-fab/kafka";

const kafka = new Kafka({ clientId: "order-svc", brokers: ["localhost:9092"] });

const { schemaId } = await registerTopic({
  kafka,
  registryUrl: "http://localhost:8081",
  topicName: "orders",
  schema: JSON.stringify({ type: "object", properties: { /* ... */ } }),
});

// producer
const wire = encodeWire(schemaId, message);
await producer.send({ topic: "orders", messages: [{ value: wire, headers: { traceparent: traceparentOf(span) } }] });

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

Broker-backed retry/DLQ tests need a real broker:

```bash
KAFKA_BROKERS=localhost:9092 npm test
```

## Related

- [`@sol-fab/obs`](https://github.com/loganbnielsen/sol-obs) — metric/label,
  Loki, and `traceparent` conventions this package re-exports.
- [Sol](https://github.com/loganbnielsen/sol) — the platform these conventions
  come from.

## License

Apache-2.0. See [LICENSE](./LICENSE). The "Sol" name and logo are trademarks
and are not covered by the licence.
