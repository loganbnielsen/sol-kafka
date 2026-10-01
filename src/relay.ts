/**
 * The retry-topic *transport*: provisioning the retry/DLQ topics, publishing
 * relay records, and the relay consumer's lifecycle.
 *
 * Deliberately transport-focused — the policy/outcome/routing decision lives
 * in `retryable.ts` (`routeOutcome`) and this module calls back into it. The
 * relay must not become a second, subtly-different worker implementation; the
 * ownership-transfer invariants it preserves are:
 *
 *   - publish the retry/DLQ record FIRST; only a successful publish lets the
 *     caller return normally (and therefore commit);
 *   - a failed publish throws, leaving the input uncommitted;
 *   - a retry record that cannot be decoded is transferred to the DLQ (with
 *     the diagnostic) before it is committed.
 */
import type { Consumer, IHeaders, Kafka, Producer } from "kafkajs";
import type { SpanContext } from "@opentelemetry/api";
import { extractTraceparent } from "@sol-fab/obs";
import { decodeWire } from "./wireFormat.js";
import { assertDeclaredPartitions, type TopicShape } from "./contract.js";
import { describeTopic } from "./admin.js";
import { deadLetter, type Outcome } from "./outcome.js";
import {
  parseAttemptHeader,
  parseRetryAtHeader,
  relayTopicName,
  retryConsumerGroupId,
  retryDecodeFailureHeaders,
  solHeadersOf,
  type RetryStrategy,
  type Rng,
} from "./retry.js";
import { routeOutcome, type RawRecord, type RetryRelay, type RetryMetrics } from "./routing.js";

export interface ProvisionRelayTopicsOptions {
  kafka: Kafka;
  /**
   * The source topic's declared shape. A `TopicContract` satisfies this
   * structurally, so the same declaration that a producer registered with is
   * what a consumer hands here.
   */
  source: TopicShape;
  groupId: string;
  replicationFactor?: number;
}

/**
 * Create `<source>.<canonical-group>.retry` and `.dlq`.
 *
 * The relay topics inherit the source's partition count, so a record's key
 * keeps working after it is transferred: the same entity lands on the same
 * partition of the retry topic as it did on the source (BUG-099). The live
 * source topic is consulted first — a TypeScript worker can therefore inherit
 * the count from a topic an OCaml service created, rather than having to
 * restate it — and the declaration is the fallback for the startup race where
 * the worker provisions its relay topics before the producer has created the
 * source. Retry/DLQ records are the source record's raw bytes, so no schema is
 * registered for them — `Kafka_service_retry_topics.consume` only calls
 * `ensure_topic` here too.
 */
export async function provisionRelayTopics(
  opts: ProvisionRelayTopicsOptions,
): Promise<{ retryTopic: string; dlqTopic: string }> {
  const retryTopic = relayTopicName(opts.source.name, opts.groupId, "retry");
  const dlqTopic = relayTopicName(opts.source.name, opts.groupId, "dlq");
  assertDeclaredPartitions(opts.source);
  const observed = await describeTopic(opts.kafka, opts.source.name);
  const partitions = observed?.partitions ?? opts.source.partitions;
  const replicationFactor = opts.replicationFactor ?? observed?.replicationFactor ?? 1;
  const admin = opts.kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        { topic: retryTopic, numPartitions: partitions, replicationFactor },
        { topic: dlqTopic, numPartitions: partitions, replicationFactor },
      ],
    });
  } finally {
    await admin.disconnect();
  }
  return { retryTopic, dlqTopic };
}

/** A `RetryRelay` backed by a kafkajs producer. */
export function kafkaRetryRelay(producer: Producer): RetryRelay {
  return {
    publish: async (record) => {
      await producer.send({
        topic: record.topic,
        messages: [{ key: record.key, value: record.value, headers: record.headers }],
      });
    },
  };
}

/** Options for processing relay records; broker-free so it is unit-testable. */
export interface RetryRelayProcessorOptions<T> {
  sourceTopic: string;
  groupId: string;
  retryStrategy: RetryStrategy;
  decode: (json: unknown) => T;
  handler: (ctx: {
    message: T;
    traceContext: SpanContext | undefined;
    attempt: number;
  }) => Promise<Outcome>;
  relay: RetryRelay;
  metrics?: RetryMetrics;
  onDecodeError?: (err: unknown) => void;
  sleep?: (seconds: number) => Promise<void>;
  nowS?: () => number;
  rng?: Rng;
}

/** The relay consumer additionally needs a Kafka client to connect with. */
export interface RetryRelayConsumerOptions<T> extends RetryRelayProcessorOptions<T> {
  kafka: Kafka;
}

const defaultSleep = (seconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds) * 1000));

/**
 * Process exactly one retry-topic record. Exported so the delivery semantics
 * are unit-testable without a broker. Throwing means "do not commit": kafkajs
 * redelivers rather than losing the record.
 */
export async function handleRetryRecord<T>(
  opts: RetryRelayProcessorOptions<T>,
  message: { key?: Buffer | null; value: Buffer | null; headers?: IHeaders },
): Promise<void> {
  const nowS = opts.nowS ?? (() => Date.now() / 1000);
  const sleep = opts.sleep ?? defaultSleep;

  const raw: RawRecord = {
    key: message.key ?? undefined,
    value: message.value,
    headers: solHeadersOf(message.headers),
  };
  const attempt = parseAttemptHeader(raw.headers);
  const retryAt = parseRetryAtHeader(raw.headers);

  // POLICY, deliberate: unreadable retry metadata (`X-Sol-Attempt` /
  // `X-Sol-Retry-At` missing or malformed) is defined as terminal and is
  // dead-lettered rather than re-scheduled. We cannot know the real attempt
  // count, so the alternative — treating it as attempt 1 — risks an unbounded
  // retry loop on a record nobody can make progress on. The attempt stamped on
  // the DLQ record is the out-of-band terminal value `max 1 maxAttempts`
  // (matching kafka_service_retry_topics.retry_handler); it is a policy
  // choice, not arithmetic that "proves" the budget was exhausted.
  if (attempt === undefined || retryAt === undefined) {
    const terminalAttempt = Math.max(1, opts.retryStrategy.policy.maxAttempts);
    const published = await routeOutcome(
      opts.retryStrategy, opts.relay, opts.groupId, opts.sourceTopic,
      raw, terminalAttempt, deadLetter("malformed retry metadata"),
      opts.metrics, nowS, opts.rng,
    );
    if (!published) throw new Error("sol-kafka: could not dead-letter malformed retry metadata; not committing");
    return;
  }

  const delayS = Math.max(0, retryAt - nowS());
  if (delayS > 0.001) await sleep(delayS);

  let decoded: T;
  try {
    if (!message.value) throw new Error("tombstone (retry record has no value)");
    decoded = opts.decode(decodeWire(message.value).json);
  } catch (err) {
    opts.onDecodeError?.(err);
    // Transfer the raw record to the DLQ with the decode diagnostic; it is
    // only committed once that publish lands.
    await opts.relay.publish({
      topic: relayTopicName(opts.sourceTopic, opts.groupId, "dlq"),
      key: raw.key,
      value: raw.value,
      headers: retryDecodeFailureHeaders({
        originalHeaders: raw.headers,
        decodeError: String(err),
        groupId: opts.groupId,
      }),
    });
    return;
  }

  const outcome = await opts.handler({
    message: decoded,
    traceContext: extractTraceparent(raw.headers.traceparent),
    attempt,
  });
  if (outcome.kind === "ack") return;

  const nextAttempt = outcome.kind === "retry" ? attempt + 1 : attempt;
  const published = await routeOutcome(
    opts.retryStrategy, opts.relay, opts.groupId, opts.sourceTopic,
    raw, nextAttempt, outcome, opts.metrics, nowS, opts.rng,
  );
  if (!published) throw new Error("sol-kafka: retry/DLQ publish failed on the relay path; not committing");
}

/**
 * Connect and run the relay consumer (group `<group_id>-sol-retry`) on the
 * retry topic. Returns the connected consumer so the caller owns shutdown.
 */
export async function runRetryRelayConsumer<T>(opts: RetryRelayConsumerOptions<T>): Promise<Consumer> {
  const consumer = opts.kafka.consumer({ groupId: retryConsumerGroupId(opts.groupId) });
  await consumer.connect();
  await consumer.subscribe({
    topic: relayTopicName(opts.sourceTopic, opts.groupId, "retry"),
    fromBeginning: true,
  });
  await consumer.run({
    eachMessage: async ({ message }) => {
      await handleRetryRecord(opts, message);
    },
  });
  return consumer;
}
