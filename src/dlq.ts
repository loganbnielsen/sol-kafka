/**
 * The group-scoped dead-letter queue: naming and record shape, byte-compatible
 * with the OCaml side (framework/ocaml/kafka-eio-service/lib/kafka_service_dlq.ml).
 *
 * FEAT-113 deleted the retry half of the old retry-topic machinery on the OCaml
 * side, keeping only this: framework decode/schema failures are published to
 * `<source>.<canonical-group>.dlq`, with the decode diagnostic and origin group
 * in headers. Nothing here talks to a broker; the consumer wiring builds on it.
 */
import { createHash } from "node:crypto";
import type { IHeaders, Kafka } from "kafkajs";
import { describeTopic } from "./admin.js";
import { assertDeclaredPartitions, type TopicShape } from "./contract.js";

export const HDR_DECODE_ERROR = "X-Sol-Decode-Error";
export const HDR_ORIGIN_GROUP = "X-Sol-Origin-Group";

/** Sol's headers, reduced to the single-string form its own records use. */
export type SolHeaders = Record<string, string>;

/**
 * Resolve a kafkajs `IHeaders` down to Sol's flat string form. Multi-valued
 * and Buffer headers are not part of Sol's own contract; the first value wins
 * and Buffers are UTF-8 decoded.
 */
export function solHeadersOf(headers: IHeaders | undefined): SolHeaders {
  const out: SolHeaders = {};
  if (!headers) return out;
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    const one = Array.isArray(value) ? value[0] : value;
    if (one === undefined) continue;
    out[key] = Buffer.isBuffer(one) ? one.toString("utf8") : one;
  }
  return out;
}

/** `kafka_service_dlq.ml` `max_group_segment_len`. */
export const MAX_GROUP_SEGMENT_LEN = 64;

/** `kafka_service_dlq.ml` `group_hash_len`. */
export const GROUP_HASH_LEN = 12;

/** `sanitize_group_id`: alphanumerics and '-' only; empty becomes "unscoped". */
export function sanitizeGroupId(groupId: string): string {
  const sanitized = groupId.replace(/[^a-zA-Z0-9-]/g, "-");
  return sanitized === "" ? "unscoped" : sanitized;
}

/**
 * `canonical_group_segment`: over-length ids are truncated and given a short
 * MD5 content-hash suffix (first 12 hex chars of `Digest.string`, which is
 * MD5), always -- not only on a detected collision -- so two different
 * over-length ids can never truncate to the same segment (BUG-117).
 */
export function canonicalGroupSegment(groupId: string): string {
  const sanitized = sanitizeGroupId(groupId);
  const hashSuffix = createHash("md5").update(groupId).digest("hex").slice(0, GROUP_HASH_LEN);
  const prefixLen = MAX_GROUP_SEGMENT_LEN - GROUP_HASH_LEN - 1;
  const prefix = sanitized.length <= prefixLen ? sanitized : sanitized.slice(0, prefixLen);
  return `${prefix}-${hashSuffix}`;
}

/** `dlq_topic_name`: `<source>.<canonical-group>.dlq`. */
export function dlqTopicName(source: string, groupId: string): string {
  return `${source}.${canonicalGroupSegment(groupId)}.dlq`;
}

/** A record to publish to the DLQ: the source record's bytes + key. */
export interface DlqRecord {
  readonly topic: string;
  readonly key?: Buffer;
  /** `null` publishes a tombstone, as OCaml's `~value:None` does. */
  readonly value: Buffer | null;
  readonly headers: SolHeaders;
}

/**
 * The publication side, injected so decode routing is unit-testable without a
 * broker. `publish` rejecting means the durable transfer failed -- the input
 * must then be left uncommitted (fail closed), never acked.
 */
export interface DlqPublisher {
  publish(record: DlqRecord): Promise<void>;
}

/**
 * `decode_failure_message`: the source record's headers are preserved, and the
 * decode diagnostic plus origin group are set. The fresh diagnostic wins over
 * any copy already on the record (a redriven DLQ record that fails again):
 * OCaml sends its new pairs first and reads the first match.
 */
export function decodeFailureHeaders(opts: {
  readonly originalHeaders?: SolHeaders;
  readonly decodeError: string;
  readonly groupId: string;
}): SolHeaders {
  return {
    ...(opts.originalHeaders ?? {}),
    [HDR_DECODE_ERROR]: opts.decodeError,
    [HDR_ORIGIN_GROUP]: opts.groupId,
  };
}

export interface ProvisionDlqTopicOptions {
  kafka: Kafka;
  /**
   * The source topic's declared shape. A `TopicContract` satisfies this
   * structurally, so the same declaration a producer registered with is what a
   * consumer hands here.
   */
  source: TopicShape;
  groupId: string;
  replicationFactor?: number;
}

/**
 * Create `<source>.<canonical-group>.dlq`, mirroring
 * `Kafka_service.consume`'s `ensure_topic` under `Route_to_dlq`.
 *
 * The DLQ inherits the source's partition count, so a record's key keeps
 * working after it is transferred: the same entity lands on the same partition
 * of the DLQ as it did on the source (BUG-099). The live source topic is
 * consulted first -- a TypeScript worker can inherit the count from a topic an
 * OCaml service created -- and the declaration is the fallback for the startup
 * race where the worker provisions before the producer has created the source.
 * DLQ records are the source record's raw bytes, so no schema is registered.
 */
export async function provisionDlqTopic(opts: ProvisionDlqTopicOptions): Promise<string> {
  const topic = dlqTopicName(opts.source.name, opts.groupId);
  assertDeclaredPartitions(opts.source);
  const observed = await describeTopic(opts.kafka, opts.source.name);
  const partitions = observed?.partitions ?? opts.source.partitions;
  const replicationFactor = opts.replicationFactor ?? observed?.replicationFactor ?? 1;
  const admin = opts.kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor }],
    });
  } finally {
    await admin.disconnect();
  }
  return topic;
}
