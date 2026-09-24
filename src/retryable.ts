/**
 * The retry-capable *source* consumer wrapper — Sol's `RETRYABLE_WORKER` tier
 * (FEAT-078) on the TypeScript side. The Ack-only tier is the existing
 * `wrapEachMessage`: its handler returns `void`, so it cannot express
 * `Retry`/`Dead_letter` at all, and there is no retry strategy to select.
 *
 * FEAT-078 removed the *implicit* retry fallback on the OCaml side. Here the
 * same property holds structurally: a retry-capable worker must name a
 * `retryStrategy`, and `retry-topics` additionally requires a relay — a
 * missing destination is a construction error, never a per-message runtime
 * surprise discovered the first time something fails.
 *
 * The routing decision itself lives in `routing.ts` (shared with the retry
 * relay), and the kafkajs wiring in `relay.ts`; this module owns only the
 * source-consumer lifecycle.
 */
import type { EachMessagePayload } from "kafkajs";
import { extractTraceparent } from "@sol-fab/obs";
import { decodeWire, WireFormatError } from "./wireFormat.js";
import type { DecodeErrorCounter, MessageHandlerContext } from "./consume.js";
import type { Outcome } from "./outcome.js";
import {
  backoffS,
  relayTopicName,
  retryDecodeFailureHeaders,
  retryTopicsPolicyError,
  solHeadersOf,
  type RetryStrategy,
  type Rng,
} from "./retry.js";
import { routeOutcome, type RawRecord, type RetryRelay, type RetryMetrics } from "./routing.js";

// Re-exported so consumers (and tests) can name the relay contract without
// reaching into routing.ts directly.
export type { RelayRecord, RetryRelay, RetryMetrics } from "./routing.js";

/**
 * What happens to a source record that cannot be decoded (FEAT-098, mirroring
 * OCaml's `decode_error_policy`, BUG-051). `route-to-dlq` publishes the raw
 * record, with the decode diagnostic, to `<source>.<group>.dlq` and lets the
 * offset commit only once that publish lands. `ack-and-drop` counts it and
 * commits past it. `retry-topics` defaults to `route-to-dlq`; `in-memory` has no
 * DLQ, so it only allows `ack-and-drop` (its default).
 */
export type DecodeErrorPolicy = "route-to-dlq" | "ack-and-drop";

export interface RetryableMessageOptions<T> {
  decode: (json: unknown) => T;
  decodeErrorCounter: DecodeErrorCounter;
  onDecodeError?: (err: unknown) => void;
  /** Defaults to `route-to-dlq` under `retry-topics`, `ack-and-drop` under `in-memory`. */
  decodeErrorPolicy?: DecodeErrorPolicy;
  retryStrategy: RetryStrategy;
  /** The consumer group id; retry/DLQ topics are scoped to it (BUG-030). */
  groupId: string;
  /** The source topic name, for `<source>.<group>.retry|dlq`. */
  sourceTopic: string;
  /** Required by `retry-topics`; ignored by `in-memory`. */
  relay?: RetryRelay;
  metrics?: RetryMetrics;
  /** Injectable sleep/clock/rng (tests). */
  sleep?: (seconds: number) => Promise<void>;
  nowS?: () => number;
  rng?: Rng;
  handler: (ctx: MessageHandlerContext<T> & { readonly attempt: number }) => Promise<Outcome>;
}

const defaultSleep = (seconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds) * 1000));

function assertStrategyUsable(opts: {
  retryStrategy: RetryStrategy;
  relay?: RetryRelay;
  decodeErrorPolicy?: DecodeErrorPolicy;
}): void {
  if (opts.retryStrategy.kind !== "retry-topics") {
    if (opts.decodeErrorPolicy === "route-to-dlq") {
      throw new Error(
        "sol-kafka: decodeErrorPolicy route-to-dlq needs a DLQ, which only retry-topics " +
          "provisions; use retry-topics, or state ack-and-drop for in-memory",
      );
    }
    return;
  }
  const err = retryTopicsPolicyError(opts.retryStrategy.policy);
  if (err) throw new Error(`sol-kafka: ${err}`);
  if (!opts.relay) {
    throw new Error("sol-kafka: retry-topics strategy requires a relay (no implicit destination)");
  }
}

/**
 * Source-topic consumer: decode, then run the handler. Mirrors
 * `kafka_service_retry_topics.decode_and_handle` (the source path decides at
 * attempt 1). On `retry-topics`, a successfully published retry/DLQ record
 * lets the offset commit; a failed publish throws so the offset is left
 * uncommitted.
 */
export function wrapEachRetryableMessage<T>(opts: RetryableMessageOptions<T>) {
  assertStrategyUsable(opts);
  const sleep = opts.sleep ?? defaultSleep;
  const nowS = opts.nowS ?? (() => Date.now() / 1000);
  const policy = opts.retryStrategy.policy;
  const decodeErrorPolicy: DecodeErrorPolicy =
    opts.decodeErrorPolicy ??
    (opts.retryStrategy.kind === "retry-topics" ? "route-to-dlq" : "ack-and-drop");

  return async ({ message }: EachMessagePayload): Promise<void> => {
    let decoded: T;
    try {
      if (!message.value) throw new WireFormatError("tombstone (message has no value)");
      decoded = opts.decode(decodeWire(message.value).json);
    } catch (err) {
      opts.decodeErrorCounter.inc();
      opts.onDecodeError?.(err);
      if (decodeErrorPolicy === "ack-and-drop") return; // counted, committed past
      // route-to-dlq (FEAT-098 / BUG-051): the raw record goes to the DLQ with the
      // diagnostic, and the offset commits only once that publish lands; a failed
      // publish throws so it stays uncommitted. Never retried, never handled.
      const relay = opts.relay;
      if (!relay) throw new Error("sol-kafka: route-to-dlq without a relay; not acking");
      try {
        await relay.publish({
          topic: relayTopicName(opts.sourceTopic, opts.groupId, "dlq"),
          key: message.key ?? undefined,
          value: message.value ?? Buffer.alloc(0),
          headers: retryDecodeFailureHeaders({
            originalHeaders: solHeadersOf(message.headers),
            decodeError: String(err),
            groupId: opts.groupId,
          }),
        });
      } catch (publishErr) {
        opts.metrics?.onRelayPublish?.({ attempt: 1, outcome: "failed" });
        throw new Error(
          `sol-kafka: could not dead-letter an undecodable record; not acking (${String(publishErr)})`,
        );
      }
      opts.metrics?.onRelayPublish?.({ attempt: 1, outcome: "published" });
      return;
    }

    const raw: RawRecord = {
      key: message.key ?? undefined,
      value: message.value,
      headers: solHeadersOf(message.headers),
    };
    const traceContext = extractTraceparent(message.headers?.traceparent?.toString());

    for (let attempt = 1; ; attempt += 1) {
      const outcome = await opts.handler({ message: decoded, traceContext, attempt });
      if (outcome.kind === "ack") return;

      if (opts.retryStrategy.kind === "retry-topics") {
        const acked = await routeOutcome(
          opts.retryStrategy, opts.relay, opts.groupId, opts.sourceTopic,
          raw, attempt, outcome, opts.metrics, nowS, opts.rng,
        );
        if (acked) return;
        throw new Error(`sol-kafka: retry/DLQ publish failed at attempt ${attempt}; not acking`);
      }

      // In_memory: sleep and re-run within this handler, bounded by maxAttempts.
      if (outcome.kind === "dead-letter" || attempt >= policy.maxAttempts) {
        throw new Error(
          `sol-kafka: ${outcome.kind} at attempt ${attempt} under in-memory retry; not acking`,
        );
      }
      await sleep(backoffS(policy, attempt, opts.rng));
    }
  };
}
