import type { Consumer, EachMessagePayload } from "kafkajs";
import type { SpanContext } from "@opentelemetry/api";
import { decodeWire, WireFormatError } from "./wireFormat.js";
import { extractTraceparent } from "@sol-fab/obs";
import { decodeFailureHeaders, dlqTopicName, solHeadersOf, type DlqPublisher } from "./dlq.js";
import type { Outcome } from "./outcome.js";

export interface DecodeErrorCounter {
  inc(): void;
}

export interface MessageHandlerContext<T> {
  message: T;
  traceContext: SpanContext | undefined;
}

/**
 * What happens to a source record that cannot be decoded (mirroring OCaml's
 * `decode_error_policy`, FEAT-113/098). `route-to-dlq` publishes the raw
 * record, with the decode diagnostic, to `<source>.<group>.dlq` and lets the
 * offset commit only once that publish lands. `ack-and-drop` counts it and
 * commits past it, for a consumer with no DLQ to route to.
 */
export type DecodeErrorPolicy = "route-to-dlq" | "ack-and-drop";

/**
 * Thrown when a handler returns `Fail`. kafkajs sees a crash, which is what
 * stops the consumer; wireCrashListener recognises this class and treats it as
 * terminal (offset uncommitted) rather than as a crash kafkajs should retry.
 */
export class MessageFailError extends Error {
  readonly reason: string;

  constructor(reason: string) {
    super(
      `sol-worker: handler failed a fact; the offset is not committed and the consumer stops: ${reason}`,
    );
    this.name = "MessageFailError";
    this.reason = reason;
  }
}

export interface EachMessageOptions<T> {
  /** Validate/transform the decoded wire JSON into T; throw to reject as a decode failure. */
  decode: (json: unknown) => T;
  decodeErrorCounter: DecodeErrorCounter;
  onDecodeError?: (err: unknown) => void;
  /** Defaults to `route-to-dlq` when a `dlq` is given, `ack-and-drop` otherwise. */
  decodeErrorPolicy?: DecodeErrorPolicy;
  /** The group-scoped DLQ; required by `route-to-dlq`. */
  dlq?: { publisher: DlqPublisher; groupId: string; sourceTopic: string };
  handler: (ctx: MessageHandlerContext<T>) => Promise<Outcome>;
}

/**
 * Wraps a kafkajs eachMessage handler with Sol's outcome contract
 * (kafka_service_intf.ml's wrap_on_decode_error + worker.ml's Ack|Fail loop), so
 * the two failure classes can't be conflated by a caller:
 *
 *   - Decode/validation failure: the message will never become valid no matter
 *     how many times it's redelivered. That is a REJECTION, not a crash and not
 *     a retry -- decodeErrorCounter.inc() fires, the raw record is routed to the
 *     group DLQ (or dropped, under `ack-and-drop`), the handler never runs, and
 *     the offset commits (this function returns normally). It is NOT a
 *     messages_total status value -- worker.ml intercepts it before the handler
 *     ever runs. A failed DLQ publish throws instead, so the offset stays
 *     uncommitted.
 *   - Handler `Fail`: fail-stop. A MessageFailError is thrown, leaving the
 *     offset uncommitted; wireCrashListener then stops the consumer.
 *
 * FEAT-033's hand-rolled TS port got the two failure classes wrong twice across
 * two independent adversarial review rounds -- first conflating them under one
 * label, then (separately, see wireCrashListener below) exiting on every
 * consumer crash including ones kafkajs was already self-healing from.
 */
export function wrapEachMessage<T>(opts: EachMessageOptions<T>) {
  const policy = opts.decodeErrorPolicy ?? (opts.dlq ? "route-to-dlq" : "ack-and-drop");
  if (policy === "route-to-dlq" && !opts.dlq) {
    throw new Error("sol-kafka: decodeErrorPolicy route-to-dlq needs a dlq publisher");
  }

  return async ({ message }: EachMessagePayload): Promise<void> => {
    let decoded: T;
    try {
      if (!message.value) throw new WireFormatError("tombstone (message has no value)");
      decoded = opts.decode(decodeWire(message.value).json);
    } catch (err) {
      opts.decodeErrorCounter.inc();
      opts.onDecodeError?.(err);
      if (policy === "ack-and-drop") return; // counted, committed past
      const dlq = opts.dlq!;
      try {
        await dlq.publisher.publish({
          topic: dlqTopicName(dlq.sourceTopic, dlq.groupId),
          key: message.key ?? undefined,
          value: message.value, // a tombstone stays a tombstone (null)
          headers: decodeFailureHeaders({
            originalHeaders: solHeadersOf(message.headers),
            decodeError: String(err),
            groupId: dlq.groupId,
          }),
        });
      } catch (publishErr) {
        throw new Error(
          `sol-kafka: could not dead-letter an undecodable record; not acking (${String(publishErr)})`,
        );
      }
      return;
    }

    const traceparent = message.headers?.traceparent?.toString();
    const outcome = await opts.handler({
      message: decoded,
      traceContext: extractTraceparent(traceparent),
    });
    if (outcome.kind === "fail") throw new MessageFailError(outcome.reason);
  };
}

export interface CrashListenerOptions {
  onCrash?: (error: unknown) => void;
  /**
   * Called once when the crash is a `MessageFailError` (a handler `Fail`),
   * before the process exits 0: the app's chance to stop the consumer and flush
   * what it has buffered. Defaults to `consumer.stop()`.
   */
  onFailStop?: (reason: string) => Promise<void> | void;
}

/**
 * Wires a consumer's CRASH listener with Sol's exit policy.
 *
 * kafkajs already self-heals from retriable errors (it sets payload.restart=true
 * and reschedules start() itself after a backoff). Exiting unconditionally on
 * every crash -- as an earlier draft of FEAT-033's port did -- kills the process
 * on crashes kafkajs was already about to recover from on its own. So:
 *
 *   - a `MessageFailError` (handler `Fail`) is terminal: stop the consumer and
 *     exit 0. Its offset was never committed, so the fact is redelivered to
 *     whoever restarts -- mirroring worker.ml's `| Fail -> ... Consumer.Stop`.
 *   - any other crash exits only when kafkajs itself has given up
 *     (payload.restart === false), so k8s restarts the pod instead of it
 *     quietly stopping progress forever.
 */
export function wireCrashListener(consumer: Consumer, opts?: CrashListenerOptions): void {
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    opts?.onCrash?.(payload.error);
    const err = payload.error;
    if (err instanceof MessageFailError) {
      const stop = opts?.onFailStop ?? (() => consumer.stop());
      void Promise.resolve(stop(err.reason)).finally(() => process.exit(0));
      return;
    }
    if (!payload.restart) process.exit(1);
  });
}
