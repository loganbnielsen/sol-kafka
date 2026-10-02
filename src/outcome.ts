/**
 * Sol's worker outcome contract (sol-worker.md, FEAT-113):
 *
 *   Ack | Fail reason
 *
 * The `reason` is diagnostic text only — the runtime never inspects it to
 * decide delay, routing or retryability (FEAT-078's policy-ownership rule).
 * Never encode policy in the string; introduce a typed concept if one is
 * needed. There is no `ack` to call: the framework commits the offset, and
 * only after the outcome is `ack`.
 *
 * FEAT-113 removed Kafka message-level retry and application-level
 * `Dead_letter` from the OCaml framework; `Fail` is fail-stop (the offset is
 * left uncommitted and the consumer stops), and independently retryable work
 * belongs to a durable job queue, not the topic. This module mirrors that
 * exactly.
 */

export type Outcome = { readonly kind: "ack" } | { readonly kind: "fail"; readonly reason: string };

/** The single `Ack` value; the only outcome a fact-handler can produce happily. */
export const ACK: Outcome = Object.freeze({ kind: "ack" });

/** `Fail reason` — the fact could not be handled; fail-stop, offset uncommitted. */
export function fail(reason: string): Outcome {
  return { kind: "fail", reason };
}
