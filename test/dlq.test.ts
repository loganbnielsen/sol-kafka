import { test } from "node:test";
import assert from "node:assert/strict";
import {
  HDR_DECODE_ERROR,
  HDR_ORIGIN_GROUP,
  MAX_GROUP_SEGMENT_LEN,
  canonicalGroupSegment,
  decodeFailureHeaders,
  dlqTopicName,
  sanitizeGroupId,
  solHeadersOf,
} from "../src/dlq.js";

// BUG-117 / FEAT-113: parity with the OCaml DLQ conventions
// (framework/ocaml/kafka-eio-service/lib/kafka_service_dlq.ml). These
// assertions pin the exact values the OCaml side produces, so a drift on either
// side fails here rather than silently splitting DLQ topics or header
// semantics between languages.

test("sanitizeGroupId: alphanumerics and '-' survive; everything else becomes '-'; empty is 'unscoped'", () => {
  assert.equal(sanitizeGroupId("orders.worker"), "orders-worker");
  assert.equal(sanitizeGroupId("a_b/c d"), "a-b-c-d");
  assert.equal(sanitizeGroupId("keep-ALREADY-123"), "keep-ALREADY-123");
  assert.equal(sanitizeGroupId(""), "unscoped");
  assert.equal(sanitizeGroupId("._."), "---");
});

test("canonicalGroupSegment: the sanitized id always carries the 12-hex MD5 of the original id", () => {
  // Fixtures computed independently (python3 hashlib.md5), not by this code.
  // kafka_service_dlq.ml's canonical_group_segment appends the hash on every
  // id, not only an over-length one.
  assert.equal(canonicalGroupSegment("orders.worker"), "orders-worker-33f9c2161734");
  assert.equal(canonicalGroupSegment("payments"), "payments-84d5eaf713c9");
  assert.equal(canonicalGroupSegment(""), "unscoped-d41d8cd98f00");
  assert.equal(canonicalGroupSegment("a"), "a-0cc175b9c0f1");
});

test("canonicalGroupSegment: over-length ids truncate the readable prefix to 51 chars, keeping the full 12-hex hash", () => {
  assert.equal(canonicalGroupSegment("g".repeat(70)), "g".repeat(51) + "-d63aff782647");
  assert.equal(
    canonicalGroupSegment("Group.One_" + "x".repeat(60)),
    "Group-One-" + "x".repeat(41) + "-237aac254fe2",
  );
  // The cap is on the whole segment (64), so a 64-char id is truncated too:
  // 51 readable characters plus '-' and the 12-hex hash is exactly 64.
  assert.equal(
    canonicalGroupSegment("x".repeat(MAX_GROUP_SEGMENT_LEN)),
    "x".repeat(51) + "-c1bb4f81d892",
  );
  // Distinct overlong ids must never truncate to the same segment.
  const a = canonicalGroupSegment("a".repeat(64) + "1");
  const b = canonicalGroupSegment("a".repeat(64) + "2");
  assert.notEqual(a, b);
});

test("canonicalGroupSegment: punctuation variants that sanitize alike stay distinct via the hash of the original id", () => {
  const variants = ["pay.ments", "pay_ments", "pay-ments"];
  const segments = variants.map(canonicalGroupSegment);
  assert.equal(new Set(segments).size, variants.length);
  assert.ok(segments.every((s) => s.startsWith("pay-ments-")));
});

test("dlqTopicName matches the OCaml naming", () => {
  assert.equal(dlqTopicName("orders", "payments"), "orders.payments-84d5eaf713c9.dlq");
  assert.equal(dlqTopicName("orders", "orders.worker"), "orders.orders-worker-33f9c2161734.dlq");
});

test("solHeadersOf: first value wins, Buffers are UTF-8 decoded, undefined is dropped", () => {
  assert.deepEqual(
    solHeadersOf({
      plain: "v",
      many: ["first", "second"],
      buf: Buffer.from("bytes", "utf8"),
      gone: undefined,
    }),
    { plain: "v", many: "first", buf: "bytes" },
  );
  assert.deepEqual(solHeadersOf(undefined), {});
});

test("decodeFailureHeaders: preserves the original headers untouched and sets the diagnostic plus origin group", () => {
  const headers = decodeFailureHeaders({
    originalHeaders: { "content-type": "application/json", [HDR_ORIGIN_GROUP]: "stale" },
    decodeError: "boom",
    groupId: "orders-worker",
  });

  assert.equal(headers["content-type"], "application/json");
  assert.equal(headers[HDR_DECODE_ERROR], "boom");
  assert.equal(headers[HDR_ORIGIN_GROUP], "orders-worker", "the current group replaces a stale copy");
});

test("decodeFailureHeaders: a redriven DLQ record that fails again reports the new diagnostic", () => {
  const headers = decodeFailureHeaders({
    originalHeaders: { [HDR_DECODE_ERROR]: "first failure", [HDR_ORIGIN_GROUP]: "g" },
    decodeError: "second failure",
    groupId: "g",
  });
  assert.equal(headers[HDR_DECODE_ERROR], "second failure");
});
