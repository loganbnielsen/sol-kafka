import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { kafkaConfigFromEnv } from "../src/config.js";

// FEAT-097: the transport posture is required, never defaulted, matching the
// OCaml `Kafka_service.config_of_env` (SEC-007).

const base = { KAFKA_BROKERS: "localhost:9092" };

test("plaintext: only brokers, and no ssl/sasl keys", () => {
  const config = kafkaConfigFromEnv({ ...base, KAFKA_SECURITY_PROTOCOL: "plaintext" });
  assert.deepEqual(config, { brokers: ["localhost:9092"] });
});

test("an absent KAFKA_SECURITY_PROTOCOL is an error naming it (never a plaintext default)", () => {
  assert.throws(
    () => kafkaConfigFromEnv({ ...base }),
    (err: unknown) => err instanceof Error && /KAFKA_SECURITY_PROTOCOL is not set/.test(err.message),
  );
});

test("a blank KAFKA_SECURITY_PROTOCOL is the same as absent", () => {
  assert.throws(
    () => kafkaConfigFromEnv({ ...base, KAFKA_SECURITY_PROTOCOL: "   " }),
    /KAFKA_SECURITY_PROTOCOL is not set/,
  );
});

test("an unknown protocol is an error naming the value and the accepted set", () => {
  assert.throws(
    () => kafkaConfigFromEnv({ ...base, KAFKA_SECURITY_PROTOCOL: "tls" }),
    /unknown KAFKA_SECURITY_PROTOCOL "tls".*plaintext, ssl, sasl_plaintext, or sasl_ssl/s,
  );
});

test("an absent KAFKA_BROKERS is an error naming it", () => {
  assert.throws(
    () => kafkaConfigFromEnv({ KAFKA_SECURITY_PROTOCOL: "plaintext" }),
    /KAFKA_BROKERS is not set/,
  );
});

test("the protocol is case-insensitive, as Kafka_security.protocol_of_string is", () => {
  assert.deepEqual(kafkaConfigFromEnv({ ...base, KAFKA_SECURITY_PROTOCOL: "SASL_SSL", KAFKA_SASL_MECHANISM: "PLAIN", KAFKA_SASL_USERNAME: "u", KAFKA_SASL_PASSWORD: "p" }).sasl?.mechanism, "plain");
});

test("ssl without a CA location is `ssl: true`", () => {
  const config = kafkaConfigFromEnv({ ...base, KAFKA_SECURITY_PROTOCOL: "ssl" });
  assert.equal(config.ssl, true);
});

test("ssl with KAFKA_SSL_CA_LOCATION reads the PEM contents (kafkajs takes contents, not a path)", () => {
  const dir = mkdtempSync(join(tmpdir(), "sol-kafka-ca-"));
  const caPath = join(dir, "ca.pem");
  const pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
  try {
    writeFileSync(caPath, pem);
    const config = kafkaConfigFromEnv({
      ...base,
      KAFKA_SECURITY_PROTOCOL: "ssl",
      KAFKA_SSL_CA_LOCATION: caPath,
    });
    assert.deepEqual(config.ssl, { ca: [pem] });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("an unreadable KAFKA_SSL_CA_LOCATION fails closed, naming the variable", () => {
  assert.throws(
    () =>
      kafkaConfigFromEnv({
        ...base,
        KAFKA_SECURITY_PROTOCOL: "ssl",
        KAFKA_SSL_CA_LOCATION: "/nonexistent/ca.pem",
      }),
    /KAFKA_SSL_CA_LOCATION .* cannot be read/,
  );
});

test("sasl_plaintext builds the SASL options and never sets ssl", () => {
  const config = kafkaConfigFromEnv({
    ...base,
    KAFKA_SECURITY_PROTOCOL: "sasl_plaintext",
    KAFKA_SASL_MECHANISM: "scram-sha-256",
    KAFKA_SASL_USERNAME: "user",
    KAFKA_SASL_PASSWORD: "pw",
  });
  assert.deepEqual(config, {
    brokers: ["localhost:9092"],
    sasl: { mechanism: "scram-sha-256", username: "user", password: "pw" },
  });
});

test("each missing SASL variable is an error naming it", () => {
  const full = {
    ...base,
    KAFKA_SECURITY_PROTOCOL: "sasl_plaintext",
    KAFKA_SASL_MECHANISM: "PLAIN",
    KAFKA_SASL_USERNAME: "user",
    KAFKA_SASL_PASSWORD: "pw",
  };
  assert.throws(() => kafkaConfigFromEnv({ ...full, KAFKA_SASL_MECHANISM: undefined }), /KAFKA_SASL_MECHANISM is not set/);
  assert.throws(() => kafkaConfigFromEnv({ ...full, KAFKA_SASL_USERNAME: undefined }), /KAFKA_SASL_USERNAME is not set/);
  assert.throws(() => kafkaConfigFromEnv({ ...full, KAFKA_SASL_PASSWORD: undefined }), /KAFKA_SASL_PASSWORD is not set/);
});

test("an unknown SASL mechanism is an error naming the accepted set", () => {
  assert.throws(
    () =>
      kafkaConfigFromEnv({
        ...base,
        KAFKA_SECURITY_PROTOCOL: "sasl_plaintext",
        KAFKA_SASL_MECHANISM: "OAUTHBEARER",
        KAFKA_SASL_USERNAME: "user",
        KAFKA_SASL_PASSWORD: "pw",
      }),
    /unknown KAFKA_SASL_MECHANISM "OAUTHBEARER".*PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512/s,
  );
});

test("sasl_ssl carries both ssl and sasl", () => {
  const config = kafkaConfigFromEnv({
    ...base,
    KAFKA_SECURITY_PROTOCOL: "sasl_ssl",
    KAFKA_SASL_MECHANISM: "PLAIN",
    KAFKA_SASL_USERNAME: "user",
    KAFKA_SASL_PASSWORD: "pw",
  });
  assert.equal(config.ssl, true);
  assert.deepEqual(config.sasl, { mechanism: "plain", username: "user", password: "pw" });
});
