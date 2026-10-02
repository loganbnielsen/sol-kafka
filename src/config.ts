/**
 * Sol's Kafka transport-security convention (Sol SEC-007), mirroring the OCaml
 * `Kafka_service.config_of_env` + `Kafka_security.of_env` pair.
 *
 * The contract the parity work fixes: `KAFKA_SECURITY_PROTOCOL` is **required**.
 * An absent value is an error, never a silent plaintext default — every
 * Sol-rendered manifest declares the posture, and a workload that does not read
 * it would stay plaintext under a `sasl_ssl` manifest. This is the one place a
 * TypeScript application reads it, so both Sol workloads and the golden path
 * cannot drift.
 */
import { readFileSync } from "node:fs";
import type { SASLOptions } from "kafkajs";

export type KafkaSecurityProtocol = "plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl";
export type KafkaSaslMechanism = "plain" | "scram-sha-256" | "scram-sha-512";

/** The kafkajs client options Sol derives from the environment. */
export interface KafkaClientEnv {
  brokers: string[];
  ssl?: boolean | { ca: string[] };
  sasl?: SASLOptions;
}

/** `Kafka_service_config.setting`: unset and blank both mean "not stated". */
function setting(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const value = env[name]?.trim();
  return value ? value : undefined;
}

function required(env: NodeJS.ProcessEnv, name: string, why: string): string {
  const value = setting(env, name);
  if (value === undefined) throw new Error(`sol-kafka: ${name} is not set${why}`);
  return value;
}

function protocolOf(raw: string): KafkaSecurityProtocol {
  switch (raw.toLowerCase()) {
    case "plaintext":
      return "plaintext";
    case "ssl":
      return "ssl";
    case "sasl_plaintext":
      return "sasl_plaintext";
    case "sasl_ssl":
      return "sasl_ssl";
    default:
      throw new Error(
        `sol-kafka: unknown KAFKA_SECURITY_PROTOCOL ${JSON.stringify(raw)} ` +
          `(expected plaintext, ssl, sasl_plaintext, or sasl_ssl)`,
      );
  }
}

function mechanismOf(raw: string): KafkaSaslMechanism {
  switch (raw.toUpperCase()) {
    case "PLAIN":
      return "plain";
    case "SCRAM-SHA-256":
      return "scram-sha-256";
    case "SCRAM-SHA-512":
      return "scram-sha-512";
    default:
      throw new Error(
        `sol-kafka: unknown KAFKA_SASL_MECHANISM ${JSON.stringify(raw)} ` +
          `(expected PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)`,
      );
  }
}

/**
 * `Kafka_security.t`'s `Ssl` case carries a CA *path*; kafkajs wants the PEM
 * contents, so the one place the two languages differ structurally is the read
 * here. An unreadable file fails closed rather than silently trusting the
 * system CAs.
 */
function sslOf(env: NodeJS.ProcessEnv): boolean | { ca: string[] } {
  const caLocation = setting(env, "KAFKA_SSL_CA_LOCATION");
  if (caLocation === undefined) return true;
  try {
    return { ca: [readFileSync(caLocation, "utf8")] };
  } catch (err) {
    throw new Error(
      `sol-kafka: KAFKA_SSL_CA_LOCATION ${JSON.stringify(caLocation)} cannot be read: ${String(err)}`,
    );
  }
}

function saslOf(env: NodeJS.ProcessEnv): SASLOptions {
  const mechanism = mechanismOf(
    required(env, "KAFKA_SASL_MECHANISM", " required for SASL protocols"),
  );
  const username = required(env, "KAFKA_SASL_USERNAME", " required for SASL protocols");
  const password = required(env, "KAFKA_SASL_PASSWORD", " required for SASL protocols");
  return { mechanism, username, password } as SASLOptions;
}

/**
 * Read the Kafka client configuration from the environment, with the same
 * required set and the same error shape as `config_of_env`: `KAFKA_BROKERS` and
 * `KAFKA_SECURITY_PROTOCOL` are always required; the SASL variables are required
 * by the two SASL protocols; `KAFKA_SSL_CA_LOCATION` is optional for the two TLS
 * protocols.
 *
 * The `env` parameter is for tests; production callers use the default.
 */
export function kafkaConfigFromEnv(env: NodeJS.ProcessEnv = process.env): KafkaClientEnv {
  const protocol = protocolOf(
    required(
      env,
      "KAFKA_SECURITY_PROTOCOL",
      ": state the Kafka transport posture explicitly (plaintext | ssl | " +
        "sasl_plaintext | sasl_ssl). Sol-rendered manifests set it; for a local " +
        "process use KAFKA_SECURITY_PROTOCOL=plaintext",
    ),
  );
  const brokers = required(
    env,
    "KAFKA_BROKERS",
    ": state the Kafka substrate addresses explicitly (Sol-rendered manifests set it)",
  ).split(",");

  switch (protocol) {
    case "plaintext":
      return { brokers };
    case "ssl":
      return { brokers, ssl: sslOf(env) };
    case "sasl_plaintext":
      return { brokers, sasl: saslOf(env) };
    case "sasl_ssl":
      return { brokers, ssl: sslOf(env), sasl: saslOf(env) };
  }
}
