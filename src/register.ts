import type { Kafka } from "kafkajs";
import {
  assertDeclaredPartitions,
  type RegisteredTopic,
  type TopicContract,
  type TopicShape,
} from "./contract.js";
import { describeTopic } from "./admin.js";
import {
  checkCompatibility,
  lookupSchema,
  registerSchema,
  setSubjectCompatibility,
} from "./schemaRegistry.js";

/**
 * Topic provisioning and the two registry roles, mirroring the split BUG-105
 * established in the OCaml framework:
 *
 *   - runtime (`connectTopic`): provision the topic, verify the declared reader
 *     contract is compatible, and RESOLVE the registered schema id. Read-only --
 *     a runtime never registers a version or changes a subject's compatibility.
 *   - deployment (`registerContract`, driven by the workspace's contract
 *     projection): set `FULL` first, then register the declared schema. This is
 *     the only write path in the package.
 *
 * The split exists because a runtime that registers makes every replica a
 * registry writer, lets a consumer's reader schema land as `latest`, and reverts
 * an operator's compatibility setting on the next restart.
 */

export interface TopicContractOptions<T> {
  /** The declared event contract: topic name, schema, partitions and key. */
  contract: TopicContract<T>;
}

export interface ProvisionTopicOptions<T> extends TopicContractOptions<T> {
  kafka: Kafka;
  replicationFactor?: number;
}

/**
 * The part of a declared contract the registry operations need: its topic
 * shape and schema text. A `TopicContract<T>` satisfies it structurally, so the
 * same functions serve a typed producer contract and a projection entry point
 * that only carries the declaration.
 */
export interface DeclaredContract extends TopicShape {
  readonly schema: string;
}

export interface RegistryOptions {
  registryUrl: string;
  contract: DeclaredContract;
}

/**
 * Bring the topic into existence at its declared partition count, mirroring
 * `Kafka_service.register`'s partition guard + `ensure_topic` steps and
 * touching no schema registry:
 *
 *   0. reject a declaration of fewer than one partition, before any broker
 *      round-trip (Sol's `Config` error).
 *   1. refuse to reduce an existing topic below its live partition count
 *      (`Partition_count_reduction`); a topic is created once and only grows.
 *   2. createTopics with the DECLARED count. Relying on broker auto-create
 *      works locally but fails hard with UNKNOWN_TOPIC_OR_PARTITION on any
 *      cluster with auto.create.topics.enable=false.
 */
export async function provisionTopic<T>(opts: ProvisionTopicOptions<T>): Promise<void> {
  const { contract } = opts;
  assertDeclaredPartitions(contract);
  const replicationFactor = opts.replicationFactor ?? 1;

  const existing = await describeTopic(opts.kafka, contract.name);
  if (existing && existing.partitions > contract.partitions) {
    throw new Error(
      `sol-kafka: topic '${contract.name}' already has ${existing.partitions} partitions; ` +
        `refusing to reduce it to the declared ${contract.partitions}`,
    );
  }

  const admin = opts.kafka.admin();
  await admin.connect();
  try {
    // createTopics resolves false (not an error) if the topic already exists --
    // same idempotent shape as ensure_topic.
    await admin.createTopics({
      topics: [{ topic: contract.name, numPartitions: contract.partitions, replicationFactor }],
    });
  } finally {
    await admin.disconnect();
  }
}

/**
 * Read-only contract verification, mirroring `Kafka_service.register`'s
 * `Schema.check` + `Schema.resolve`.
 *
 *   - `checkCompatibility` verifies the declared schema can read the registered
 *     versions (a subject that has never been registered is compatible -- there
 *     is nothing to conflict with).
 *   - `lookupSchema` resolves the id of the registered version that matches the
 *     declaration, and fails when the declared contract is not registered. That
 *     failure is the point: a producer whose schema was never reconciled must
 *     not start and invent a version at runtime.
 */
export async function resolveContract(opts: RegistryOptions): Promise<{ schemaId: number }> {
  const { compatible } = await checkCompatibility(
    opts.registryUrl,
    opts.contract.name,
    opts.contract.schema,
  );
  if (!compatible) {
    throw new Error(
      `sol-kafka: schema for topic '${opts.contract.name}' is not compatible with the registered version`,
    );
  }
  const schemaId = await lookupSchema(opts.registryUrl, opts.contract.name, opts.contract.schema);
  return { schemaId };
}

export interface ConnectTopicOptions<T> extends ProvisionTopicOptions<T> {
  registryUrl: string;
}

/**
 * What a producer runs at startup: provision the topic, then resolve the
 * registered schema id read-only. Returns the registered topic `publish` keys
 * and wire-encodes with. Mirrors `Kafka_service.register` end to end, and never
 * writes to the registry.
 */
export async function connectTopic<T>(opts: ConnectTopicOptions<T>): Promise<RegisteredTopic<T>> {
  await provisionTopic(opts);
  const { schemaId } = await resolveContract({
    registryUrl: opts.registryUrl,
    contract: opts.contract,
  });
  return { ...opts.contract, schemaId };
}

/**
 * The deployment write path, mirroring `Schema.register_contract`: set the
 * subject's compatibility to `FULL` FIRST, then register the declared schema.
 * Either failure is fatal -- unlike the old runtime `registerTopic`, a failed
 * compatibility PUT is not swallowed, because the contract the deploy step
 * promises is "FULL, then this version".
 *
 * Idempotent: registering an already-registered schema returns its existing id.
 */
export async function registerContract(opts: RegistryOptions): Promise<{ schemaId: number }> {
  await setSubjectCompatibility(opts.registryUrl, opts.contract.name);
  const schemaId = await registerSchema(opts.registryUrl, opts.contract.name, opts.contract.schema);
  return { schemaId };
}

/** Read-only compatibility gate for `--check`, mirroring `Schema.check`. */
export async function checkContract(opts: RegistryOptions): Promise<void> {
  const { compatible } = await checkCompatibility(
    opts.registryUrl,
    opts.contract.name,
    opts.contract.schema,
  );
  if (!compatible) {
    throw new Error(
      `schema for topic '${opts.contract.name}' is not compatible with the registered version`,
    );
  }
}
