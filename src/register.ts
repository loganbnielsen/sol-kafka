import type { Kafka } from "kafkajs";
import { assertDeclaredPartitions, type RegisteredTopic, type TopicContract } from "./contract.js";
import { describeTopic } from "./admin.js";
import { registerSchema, setSubjectCompatibility } from "./schemaRegistry.js";

export interface RegisterTopicOptions<T> {
  kafka: Kafka;
  registryUrl: string;
  /** The event contract: topic name, schema, declared partitions and key. */
  contract: TopicContract<T>;
  replicationFactor?: number;
}

/**
 * The single entry point for bringing a topic into existence and registering
 * its schema, in the exact order and fatality split Kafka_service.register
 * requires (kafka_service.ml:236-296):
 *
 *   0. reject a declaration of fewer than one partition -- before any broker
 *      round-trip, matching Sol's `Config` error.
 *   1. refuse to reduce an existing topic below its live partition count
 *      (`Partition_count_reduction`); a topic is created once and only ever
 *      grows.
 *   2. ensure_topic (idempotent create) with the DECLARED count -- BEFORE
 *      touching the schema registry. Relying on broker auto-create-on-produce
 *      works locally (Redpanda has it on by default) but fails hard with
 *      UNKNOWN_TOPIC_OR_PARTITION on any cluster with
 *      auto.create.topics.enable=false.
 *   3. register_schema -- FATAL. A caller MUST let this throw; there is no
 *      valid way to publish to this topic without a registered schema.
 *   4. set_subject_compatibility -- NON-FATAL. Some registries don't support
 *      configuring compatibility; failure here is logged as a warning and
 *      registration proceeds regardless.
 *
 * Creating and registering stay one function rather than independently
 * callable steps at this ordering: FEAT-033's hand-rolled TS port got the order
 * and fatality backwards on its first draft, and only caught it via adversarial
 * review reading the OCaml source line by line. Encoding the policy in the
 * function's own control flow removes the chance to get it wrong.
 */
export async function registerTopic<T>(opts: RegisterTopicOptions<T>): Promise<RegisteredTopic<T>> {
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
    // createTopics resolves false (not an error) if the topic already
    // exists -- same idempotent shape as ensure_topic.
    await admin.createTopics({
      topics: [{ topic: contract.name, numPartitions: contract.partitions, replicationFactor }],
    });
  } finally {
    await admin.disconnect();
  }

  const schemaId = await registerSchema(opts.registryUrl, contract.name, contract.schema);

  try {
    await setSubjectCompatibility(opts.registryUrl, contract.name);
  } catch (err) {
    console.warn(
      `[sol-kafka] warn: could not set schema compatibility for ${contract.name}: ${String(err)}`,
    );
  }

  return { ...contract, schemaId };
}
