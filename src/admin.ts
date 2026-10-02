/**
 * Reading a topic's live shape from the broker.
 *
 * The declaration says what a topic *should* be (see `contract.ts`); the broker
 * says what it *is*. Both matter: the declaration is checked against the live
 * topic so a remembered count can never shrink it, and the DLQ topic is
 * inherited from the live source topic so a TypeScript worker and an OCaml
 * service on the same topic provision the same shape.
 */
import type { Kafka } from "kafkajs";

/** A topic as the broker reports it. */
export interface ObservedTopic {
  readonly partitions: number;
  readonly replicationFactor: number;
}

/** kafkajs surfaces a topic that does not exist as this protocol error type. */
function isUnknownTopic(err: unknown): boolean {
  return (
    typeof err === "object" &&
    err !== null &&
    (err as { type?: unknown }).type === "UNKNOWN_TOPIC_OR_PARTITION"
  );
}

/**
 * What the broker knows about `topic`, or `undefined` when the topic does not
 * exist yet (which is not an error — the caller is about to create it, exactly
 * as `Kafka_service.Admin.query_topic_partitions` distinguishes
 * `Topic_not_found` from a metadata failure).
 */
export async function describeTopic(kafka: Kafka, topic: string): Promise<ObservedTopic | undefined> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    // Ask for the topic names first: a targeted metadata request for a topic
    // that does not exist is answered with UNKNOWN_TOPIC_OR_PARTITION, which
    // kafkajs logs at ERROR level. "Not created yet" is the normal first
    // startup, not an error, so it should not look like one.
    const names = await admin.listTopics();
    if (!names.includes(topic)) return undefined;
    const { topics } = await admin.fetchTopicMetadata({ topics: [topic] });
    const found = topics.find((t) => t.name === topic);
    if (!found || found.partitions.length === 0) return undefined;
    return {
      partitions: found.partitions.length,
      replicationFactor: found.partitions[0]?.replicas.length ?? 1,
    };
  } catch (err) {
    // The topic can still be deleted between the two calls; keep treating
    // exactly that as "not there".
    if (isUnknownTopic(err)) return undefined;
    throw err;
  } finally {
    await admin.disconnect();
  }
}
