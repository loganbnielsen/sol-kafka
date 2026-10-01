/**
 * A topic's declared event contract, mirroring Sol's OCaml
 * `Kafka_service.MESSAGE` (framework/kafka-eio-service/lib/kafka_service.mli).
 *
 * `partitions` and `key` are the two halves of one promise
 * (`internal/specs/framework-conventions.md` § Partitioning and key, BUG-099):
 * the topic is created with `partitions` and never reduced, every record that
 * shares a key lands on one partition, and a single consumer handles that
 * key's records in publication order. Declaring them together is what keeps the
 * topic's real shape and what the producer actually sends from drifting apart —
 * FEAT-117's whole point is that a TS author gets the same resolution an OCaml
 * author does from `MESSAGE.partitions` / `MESSAGE.key`.
 */

/**
 * The part of a topic that both a producer and a consumer need to agree on: its
 * name and the partition count it is created with. A full {@link TopicContract}
 * satisfies this structurally, so a producer can hand its contract to the
 * consumer-side relay provisioning without restating the count.
 */
export interface TopicShape {
  /** The Kafka topic name. */
  readonly name: string;
  /** Partitions the topic is created with; at least 1. Never reduced. */
  readonly partitions: number;
}

/** A declared event contract: the topic's shape plus its schema and key. */
export interface TopicContract<T> extends TopicShape {
  /** JSON Schema text registered against the topic's subject. */
  readonly schema: string;
  /**
   * The record key. `undefined` means unkeyed: records spread across partitions
   * and no ordering is claimed. Mirrors `MESSAGE.key : t -> string option`.
   */
  readonly key: (message: T) => string | undefined;
}

/** A registered contract: the declaration plus the schema id registration resolved. */
export interface RegisteredTopic<T> extends TopicContract<T> {
  readonly schemaId: number;
}

/**
 * A declared count below 1 is a contract error, resolved before any broker
 * round-trip — matching `Kafka_service.register`'s `Config` error
 * (kafka_service.ml:236-247), which rejects the same declaration up front.
 */
export function assertDeclaredPartitions(shape: TopicShape): void {
  if (!Number.isInteger(shape.partitions) || shape.partitions < 1) {
    throw new Error(
      `sol-kafka: topic '${shape.name}' declares ${shape.partitions} partitions; a topic has at least one`,
    );
  }
}
