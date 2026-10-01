/**
 * Publishing under a topic's declared contract.
 *
 * The point of this helper is the one line an author would otherwise have to
 * remember and get right by hand: the record is keyed by `contract.key`. That
 * key is what pins every record for one entity to one partition, which is what
 * lets a consumer group keep that entity's events in order across a
 * multi-partition topic (BUG-099). The same declaration that created the topic
 * decides the key here, so the two cannot disagree — mirroring
 * `Kafka_service.publish`, which reads `topic.key` from the registered topic.
 */
import type { IHeaders, Message, Producer } from "kafkajs";
import { encodeWire } from "./wireFormat.js";
import type { RegisteredTopic } from "./contract.js";

export interface PublishOptions {
  /** Extra headers, e.g. Sol's `traceparent` (string or bytes, kafkajs shape). */
  readonly headers?: IHeaders;
}

/** Publish one message: wire-encoded with the registered schema id, and keyed. */
export async function publish<T>(
  producer: Producer,
  topic: RegisteredTopic<T>,
  message: T,
  opts: PublishOptions = {},
): Promise<void> {
  const key = topic.key(message);
  const record: Message = { value: encodeWire(topic.schemaId, message) };
  if (key !== undefined) record.key = key;
  if (opts.headers !== undefined) record.headers = opts.headers;
  await producer.send({ topic: topic.name, messages: [record] });
}
