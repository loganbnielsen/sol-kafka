import type { Kafka, Producer } from "kafkajs";

// Shared broker/producer doubles for the unit tests. Not a `*.test.ts` file, so
// `node --test test/*.test.ts` never runs it directly.

export interface CreatedTopic {
  topic: string;
  numPartitions: number;
  replicationFactor: number;
}

export interface FakeKafka {
  kafka: Kafka;
  calls: string[];
  created: CreatedTopic[];
}

/**
 * A kafkajs client whose admin API answers `listTopics`/`fetchTopicMetadata`
 * from a fixed live shape. `livePartitions: undefined` means "the topic does
 * not exist yet", which is the broker's real answer for an unknown topic.
 */
export function fakeKafka(config: { livePartitions?: number; topicName?: string } = {}): FakeKafka {
  const calls: string[] = [];
  const created: CreatedTopic[] = [];
  const topicName = config.topicName ?? "orders";
  const kafka = {
    admin: () => ({
      connect: async () => {},
      disconnect: async () => {},
      listTopics: async () => {
        calls.push("listTopics");
        return config.livePartitions === undefined ? [] : [topicName];
      },
      fetchTopicMetadata: async ({ topics }: { topics: string[] }) => {
        calls.push("fetchTopicMetadata");
        if (config.livePartitions === undefined) return { topics: [] };
        return {
          topics: topics.map((name) => ({
            name,
            partitions: Array.from({ length: config.livePartitions as number }, (_, partitionId) => ({
              partitionId,
              leader: 0,
              replicas: [0],
              isr: [0],
              partitionErrorCode: 0,
            })),
          })),
        };
      },
      createTopics: async (args: { topics: CreatedTopic[] }) => {
        calls.push("createTopics");
        created.push(...args.topics);
        return true;
      },
    }),
  } as unknown as Kafka;
  return { kafka, calls, created };
}

export interface SentMessageSet {
  topic: string;
  messages: Array<{
    key?: Buffer | string | null;
    value: Buffer | string | null;
    headers?: unknown;
  }>;
}

/** A producer that records what it was asked to send. */
export function fakeProducer(): { producer: Producer; sent: SentMessageSet[] } {
  const sent: SentMessageSet[] = [];
  const producer = {
    send: async (args: SentMessageSet) => {
      sent.push(args);
    },
  } as unknown as Producer;
  return { producer, sent };
}
