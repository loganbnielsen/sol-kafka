export { encodeWire, decodeWire, WireFormatError } from "./wireFormat.js";
export { kafkaConfigFromEnv } from "./config.js";
export type {
  KafkaClientEnv,
  KafkaSaslMechanism,
  KafkaSecurityProtocol,
} from "./config.js";
export { registerSchema, setSubjectCompatibility, checkCompatibility } from "./schemaRegistry.js";
export type { TopicShape, TopicContract, RegisteredTopic } from "./contract.js";
export { describeTopic } from "./admin.js";
export type { ObservedTopic } from "./admin.js";
export { registerTopic } from "./register.js";
export type { RegisterTopicOptions } from "./register.js";
export { publish } from "./publish.js";
export type { PublishOptions } from "./publish.js";
export { wrapEachMessage, wireCrashListener } from "./consume.js";
export type { DecodeErrorCounter, MessageHandlerContext } from "./consume.js";
export { traceparentOf, extractTraceparent } from "@sol-fab/obs";
export {
  DEFAULT_RETRY_POLICY,
  HDR_ATTEMPT,
  HDR_DECODE_ERROR,
  HDR_ORIGIN_GROUP,
  HDR_RETRY_AT,
  MAX_GROUP_SEGMENT_LEN,
  backoffS,
  canonicalGroupSegment,
  deadLetterHeaders,
  decideAction,
  epochSeconds,
  parseAttemptHeader,
  parseRetryAtHeader,
  relayTopicName,
  retryConsumerGroupId,
  retryDecodeFailureHeaders,
  retryRecordHeaders,
  retryTopicsPolicyError,
  sanitizeGroupId,
  solHeadersOf,
} from "./retry.js";
export type {
  RetryAction,
  RetryPolicy,
  RetryRecordOptions,
  RetryStrategy,
  Rng,
  SolHeaders,
} from "./retry.js";
export { ACK, deadLetter, retry } from "./outcome.js";
export type { Outcome } from "./outcome.js";
export { wrapEachRetryableMessage, type DecodeErrorPolicy } from "./retryable.js";
export type { RetryableMessageOptions } from "./retryable.js";
export type { RawRecord, RelayRecord, RetryMetrics, RetryRelay } from "./routing.js";
export type { RetryRelayProcessorOptions } from "./relay.js";
export {
  handleRetryRecord,
  kafkaRetryRelay,
  provisionRelayTopics,
  runRetryRelayConsumer,
} from "./relay.js";
export type { ProvisionRelayTopicsOptions, RetryRelayConsumerOptions } from "./relay.js";
