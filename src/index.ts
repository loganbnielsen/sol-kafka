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
export { wrapEachMessage, wireCrashListener, MessageFailError } from "./consume.js";
export type {
  CrashListenerOptions,
  DecodeErrorCounter,
  DecodeErrorPolicy,
  EachMessageOptions,
  MessageHandlerContext,
} from "./consume.js";
export { traceparentOf, extractTraceparent } from "@sol-fab/obs";
export { ACK, fail } from "./outcome.js";
export type { Outcome } from "./outcome.js";
export {
  GROUP_HASH_LEN,
  HDR_DECODE_ERROR,
  HDR_ORIGIN_GROUP,
  MAX_GROUP_SEGMENT_LEN,
  canonicalGroupSegment,
  decodeFailureHeaders,
  dlqTopicName,
  provisionDlqTopic,
  sanitizeGroupId,
  solHeadersOf,
} from "./dlq.js";
export type { DlqPublisher, DlqRecord, ProvisionDlqTopicOptions, SolHeaders } from "./dlq.js";
