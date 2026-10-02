// Sol's convention on top of the Confluent-compatible schema registry --
// kafkajs has no opinion on schema registries at all, so every line here is
// glue a TS app author would otherwise have to write themselves. Matches
// framework/kafka-eio-service/lib/kafka_service_schema.ml.

const REQUEST_TIMEOUT_MS = 10_000;
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

async function readBounded(resp: Response): Promise<string> {
  const reader = resp.body?.getReader();
  if (!reader) return "";
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > MAX_RESPONSE_BYTES) {
      await reader.cancel();
      throw new Error(`schema registry response exceeded ${MAX_RESPONSE_BYTES} bytes`);
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function registryRequest(
  registryUrl: string,
  method: string,
  path: string,
  body: unknown
): Promise<{ status: number; body: string }> {
  const resp = await fetch(`${registryUrl}${path}`, {
    method,
    headers: { "content-type": "application/vnd.schemaregistry.v1+json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  return { status: resp.status, body: await readBounded(resp) };
}

/**
 * Mirrors kafka_service_schema.ml's set_subject_compatibility exactly -- a
 * plain PUT, nothing else. In the real runtime path (Kafka_service.register,
 * kafka_service.ml:172-177) a failure here is NON-FATAL -- logged as a
 * warning and ignored, registration proceeds without it. See registerTopic()
 * below, which encodes that policy so callers can't get the ordering or
 * fatality wrong by calling these two functions directly themselves.
 */
export async function setSubjectCompatibility(registryUrl: string, topicName: string): Promise<void> {
  const subject = `${topicName}-value`;
  const { status, body } = await registryRequest(registryUrl, "PUT", `/config/${subject}`, {
    compatibility: "FULL",
  });
  if (status !== 200 && status !== 204) {
    throw new Error(`set compatibility: HTTP ${status}: ${body}`);
  }
}

/**
 * Mirrors kafka_service_schema.ml's register_schema exactly: a plain POST to
 * /subjects/{subject}/versions. In the real runtime path this call's failure
 * IS fatal (Kafka_service.register propagates it as an Error).
 */
export async function registerSchema(
  registryUrl: string,
  topicName: string,
  schema: string
): Promise<number> {
  const subject = `${topicName}-value`;
  const reg = await registryRequest(registryUrl, "POST", `/subjects/${subject}/versions`, {
    schemaType: "JSON",
    schema,
  });
  if (reg.status !== 200 && reg.status !== 201) {
    throw new Error(`schema registry: HTTP ${reg.status}: ${reg.body}`);
  }
  const parsed = JSON.parse(reg.body) as { id: number };
  return parsed.id;
}

/**
 * Standalone compatibility check (POST .../compatibility/subjects/.../versions/latest).
 * Exists in the OCaml original as a CI-gate helper (Schema.check), not part
 * of the runtime registration path -- exported for the same purpose here,
 * not called by registerTopic().
 */
/**
 * `kafka_service_schema.ml`'s `is_subject_not_found`: only the registry's two
 * "no such subject/version" error codes mean "nothing registered yet". Any
 * other 404 is a misconfigured base URL, not an empty registry, and treating it
 * as compatible would silently admit an unregistered contract.
 */
export function isSubjectNotFound(body: string): boolean {
  try {
    const parsed = JSON.parse(body) as { error_code?: number };
    return parsed.error_code === 40401 || parsed.error_code === 40402;
  } catch {
    return false;
  }
}

export async function checkCompatibility(
  registryUrl: string,
  topicName: string,
  schema: string
): Promise<{ compatible: boolean }> {
  const subject = `${topicName}-value`;
  const { status, body } = await registryRequest(
    registryUrl,
    "POST",
    `/compatibility/subjects/${subject}/versions/latest`,
    { schemaType: "JSON", schema }
  );
  if (status === 404 && isSubjectNotFound(body)) return { compatible: true };
  if (status !== 200) {
    throw new Error(
      status === 404
        ? `schema registry HTTP 404 that is not 'subject not found' (is ${registryUrl} the registry's base URL?): ${body}`
        : `schema registry: HTTP ${status}: ${body}`
    );
  }
  const parsed = JSON.parse(body) as { is_compatible: boolean };
  return { compatible: parsed.is_compatible };
}

/**
 * `kafka_service_schema.ml`'s `lookup_schema`: resolve the id of the registered
 * schema that matches the declared contract, WITHOUT registering anything. The
 * registry answers `POST /subjects/{subject}` with the id of a matching version,
 * or 404 when no registered version matches the declaration. This is the
 * read-only half of a producer's startup: the deploy step registered the
 * schema, the runtime only resolves it.
 */
export async function lookupSchema(
  registryUrl: string,
  topicName: string,
  schema: string
): Promise<number> {
  const subject = `${topicName}-value`;
  const { status, body } = await registryRequest(registryUrl, "POST", `/subjects/${subject}`, {
    schemaType: "JSON",
    schema,
  });
  if (status === 200) {
    const parsed = JSON.parse(body) as { id: number };
    return parsed.id;
  }
  if (status === 404) {
    throw new Error(
      `subject '${subject}' has no registered schema matching the declared contract`
    );
  }
  throw new Error(`schema registry HTTP ${status}: ${body}`);
}
