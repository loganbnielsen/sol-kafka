import type { DeclaredContract } from "./register.js";
import { checkContract, registerContract } from "./register.js";

/**
 * The workspace's contract projection: the language-neutral object Sol's
 * deployment lifecycle consumes, identical to the OCaml
 * `Kafka_service.Contract.projection` (BUG-105). One event per declared
 * contract, each carrying the module label, topic, declared partition count and
 * schema text. Sol's `sol plan` reads `--json`; `sol up`/`sol deploy` run
 * `--check` and `--apply`.
 *
 * The shape is the wire contract: an OCaml `contract/contract.exe` and a
 * TypeScript entry point emit byte-comparable JSON, so Sol needs no
 * language-specific handling to consume either.
 */
export interface NamedContract {
  /** The event's module label, as OCaml's `(string * (module MESSAGE))` carries. */
  readonly module: string;
  readonly contract: DeclaredContract;
}

export interface ProjectedEvent {
  readonly module: string;
  readonly topic: string;
  readonly partitions: number;
  readonly schema: string;
}

export interface ContractProjection {
  readonly version: 1;
  readonly events: ReadonlyArray<ProjectedEvent>;
}

export function contractProjection(events: ReadonlyArray<NamedContract>): ContractProjection {
  return {
    version: 1,
    events: events.map(({ module, contract }) => ({
      module,
      topic: contract.name,
      partitions: contract.partitions,
      schema: contract.schema,
    })),
  };
}

export type ContractMode = "--json" | "--check" | "--apply";

export interface ContractCliOptions {
  /** Defaults to `process.env.SCHEMA_REGISTRY_URL`; required by `--check`/`--apply`. */
  readonly registryUrl?: string;
  readonly stdout?: (line: string) => void;
  readonly stderr?: (line: string) => void;
}

/**
 * The projection program's `main`, byte-compatible with the generated OCaml
 * `contract/contract.exe`:
 *
 *   - `--json`  print `contractProjection` and exit 0 (offline).
 *   - `--check` read-only: report each event's compatibility; exit 0 even when
 *     an event is incompatible (matching the OCaml program, whose `check`
 *     prints the error but does not set a failing exit status).
 *   - `--apply` register each subject (`FULL` then the declared schema); print
 *     each result and exit 1 if any registration failed.
 *   - no mode prints usage and exits 2.
 *
 * `--check`/`--apply` require `SCHEMA_REGISTRY_URL`; a missing value is an
 * error, not a silent skip.
 */
export async function runContractCli(
  events: ReadonlyArray<NamedContract>,
  argv: ReadonlyArray<string> = process.argv.slice(2),
  opts: ContractCliOptions = {},
): Promise<number> {
  const out = opts.stdout ?? ((line: string) => console.log(line));
  const err = opts.stderr ?? ((line: string) => console.error(line));
  const mode = argv.find((arg): arg is ContractMode =>
    arg === "--json" || arg === "--check" || arg === "--apply",
  );

  if (mode === undefined) {
    err("usage: contract [--json | --check | --apply]");
    return 2;
  }

  if (mode === "--json") {
    out(JSON.stringify(contractProjection(events)));
    return 0;
  }

  const registryUrl = opts.registryUrl ?? process.env.SCHEMA_REGISTRY_URL;
  if (registryUrl === undefined || registryUrl === "") {
    err("SCHEMA_REGISTRY_URL is not set");
    return 1;
  }

  if (mode === "--check") {
    for (const { module, contract } of events) {
      try {
        await checkContract({ registryUrl, contract });
        out(`contract ${module}: compatible`);
      } catch (error) {
        out(`contract ${module}: ${messageOf(error)}`);
      }
    }
    return 0;
  }

  let failed = false;
  for (const { module, contract } of events) {
    try {
      const { schemaId } = await registerContract({ registryUrl, contract });
      out(`contract ${module}: registered (schema id ${schemaId})`);
    } catch (error) {
      err(`contract ${module}: ${messageOf(error)}`);
      failed = true;
    }
  }
  return failed ? 1 : 0;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
