/**
 * The wire types of `spec/PROTOCOL.md`.
 *
 * These mirror `stepd-proto`, not `spec/schemas/`. The two disagree in about a
 * dozen places — `fence` is a string in the schema and an integer on the wire,
 * `sleep` accepts `duration` in the schema and only `until` on the wire,
 * `wait_event` carries `expr`/`timeout` in the schema and `timeout_at` and no
 * `expr` on the wire — and the server parses the crate. An SDK written from the
 * schemas alone does not interoperate today. See `spec/rust/README.md`.
 */

/** The protocol major version, as it appears on the wire: a string, not a number. */
export const PROTOCOL_VERSION = '1' as const;

/** Headers the protocol assigns meaning to (§2.1). */
export const HEADER_PROTOCOL = 'stepd-protocol';
export const HEADER_SIGNATURE = 'stepd-signature';
export const HEADER_NONCE = 'stepd-nonce';
export const HEADER_RUN_ID = 'stepd-run-id';
export const HEADER_ATTEMPT = 'stepd-attempt';
export const HEADER_FENCE = 'stepd-fence';
export const HEADER_SDK = 'stepd-sdk';

/** Any JSON value. */
export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/**
 * A managed blob reference (§8.3.1).
 *
 * `url` is minted by the server per attempt and MUST NOT be persisted by an SDK:
 * it lives about five minutes, and journalling one produces a failure hours
 * later that reads like the blob having disappeared. An SDK constructing a
 * reference leaves it absent.
 */
export interface BlobRef {
  $blob: {
    id: string;
    size: number;
    sha256: string;
    content_type?: string;
    filename?: string;
    url?: string;
  };
}

/** An external reference (§8.4). The server never fetches or validates one. */
export interface ExternalRef {
  $ref: {
    uri: string;
    size?: number;
    sha256?: string;
    content_type?: string;
    filename?: string;
    meta?: Record<string, Json>;
  };
}

/** An error body (`common.schema.json#/$defs/error`). */
export interface ErrorBody {
  code?: string;
  message: string;
  stack?: string;
  retry_after?: string;
  attempts?: number;
  data?: Json;
}

/** A CloudEvent in structured mode, with the `stepd*` extensions (§3). */
export interface Event {
  specversion: string;
  id?: string | null;
  source: string;
  type: string;
  time?: string | null;
  data: Json;
  key?: string | null;
  idempotency?: string | null;
}

/** Which op recorded a step. */
export type StepOp = 'step' | 'sleep' | 'wait_event' | 'invoke' | 'signal';

/**
 * How a recorded step came out.
 *
 * `unknown` means an attempt was abandoned mid-step: the step will re-execute,
 * which is why at-least-once execution is the guarantee and exactly-once
 * recording is the one that holds.
 */
export type StepStatus =
  | 'pending'
  | 'completed'
  | 'failed'
  | 'timed_out'
  | 'cancelled'
  | 'unknown';

/** A step the server has already recorded. */
export interface RecordedStep {
  id: string;
  op: StepOp;
  status: StepStatus;
  data?: Json;
  error?: ErrorBody;
}

/** Run identity, carried on every attempt. */
export interface RunContext {
  id: string;
  function_id: string;
  namespace: string;
  key?: string | null;
  started_at: string;
  input?: Json;
  lineage_id: string;
  chain_position: number;
  cancelling: boolean;
}

/**
 * Server → app: everything the handler needs to replay and advance one step.
 *
 * `steps` holds only completed or terminally-failed steps, keyed by hash.
 * Pending rows exist in the store and are never sent — an app that received one
 * would treat an unresolved sleep as already done.
 */
export interface Attempt {
  protocol: string;
  attempt: number;
  fence: number;
  deadline?: string | null;
  run: RunContext;
  events: Event[];
  steps: Record<string, RecordedStep>;
  state_truncated: boolean;
}

/** App → server: one control instruction (§5.1). Discriminated on `op`. */
export type Op =
  | {
      op: 'step';
      id: string;
      hash: string;
      /** Absent when `error` is present. A step has one outcome, never both. */
      data?: Json;
      meta?: Json;
      /**
       * A terminal failure of this step's body. Present means recorded `failed`.
       *
       * A *retryable* failure must never be recorded this way: the memo would
       * hand the same error back forever and the retry would never run (§5.2.2).
       */
      error?: ErrorBody;
    }
  | { op: 'sleep'; id: string; hash: string; until: string }
  | {
      op: 'wait_event';
      id: string;
      hash: string;
      event: string;
      /** `run_start` by default, which is what closes the lost-signal race (§7.6). */
      since?: string;
      timeout_at?: string | null;
      prompt?: Json;
    }
  | {
      op: 'invoke';
      id: string;
      hash: string;
      function: string;
      input?: Json;
      detach?: boolean;
    }
  | { op: 'signal'; id: string; hash: string; target_run: string; event: Event }
  | { op: 'continue_as_new'; id: string; hash: string; input?: Json }
  | { op: 'done'; data?: Json }
  | { op: 'error'; retryable?: boolean; step?: string; error: ErrorBody };

/** App → server: the op envelope (§5). */
export interface AttemptResponse {
  protocol: string;
  ops: Op[];
  emit?: Event[];
  orphaned_steps?: number;
  /**
   * Retired in rev 1.2 (ADR-023), and captured only so it can be refused.
   *
   * §11 tells a receiver to ignore unknown fields, which is right for a field
   * added by a newer minor. A *retired* field is different: it had a meaning, a
   * sender may still expect that meaning, and ignoring it would let an app
   * believe it had requested a join policy that no longer exists.
   */
  join?: unknown;
}

/** The ops that conclude a run, and so may not be batched with anything. */
export function isExclusive(op: Op): boolean {
  return op.op === 'done' || op.op === 'continue_as_new';
}

/** The step hash an op records, if it records one. */
export function opHash(op: Op): string | undefined {
  return 'hash' in op ? op.hash : undefined;
}
