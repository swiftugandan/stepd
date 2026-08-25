import { PROTOCOL_VERSION, type Attempt } from './types.js';

/** Why an attempt request could not be read. */
export type AttemptDecodeError =
  | { kind: 'not_an_object' }
  | { kind: 'bad_version'; protocol: unknown }
  | { kind: 'missing'; field: string }
  | { kind: 'bad_field'; field: string; why: string };

export type DecodeResult =
  | { ok: true; attempt: Attempt }
  | { ok: false; error: AttemptDecodeError };

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/**
 * Read an attempt request.
 *
 * `fence` is an integer on the wire. Unknown fields are ignored (§11). Missing
 * required fields are not: an attempt with no `run.id` cannot be replayed
 * against anything, and guessing would be worse than the 400 this produces.
 */
export function decodeAttempt(raw: unknown): DecodeResult {
  if (!isObject(raw)) return { ok: false, error: { kind: 'not_an_object' } };

  if (raw.protocol !== PROTOCOL_VERSION) {
    return { ok: false, error: { kind: 'bad_version', protocol: raw.protocol } };
  }

  let fence: number;
  if (typeof raw.fence === 'number' && Number.isInteger(raw.fence)) {
    fence = raw.fence;
  } else {
    return {
      ok: false,
      error: {
        kind: 'bad_field',
        field: 'fence',
        why: 'expected an integer',
      },
    };
  }

  if (typeof raw.attempt !== 'number') {
    return { ok: false, error: { kind: 'missing', field: 'attempt' } };
  }
  if (!isObject(raw.run)) {
    return { ok: false, error: { kind: 'missing', field: 'run' } };
  }
  for (const field of ['id', 'function_id', 'namespace', 'started_at'] as const) {
    if (typeof raw.run[field] !== 'string') {
      return { ok: false, error: { kind: 'missing', field: `run.${field}` } };
    }
  }

  const run = raw.run;
  if (typeof run.lineage_id !== 'string') {
    return { ok: false, error: { kind: 'missing', field: 'run.lineage_id' } };
  }

  const attempt: Attempt = {
    protocol: PROTOCOL_VERSION,
    attempt: raw.attempt,
    fence,
    deadline: typeof raw.deadline === 'string' ? raw.deadline : null,
    run: {
      id: run.id as string,
      function_id: run.function_id as string,
      namespace: run.namespace as string,
      key: typeof run.key === 'string' ? run.key : null,
      started_at: run.started_at as string,
      input: run.input as Attempt['run']['input'],
      lineage_id: run.lineage_id,
      chain_position: typeof run.chain_position === 'number' ? run.chain_position : 0,
      cancelling: run.cancelling === true,
    },
    // Passed through. The CloudEvent extensions are `stepdkey` /
    // `stepdidempotency` on the wire; renaming them to `key` would make
    // `event.key` look populated in TypeScript while ingest still reads
    // `stepdkey` and drops the business key.
    events: Array.isArray(raw.events) ? (raw.events as Attempt['events']) : [],
    // Only completed or terminally-failed steps ever arrive here; a pending row
    // would make the handler treat an unresolved sleep as already done.
    steps: isObject(raw.steps) ? (raw.steps as Attempt['steps']) : {},
    state_truncated: raw.state_truncated === true,
  };
  return { ok: true, attempt };
}
