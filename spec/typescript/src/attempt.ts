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
 * Read an attempt request, leniently where the specification and the wire
 * disagree.
 *
 * `spec/schemas/attempt-request.schema.json` types `fence` as a string; the
 * server sends an integer. Both are accepted here and normalised to a number,
 * because an SDK that took either side literally would fail against the other,
 * and which of them is the defect is not this decoder's business to decide.
 *
 * Unknown fields are ignored (§11). Missing *required* fields are not: an
 * attempt with no `run.id` cannot be replayed against anything, and guessing
 * would be worse than the 400 this produces.
 */
export function decodeAttempt(raw: unknown): DecodeResult {
  if (!isObject(raw)) return { ok: false, error: { kind: 'not_an_object' } };

  if (raw.protocol !== PROTOCOL_VERSION) {
    return { ok: false, error: { kind: 'bad_version', protocol: raw.protocol } };
  }

  let fence: number;
  if (typeof raw.fence === 'number') {
    fence = raw.fence;
  } else if (typeof raw.fence === 'string' && /^-?\d+$/.test(raw.fence)) {
    fence = Number(raw.fence);
  } else {
    return {
      ok: false,
      error: {
        kind: 'bad_field',
        field: 'fence',
        why: 'expected an integer, or a string holding one',
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
      // Required by the crate and optional in the schema. Defaulted to the run
      // id, which is what it is for a run that never continued as new.
      lineage_id: typeof run.lineage_id === 'string' ? run.lineage_id : (run.id as string),
      chain_position: typeof run.chain_position === 'number' ? run.chain_position : 0,
      cancelling: run.cancelling === true,
    },
    events: Array.isArray(raw.events) ? (raw.events as Attempt['events']) : [],
    // Only completed or terminally-failed steps ever arrive here; a pending row
    // would make the handler treat an unresolved sleep as already done.
    steps: isObject(raw.steps) ? (raw.steps as Attempt['steps']) : {},
    state_truncated: raw.state_truncated === true,
  };
  return { ok: true, attempt };
}
