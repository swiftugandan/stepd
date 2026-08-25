import { PROTOCOL_VERSION, isExclusive, opHash, type AttemptResponse } from './types.js';

/** Why an envelope is not well-formed (§5.2). */
export type EnvelopeError =
  | { kind: 'empty' }
  | { kind: 'not_alone'; op: 'done' | 'continue_as_new' }
  | { kind: 'must_be_last'; op: 'error' }
  | { kind: 'duplicate_hash'; hash: string }
  | { kind: 'bad_version'; protocol: string }
  | { kind: 'retired_field'; field: string }
  | { kind: 'retryable_error_batched' };

/** A human-readable rendering, for the message an app sends back. */
export function describeEnvelopeError(e: EnvelopeError): string {
  switch (e.kind) {
    case 'empty':
      return 'envelope contains no ops';
    case 'not_alone':
      return `\`${e.op}\` must appear alone in an envelope`;
    case 'must_be_last':
      return `\`${e.op}\` must be the last op in an envelope`;
    case 'duplicate_hash':
      return `duplicate step hash \`${e.hash}\` within one envelope`;
    case 'bad_version':
      return `unsupported protocol version \`${e.protocol}\``;
    case 'retired_field':
      return `\`${e.field}\` was retired from this protocol and is refused rather than ignored; remove it from the envelope`;
    case 'retryable_error_batched':
      return 'a retryable `error` must appear alone; emit the recorded ops on their own and re-raise on the next attempt (§5.2.2)';
  }
}

/**
 * Check an envelope before it leaves the SDK.
 *
 * Returns `null` when the envelope is well formed.
 *
 * An SDK validates its own output so that a malformed envelope names the SDK
 * rather than surfacing as a server-side rejection an app author cannot act on.
 * The server validates too — this is not a substitute for that.
 */
export function validateEnvelope(response: AttemptResponse): EnvelopeError | null {
  if (response.protocol !== PROTOCOL_VERSION) {
    return { kind: 'bad_version', protocol: response.protocol };
  }
  // Refused, not ignored. §11 says to ignore unknown fields, which is right for
  // one a newer minor added. A *retired* field had a meaning, and being ignored
  // is the state it was removed for being in.
  if (response.join !== undefined && response.join !== null) {
    return { kind: 'retired_field', field: 'join' };
  }
  if (response.ops.length === 0) {
    return { kind: 'empty' };
  }

  if (response.ops.length > 1) {
    for (const op of response.ops) {
      if (isExclusive(op)) {
        return { kind: 'not_alone', op: op.op as 'done' | 'continue_as_new' };
      }
    }
    // An `error` anywhere but the end would have the engine fail the run and
    // then keep recording steps into it. Last is the only unambiguous position:
    // everything before it happened, and then the run stopped.
    for (const op of response.ops.slice(0, -1)) {
      if (op.op === 'error') return { kind: 'must_be_last', op: 'error' };
    }
    // A retryable error means "re-execute me", and the retry policy lives in the
    // dispatcher, which does not commit. The two cannot travel together: an SDK
    // holding recorded ops and a retryable failure emits the ops now and
    // re-raises next attempt, where the work is memoised and the error arrives
    // alone. That terminates, because each pass records strictly less.
    const last = response.ops[response.ops.length - 1]!;
    if (last.op === 'error' && (last.retryable ?? true)) {
      return { kind: 'retryable_error_batched' };
    }
  }

  const seen = new Set<string>();
  for (const op of response.ops) {
    const hash = opHash(op);
    if (hash !== undefined) {
      if (seen.has(hash)) return { kind: 'duplicate_hash', hash };
      seen.add(hash);
    }
  }
  return null;
}
