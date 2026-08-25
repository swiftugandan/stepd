import { sha256 } from '@noble/hashes/sha256';
import { bytesToHex, utf8ToBytes } from '@noble/hashes/utils';

/**
 * Unit separator. Cannot appear in a function or step id, which is what stops
 * `("a", "bc")` and `("ab", "c")` hashing the same.
 */
const SEP = 0x1f;

/**
 * The step identity hash (§6).
 *
 * `sha256(function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ decimal(occurrence))[0..8]`, hex.
 * Sixteen lowercase hex characters.
 *
 * Identity is `(function_id, step_id, occurrence)` and deliberately not
 * positional: inserting, removing or reordering steps around an existing one
 * leaves its hash untouched, which is what makes it safe to change workflow code
 * while runs are in flight. `function_id` is in there so a child run of a
 * different function cannot collide with its parent.
 *
 * **This function is synchronous, and must stay that way.** `ctx.step()` claims
 * an occurrence at the moment it is called rather than when its promise is
 * awaited, and it cannot do that if computing the hash requires an `await`. That
 * is also why this package uses `@noble/hashes` rather than WebCrypto, whose
 * `subtle.digest` is async.
 */
export function stepHash(functionId: string, stepId: string, occurrence: number): string {
  if (!Number.isInteger(occurrence) || occurrence < 0) {
    throw new RangeError(
      `occurrence must be a non-negative integer, got ${occurrence}. It is a counter, ` +
        `and a fractional or negative one means the caller is not tracking occurrences.`,
    );
  }
  const fid = utf8ToBytes(functionId);
  const sid = utf8ToBytes(stepId);
  // Decimal *text*, not a fixed-width or binary field: 100 must not hash as 1
  // followed by 00.
  const occ = utf8ToBytes(String(occurrence));

  const buf = new Uint8Array(fid.length + 1 + sid.length + 1 + occ.length);
  let i = 0;
  buf.set(fid, i);
  i += fid.length;
  buf[i++] = SEP;
  buf.set(sid, i);
  i += sid.length;
  buf[i++] = SEP;
  buf.set(occ, i);

  return bytesToHex(sha256(buf).subarray(0, 8));
}

/**
 * Per-`step_id` occurrence counters for one replay pass.
 *
 * Reset at the start of every attempt, which is what makes a hash a function of
 * the handler's shape rather than of its history.
 */
export class OccurrenceCounter {
  readonly #counts = new Map<string, number>();

  /** Claim the next occurrence for `stepId` and return its hash. */
  claim(functionId: string, stepId: string): string {
    const n = this.#counts.get(stepId) ?? 0;
    this.#counts.set(stepId, n + 1);
    return stepHash(functionId, stepId, n);
  }

  /** How many occurrences of `stepId` have been claimed this pass. */
  claimed(stepId: string): number {
    return this.#counts.get(stepId) ?? 0;
  }
}
