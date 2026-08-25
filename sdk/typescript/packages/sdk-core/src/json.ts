import type { Json } from '@stepd/protocol';

/**
 * Project a value through JSON, the way the wire will.
 *
 * Called when a step *records* its result and again when the result comes back
 * from the memo, so a handler sees the same shape on attempt one as on attempt
 * forty. The Rust SDK returns the original value on first execution and the JSON
 * projection on replay; this one does not, deliberately.
 *
 * The asymmetry is a trap. `ctx.step('t', async () => new Date())` hands back a
 * `Date` the first time and an ISO string every time after, so a handler that
 * calls `.getTime()` on it works until the first retry — and a retry is exactly
 * the moment nobody is watching. Paying one round trip per step to make that
 * fail immediately is worth it.
 */
export function toJson(value: unknown, describe: () => string): Json {
  if (value === undefined) return null;
  let text: string;
  try {
    text = JSON.stringify(value) as string;
  } catch (e) {
    throw new TypeError(
      `${describe()} produced a result that cannot be serialised: ${
        e instanceof Error ? e.message : String(e)
      }`,
    );
  }
  if (text === undefined) {
    // `JSON.stringify` returns undefined rather than throwing for a function or
    // a symbol at the top level.
    throw new TypeError(
      `${describe()} produced a result that cannot be serialised: ` +
        `${typeof value} has no JSON representation`,
    );
  }
  return JSON.parse(text) as Json;
}
