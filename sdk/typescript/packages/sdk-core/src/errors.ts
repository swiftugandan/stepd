/**
 * How a pass stops.
 *
 * Rust's SDK propagates these with `?` on an `Err` variant. TypeScript has no
 * analogue, so they are thrown — and `throw` across an `await` is safe in a way
 * Rust unwinding across an async boundary is not, which is why the Rust design
 * doc rejects `panic!` and this file does not.
 *
 * The cost is that `catch` is far more idiomatic in JavaScript than
 * `unwrap_or_default()` is in Rust. A handler that swallows one of these looks
 * like it returned cleanly, and the run would be committed as complete with work
 * left undone. That is what {@link Ctx.halted} and the `swallowed_halt` check in
 * `runPass` exist for, and it is why they matter more here than there.
 */

/** Marks every error this module throws, so a `catch` can tell them apart. */
export const STEPD_ERROR = Symbol.for('stepd.error');

/**
 * The pass must stop. Not an application error.
 *
 * `yield` — work was recorded and the server should commit it. The next attempt
 * replays past this point from the memo.
 *
 * `fatal` — the handler broke a protocol rule. Non-retryable: retrying would
 * break it again.
 */
export class Halt extends Error {
  readonly [STEPD_ERROR] = true;
  override readonly name = 'Halt';

  constructor(
    readonly reason: 'yield' | 'fatal',
    message: string,
  ) {
    super(message);
  }

  static yield_(message = 'the pass recorded work and stopped'): Halt {
    return new Halt('yield', message);
  }

  static fatal(message: string): Halt {
    return new Halt('fatal', message);
  }
}

/** A step's body, or the handler, raised. */
export class StepFailure extends Error {
  readonly [STEPD_ERROR] = true;
  override readonly name = 'StepFailure';
  readonly retryable: boolean;
  readonly code: string | undefined;

  constructor(message: string, options: { retryable: boolean; code?: string }) {
    super(message);
    this.retryable = options.retryable;
    this.code = options.code;
  }
}

/** A failure a retry could clear: a timeout, a 503, a lock held elsewhere. */
export function retryable(message: string): StepFailure {
  return new StepFailure(message, { retryable: true });
}

/** A failure that will recur: a declined card, a malformed input, a 404. */
export function fatal(message: string): StepFailure {
  return new StepFailure(message, { retryable: false });
}

/**
 * A **retryable** failure carrying a stable code.
 *
 * Retryable is the right default here and the pairing is deliberate: a code
 * exists so that repeated identical failures can be recognised as one poison
 * pill, and a failure that is never retried cannot repeat. If you want the code
 * *and* no retry, that is {@link fatalCoded} — which exists because callers
 * reached for this one because they wanted the code, and silently got a retry
 * loop against a condition that would never change.
 */
export function coded(code: string, message: string): StepFailure {
  return new StepFailure(message, { retryable: true, code });
}

/** A failure that will recur, carrying a stable code. */
export function fatalCoded(code: string, message: string): StepFailure {
  return new StepFailure(message, { retryable: false, code });
}

export function isHalt(e: unknown): e is Halt {
  return e instanceof Halt;
}

export function isStepFailure(e: unknown): e is StepFailure {
  return e instanceof StepFailure;
}

/**
 * Read an arbitrary thrown value as a failure.
 *
 * A handler can throw anything — a `TypeError` from a typo, a string, an
 * `AbortError`. None of them is a {@link StepFailure}, and the protocol's
 * default for an `error` op is `retryable: true` (§5.1), so that is what these
 * become. A bug that fails identically every time then burns the retry budget
 * and is caught by the engine's poison-pill grouping, which is the machinery
 * built for exactly that; guessing "terminal" instead would turn one transient
 * exception into a permanently failed run.
 *
 * An author who knows better says so with {@link fatal}.
 */
export function asFailure(e: unknown): StepFailure {
  if (isStepFailure(e)) return e;
  if (e instanceof Error) {
    const f = new StepFailure(e.message || String(e), { retryable: true });
    f.stack = e.stack;
    return f;
  }
  return new StepFailure(typeof e === 'string' ? e : JSON.stringify(e), { retryable: true });
}
