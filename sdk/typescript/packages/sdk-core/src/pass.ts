import type { ErrorBody, Event, Json, Op } from '@stepd/protocol';
import type { Ctx, LogLine } from './ctx.js';
import { asFailure, isHalt } from './errors.js';
import { toJson } from './json.js';

/** What one replay pass concluded. */
export type PassOutcome =
  | { kind: 'done'; data: Json }
  | { kind: 'yield'; ops: Op[] }
  | { kind: 'error'; ops: Op[]; retryable: boolean; error: ErrorBody };

export interface PassResult {
  outcome: PassOutcome;
  emit: Event[];
  logs: LogLine[];
  /** Recorded hashes this pass never asked for: a renamed or removed step. */
  orphaned: string[];
}

export type Handler = (ctx: Ctx) => unknown;

/**
 * Run a handler once and say what happened.
 *
 * This is the seam. Every adapter — an HTTP handler, a Lambda entry point, the
 * test harness — goes through it, so none of them can disagree about what a
 * handler's return value means, and the swallowed-halt check exists exactly
 * once.
 *
 * The ops are drained **once, after the pass, in every arm** — including the
 * error arms. An earlier version of the Rust SDK drained only in the yield arm,
 * so a pass that recorded three steps and then failed committed none of them
 * (ADR-023). The work had happened and nothing remembered it.
 */
export async function runPass(ctx: Ctx, handler: Handler): Promise<PassResult> {
  let returned: unknown;
  let thrown: unknown;
  let threw = false;

  try {
    returned = await handler(ctx);
  } catch (e) {
    threw = true;
    thrown = e;
  }

  // Nothing may claim after this: a step created by a timer or a floating
  // promise would be assigned an occurrence that depends on scheduler timing.
  ctx.seal();

  const recorded = ctx.takePending();
  const extras = {
    emit: ctx.takeEmit(),
    logs: ctx.takeLogs(),
    orphaned: ctx.orphaned(),
  };
  const result = (outcome: PassOutcome): PassResult => ({ outcome, ...extras });

  if (!threw) {
    if (ctx.halted) {
      // The handler recorded work and then returned as though it had not. In
      // Rust this is `let _ =` or `.unwrap_or_default()`; here it is almost
      // always `try { await ctx.step(..) } catch {}`, which is ordinary enough
      // JavaScript that it will be written by accident.
      return result({
        kind: 'error',
        ops: recorded,
        retryable: false,
        error: {
          code: 'swallowed_halt',
          message:
            'the handler caught the control-flow signal a step raises and returned normally. ' +
            'A step that has just recorded its result throws to stop the pass so the server ' +
            'can commit it; swallowing that would complete the run with work left undone. ' +
            'Do not wrap ctx.step in a bare `catch`; to handle a step failure, catch and ' +
            're-throw anything that is not a StepFailure.',
        },
      });
    }
    let data: Json;
    try {
      data = toJson(returned, () => 'the handler');
    } catch (e) {
      return result({
        kind: 'error',
        ops: recorded,
        retryable: false,
        error: {
          code: 'output_not_serialisable',
          message: e instanceof Error ? e.message : String(e),
        },
      });
    }
    return result({ kind: 'done', data });
  }

  if (isHalt(thrown)) {
    if (thrown.reason === 'yield') {
      return result({ kind: 'yield', ops: recorded });
    }
    return result({
      kind: 'error',
      ops: recorded,
      retryable: false,
      error: { code: 'protocol_violation', message: thrown.message },
    });
  }

  const failure = asFailure(thrown);

  // A retryable failure cannot travel with recorded ops: retrying is the
  // dispatcher's decision and committing is the store's, and one envelope cannot
  // ask for both (§5.2.2). So the recorded work goes now, alone, and the failure
  // is raised again on the next attempt — where the work is memoised and the
  // error arrives by itself. This terminates: each pass records strictly less
  // than the last, because what it recorded is now in the memo.
  if (failure.retryable && recorded.length > 0) {
    return result({ kind: 'yield', ops: recorded });
  }

  return result({
    kind: 'error',
    ops: recorded,
    retryable: failure.retryable,
    error: {
      code: failure.code,
      message: failure.message,
      ...(failure.stack === undefined ? {} : { stack: failure.stack }),
    },
  });
}
