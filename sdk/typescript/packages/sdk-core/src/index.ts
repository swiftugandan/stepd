/**
 * `@stepd/sdk-core` — the replay machinery.
 *
 * Only the mechanisms that carry the risk: eager occurrence claiming, memo
 * lookup, and the short-circuit control flow that ends a pass. No I/O, no HTTP,
 * no framework — so the logic whose defects corrupt *silently* can be tested
 * exhaustively, without scheduling noise or a server.
 *
 * The one rule to keep: `ctx.step(id, fn)` claims when it is **called**, not
 * when it is awaited. Everything else here follows from that.
 */
export { Ctx, type CtxOptions, type InvokeOptions, type LogLine, type WaitOptions } from './ctx.js';
export {
  Halt,
  STEPD_ERROR,
  StepFailure,
  asFailure,
  coded,
  fatal,
  fatalCoded,
  isHalt,
  isStepFailure,
  retryable,
} from './errors.js';
export { groupOutcome, rejectDuplicateIds } from './parallel.js';
export { runPass, type Handler, type PassOutcome, type PassResult } from './pass.js';
export { StepFuture } from './step.js';
export { toJson } from './json.js';
