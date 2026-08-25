import { Halt, isHalt, isStepFailure, type StepFailure } from './errors.js';
import { StepFuture } from './step.js';

/**
 * Decide a group's outcome once every member has settled.
 *
 * Mirrors the `all_settled` policy the server applies to the resulting batch
 * (§5.2.1): every member reaches a terminal state before the group resolves, and
 * a member's failure is surfaced rather than cancelling its siblings.
 *
 * This is why `ctx.parallel` is not `Promise.all`. `Promise.all` rejects on the
 * first failure while the others are still running, so a sibling that had
 * already executed would never have its op recorded — the work happened and
 * nothing remembers it, which is the one thing a durable engine must never do.
 */
export function groupOutcome<T>(settled: Array<PromiseSettledResult<T>>): T[] {
  const values: T[] = [];
  let failure: StepFailure | undefined;
  let yielded = false;

  for (const r of settled) {
    if (r.status === 'fulfilled') {
      values.push(r.value);
      continue;
    }
    const reason: unknown = r.reason;
    if (isHalt(reason)) {
      // A protocol violation short-circuits: nothing about the other members
      // makes a guessed hash safe.
      if (reason.reason === 'fatal') throw reason;
      yielded = true;
    } else if (isStepFailure(reason)) {
      failure ??= reason;
    } else {
      throw reason;
    }
  }

  // A genuine failure outranks a yield: the handler should see the error rather
  // than be replayed into the same failing step forever.
  if (failure !== undefined) throw failure;
  if (yielded) throw Halt.yield_('a parallel group recorded work and stopped');
  return values;
}

/**
 * Reject a repeated step id before any member's body has run.
 *
 * Two members with the same id would get occurrences 0 and 1 in declaration
 * order, which is stable — and almost never what the developer meant. A loop
 * that fans out needs an explicit discriminator, and quietly numbering them
 * makes a real bug look like it works until the loop's length changes.
 */
export function rejectDuplicateIds(members: ReadonlyArray<StepFuture<unknown>>): void {
  const seen = new Set<string>();
  for (const m of members) {
    if (seen.has(m.stepId)) {
      throw Halt.fatal(
        `ambiguous_step_id: '${m.stepId}' appears more than once in one parallel group. ` +
          `Add a discriminator to the id, e.g. \`charge-\${invoiceId}\` (protocol §6.1 rule 3).`,
      );
    }
    seen.add(m.stepId);
  }
}
