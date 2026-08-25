import { describe, expect, it } from 'vitest';
import { stepHash } from '@stepd/protocol';
import { fatal, retryable, runPass, type Ctx } from '../src/index.js';
import { Driver, Effects } from './driver.js';

describe('ctx.parallel', () => {
  it('puts every member in one envelope, not one round trip each', async () => {
    const d = new Driver();
    const e = new Effects();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.parallel([
        ctx.step('a', () => {
          e.record('a');
          return 1;
        }),
        ctx.step('b', () => {
          e.record('b');
          return 2;
        }),
        ctx.step('c', () => {
          e.record('c');
          return 3;
        }),
      ]);
      return null;
    });
    expect(outcome.kind).toBe('yield');
    if (outcome.kind === 'yield') expect(outcome.ops).toHaveLength(3);
    expect(e.log.sort()).toEqual(['a', 'b', 'c']);
  });

  it('resolves to the members\' values once they are memoised', async () => {
    const d = new Driver();
    const outcome = await d.runToCompletion(async (ctx: Ctx) => {
      const [a, b] = await ctx.parallel([
        ctx.step('a', () => 1),
        ctx.step('b', () => 'two'),
      ]);
      return { a, b };
    });
    expect(outcome).toEqual({ kind: 'done', data: { a: 1, b: 'two' } });
  });

  it('records a sibling that succeeded even though another failed', async () => {
    // The one thing a durable engine must never do is lose work that happened.
    // `Promise.all` rejects on the first failure while the others are still
    // running, so a sibling that had already executed would go unrecorded.
    const d = new Driver();
    const e = new Effects();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.parallel([
        ctx.step('ok', () => {
          e.record('ok');
          return 1;
        }),
        ctx.step('bad', () => {
          e.record('bad');
          throw fatal('declined');
        }),
      ]);
      return null;
    });

    expect(e.log.sort()).toEqual(['bad', 'ok']);
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      // Both ops travel: the one that succeeded and the one that failed
      // terminally. Dropping either loses information the journal needs.
      expect(outcome.ops).toHaveLength(2);
      expect(outcome.retryable).toBe(false);
    }
  });

  it('lets a genuine failure outrank a yield', async () => {
    // Otherwise the handler is replayed into the same failing step forever,
    // never seeing the error.
    const d = new Driver();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.parallel([
        ctx.step('fine', () => 1),
        ctx.step('bad', () => {
          throw fatal('declined');
        }),
      ]);
      return null;
    });
    expect(outcome.kind).toBe('error');
  });

  it('emits recorded work alone when a member fails retryably', async () => {
    const d = new Driver();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.parallel([
        ctx.step('fine', () => 1),
        ctx.step('flaky', () => {
          throw retryable('gateway down');
        }),
      ]);
      return null;
    });
    // §5.2.2: the retryable failure cannot travel with the recorded op, so the
    // op goes now and the failure is raised again next attempt.
    expect(outcome.kind).toBe('yield');
    if (outcome.kind === 'yield') expect(outcome.ops).toHaveLength(1);
  });

  it('rejects a repeated id before any member body runs', async () => {
    // Two members with the same id would get occurrences 0 and 1 in declaration
    // order — stable, and almost never what the developer meant.
    const d = new Driver();
    const e = new Effects();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.parallel([
        ctx.step('charge', () => {
          e.record('first');
          return 1;
        }),
        ctx.step('charge', () => {
          e.record('second');
          return 2;
        }),
      ]);
      return null;
    });

    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.error.code).toBe('protocol_violation');
      expect(outcome.error.message).toContain('ambiguous_step_id');
    }
    // Nothing ran: that is what "before any member body runs" means, and it is
    // only possible because a StepFuture is lazy.
    expect(e.log).toEqual([]);
  });

  it('accepts a fan-out that discriminates its ids', async () => {
    const d = new Driver();
    const outcome = await d.runToCompletion(async (ctx: Ctx) => {
      const invoices = ['a', 'b', 'c'];
      const results = await ctx.parallel(
        invoices.map((i) => ctx.step(`charge-${i}`, () => `charged ${i}`)),
      );
      return results;
    });
    expect(outcome).toEqual({
      kind: 'done',
      data: ['charged a', 'charged b', 'charged c'],
    });
  });

  it('is safe under Promise.all too, because claiming already happened', async () => {
    // Not the recommended call — it loses the duplicate check and the single
    // envelope — but it must not *corrupt* anything, or users would be one
    // forgotten import away from silent damage. Both hashes were fixed at
    // declaration, so awaiting in the other order changes nothing.
    const d = new Driver();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      const a = ctx.step('a', () => 1);
      const b = ctx.step('b', () => 2);
      await Promise.all([b, a]);
      return null;
    });
    expect(outcome.kind).toBe('yield');
    if (outcome.kind === 'yield') {
      expect(outcome.ops.map((o) => ('id' in o ? o.id : '')).sort()).toEqual(['a', 'b']);
      const byId = Object.fromEntries(
        outcome.ops.flatMap((o) => ('id' in o && 'hash' in o ? [[o.id, o.hash]] : [])),
      );
      // Declaration order, not await order: `a` still holds occurrence 0 of `a`.
      expect(byId).toEqual({
        a: stepHash('test-fn', 'a', 0),
        b: stepHash('test-fn', 'b', 0),
      });
    }
  });

  it('catches Promise.allSettled over steps as a swallowed halt', async () => {
    // Found by writing the test above and getting it wrong. `allSettled` does
    // not reject, so the Halt each step raises to stop the pass is absorbed and
    // the handler runs on to its return — a run committed as complete with the
    // rest of the workflow never executed.
    //
    // This is the JavaScript-specific shape of the hazard, and it is why the
    // swallowed-halt check matters more here than in Rust: `allSettled` is a
    // reasonable thing to reach for, not a mistake anyone would flag in review.
    const d = new Driver();
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      const a = ctx.step('a', () => 1);
      const b = ctx.step('b', () => 2);
      await Promise.allSettled([a, b]);
      return 'looks fine';
    });
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.error.code).toBe('swallowed_halt');
      // The work still travels. It happened, and something has to remember it.
      expect(outcome.ops).toHaveLength(2);
    }
  });
});
