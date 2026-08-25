import { describe, expect, it } from 'vitest';
import { fatal, retryable, type Ctx } from '../src/index.js';
import { HarnessFailure, harness } from '../src/testing.js';

describe('the early-signal guarantee, in three lines', () => {
  it('resolves a wait from an event delivered before the handler reached it', async () => {
    // §7.6. This is the case that used to lose events entirely, and the reason
    // every run has a durable inbox. Being able to write it this short is the
    // difference between a developer trusting the guarantee and hoping for it.
    const t = harness(async (ctx: Ctx) => {
      await ctx.step('charge', () => 'ch_1');
      await ctx.sleep('cooldown', 86_400_000);
      return (await ctx.waitEvent<boolean>('approval', 'order.approved')) ?? false;
    });

    t.sendEvent('order.approved', true); // BEFORE the wait

    expect(await t.runToCompletion()).toBe(true);
    t.assertStepExecutedOnce('charge');
  });
});

describe('memoisation', () => {
  it('runs each body once across a run that takes many attempts', async () => {
    const t = harness(async (ctx: Ctx) => {
      const a = await ctx.step('a', () => 1);
      const b = await ctx.step('b', () => a + 1);
      const c = await ctx.step('c', () => b + 1);
      return c;
    });
    expect(await t.runToCompletion()).toBe(3);
    for (const id of ['a', 'b', 'c']) t.assertStepExecutedOnce(id);
    expect(t.attempts).toBe(4);
  });

  it('re-runs nothing after a retryable failure part-way through', async () => {
    let attempt = 0;
    const t = harness(async (ctx: Ctx) => {
      attempt = ctx.attempt;
      await ctx.step('work', () => 'done');
      if (ctx.attempt === 1) throw retryable('forced retry');
      await ctx.step('after', () => 'after');
      return 'finished';
    });
    expect(await t.runToCompletion()).toBe('finished');
    t.assertStepExecutedOnce('work');
    t.assertStepExecutedOnce('after');
    expect(attempt).toBeGreaterThan(1);
  });
});

describe('ctx.attempt', () => {
  it('starts at 1, as the protocol counts it', async () => {
    // A handler reading `ctx.attempt` must see the same number here as in
    // production, or the test is testing a different handler. The Rust harness
    // adds one on top of its own counter and reports 2 on the first attempt;
    // this does not reproduce that.
    const seen: number[] = [];
    const t = harness(async (ctx: Ctx) => {
      seen.push(ctx.attempt);
      await ctx.step('a', () => 1);
      return 'done';
    });
    await t.runToCompletion();
    expect(seen[0]).toBe(1);
    expect(seen).toEqual([1, 2]);
  });
});

describe('forcing outcomes', () => {
  it('substitutes a result without changing what "executed" means', async () => {
    // A stubbed step still counts as executed: from the workflow's point of view
    // it was, and quietly changing execution counts would undermine the one
    // assertion this harness exists to support.
    const t = harness(async (ctx: Ctx) => await ctx.step('charge', () => 'real'));
    t.expectStep('charge', 'stubbed');
    expect(await t.runToCompletion()).toBe('stubbed');
    t.assertStepExecutedOnce('charge');
  });

  it('forces a step to fail so the compensation path can be tested', async () => {
    const t = harness(async (ctx: Ctx) => {
      try {
        await ctx.step('charge', () => 'ch_1');
      } catch {
        await ctx.step('refund', () => 'refunded');
        return 'compensated';
      }
      return 'charged';
    });
    t.failStep('charge', 'card declined');
    expect(await t.runToCompletion()).toBe('compensated');
    t.assertStepExecutedOnce('refund');
  });

  it('forces a wait to time out, resolving it to null', async () => {
    const t = harness(async (ctx: Ctx) => {
      const approval = await ctx.waitEvent('approval', 'order.approved', { timeoutMs: 1000 });
      return approval === null ? 'nobody approved' : 'approved';
    });
    t.timeOutWait('approval');
    expect(await t.runToCompletion()).toBe('nobody approved');
  });
});

describe('when a workflow cannot finish', () => {
  it('names the event it is waiting for', async () => {
    // "did not complete in 1000 attempts" is a true statement that helps nobody.
    const t = harness(async (ctx: Ctx) => {
      await ctx.waitEvent('approval', 'order.approved');
      return 'done';
    });
    await expect(t.runToCompletion()).rejects.toThrow(/waiting for 'order.approved'/);
    await expect(t.runToCompletion()).rejects.toThrow(/sendEvent|timeOutWait/);
  });

  it('reports a failure with its code', async () => {
    const t = harness(async () => {
      throw fatal('the order id was not a number');
    });
    await expect(t.runToCompletion()).rejects.toThrow(HarnessFailure);
    await expect(t.runToCompletion()).rejects.toThrow(/the order id was not a number/);
  });

  it('catches a swallowed halt, exactly as production does', async () => {
    const t = harness(async (ctx: Ctx) => {
      try {
        await ctx.step('a', () => 1);
      } catch {
        // swallowed
      }
      return 'looks fine';
    });
    await expect(t.runToCompletion()).rejects.toThrow(/swallowed_halt|control-flow signal/);
  });
});

describe('fidelity with production', () => {
  it('ships only settled steps, so a pending wait does not read as done', async () => {
    // A harness that shipped pending rows would be more permissive than the
    // engine, and a workflow could pass its tests and hang once deployed.
    const t = harness(async (ctx: Ctx) => {
      await ctx.step('before', () => 1);
      await ctx.waitEvent('parked', 'never.arrives');
      await ctx.step('after', () => 2);
      return 'done';
    });
    await expect(t.runToCompletion()).rejects.toThrow(/waiting for 'never.arrives'/);
    t.assertStepExecutedOnce('before');
    t.assertStepNotExecuted('after');
  });

  it('lets a test interleave events between attempts', async () => {
    // How the interesting bugs are actually reproduced: deliver the signal after
    // the second step, not before the run.
    const t = harness(async (ctx: Ctx) => {
      await ctx.step('one', () => 1);
      await ctx.step('two', () => 2);
      const got = await ctx.waitEvent<string>('signal', 'late.arrival');
      return got ?? 'nothing';
    });

    expect((await t.stepOnce()).done).toBe(false);
    expect((await t.stepOnce()).done).toBe(false);
    t.sendEvent('late.arrival', 'here');
    expect(await t.runToCompletion()).toBe('here');
  });

  it('exposes the ops each attempt yielded', async () => {
    const t = harness(async (ctx: Ctx) => {
      await ctx.step('a', () => 1);
      await ctx.sleep('nap', 1000);
      return 'done';
    });
    await t.runToCompletion();
    t.assertOps(1, ['step']);
    t.assertOps(2, ['sleep']);
    t.assertOps(3, ['done']);
    expect(t.recordedSteps()).toEqual(['a', 'nap']);
  });

  it('runs the cancellation path when asked', async () => {
    const t = harness(async (ctx: Ctx) => {
      if (ctx.run.cancelling) {
        await ctx.step('compensate', () => 'undone');
        return 'compensated';
      }
      await ctx.step('work', () => 'worked');
      return 'finished';
    }).cancelling();
    expect(await t.runToCompletion()).toBe('compensated');
    t.assertStepNotExecuted('work');
  });

  it('gives the handler the input and key it was configured with', async () => {
    const t = harness(async (ctx: Ctx) => ({
      key: ctx.run.key ?? null,
      input: ctx.run.input ?? null,
    }))
      .key('order:4711')
      .givenInput({ n: 7 });
    expect(await t.runToCompletion()).toEqual({ key: 'order:4711', input: { n: 7 } });
  });

  it('moves the virtual clock without costing wall-clock time', async () => {
    const seen: string[] = [];
    const t = harness(async (ctx: Ctx) => {
      await ctx.step('when', () => {
        seen.push(new Date().toISOString());
        return 1;
      });
      return 'done';
    });
    t.advanceClock(86_400_000 * 30);
    await t.runToCompletion();
    // The step body reads the real clock; what moved is the one `ctx.sleep`
    // computes from, which is why a month-long sleep costs nothing here.
    expect(seen).toHaveLength(1);
  });
});
