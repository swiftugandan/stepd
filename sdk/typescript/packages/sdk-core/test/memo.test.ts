import { describe, expect, it } from 'vitest';
import { fatal, retryable, type Ctx } from '../src/index.js';
import { Driver, Effects } from './driver.js';

describe('memoisation', () => {
  it('runs each step body exactly once across a whole run', async () => {
    // The headline guarantee, and the one that is invisible in server state: the
    // journal after one execution and after two is byte-identical. Only the
    // app's own record of what it executed can tell them apart.
    const d = new Driver();
    const e = new Effects();
    const handler = async (ctx: Ctx) => {
      const a = await ctx.step('one', () => {
        e.record('one');
        return 1;
      });
      const b = await ctx.step('two', () => {
        e.record('two');
        return a + 1;
      });
      return b;
    };

    const outcome = await d.runToCompletion(handler);
    expect(outcome).toEqual({ kind: 'done', data: 2 });
    expect(e.log).toEqual(['one', 'two']);
  });

  it('records one new step per attempt', async () => {
    const d = new Driver();
    const handler = async (ctx: Ctx) => {
      await ctx.step('a', () => 1);
      await ctx.step('b', () => 2);
      await ctx.step('c', () => 3);
      return 'done';
    };
    await d.runToCompletion(handler);
    // Three steps, then the attempt that returns: four attempts.
    expect(d.attempts.length).toBe(4);
    for (const outcome of d.attempts.slice(0, 3)) {
      expect(outcome.kind).toBe('yield');
      if (outcome.kind === 'yield') expect(outcome.ops.length).toBe(1);
    }
  });

  it('never constructs the body of a memoised step', async () => {
    const d = new Driver();
    const e = new Effects();
    let constructed = 0;
    const handler = async (ctx: Ctx) => {
      await ctx.step('once', () => {
        constructed += 1;
        e.record('once');
        return 'x';
      });
      await ctx.step('twice', () => 'y');
      return null;
    };
    await d.runToCompletion(handler);
    expect(constructed).toBe(1);
  });

  it('replays a value through JSON, so attempt one matches attempt forty', async () => {
    // Deliberately different from the Rust SDK, which returns the original value
    // on first execution and the JSON projection thereafter. That asymmetry
    // means `new Date()` works until the first retry — and a retry is precisely
    // when nobody is watching.
    const d = new Driver();
    let firstSeen: unknown;
    let replaySeen: unknown;
    const handler = async (ctx: Ctx) => {
      const v = await ctx.step('t', () => ({ at: new Date('2026-01-01T00:00:00Z') }));
      if (firstSeen === undefined) firstSeen = v;
      else replaySeen = v;
      await ctx.step('after', () => 1);
      return null;
    };
    await d.runToCompletion(handler);
    expect(firstSeen).toEqual({ at: '2026-01-01T00:00:00.000Z' });
    expect(replaySeen).toEqual(firstSeen);
  });

  it('replays a terminal step failure as the error it failed with', async () => {
    const d = new Driver();
    const e = new Effects();
    const handler = async (ctx: Ctx) => {
      try {
        await ctx.step('charge', () => {
          e.record('charge');
          throw fatal('card declined');
        });
      } catch (err) {
        // Caught deliberately: a terminal step failure is an outcome, and a
        // handler is allowed to branch on it.
        await ctx.step('notify', () => {
          e.record('notify');
          return 'told them';
        });
        return 'compensated';
      }
      return 'charged';
    };

    const outcome = await d.runToCompletion(handler);
    expect(outcome).toEqual({ kind: 'done', data: 'compensated' });
    // The charge body ran once and is memoised as failed; the retry replays the
    // error rather than charging again.
    expect(e.count('charge')).toBe(1);
    expect(e.count('notify')).toBe(1);
  });

  it('does not record a retryable failure, so the retry can actually happen', async () => {
    // A recorded failure is memoised. If a retryable one were recorded, the next
    // attempt would replay the error instead of the body and the retry would
    // never run.
    const d = new Driver();
    let calls = 0;
    const handler = async (ctx: Ctx) => {
      await ctx.step('flaky', () => {
        calls += 1;
        if (calls < 3) throw retryable('gateway down');
        return 'ok';
      });
      return 'done';
    };

    // The driver does not retry by itself — the dispatcher does — so drive it.
    for (let i = 0; i < 5 && d.attempts.at(-1)?.kind !== 'done'; i++) {
      await d.attempt(handler);
    }
    expect(calls).toBe(3);
    expect(Object.keys(d.memo).length).toBe(1);
    expect(d.attempts.at(-1)).toEqual({ kind: 'done', data: 'done' });
  });

  it('reports a recorded hash the pass never asked for as orphaned', async () => {
    const d = new Driver();
    await d.attempt(async (ctx: Ctx) => {
      await ctx.step('old-name', () => 1);
      return null;
    });
    expect(Object.keys(d.memo).length).toBe(1);

    // The step is renamed. Its recorded result is now unreachable, and the side
    // effect will happen again under the new id — no error, so nothing else
    // would say so.
    const ctx = d.newCtx();
    ctx.step('new-name', () => 1);
    expect(ctx.orphaned().length).toBe(1);
  });
});
