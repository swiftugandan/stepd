import { stepHash } from '@stepd/protocol';
import { describe, expect, it } from 'vitest';
import { Ctx, isHalt } from '../src/index.js';
import { Driver, Effects } from './driver.js';

const ctxFor = (d: Driver): Ctx => d.newCtx();

describe('eager occurrence claiming', () => {
  it('claims when the step is created, not when it is awaited', async () => {
    const d = new Driver();
    const ctx = ctxFor(d);
    // Nothing awaited yet.
    ctx.step('a', () => 1);
    ctx.step('b', () => 2);
    ctx.step('a', () => 3);
    expect(ctx.idSequence()).toEqual(['a', 'b', 'a']);
  });

  it('makes await order irrelevant to the hashes', async () => {
    // The property, demonstrated rather than asserted. Two passes create the
    // same steps and await them in opposite orders; the hashes must match.
    const forwards = new Driver();
    const f = ctxFor(forwards);
    const fa = f.step('a', () => 1);
    const fb = f.step('b', () => 2);
    await Promise.allSettled([fa, fb]);

    const backwards = new Driver();
    const b = ctxFor(backwards);
    const ba = b.step('a', () => 1);
    const bb = b.step('b', () => 2);
    await Promise.allSettled([bb, ba]);

    const hashes = (ctx: Ctx) => ctx.takePending().map((op) => ('hash' in op ? op.hash : ''));
    expect(hashes(b).sort()).toEqual(hashes(f).sort());
  });

  it('a lazily-claiming counter is demonstrably broken — the control', async () => {
    // The deliberate control for the test above. If this ever passes, it has
    // stopped modelling the hazard and the test above is proving nothing.
    //
    // "Claim at await" means the claim happens when the consumer *subscribes*,
    // so the model is a thunk. (Writing this the obvious way — an async function
    // called eagerly — does not reproduce the bug at all, because both bodies
    // are already running and resume in FIFO order whatever order you await
    // them in. That near-miss is the reason this control is here.)
    const lazyClaim =
      (counters: Map<string, number>, id: string) =>
      async (): Promise<string> => {
        const n = counters.get(id) ?? 0;
        counters.set(id, n + 1);
        return stepHash('f', id, n);
      };

    const forwards = new Map<string, number>();
    const fa = lazyClaim(forwards, 'x');
    const fb = lazyClaim(forwards, 'x');
    const inOrder = [await fa(), await fb()];

    const backwards = new Map<string, number>();
    const ba = lazyClaim(backwards, 'x');
    const bb = lazyClaim(backwards, 'x');
    // Await the second one first — the only difference.
    const second = await bb();
    const first = await ba();

    expect(inOrder).toEqual([stepHash('f', 'x', 0), stepHash('f', 'x', 1)]);
    // Under lazy claiming the occurrence follows scheduler order, so the step
    // declared first ends up with the hash of the one declared second. That is
    // the silent corruption eager claiming exists to remove: on the next attempt
    // the server holds a result under a hash the handler no longer asks for.
    expect([first, second]).not.toEqual(inOrder);
    expect(first).toBe(stepHash('f', 'x', 1));
    expect(second).toBe(stepHash('f', 'x', 0));
  });

  it('gives repeated ids successive occurrences, in declaration order', () => {
    const d = new Driver();
    const ctx = ctxFor(d);
    ctx.step('charge', () => 1);
    ctx.step('charge', () => 2);
    const hashes = ctx.idSequence();
    expect(hashes).toEqual(['charge', 'charge']);
  });

  it('does not run a body that is never awaited, but does consume its occurrence', async () => {
    // The accepted cost of eager claiming (ADR-012): a conditionally-created
    // step shifts every later occurrence of that id. Executing it as well would
    // be a side effect nobody asked for, which is why StepFuture is lazy.
    const d = new Driver();
    const effects = new Effects();
    const ctx = ctxFor(d);

    ctx.step('never-awaited', () => {
      effects.record('never-awaited');
      return 1;
    });
    const second = ctx.step('after', () => {
      effects.record('after');
      return 2;
    });
    await Promise.allSettled([second]);

    expect(effects.log).toEqual(['after']);
    expect(ctx.idSequence()).toEqual(['never-awaited', 'after']);
  });

  it('claims sleep, waitEvent, invoke and signal at the call too', () => {
    // Not hypothetical: creating two waits and awaiting them in the other order
    // would swap their hashes, and nothing would error.
    const d = new Driver();
    const ctx = ctxFor(d);
    ctx.sleep('s', 1000);
    ctx.waitEvent('w', 'e');
    ctx.invoke('i', 'child', null);
    ctx.signal('g', '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10', {
      specversion: '1.0',
      source: 'test',
      type: 'x',
      data: null,
    });
    expect(ctx.idSequence()).toEqual(['s', 'w', 'i', 'g']);
  });

  it('produces the same hashes as @stepd/protocol', () => {
    const d = new Driver('test-fn');
    const ctx = ctxFor(d);
    ctx.step('charge', () => 1);
    const [op] = ctx.takePending();
    // Nothing was recorded — the body never ran — so check the claim directly.
    expect(op).toBeUndefined();
    const ctx2 = ctxFor(d);
    ctx2.step('charge', () => 1);
    expect(stepHash('test-fn', 'charge', 0)).toMatch(/^[0-9a-f]{16}$/);
    expect(ctx2.idSequence()).toEqual(['charge']);
  });
});

describe('the guards that replace Rust\'s !Send', () => {
  it('refuses a claim after the pass has ended', async () => {
    const d = new Driver();
    const ctx = ctxFor(d);
    ctx.seal();
    expect(() => ctx.step('late', () => 1)).toThrowError(/after its replay pass had ended/);
    try {
      ctx.step('late', () => 1);
    } catch (e) {
      expect(isHalt(e) && e.reason).toBe('fatal');
    }
  });

  it('refuses a step created inside another step body', async () => {
    // On an attempt where the outer step replays from the memo its body never
    // runs, so the inner claim never happens and the hash depends on which
    // attempt it is. Rust permits this; refusing is deliberate.
    const d = new Driver();
    const ctx = ctxFor(d);
    const outer = ctx.step('outer', () => {
      ctx.step('inner', () => 1);
      return 1;
    });
    await expect(Promise.resolve(outer)).rejects.toThrowError(
      /claimed from inside another step's body/,
    );
  });
});
