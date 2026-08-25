import { describe, expect, it } from 'vitest';
import { Ctx, coded, fatal, fatalCoded, retryable, runPass } from '../src/index.js';
import { Driver } from './driver.js';

const ctxOf = (d: Driver): Ctx => d.newCtx();

describe('runPass', () => {
  it('reports a clean return as done', async () => {
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async () => ({ shipped: true }));
    expect(outcome).toEqual({ kind: 'done', data: { shipped: true } });
  });

  it('maps an undefined return to null rather than dropping the op', async () => {
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async () => undefined);
    expect(outcome).toEqual({ kind: 'done', data: null });
  });

  it('refuses an unserialisable return, naming it', async () => {
    const d = new Driver();
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const { outcome } = await runPass(ctxOf(d), async () => circular);
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.retryable).toBe(false);
      expect(outcome.error.code).toBe('output_not_serialisable');
    }
  });

  it('catches a swallowed halt rather than completing the run', async () => {
    // The failure mode this SDK is most exposed to. `try { … } catch {}` around
    // a step is ordinary JavaScript, and without this check the run would be
    // committed as complete with the rest of the handler never run.
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async (ctx) => {
      try {
        await ctx.step('a', () => 1);
      } catch {
        // swallowed
      }
      return 'looks fine';
    });
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.retryable).toBe(false);
      expect(outcome.error.code).toBe('swallowed_halt');
      // The work still travels: it happened, and something must remember it.
      expect(outcome.ops.length).toBe(1);
    }
  });

  it('yields the recorded op when a step completes', async () => {
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async (ctx) => {
      await ctx.step('a', () => 1);
      return 'never reached this attempt';
    });
    expect(outcome.kind).toBe('yield');
    if (outcome.kind === 'yield') expect(outcome.ops).toHaveLength(1);
  });

  it('turns a protocol violation into a non-retryable error', async () => {
    const d = new Driver();
    const ctx = ctxOf(d);
    const { outcome } = await runPass(ctx, async (c) => {
      c.seal();
      c.step('too late', () => 1);
      return null;
    });
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.retryable).toBe(false);
      expect(outcome.error.code).toBe('protocol_violation');
    }
  });

  it('sends a retryable failure alone, never with recorded ops', async () => {
    // §5.2.2. Retrying is the dispatcher's decision and committing is the
    // store's; one envelope cannot ask for both. So the recorded work goes now,
    // and the failure is raised again next attempt where the work is memoised.
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async (ctx) => {
      // A step that completes, recorded — then a retryable failure in the
      // handler itself.
      await ctx.step('a', () => 1).catch(() => undefined);
      throw retryable('gateway down');
    });
    expect(outcome.kind).toBe('yield');
    if (outcome.kind === 'yield') expect(outcome.ops).toHaveLength(1);
  });

  it('sends a retryable failure with no recorded ops as an error', async () => {
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async () => {
      throw retryable('gateway down');
    });
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.retryable).toBe(true);
      expect(outcome.error.message).toBe('gateway down');
    }
  });

  it('lets a non-retryable failure ride at the end of a batch', async () => {
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async (ctx) => {
      await ctx.step('a', () => 1).catch(() => undefined);
      throw fatalCoded('bad_input', 'the order id was not a number');
    });
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.retryable).toBe(false);
      expect(outcome.error.code).toBe('bad_input');
      expect(outcome.ops).toHaveLength(1);
    }
  });

  it('treats an arbitrary thrown error as retryable, per §5.1', async () => {
    // The protocol's default for an `error` op is retryable, and a bug that
    // fails identically every time is what the poison-pill machinery is for.
    // Guessing "terminal" would turn one transient exception into a permanently
    // failed run.
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async () => {
      throw new TypeError('undefined is not a function');
    });
    expect(outcome.kind).toBe('error');
    if (outcome.kind === 'error') {
      expect(outcome.retryable).toBe(true);
      expect(outcome.error.message).toBe('undefined is not a function');
      expect(outcome.error.stack).toBeTypeOf('string');
    }
  });

  it('drains emit, logs and orphaned exactly once, in every arm', async () => {
    const d = new Driver();
    const ctx = ctxOf(d);
    const result = await runPass(ctx, async (c) => {
      c.emit({
        specversion: '1.0',
        source: 's',
        type: 'shipped',
        data: null,
        stepdkey: 'order:4711',
      });
      c.log('info', 'on the way');
      throw fatal('stop');
    });
    expect(result.emit).toHaveLength(1);
    expect(result.emit[0]?.stepdkey).toBe('order:4711');
    expect(result.logs).toHaveLength(1);
    // Drained, so a second look finds nothing to send twice.
    expect(ctx.takeEmit()).toHaveLength(0);
    expect(ctx.takeLogs()).toHaveLength(0);
  });

  it('seals the context, so nothing can claim after the pass', async () => {
    const d = new Driver();
    const ctx = ctxOf(d);
    let escaped: Ctx | undefined;
    await runPass(ctx, async (c) => {
      escaped = c;
      return null;
    });
    expect(() => escaped!.step('later', () => 1)).toThrowError(/after its replay pass had ended/);
  });
});

describe('the four failure constructors', () => {
  it('makes `coded` retryable, because a code is for recognising repeats', async () => {
    // A code exists so repeated identical failures can be grouped as one poison
    // pill, and a failure that is never retried cannot repeat.
    expect(coded('gateway_down', 'x').retryable).toBe(true);
    expect(fatalCoded('card_declined', 'x').retryable).toBe(false);
    expect(retryable('x').retryable).toBe(true);
    expect(fatal('x').retryable).toBe(false);
  });

  it('keeps the code on the wire', async () => {
    const d = new Driver();
    const { outcome } = await runPass(ctxOf(d), async () => {
      throw coded('gateway_down', 'upstream 503');
    });
    if (outcome.kind !== 'error') throw new Error('expected an error');
    expect(outcome.error.code).toBe('gateway_down');
    expect(outcome.retryable).toBe(true);
  });
});
