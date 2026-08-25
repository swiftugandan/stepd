import { describe, expect, it } from 'vitest';
import { runPass, type Ctx } from '../src/index.js';
import { Driver } from './driver.js';

const CLOCK = new Date('2026-01-01T00:00:00Z');

describe('sleep', () => {
  it('records an absolute instant, computed from the injected clock', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.sleep('cooldown', 24 * 60 * 60 * 1000);
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toEqual({
      op: 'sleep',
      id: 'cooldown',
      hash: expect.any(String),
      until: '2026-01-02T00:00:00.000Z',
    });
  });

  it('resolves to null on replay, and does not sleep twice', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    let seen: unknown = 'unset';
    const outcome = await d.runToCompletion(async (ctx: Ctx) => {
      seen = await ctx.sleep('cooldown', 1000);
      await ctx.step('after', () => 'done');
      return seen;
    });
    expect(seen).toBeNull();
    expect(outcome).toEqual({ kind: 'done', data: null });
    expect(d.committed.filter((o) => o.op === 'sleep')).toHaveLength(1);
  });
});

describe('waitEvent', () => {
  it('defaults to matching from run start, which closes the lost-signal race', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.waitEvent('approval', 'order.approved');
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({
      op: 'wait_event',
      event: 'order.approved',
      since: 'run_start',
    });
  });

  it('resolves from an event delivered before the handler reached the wait', async () => {
    // §7.6, in three lines. The event arrives first; the run still resolves,
    // because the server matches against the run's durable inbox.
    const d = new Driver('test-fn', () => CLOCK);
    d.inbox.push({ type: 'order.approved', data: { by: 'priya' } });

    const outcome = await d.runToCompletion(async (ctx: Ctx) => {
      const approval = await ctx.waitEvent<{ by: string }>('approval', 'order.approved');
      return approval;
    });
    expect(outcome).toEqual({ kind: 'done', data: { by: 'priya' } });
  });

  it('carries an absolute timeout when one is asked for', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.waitEvent('approval', 'order.approved', { timeoutMs: 7 * 86_400_000 });
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({ timeout_at: '2026-01-08T00:00:00.000Z' });
  });

  it('resolves a timed-out wait to null rather than failing it', async () => {
    // "Nobody approved in seven days" is an outcome to branch on, not an error.
    const d = new Driver('test-fn', () => CLOCK);
    d.timedOut.add('approval');
    const outcome = await d.runToCompletion(async (ctx: Ctx) => {
      const approval = await ctx.waitEvent('approval', 'order.approved', { timeoutMs: 1000 });
      return approval === null ? 'timed out' : 'approved';
    });
    expect(outcome).toEqual({ kind: 'done', data: 'timed out' });
  });

  it('can opt out of run-start matching, reopening the race deliberately', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.waitEvent('a', 'e', { since: 'registration' });
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({ since: 'registration' });
  });
});

describe('invoke', () => {
  it('records the child function and its input', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.invoke('refund', 'refunds/issue', { order: 4711 });
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({
      op: 'invoke',
      function: 'refunds/issue',
      input: { order: 4711 },
    });
    expect(outcome.ops[0]).not.toHaveProperty('detach');
  });

  it('marks a detached invoke', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.invoke('fire', 'audit/log', null, { detach: true });
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({ detach: true });
  });

  it('returns the child result on replay', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const outcome = await d.runToCompletion(async (ctx: Ctx) => {
      return await ctx.invoke('refund', 'refunds/issue', { order: 4711 });
    });
    expect(outcome).toEqual({
      kind: 'done',
      data: { invoked: 'refunds/issue', input: { order: 4711 } },
    });
  });
});

describe('continueAsNew', () => {
  it('always halts, because the successor is a different run', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      ctx.continueAsNew('next', { cursor: 41_200 });
      // Unreachable: continueAsNew's return type is `never`.
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({
      op: 'continue_as_new',
      id: 'next',
      input: { cursor: 41_200 },
    });
  });
});

describe('signal', () => {
  it('records the target run and the event', async () => {
    const d = new Driver('test-fn', () => CLOCK);
    const { outcome } = await runPass(d.newCtx(), async (ctx: Ctx) => {
      await ctx.signal('tell', '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c99', {
        specversion: '1.0',
        source: 'test',
        type: 'order.shipped',
        data: { id: 1 },
      });
      return null;
    });
    if (outcome.kind !== 'yield') throw new Error('expected a yield');
    expect(outcome.ops[0]).toMatchObject({
      op: 'signal',
      target_run: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c99',
    });
  });
});

describe('idempotency', () => {
  it('is stable across attempts, because the step hash is', () => {
    const d = new Driver('test-fn', () => CLOCK);
    const a = d.newCtx();
    const b = d.newCtx();
    expect(a.idempotencyKey('abc')).toBe(b.idempotencyKey('abc'));
    expect(a.idempotencyKey('abc')).toBe(`${a.run.id}:abc`);
  });
});
