import { App, fn, retryable, fatalCoded, type Ctx } from '@stepd/sdk';
import type { AppState } from './state.ts';

/**
 * The level-1 half of the §12.2 battery.
 *
 * Every function is triggered by an event of the same name unless stated, and
 * several branch on `ctx.attempt` — the suites are about what happens across
 * attempts, so a handler that could not tell them apart could not demonstrate
 * anything.
 */
export function buildApp(state: AppState, url: string, signingKey: string): App {
  const app = new App({ appId: 'stepd-conformance-ts', url }).signingKey(signingKey);

  // ---------------------------------------------------------- memoization

  app.function(
    fn('conf-memoize')
      .onEvent('conf.memoize')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const work = await ctx.step('work', () => {
          state.record(run, 'work');
          return 'did-the-work';
        });

        // Forced retry. The point of the case is that `work` is memoised across
        // it and its body does not run again.
        if (ctx.attempt === 1) throw retryable('conformance: forced retry');

        const after = await ctx.step('after', () => {
          state.record(run, 'after');
          return 'after';
        });
        return { work, after };
      }),
  );

  // ---------------------------------------------------------------- loops

  app.function(
    fn('conf-loops')
      .onEvent('conf.loops')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const out: number[] = [];
        for (let i = 0; i < 5; i++) {
          // The discriminator is what makes a loop replayable: the occurrence
          // counter alone would tie the hash to iteration order, so inserting an
          // item would re-execute everything after it.
          out.push(
            await ctx.step(`item-${i}`, () => {
              state.record(run, `item-${i}`);
              return i;
            }),
          );
          if (i === 2 && ctx.attempt === 1) {
            throw retryable('conformance: forced retry mid-loop');
          }
        }
        return out;
      }),
  );

  // ---------------------------------------------------------- determinism

  app.function(
    fn('conf-order')
      .onEvent('conf.order')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        for (const id of ['a', 'b', 'c']) {
          await ctx.step(id, () => {
            state.record(run, id);
            return id;
          });
        }
        return 'ordered';
      }),
  );

  // Registered, not declared `statically_prevented`.
  //
  // The Rust SDK omits this function because its `Ctx` is `!Send`, so the
  // program does not compile and there is nothing to drive. TypeScript cannot
  // make the hazard unrepresentable, so the honest answer is to implement the
  // case and fail it at run time — §12.1's `statically_prevented` enum is closed
  // precisely so an SDK cannot wave this through.
  app.function(
    fn('conf-offpath')
      .onEvent('conf.offpath')
      .run(async (ctx: Ctx) => {
        await ctx.step('outer', () => {
          // Off the sequential pass: a claim made from inside a step body
          // happens only on the attempts where that body actually runs, so its
          // occurrence depends on what was memoised. Refused, non-retryably —
          // retrying cannot make a conditional claim unconditional.
          ctx.step('offpath', () => 1);
          return 1;
        });
        return 'unreachable';
      }),
  );

  app.function(
    fn('conf-ambiguous')
      .onEvent('conf.ambiguous')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        // Two members with the same id. Stable — occurrences 0 and 1 in
        // declaration order — and almost never what anyone meant.
        return await ctx.parallel([
          ctx.step('dup', () => {
            state.record(run, 'dup-a');
            return 1;
          }),
          ctx.step('dup', () => {
            state.record(run, 'dup-b');
            return 2;
          }),
        ]);
      }),
  );

  // ---------------------------------------------------------------- sleep

  app.function(
    fn('conf-sleep')
      .onEvent('conf.sleep')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        await ctx.step('before', () => {
          state.record(run, 'before');
          return 'before';
        });
        await ctx.sleep('nap', 2_000);
        await ctx.step('after', () => {
          state.record(run, 'after');
          return 'after';
        });
        return 'slept';
      }),
  );

  // --------------------------------------------------------------- errors

  app.function(
    fn('conf-errors-retryable')
      .onEvent('conf.errors_retryable')
      .run(async (ctx: Ctx) => {
        state.record(ctx.run.id, `attempt-${ctx.attempt}`);
        if (ctx.attempt < 3) throw retryable('conformance: retry me');
        return { attempts: ctx.attempt };
      }),
  );

  app.function(
    fn('conf-errors-terminal')
      .onEvent('conf.errors_terminal')
      .run(async (ctx: Ctx) => {
        state.record(ctx.run.id, `attempt-${ctx.attempt}`);
        throw fatalCoded('conformance_terminal', 'this must not be retried');
      }),
  );

  // ---------------------------------------------------------- abandonment

  app.function(
    fn('conf-abandon')
      .onEvent('conf.abandon')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const attempt = ctx.attempt;
        // The hang is INSIDE the step body, which is the only place it
        // demonstrates anything. Hanging after the step would prove nothing: the
        // pass halts as soon as a step is recorded, so the envelope commits
        // normally and the step is memoised next time. The case has to abandon a
        // response for work that has already happened.
        //
        // §7.1.1: an SDK MUST NOT drop a running step to meet the attempt
        // deadline. Nothing here is wired to an AbortSignal, and the client
        // disconnecting does not cancel this promise.
        await ctx.step('slow', async () => {
          state.record(run, `slow-${attempt}`);
          if (attempt === 1) await new Promise(() => {});
          return 'slow';
        });
        return 'finished';
      }),
  );

  return app;
}

/**
 * What this app claims to implement (§12.1).
 *
 * Over-declaring is punished by design: a declared suite that fails is a
 * failure, while one left undeclared is reported as an unknown that bars the
 * level without failing the build. Level 2's suites are absent because the
 * handler side of them is not written yet.
 */
export const SUITES = [
  'memoization',
  'loops',
  'determinism',
  'sleep',
  'errors',
  'abandonment',
  'signature',
] as const;

/**
 * Empty, deliberately.
 *
 * The only defined value is `offpath_claim`, and TypeScript cannot make an
 * off-path claim a compile error the way Rust's `!Send` `Ctx` does. Declaring it
 * would be claiming a defence this SDK does not have.
 */
export const STATICALLY_PREVENTED: string[] = [];
