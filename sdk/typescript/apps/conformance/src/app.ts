import { App, Blob, Blobs, fn, retryable, fatal, fatalCoded, type Ctx } from '@stepd/sdk';
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

  // ------------------------------------------------------------- parallel

  app.function(
    fn('conf-parallel')
      .onEvent('conf.parallel')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        return await ctx.parallel([
          ctx.step('p-a', () => {
            state.record(run, 'p-a');
            return 1;
          }),
          ctx.step('p-b', () => {
            state.record(run, 'p-b');
            return 2;
          }),
          ctx.step('p-c', () => {
            state.record(run, 'p-c');
            return 3;
          }),
        ]);
      }),
  );

  app.function(
    fn('conf-parallel-partial')
      .onEvent('conf.parallel_partial')
      .run(async (ctx: Ctx) => {
        // The rule the retired join policies were gesturing at, stated as the
        // one that actually holds: a failing member neither cancels its siblings
        // nor hides their outcomes. Every body runs, every outcome is recorded,
        // and the handler decides what a failure means.
        const run = ctx.run.id;
        return await ctx.parallel([
          ctx.step('q-ok-1', () => {
            state.record(run, 'q-ok-1');
            return 1;
          }),
          ctx.step('q-bad', () => {
            state.record(run, 'q-bad');
            throw fatalCoded('conformance_member_failed', 'this member fails on purpose');
          }),
          ctx.step('q-ok-2', () => {
            state.record(run, 'q-ok-2');
            return 3;
          }),
        ]);
      }),
  );

  // ------------------------------------------------------------------ wait

  app.function(
    fn('conf-wait')
      .onEvent('conf.wait')
      .run(async (ctx: Ctx) => {
        const got = await ctx.waitEvent<{ token?: unknown }>('await-signal', 'conf.signal', {
          timeoutMs: 10_000,
        });
        return { token: got === null ? null : (got.token ?? null) };
      }),
  );

  app.function(
    fn('conf-wait-timeout')
      .onEvent('conf.wait_timeout')
      .run(async (ctx: Ctx) => {
        const got = await ctx.waitEvent('never', 'conf.never', { timeoutMs: 2_000 });
        return { timed_out: got === null };
      }),
  );

  app.function(
    fn('conf-early-signal')
      .onEvent('conf.early_signal')
      .run(async (ctx: Ctx) => {
        // A step first, so the runner can deliver the signal before the wait is
        // registered. That is the case that used to lose events entirely; the
        // durable inbox is what makes it resolve anyway (§7.6).
        const run = ctx.run.id;
        await ctx.step('settle', () => {
          state.record(run, 'settle');
          return 'settled';
        });
        const got = await ctx.waitEvent<{ token?: unknown }>('await-early', 'conf.signal', {
          timeoutMs: 10_000,
        });
        return { token: got === null ? null : (got.token ?? null) };
      }),
  );

  // ---------------------------------------------------------------- invoke

  app.function(
    fn('conf-invoke')
      .onEvent('conf.invoke')
      .run(async (ctx: Ctx) => {
        const child = await ctx.invoke('child', 'conf-invoke-child', { n: 7 });
        return { child };
      }),
  );

  app.function(
    fn('conf-invoke-child')
      .onInvoke()
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const input = ctx.run.input ?? null;
        return await ctx.step('child-work', () => {
          state.record(run, 'child-work');
          return input;
        });
      }),
  );

  // --------------------------------------------------------------- cascade

  app.function(
    fn('conf-cascade')
      .onEvent('conf.cascade')
      .run(async (ctx: Ctx) => {
        // The detached one first, and awaited: a detached invoke records as
        // complete immediately, so the pass resumes next attempt with it
        // memoised. The other order would park on the attached child and the
        // detached one would never be created.
        const detached = await ctx.invoke('detached', 'conf-cascade-detached', {}, { detach: true });
        // This one parks the parent until the child finishes, which it will not
        // within the case. That is the state the runner needs: one attached and
        // one detached child both live, so cancelling the parent tests both
        // halves of the rule at once.
        const attached = await ctx.invoke('attached', 'conf-cascade-child', {});
        return { attached, detached };
      }),
  );

  const longSleep = async (ctx: Ctx) => {
    await ctx.sleep('long', 30_000);
    return 'finished';
  };
  app.function(fn('conf-cascade-child').onInvoke().run(longSleep));
  app.function(fn('conf-cascade-detached').onInvoke().run(longSleep));

  // ------------------------------------------------------- continue_as_new

  app.function(
    fn('conf-continue')
      .onEvent('conf.continue')
      .key("'conf-continue'")
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const input = ctx.run.input as { n?: number } | null;
        const n = typeof input?.n === 'number' ? input.n : 0;

        await ctx.step('tick', () => {
          state.record(run, `tick-${n}`);
          return n;
        });

        if (n < 2) ctx.continueAsNew('go-again', { n: n + 1 });
        return { ticks: n + 1 };
      }),
  );

  // ---------------------------------------------------------------- cancel

  app.function(
    fn('conf-cancel')
      .onEvent('conf.cancel')
      .onCancel()
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        // §7.4: a cancelled run still gets one attempt so the handler can
        // compensate, and it is told which mode it is in.
        if (ctx.run.cancelling) {
          await ctx.step('compensate', () => {
            state.record(run, 'cancelled');
            return 'compensated';
          });
          return 'compensated';
        }

        await ctx.step('work', () => {
          state.record(run, 'work');
          return 'worked';
        });
        await ctx.waitEvent('park', 'conf.signal', { timeoutMs: 60_000 });
        return 'finished';
      }),
  );

  // ------------------------------------------------------------------ refs

  app.function(
    fn('conf-refs')
      .onEvent('conf.refs')
      .run(async (ctx: Ctx) => {
        // §8.4: the server never fetches, validates or mints credentials for a
        // `$ref`. It is a pointer into the app's own storage, and a server that
        // dereferenced it would be reaching into a system it has no business in.
        const value = { $ref: 's3://conformance/opaque-object' };
        return await ctx.step('passthrough', () => value);
      }),
  );

  // --------------------------------------------------------------- fencing

  app.function(
    fn('conf-fencing')
      .onEvent('conf.fencing')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        await ctx.step('work', () => {
          state.record(run, 'work');
          return 'worked';
        });
        return 'done';
      }),
  );

  // ------------------------------------------------------------ truncation

  app.function(
    fn('conf-truncation')
      .onEvent('conf.truncation')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        let total = 0;
        for (let i = 0; i < 40; i++) {
          total += await ctx.step(`t-${i}`, () => {
            state.record(run, `t-${i}`);
            return i;
          });
        }
        return { total };
      }),
  );

  // ------------------------------------------------------------------ cron

  app.function(
    fn('conf-cron')
      .onCron('*/5 * * * *', 'UTC')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const input = ctx.run.input as { cron?: { occurrence_at?: string } } | null;
        const occurrence = input?.cron?.occurrence_at ?? 'missing';
        const out = await ctx.step('tick', () => {
          state.record(run, 'tick');
          return occurrence;
        });
        return { occurrence_at: out };
      }),
  );

  // ----------------------------------------------------------------- blobs

  app.function(
    fn('conf-blobs')
      .onEvent('conf.blobs')
      .run(async (ctx: Ctx) => {
        const run = ctx.run.id;
        const api = state.api;
        if (api === undefined) {
          throw fatal(
            'this app was never told where the server is; the runner supplies it through ' +
              'POST /_conformance/configure (§12.1)',
          );
        }
        const blobs = new Blobs(api.base, api.token);

        // Deterministic content, and large enough that no inline path could
        // carry it. Random bytes would make the dedupe assertion untestable,
        // because the second upload would have a different digest.
        const payload = new Uint8Array(64_000);
        for (let i = 0; i < payload.length; i++) payload[i] = i % 251;

        // Uploaded *inside* a step: that is what records the reference in the
        // journal, and the journal reference is what keeps the bytes alive.
        const first = await ctx.step('upload', async () => {
          state.record(run, 'upload');
          const blob = await blobs.put(run, payload, {
            contentType: 'application/octet-stream',
          });
          return blob.toJSON();
        });

        // Identical bytes again. Content addressing means the server returns the
        // existing id and the app skips the transfer.
        const second = await ctx.step('upload-again', async () => {
          state.record(run, 'upload-again');
          const blob = await blobs.put(run, payload, {
            contentType: 'application/octet-stream',
          });
          return blob.toJSON();
        });

        // Read back through the URL the server minted for *this* attempt.
        const ref = Blob.from(first);
        const whole = await blobs.read(ref);
        const head = await blobs.readRange(ref, 0, 15);

        return {
          size: ref.size,
          sha256: ref.sha256,
          read_len: whole.length,
          head_len: head.length,
          head_matches: head.every((b: number, i: number) => b === payload[i]),
          content_matches:
            whole.length === payload.length &&
            whole.every((b: number, i: number) => b === payload[i]),
          deduplicated: Blob.from(second).id === ref.id,
        };
      }),
  );

  return app;
}

/**
 * What this app claims to implement (§12.1).
 *
 * Over-declaring is punished by design: a declared suite that fails is a
 * failure, while one left undeclared is reported as an unknown that bars the
 * level without failing the build. All nineteen are declared.
 */
export const SUITES = [
  'memoization',
  'loops',
  'determinism',
  'parallel',
  'sleep',
  'wait',
  'early_signal',
  'invoke',
  'cascade',
  'continue_as_new',
  'errors',
  'cancel',
  'abandonment',
  'blobs',
  'refs',
  'fencing',
  'signature',
  'truncation',
  'cron',
] as const;

/**
 * Empty, deliberately.
 *
 * The only defined value is `offpath_claim`, and TypeScript cannot make an
 * off-path claim a compile error the way Rust's `!Send` `Ctx` does. Declaring it
 * would be claiming a defence this SDK does not have.
 */
export const STATICALLY_PREVENTED: string[] = [];
