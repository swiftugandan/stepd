import type { Op, RecordedStep, RunContext } from '@stepd/protocol';
import { Ctx, runPass, type Handler, type PassOutcome } from '../src/index.js';

/**
 * A miniature server, for testing the machinery against something that behaves
 * like one.
 *
 * Deliberately small, and deliberately strict about the two rules that make a
 * test here mean anything in production:
 *
 * * **Only settled steps enter the memo.** Shipping a pending row would be more
 *   permissive than the real store, and a workflow could pass its tests and then
 *   hang the first time it was deployed.
 * * **First write wins**, mirroring `ON CONFLICT DO NOTHING` in the commit path.
 */
export class Driver {
  readonly memo: Record<string, RecordedStep> = {};
  readonly attempts: PassOutcome[] = [];
  /** Every op committed, in order, across all attempts. */
  readonly committed: Op[] = [];
  /** Events waiting to match a `wait_event`, oldest first. */
  readonly inbox: Array<{ type: string; data: unknown }> = [];
  /** Wait ids that should resolve as a timeout rather than stay pending. */
  readonly timedOut = new Set<string>();

  #pass = 0;

  constructor(
    readonly functionId = 'test-fn',
    readonly now: () => Date = () => new Date('2026-01-01T00:00:00Z'),
  ) {}

  run(): RunContext {
    return {
      id: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10',
      function_id: this.functionId,
      namespace: 'test',
      started_at: '2026-01-01T00:00:00Z',
      lineage_id: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10',
      chain_position: 0,
      cancelling: false,
    };
  }

  newCtx(): Ctx {
    this.#pass += 1;
    return new Ctx({
      functionId: this.functionId,
      run: this.run(),
      memo: { ...this.memo },
      attempt: this.#pass,
      pass: this.#pass,
      now: this.now,
    });
  }

  /** One attempt: replay, then commit whatever it recorded. */
  async attempt(handler: Handler): Promise<PassOutcome> {
    const ctx = this.newCtx();
    const { outcome } = await runPass(ctx, handler);
    this.attempts.push(outcome);
    if (outcome.kind !== 'done') this.#commit(outcome.ops);
    return outcome;
  }

  /** Drive to a terminal outcome, or give up loudly. */
  async runToCompletion(handler: Handler, max = 100): Promise<PassOutcome> {
    for (let i = 0; i < max; i++) {
      const outcome = await this.attempt(handler);
      if (outcome.kind === 'done') return outcome;
      if (outcome.kind === 'error') return outcome;
      if (outcome.ops.length === 0) {
        throw new Error(
          `attempt ${i + 1} recorded nothing and did not finish; the run would hang`,
        );
      }
    }
    throw new Error(`did not settle in ${max} attempts`);
  }

  #commit(ops: Op[]): void {
    for (const op of ops) {
      this.committed.push(op);
      switch (op.op) {
        case 'step':
          this.#record(op.hash, {
            id: op.id,
            op: 'step',
            status: op.error === undefined ? 'completed' : 'failed',
            ...(op.error === undefined ? { data: op.data ?? null } : { error: op.error }),
          });
          break;
        case 'sleep':
          // Resolved immediately: this driver has no clock to wait on, and the
          // property under test is memoisation, not timing.
          this.#record(op.hash, { id: op.id, op: 'sleep', status: 'completed', data: null });
          break;
        case 'wait_event': {
          const at = this.inbox.findIndex((e) => e.type === op.event);
          if (at >= 0) {
            const [event] = this.inbox.splice(at, 1);
            this.#record(op.hash, {
              id: op.id,
              op: 'wait_event',
              status: 'completed',
              data: event!.data as RecordedStep['data'],
            });
          } else if (this.timedOut.has(op.id)) {
            // `timed_out`, which is what the engine records — not `completed`
            // with no data. The distinction matters: this test recorded the
            // convenient shape rather than the real one and passed while
            // `conf-wait-timeout` failed the whole battery.
            this.#record(op.hash, {
              id: op.id,
              op: 'wait_event',
              status: 'timed_out',
              data: null,
            });
          }
          // Otherwise it stays pending and is never shipped, so the next attempt
          // records it again — which is what a real run does while it waits.
          break;
        }
        case 'invoke':
          this.#record(op.hash, {
            id: op.id,
            op: 'invoke',
            status: 'completed',
            data: { invoked: op.function, input: op.input ?? null },
          });
          break;
        case 'signal':
          this.#record(op.hash, { id: op.id, op: 'signal', status: 'completed', data: null });
          break;
        default:
          break;
      }
    }
  }

  /** First write wins, mirroring `ON CONFLICT DO NOTHING`. */
  #record(hash: string, step: RecordedStep): void {
    this.memo[hash] ??= step;
  }
}

/** Records what a workflow's bodies actually executed, in order. */
export class Effects {
  readonly log: string[] = [];
  record(name: string): void {
    this.log.push(name);
  }
  count(name: string): number {
    return this.log.filter((e) => e === name).length;
  }
}
