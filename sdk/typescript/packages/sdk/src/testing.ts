import type { Event, Json, Op, RecordedStep, RunContext, StepStatus } from '@stepd/protocol';
import { Ctx, runPass, type Handler } from '@stepd/sdk-core';

/**
 * Unit-testing a workflow, with no database, no server and no HTTP.
 *
 * The gap register rates "no way to unit-test a workflow" adoption-critical
 * rather than a nicety, and it is right to: a developer who cannot test a
 * workflow without standing up an engine will not test it, and durable
 * workflows are exactly the code where an untested branch surfaces a month later
 * in production.
 *
 * The harness drives the **same `runPass`** the real handler drives, against an
 * in-memory model of the engine. A harness with its own replay logic would let a
 * workflow pass here and fail in production for reasons the test could not see.
 *
 * ```ts
 * const t = harness(async (ctx) => {
 *   await ctx.step('charge', () => 'ch_1');
 *   await ctx.sleep('cooldown', 86_400_000);
 *   return (await ctx.waitEvent<boolean>('approval', 'order.approved')) ?? false;
 * });
 *
 * t.sendEvent('order.approved', true);          // BEFORE the wait
 * expect(await t.runToCompletion()).toBe(true);
 * t.assertStepExecutedOnce('charge');
 * ```
 *
 * Note what that proves: the event was delivered *before* the handler reached
 * its `waitEvent`, and the run still resolved. That is §7.6's early-signal
 * guarantee, and being able to write it in three lines is the difference between
 * a developer trusting it and hoping for it.
 */

const BASE_TIME = Date.UTC(2026, 0, 1);

export type HarnessError =
  | { kind: 'failed'; code: string | undefined; message: string }
  | { kind: 'waiting'; id: string; event: string }
  | { kind: 'exhausted'; attempts: number };

export class HarnessFailure extends Error {
  override readonly name = 'HarnessFailure';
  constructor(readonly detail: HarnessError) {
    super(describe(detail));
  }
}

function describe(e: HarnessError): string {
  switch (e.kind) {
    case 'failed':
      return `the workflow failed: ${e.code ?? '(no code)'}: ${e.message}`;
    case 'waiting':
      return (
        `the workflow is waiting for '${e.event}' and no matching event was sent. ` +
        `Call sendEvent('${e.event}', ..) before runToCompletion, or timeOutWait('${e.id}').`
      );
    case 'exhausted':
      return (
        `the workflow did not complete within ${e.attempts} attempts. Each attempt advances ` +
        `by one step, so either the workflow has more steps than the budget or it is looping.`
      );
  }
}

/** An in-memory engine for one run. */
export class Harness {
  readonly #handler: Handler;
  readonly #memo = new Map<string, RecordedStep>();
  readonly #inbox: Array<{ type: string; data: Json }> = [];
  readonly #stubs = new Map<string, Json>();
  readonly #failures = new Map<string, string>();
  readonly #timeouts = new Set<string>();
  #run: RunContext;
  #clockOffsetMs = 0;

  /** Ops the handler yielded, per attempt. */
  readonly emitted: Op[][] = [];
  /** Step ids executed, in order, across every attempt. */
  readonly executed: string[] = [];
  /** Events the handler published. */
  readonly emittedEvents: Event[] = [];
  /** Attempts made. */
  attempts = 0;

  constructor(handler: Handler) {
    this.#handler = handler;
    const id = '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10';
    this.#run = {
      id,
      function_id: 'test-function',
      namespace: 'test',
      started_at: new Date(BASE_TIME).toISOString(),
      lineage_id: id,
      chain_position: 0,
      cancelling: false,
    };
  }

  // ------------------------------------------------------------------ setup

  /** Set the function id, which participates in the step hash. */
  functionId(id: string): this {
    this.#run = { ...this.#run, function_id: id };
    return this;
  }

  key(key: string): this {
    this.#run = { ...this.#run, key };
    return this;
  }

  /** Set the run input, for an invoke-triggered or continued function. */
  givenInput(input: Json): this {
    this.#run = { ...this.#run, input };
    return this;
  }

  /** Run the handler down its cancellation path (§7.4). */
  cancelling(): this {
    this.#run = { ...this.#run, cancelling: true };
    return this;
  }

  /**
   * Force a step's result instead of running its body.
   *
   * For a step that calls something the test has no business calling. The step
   * still counts as executed, because from the workflow's point of view it was:
   * substituting a result must not quietly change what the test proves about
   * execution counts.
   */
  expectStep(id: string, returning: Json): this {
    this.#stubs.set(id, returning);
    return this;
  }

  /** Force a step to fail, so the handler's error path can be tested. */
  failStep(id: string, message: string): this {
    this.#failures.set(id, message);
    return this;
  }

  /** Force a wait to time out rather than resolve. */
  timeOutWait(id: string): this {
    this.#timeouts.add(id);
    return this;
  }

  /**
   * Deliver an event.
   *
   * May be called **before** the handler reaches the corresponding `waitEvent`;
   * the harness buffers it exactly as the engine's durable inbox does, so the
   * early-signal case is testable rather than theoretical.
   */
  sendEvent(type: string, data: Json): this {
    this.#inbox.push({ type, data });
    return this;
  }

  /**
   * Advance the virtual clock.
   *
   * Sleeps resolve instantly here regardless; this exists so a handler that
   * reads the clock sees time move, and so a test can say "a day passes" as
   * intent rather than as a comment.
   */
  advanceClock(ms: number): this {
    this.#clockOffsetMs += ms;
    return this;
  }

  // ------------------------------------------------------------- assertions

  /** How many times a step id executed across every attempt. */
  executionCount(id: string): number {
    return this.executed.filter((s) => s === id).length;
  }

  /**
   * The single most valuable assertion a workflow test can make.
   *
   * At-least-once execution is the contract, so "ran once" is a property of
   * memoisation working — not something the handler can arrange for itself.
   */
  assertStepExecutedOnce(id: string): void {
    const n = this.executionCount(id);
    if (n !== 1) {
      throw new Error(
        `step '${id}' executed ${n} times across ${this.attempts} attempts; expected exactly ` +
          `once. Executed steps in order: ${JSON.stringify(this.executed)}`,
      );
    }
  }

  assertStepNotExecuted(id: string): void {
    const n = this.executionCount(id);
    if (n !== 0) {
      throw new Error(`step '${id}' ran, but the test expected the handler not to reach it`);
    }
  }

  /** Assert the ops one attempt yielded, by kind. Attempts are 1-based. */
  assertOps(attempt: number, expected: string[]): void {
    const ops = this.emitted[attempt - 1];
    if (ops === undefined) {
      throw new Error(`no attempt ${attempt}; only ${this.emitted.length} were made`);
    }
    const actual = ops.map((o) => o.op);
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error(
        `attempt ${attempt} emitted ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`,
      );
    }
  }

  /** Every step id in the journal, sorted. */
  recordedSteps(): string[] {
    return [...this.#memo.values()].map((s) => s.id).sort();
  }

  // ---------------------------------------------------------------- driving

  /** Drive the workflow until it returns. */
  async runToCompletion(): Promise<Json> {
    return await this.runBounded(1000);
  }

  async runBounded(maxAttempts: number): Promise<Json> {
    for (let i = 0; i < maxAttempts; i++) {
      const outcome = await this.stepOnce();
      if (outcome.done) return outcome.output;
    }
    throw new HarnessFailure({ kind: 'exhausted', attempts: maxAttempts });
  }

  /**
   * Run exactly one attempt.
   *
   * Exposed so a test can interleave engine events between attempts — deliver a
   * signal after the second step, cancel after the third — which is how the
   * interesting bugs in a workflow are actually reproduced.
   */
  async stepOnce(): Promise<{ done: true; output: Json } | { done: false }> {
    this.attempts += 1;

    // Ship only settled steps, exactly as the engine does. A handler that saw a
    // pending sleep or wait in its memo would treat it as done and step straight
    // past a timer that has not fired, so a harness that shipped them would be
    // more permissive than production — and a workflow could pass its tests and
    // hang the first time it was deployed.
    const shipped: Record<string, RecordedStep> = {};
    for (const [hash, step] of this.#memo) {
      if (step.status !== 'pending') shipped[hash] = step;
    }
    const settledBefore = Object.keys(shipped).length;

    const ctx = new Ctx({
      functionId: this.#run.function_id,
      run: { ...this.#run, started_at: this.#run.started_at },
      memo: shipped,
      // The protocol counts attempts from 1 and so does this. A handler reading
      // `ctx.attempt` must see the same number here as in production, or the
      // test is testing a different handler. (The Rust harness adds one on top
      // of its own counter, so a handler there sees 2 on its first attempt —
      // filed as a defect rather than reproduced.)
      attempt: this.attempts,
      pass: this.attempts,
      now: () => new Date(BASE_TIME + this.#clockOffsetMs),
    });

    const { outcome, emit } = await runPass(ctx, this.#handler);
    this.emittedEvents.push(...emit);

    if (outcome.kind === 'done') {
      this.emitted.push([{ op: 'done', data: outcome.data }]);
      return { done: true, output: outcome.data };
    }
    if (outcome.kind === 'error') {
      this.emitted.push([
        ...outcome.ops,
        { op: 'error', retryable: outcome.retryable, error: outcome.error },
      ]);
      throw new HarnessFailure({
        kind: 'failed',
        code: outcome.error.code,
        message: outcome.error.message,
      });
    }

    for (const op of outcome.ops) {
      // A `step` op in the envelope means its body ran during this attempt,
      // which is what makes execution counting exact rather than inferred.
      if (op.op === 'step') this.executed.push(op.id);
      this.#commit(op);
    }
    this.emitted.push(outcome.ops);

    // A pass that settled nothing new means the run is parked. Say so with the
    // event name: "did not complete in 1000 attempts" is a true statement that
    // helps nobody.
    const settledAfter = [...this.#memo.values()].filter((s) => s.status !== 'pending').length;
    if (settledAfter === settledBefore) {
      const parked = [...this.#memo.values()].find((s) => s.status === 'pending');
      if (parked !== undefined) {
        const wait = outcome.ops.find((o) => o.op === 'wait_event');
        throw new HarnessFailure({
          kind: 'waiting',
          id: parked.id,
          event: wait !== undefined && wait.op === 'wait_event' ? wait.event : '?',
        });
      }
    }
    return { done: false };
  }

  /** Apply one op to the in-memory journal, as the commit path would. */
  #commit(op: Op): void {
    let hash: string;
    let id: string;
    let kind: RecordedStep['op'];
    let data: Json | undefined;
    let status: StepStatus = 'completed';

    switch (op.op) {
      case 'step':
        ({ hash, id } = op);
        kind = 'step';
        data = op.data ?? null;
        break;
      case 'sleep':
        ({ hash, id } = op);
        kind = 'sleep';
        data = null;
        break;
      case 'wait_event': {
        ({ hash, id } = op);
        kind = 'wait_event';
        // FIFO over the buffered inbox, matching by type — the same rule the
        // engine applies, so a test cannot pass here by relying on an ordering
        // the engine does not guarantee.
        const at = this.#inbox.findIndex((e) => e.type === op.event);
        if (this.#timeouts.has(id)) {
          status = 'timed_out';
          data = null;
        } else if (at >= 0) {
          data = this.#inbox.splice(at, 1)[0]!.data;
        } else {
          // No event and no forced timeout: the run would park forever.
          // Recording it pending makes `runToCompletion` stop and say so rather
          // than looping to the attempt cap.
          status = 'pending';
          data = null;
        }
        break;
      }
      case 'invoke':
        ({ hash, id } = op);
        kind = 'invoke';
        data = null;
        break;
      case 'signal':
        ({ hash, id } = op);
        kind = 'signal';
        data = null;
        break;
      case 'continue_as_new':
        ({ hash, id } = op);
        kind = 'step';
        data = null;
        break;
      default:
        return;
    }

    const stub = this.#stubs.get(id);
    if (stub !== undefined) data = stub;

    let error: RecordedStep['error'];
    const failure = this.#failures.get(id);
    if (failure !== undefined) {
      status = 'failed';
      error = { message: failure };
      data = undefined;
    }

    // First write wins, mirroring `ON CONFLICT DO NOTHING`: a step already
    // recorded is never overwritten.
    if (!this.#memo.has(hash)) {
      this.#memo.set(hash, {
        id,
        op: kind,
        status,
        ...(data === undefined ? {} : { data }),
        ...(error === undefined ? {} : { error }),
      });
    }
  }
}

/** Build a harness for a workflow handler. */
export function harness(handler: Handler): Harness {
  return new Harness(handler);
}
