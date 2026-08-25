import {
  stepHash,
  type Event,
  type Json,
  type Op,
  type RecordedStep,
  type RunContext,
} from '@stepd/protocol';
import { Halt, StepFailure, asFailure, fatalCoded, isHalt } from './errors.js';
import { toJson } from './json.js';
import { groupOutcome, rejectDuplicateIds } from './parallel.js';
import { StepFuture } from './step.js';

/** Options for {@link Ctx.waitEvent}. */
export interface WaitOptions {
  /** How long to wait before resolving to `null`. */
  timeoutMs?: number;
  /** A pending decision, rendered in the console. Presentational only. */
  prompt?: Json;
  /**
   * Match events from this point rather than from run start.
   *
   * `run_start` is the default and is what closes the lost-signal race (§7.6):
   * an event sent before the handler reaches this line is still matched, because
   * the server checks the run's durable inbox in the same transaction that
   * registers the wait. Setting `registration` reopens that race, deliberately.
   */
  since?: 'run_start' | 'registration';
}

/** Options for {@link Ctx.invoke}. */
export interface InvokeOptions {
  /** Fire and forget: the child's lifecycle becomes independent of this run. */
  detach?: boolean;
}

/** A diagnostic line returned with the envelope. */
export interface LogLine {
  level: string;
  message: string;
}

export interface CtxOptions {
  functionId: string;
  run: RunContext;
  /** Recorded steps, keyed by hash. Only settled ones — a pending row would make
   * the handler treat an unresolved sleep as already done. */
  memo: Record<string, RecordedStep>;
  /** Attempt number from the server (§4), passed to the handler verbatim. */
  attempt: number;
  /** Identifies this replay pass. A claim carrying a different one is refused. */
  pass: number;
  /** Injected so a test can fix time; `sleep` needs an absolute instant. */
  now?: () => Date;
}

/**
 * The handler's view of a run.
 *
 * Rust makes this type `!Send`, so `tokio::spawn`-ing a task that claims a step
 * fails to compile — the earliest and cheapest place to catch the mistake.
 * TypeScript has no equivalent, which is why the runtime guards here are not
 * belt-and-braces: they are the only line of defence there is.
 *
 * Three of them:
 *
 * 1. **The pass token.** A claim after the pass has ended — from a `setTimeout`,
 *    a floating promise, a `Ctx` captured in a closure that outlived its
 *    attempt — is refused rather than given a guessed hash.
 * 2. **No claiming inside a step body.** A nested `ctx.step` claims only on the
 *    attempts where the outer body actually runs, so its hash depends on whether
 *    its parent was memoised.
 * 3. **No repeated id inside one parallel group** — see `ctx.parallel`.
 */
export class Ctx {
  readonly #functionId: string;
  readonly #run: RunContext;
  readonly #memo: Record<string, RecordedStep>;
  readonly #counters = new Map<string, number>();
  readonly #pending: Op[] = [];
  readonly #emit: Event[] = [];
  readonly #logs: LogLine[] = [];
  readonly #idSequence: string[] = [];
  readonly #encountered = new Set<string>();
  readonly #attempt: number;
  readonly #pass: number;
  readonly #now: () => Date;
  #halted = false;
  #sealed = false;
  #executing = 0;

  constructor(options: CtxOptions) {
    this.#functionId = options.functionId;
    this.#run = options.run;
    this.#memo = options.memo;
    this.#attempt = options.attempt;
    this.#pass = options.pass;
    this.#now = options.now ?? (() => new Date());
  }

  // ------------------------------------------------------------- identity

  /** Which attempt this is, from the server. Starts at 1. */
  get attempt(): number {
    return this.#attempt;
  }

  get run(): RunContext {
    return this.#run;
  }

  /** This pass's token. A claim carrying another is refused. */
  get token(): number {
    return this.#pass;
  }

  /**
   * A key a downstream service can deduplicate on (§7.2).
   *
   * Stable across attempts, because the step hash is.
   */
  idempotencyKey(stepHashValue: string): string {
    return `${this.#run.id}:${stepHashValue}`;
  }

  /** Steps are at-least-once to execute; this says whether one already ran. */
  get halted(): boolean {
    return this.#halted;
  }

  // ------------------------------------------------------------ side channels

  /**
   * Publish an event in the same transaction that commits this pass's ops.
   *
   * The envelope is a CloudEvent. The business key is `stepdkey`, not `key`
   * — a `key` field is ignored on ingest. `ctx.run.key` is the run's key.
   */
  emit(event: Event): void {
    this.#emit.push(event);
  }

  log(level: string, message: string): void {
    this.#logs.push({ level, message });
  }

  // ------------------------------------------------------------ driver-facing

  /** Drain the ops discovered this pass. Called once, after the pass. */
  takePending(): Op[] {
    return this.#pending.splice(0, this.#pending.length);
  }

  takeEmit(): Event[] {
    return this.#emit.splice(0, this.#emit.length);
  }

  takeLogs(): LogLine[] {
    return this.#logs.splice(0, this.#logs.length);
  }

  /**
   * Recorded hashes this pass never encountered.
   *
   * A renamed or removed step id. Not an error — the run is fine — but its side
   * effect will happen again under the new id, and nothing else would say so.
   */
  orphaned(): string[] {
    return Object.keys(this.#memo).filter((h) => !this.#encountered.has(h));
  }

  /**
   * The ids claimed this pass, in order.
   *
   * Every pass's sequence should be a prefix of the final one. Exposed for a
   * strict mode that compares attempts (§6.1 rule 5); nothing here enforces it.
   */
  idSequence(): string[] {
    return [...this.#idSequence];
  }

  /** End the pass. A claim after this is refused rather than guessed. */
  seal(): void {
    this.#sealed = true;
  }

  /** Advisory: run state is growing without bound (§5.1 `continue_as_new`). */
  shouldContinueAsNew(): boolean {
    return Object.keys(this.#memo).length >= 500;
  }

  // ------------------------------------------------------------ internals

  /** @internal Claim the next occurrence for `stepId`, in program order. */
  claim(stepId: string): string {
    if (this.#sealed) {
      throw Halt.fatal(
        `step '${stepId}' claimed an occurrence after its replay pass had ended. ` +
          `A Ctx does not outlive its attempt: creating steps from a timer, an ` +
          `unawaited promise or a captured closure ties occurrence assignment to ` +
          `scheduler order, and a guessed hash silently re-executes recorded work ` +
          `(protocol §6.1 rule 4).`,
      );
    }
    if (this.#executing > 0) {
      throw Halt.fatal(
        `step '${stepId}' was claimed from inside another step's body. A step body ` +
          `must not create steps: on an attempt where the outer step is replayed ` +
          `from the memo its body never runs, so this claim never happens, and the ` +
          `hash depends on which attempt it is (protocol §6.1). Create both steps ` +
          `in the handler, and use ctx.parallel(..) if they should run together.`,
      );
    }
    const occurrence = this.#counters.get(stepId) ?? 0;
    this.#counters.set(stepId, occurrence + 1);
    this.#idSequence.push(stepId);
    return stepHash(this.#functionId, stepId, occurrence);
  }

  /** @internal Look up a recorded step, marking its hash as encountered. */
  lookup(hash: string): RecordedStep | undefined {
    const hit = this.#memo[hash];
    if (hit !== undefined) this.#encountered.add(hash);
    return hit;
  }

  /** @internal Record an op, and mark the pass as having done something. */
  pushOp(op: Op): void {
    this.#pending.push(op);
    this.#halted = true;
  }

  /**
   * @internal A failure is not a halt, but it still ends the pass — and the
   * driver has to know a step was reached, or a swallowed failure looks like a
   * clean return.
   */
  markHalted(): void {
    this.#halted = true;
  }

  /** @internal Bracket a step body, so a nested claim can be refused. */
  enterStepBody(): void {
    this.#executing += 1;
  }

  /** @internal */
  exitStepBody(): void {
    this.#executing -= 1;
  }

  /** @internal */
  clock(): Date {
    return this.#now();
  }

  /** @internal */
  jsonMemo(hash: string): Json | undefined {
    return this.#memo[hash]?.data;
  }

  // ------------------------------------------------------------------ ops

  /**
   * Run a unit of work once, and remember its result.
   *
   * **The occurrence is claimed here, when this method is called — not when the
   * returned value is awaited.** That is the load-bearing sentence of the whole
   * SDK. Claiming at `await` would make occurrence follow scheduler order rather
   * than declaration order, so a completed step's hash would differ between
   * attempts: the server would hold a result the handler never asks for, the
   * step would re-execute, the payment would be taken twice — and **nothing
   * would error** (ADR-012).
   *
   * Because the claim is already done, `Promise.all([a, b])` over steps is safe.
   * `ctx.parallel` adds two things on top: it rejects a repeated id, and it puts
   * every member in one envelope instead of one round trip each.
   *
   * `body` is not called at all when the step is already recorded.
   */
  step<T>(id: string, body: () => T | PromiseLike<T>): StepFuture<T> {
    // Synchronous, in program order, before anything can be awaited.
    const hash = this.claim(id);
    const recorded = this.lookup(hash);

    if (recorded !== undefined) {
      return new StepFuture<T>(id, async () => this.#replay<T>(id, recorded));
    }

    return new StepFuture<T>(id, async () => {
      let value: T;
      this.enterStepBody();
      try {
        value = await body();
      } catch (e) {
        // A Halt is control flow, not an application failure, and must reach
        // `runPass` unchanged. Passing it through `asFailure` would turn a
        // protocol violation raised inside a body — a nested `ctx.step`, which
        // is refused at claim time — into an ordinary retryable error, so the
        // engine would retry a program that cannot succeed and the violation
        // would never be reported as one.
        if (isHalt(e)) throw e;

        // A failure is not a halt, but it still ends the pass, and the driver
        // needs to know a step was reached — otherwise a swallowed failure
        // masquerades as a clean return.
        this.markHalted();
        const failure = asFailure(e);
        // A *terminal* failure is an outcome, and outcomes are recorded. A
        // retryable one deliberately is not: a recorded failure is memoised, so
        // the next attempt would replay the error instead of the body and the
        // retry would never happen (§5.2.2).
        if (!failure.retryable) {
          this.pushOp({
            op: 'step',
            id,
            hash,
            error: { code: failure.code, message: failure.message },
          });
        }
        throw failure;
      } finally {
        this.exitStepBody();
      }

      let data: Json;
      try {
        data = toJson(value, () => `step '${id}'`);
      } catch (e) {
        this.markHalted();
        throw new StepFailure(e instanceof Error ? e.message : String(e), { retryable: false });
      }
      this.pushOp({ op: 'step', id, hash, data });
      // The work is done and recorded, so the handler stops here and lets the
      // server commit it. The next attempt replays past this point from the memo.
      throw Halt.yield_(`step '${id}' recorded`);
    });
  }

  /** Translate a recorded step back into a value, or the error it failed with. */
  #replay<T>(id: string, recorded: RecordedStep): T {
    switch (recorded.status) {
      case 'completed':
        return (recorded.data ?? null) as T;
      case 'failed':
        throw new StepFailure(recorded.error?.message ?? `step '${id}' failed`, {
          retryable: false,
          code: recorded.error?.code,
        });
      case 'timed_out':
        throw fatalCoded('timed_out', `step '${id}' timed out`);
      case 'cancelled':
        throw fatalCoded('cancelled', `step '${id}' was cancelled`);
      case 'pending':
      case 'unknown':
        // Pending rows are never shipped, and `unknown` means an attempt was
        // abandoned mid-step. Either way the right move is to re-execute, which
        // the server arranges by retrying the attempt.
        throw Halt.yield_(`step '${id}' is unresolved; the server will retry`);
    }
  }

  /**
   * A wait's outcomes are not a step's, so it does not share `#replay`.
   *
   * A timeout is an outcome, not an error: "nobody approved in seven days" is
   * something the handler branches on, and the protocol records it as a `null`
   * result rather than a failure. Reusing the step path here made
   * `conf-wait-timeout` fail the run instead of resolving — caught by the
   * conformance battery, and by nothing else.
   */
  #replayWait<T>(id: string, recorded: RecordedStep): T | null {
    if (recorded.status === 'timed_out') return null;
    if (recorded.status === 'pending' || recorded.status === 'unknown') {
      // The engine never ships pending rows, so seeing one means a store or a
      // harness did. Treating it as "resolved with no data" would silently skip
      // the wait, which is the failure the whole inbox mechanism exists to stop.
      throw Halt.yield_(`wait '${id}' is unresolved; the server will retry`);
    }
    if (recorded.status === 'failed' || recorded.status === 'cancelled') {
      return this.#replay<T>(id, recorded);
    }
    return (recorded.data ?? null) as T | null;
  }

  /** Suspend for a duration. Consumes no app compute while parked. */
  sleep(id: string, ms: number): StepFuture<null> {
    return this.sleepUntil(id, new Date(this.clock().getTime() + ms));
  }

  /** Suspend until an absolute instant. */
  sleepUntil(id: string, until: Date | string): StepFuture<null> {
    const hash = this.claim(id);
    const recorded = this.lookup(hash);
    const at = typeof until === 'string' ? until : until.toISOString();
    return new StepFuture<null>(id, async () => {
      if (recorded !== undefined) return this.#replay<null>(id, recorded);
      this.pushOp({ op: 'sleep', id, hash, until: at });
      throw Halt.yield_(`sleep '${id}' recorded`);
    });
  }

  /**
   * Suspend until a matching event arrives. Resolves to `null` on timeout.
   *
   * Claimed here, when called — not on `await`. `const a = ctx.waitEvent(..);
   * const b = ctx.waitEvent(..); await b; await a;` would otherwise swap their
   * hashes, and nothing would error.
   */
  waitEvent<T = Json>(id: string, event: string, options: WaitOptions = {}): StepFuture<T | null> {
    const hash = this.claim(id);
    const recorded = this.lookup(hash);
    const timeoutAt =
      options.timeoutMs === undefined
        ? undefined
        : new Date(this.clock().getTime() + options.timeoutMs).toISOString();

    return new StepFuture<T | null>(id, async () => {
      if (recorded !== undefined) return this.#replayWait<T>(id, recorded);
      this.pushOp({
        op: 'wait_event',
        id,
        hash,
        event,
        since: options.since ?? 'run_start',
        ...(timeoutAt === undefined ? {} : { timeout_at: timeoutAt }),
        ...(options.prompt === undefined ? {} : { prompt: options.prompt }),
      });
      throw Halt.yield_(`wait '${id}' recorded`);
    });
  }

  /** Call another function as a child run. */
  invoke<T = Json>(
    id: string,
    fn: string,
    input: Json,
    options: InvokeOptions = {},
  ): StepFuture<T> {
    const hash = this.claim(id);
    const recorded = this.lookup(hash);
    return new StepFuture<T>(id, async () => {
      if (recorded !== undefined) return this.#replay<T>(id, recorded);
      this.pushOp({
        op: 'invoke',
        id,
        hash,
        function: fn,
        input,
        ...(options.detach === true ? { detach: true } : {}),
      });
      throw Halt.yield_(`invoke '${id}' recorded`);
    });
  }

  /** Send an event to another run. */
  signal(id: string, targetRun: string, event: Event): StepFuture<null> {
    const hash = this.claim(id);
    const recorded = this.lookup(hash);
    return new StepFuture<null>(id, async () => {
      if (recorded !== undefined) return this.#replay<null>(id, recorded);
      this.pushOp({ op: 'signal', id, hash, target_run: targetRun, event });
      throw Halt.yield_(`signal '${id}' recorded`);
    });
  }

  /**
   * Close this run and start a successor with an empty journal.
   *
   * How an unbounded loop keeps run state finite. Always throws: there is no
   * "already done" case, because the successor is a different run.
   */
  continueAsNew(id: string, input: Json): never {
    const hash = this.claim(id);
    this.pushOp({ op: 'continue_as_new', id, hash, input });
    throw Halt.yield_(`continue_as_new '${id}' recorded`);
  }

  /**
   * Run claimed ops together, in one envelope.
   *
   * Every member settles before this resolves, and a failure is surfaced rather
   * than cancelling its siblings — see {@link groupOutcome}. Prefer this over
   * `Promise.all`, which rejects on the first failure and would leave a sibling
   * that had already executed unrecorded.
   */
  async parallel<T extends ReadonlyArray<StepFuture<unknown>>>(
    members: T,
  ): Promise<{ -readonly [K in keyof T]: T[K] extends StepFuture<infer U> ? U : never }> {
    // Before anything is subscribed, so a rejection has nothing to undo.
    rejectDuplicateIds(members);
    const settled = await Promise.allSettled(members.map(async (m) => m));
    return groupOutcome(settled) as never;
  }
}
