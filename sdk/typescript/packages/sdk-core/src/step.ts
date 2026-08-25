/**
 * A claimed op, awaiting execution.
 *
 * Lazy on purpose. `ctx.step(id, fn)` *claims* synchronously — that is the rule
 * the whole design rests on — but it does not start `fn`. The body runs on first
 * subscription, which is what `await` and `Promise.all` both do.
 *
 * Two things follow, and both matter:
 *
 * * A step created and never awaited consumes its occurrence but does not
 *   execute. Consuming the occurrence is the accepted cost of eager claiming
 *   (ADR-012); executing as well would be a side effect nobody asked for.
 * * `ctx.parallel` can reject a repeated id *before any member's body has run*,
 *   so there is nothing a rejection would have to undo.
 *
 * It is a `PromiseLike`, not a `Promise`. The difference is only visible if you
 * keep one unsubscribed: no work starts, and an eventual rejection cannot be
 * reported as unhandled, because there is nothing to reject yet.
 */
export class StepFuture<T> implements PromiseLike<T> {
  readonly #start: () => Promise<T>;
  #promise: Promise<T> | undefined;

  constructor(
    /** The developer-supplied id. Read by `ctx.parallel` for its uniqueness check. */
    readonly stepId: string,
    start: () => Promise<T>,
  ) {
    this.#start = start;
  }

  /** Whether the body has been started. Used by tests, and by `ctx.parallel`. */
  get started(): boolean {
    return this.#promise !== undefined;
  }

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): PromiseLike<TResult1 | TResult2> {
    // Memoised, so subscribing twice does not run the body twice.
    this.#promise ??= this.#start();
    return this.#promise.then(onfulfilled, onrejected);
  }

  catch<TResult = never>(
    onrejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
  ): PromiseLike<T | TResult> {
    return this.then(null, onrejected);
  }

  finally(onfinally?: (() => void) | null): PromiseLike<T> {
    this.#promise ??= this.#start();
    return this.#promise.finally(onfinally);
  }
}
