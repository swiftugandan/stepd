/**
 * One app instance's state.
 *
 * **Not module-level.** Every component in this project that reached for a
 * `static` here has been bitten by two concurrent batteries sharing it — a blob
 * client configured by whichever started last, and an effect log any battery's
 * `reset` wiped for all of them. `CLAUDE.md` records three independent arrivals
 * at that bug. An instance owns this; the process does not.
 */
export class AppState {
  /** What each run's handler actually executed, in order. */
  readonly #effects = new Map<string, string[]>();
  /** Where the runner's API is, and a token for it (§12.1). */
  #api: { base: string; token: string } | undefined;

  /**
   * One entry per *body execution*, not per recorded step.
   *
   * The whole reason §12.1 requires this endpoint: "the step body did not run a
   * second time" is invisible in server state, because the journal after one
   * execution and after two is byte-identical. Only the app can report it.
   */
  record(run: string, effect: string): void {
    const log = this.#effects.get(run);
    if (log === undefined) this.#effects.set(run, [effect]);
    else log.push(effect);
  }

  effectsOf(run: string): string[] {
    return this.#effects.get(run) ?? [];
  }

  reset(): void {
    this.#effects.clear();
  }

  configure(base: string, token: string): void {
    this.#api = { base, token };
  }

  get api(): { base: string; token: string } | undefined {
    return this.#api;
  }
}
