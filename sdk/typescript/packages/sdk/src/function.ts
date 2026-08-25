import type { Json } from '@stepd/protocol';
import type { Ctx } from '@stepd/sdk-core';

/** What starts a run (§3). */
export type Trigger =
  | { type: 'event'; event: string; expr?: string }
  | {
      type: 'cron';
      cron: string;
      tz: string;
      catchup?: 'one' | 'skip' | 'all';
      catchup_limit?: number;
      misfire_window?: string;
      singleton?: boolean;
      run_key?: string;
    }
  | { type: 'invoke' };

/**
 * What to do when the server was down over a fire time.
 *
 * Every option here answers a question that only comes up then — which is
 * exactly when nobody wants to discover what the default was (ADR-016). They
 * live next to the schedule rather than in server config for that reason.
 *
 * The defaults are `catchup: 'one'` within `PT1H`, overlapping freely: right for
 * "this should have run recently", wrong for a billing tick.
 */
export interface CronOptions {
  /** Fire once on recovery (default), skip the missed window, or fire them all. */
  catchup?: 'one' | 'skip' | 'all';
  /** Cap on occurrences fired in one recovery, for `all`. */
  catchupLimit?: number;
  /** Occurrences older than this are never caught up. ISO 8601 duration. */
  misfireWindow?: string;
  /** Skip a fire while a run on this key is still active. */
  singleton?: string;
}

export interface Retries {
  max_attempts: number;
  backoff: 'exponential' | 'linear' | 'constant';
  initial: string;
  max: string;
  jitter: boolean;
}

export interface Timeouts {
  attempt: string;
  run: string;
}

export const DEFAULT_RETRIES: Retries = {
  max_attempts: 4,
  backoff: 'exponential',
  initial: 'PT10S',
  max: 'PT1H',
  jitter: true,
};

export const DEFAULT_TIMEOUTS: Timeouts = { attempt: 'PT60S', run: 'P30D' };

export type WorkflowHandler = (ctx: Ctx) => unknown;

/** Seconds in an ISO 8601 duration, or `undefined` if it is not one we parse. */
export function iso8601Seconds(text: string): number | undefined {
  // Deliberately partial, matching the Rust SDK: years and months have no fixed
  // length, so a lint that guessed at them would be confidently wrong.
  const m = /^P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/.exec(
    text,
  );
  if (m === null || text === 'P') return undefined;
  const [, w, d, h, min, s] = m;
  return (
    Number(w ?? 0) * 604_800 +
    Number(d ?? 0) * 86_400 +
    Number(h ?? 0) * 3_600 +
    Number(min ?? 0) * 60 +
    Number(s ?? 0)
  );
}

/** A workflow: its configuration and its handler. */
export class Function {
  #name: string | undefined;
  #version = '1';
  readonly #triggers: Trigger[] = [];
  #keyExpr: string | undefined;
  #singleton = false;
  #onCancel = false;
  #retries: Retries = DEFAULT_RETRIES;
  #timeouts: Timeouts = DEFAULT_TIMEOUTS;
  #handler: WorkflowHandler | undefined;

  private constructor(readonly id: string) {}

  static create(id: string): Function {
    return new Function(id);
  }

  /** Console label. Not the identity — the id is. */
  name(name: string): this {
    this.#name = name;
    return this;
  }

  /** Opaque version string. Informational: identity is the id. */
  version(version: string): this {
    this.#version = version;
    return this;
  }

  onEvent(event: string, expr?: string): this {
    this.#triggers.push(expr === undefined ? { type: 'event', event } : { type: 'event', event, expr });
    return this;
  }

  /** `tz` is required, and not defaulted to UTC: "3am in London" is the requirement. */
  onCron(cron: string, tz: string, options: CronOptions = {}): this {
    this.#triggers.push({
      type: 'cron',
      cron,
      tz,
      ...(options.catchup === undefined ? {} : { catchup: options.catchup }),
      ...(options.catchupLimit === undefined ? {} : { catchup_limit: options.catchupLimit }),
      ...(options.misfireWindow === undefined ? {} : { misfire_window: options.misfireWindow }),
      ...(options.singleton === undefined
        ? {}
        : { singleton: true, run_key: options.singleton }),
    });
    return this;
  }

  onInvoke(): this {
    this.#triggers.push({ type: 'invoke' });
    return this;
  }

  /** A CEL expression producing this run's business key. */
  key(expr: string): this {
    this.#keyExpr = expr;
    return this;
  }

  /** One active run per key; others queue. Needs {@link Function.key}. */
  singleton(): this {
    this.#singleton = true;
    return this;
  }

  /** Declare a compensation path, run when the run is cancelled (§7.4). */
  onCancel(): this {
    this.#onCancel = true;
    return this;
  }

  retries(retries: Partial<Retries>): this {
    this.#retries = { ...this.#retries, ...retries };
    return this;
  }

  timeouts(timeouts: Partial<Timeouts>): this {
    this.#timeouts = { ...this.#timeouts, ...timeouts };
    return this;
  }

  run(handler: WorkflowHandler): this {
    this.#handler = handler;
    return this;
  }

  get handler(): WorkflowHandler | undefined {
    return this.#handler;
  }

  /** The `FunctionConfig` sent to the server. */
  config(): Json {
    const v: Record<string, Json> = {
      id: this.id,
      version: this.#version,
      triggers: this.#triggers as unknown as Json,
      retries: this.#retries as unknown as Json,
      timeouts: this.#timeouts as unknown as Json,
    };
    if (this.#name !== undefined) v.name = this.#name;
    if (this.#keyExpr !== undefined) v.key_expr = this.#keyExpr;
    if (this.#singleton) v.singleton = true;
    if (this.#onCancel) v.on_cancel = true;
    return v;
  }

  /**
   * Problems worth saying out loud at start-up.
   *
   * Every one of these produces a run that looks fine until it does not: a
   * function with no trigger simply never runs, and nothing ever says so.
   */
  lint(): string[] {
    const out: string[] = [];
    if (this.#handler === undefined) {
      out.push(
        `function '${this.id}' has no handler; it will be registered and every attempt will 404`,
      );
    }
    if (this.#triggers.length === 0) {
      out.push(`function '${this.id}' has no triggers; nothing will ever start a run of it`);
    }
    if (this.#singleton && this.#keyExpr === undefined) {
      out.push(
        `function '${this.id}' is singleton but has no key; singleton skipping is defined ` +
          `per key, so it has no effect`,
      );
    }
    const attempt = iso8601Seconds(this.#timeouts.attempt);
    const run = iso8601Seconds(this.#timeouts.run);
    if (attempt !== undefined && run !== undefined && attempt > run) {
      out.push(
        `function '${this.id}' has an attempt timeout (${this.#timeouts.attempt}) longer than ` +
          `its run timeout (${this.#timeouts.run}); the run will be killed before its first ` +
          `attempt can finish`,
      );
    }
    return out;
  }
}

/** Start a function definition. */
export function fn(id: string): Function {
  return Function.create(id);
}
