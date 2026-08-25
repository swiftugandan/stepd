import { describe, expect, it } from 'vitest';
import { App, fn } from '../src/index.js';

const build = (): App =>
  new App({ appId: 'billing', url: 'http://x' })
    .signingKey('k')
    .function(fn('b').onEvent('e').run(() => null))
    .function(fn('a').onEvent('e').run(() => null));

describe('manifest', () => {
  it('is stable across restarts', () => {
    // Otherwise every restart looks like a config change and the server
    // re-registers for nothing.
    expect(build().manifest()).toEqual(build().manifest());
  });

  it('sorts functions by id, so the checksum is a function of content', () => {
    const m = build().manifest();
    expect(m.functions.map((f) => f.id)).toEqual(['a', 'b']);
  });

  it('changes when a function changes', () => {
    const other = new App({ appId: 'billing', url: 'http://x' })
      .signingKey('k')
      .function(fn('b').onEvent('different').run(() => null))
      .function(fn('a').onEvent('e').run(() => null));
    expect(build().manifest().checksum).not.toBe(other.manifest().checksum);
  });

  it('has a checksum the schema accepts', () => {
    expect(build().manifest().checksum).toMatch(/^sha256:[0-9a-f]{64}$/);
  });

  it('carries an sdk identifier the schema accepts', () => {
    expect(build().manifest().sdk).toMatch(/^[a-z0-9_-]+\/[0-9A-Za-z.+-]+$/);
  });

  it('never contains the signing key', () => {
    // A key that travelled with the manifest would not be a secret.
    //
    // Checked with a distinctive value rather than the `'k'` used elsewhere in
    // this file: a one-character needle matches `"sdk"` and the assertion fails
    // for a reason that has nothing to do with the property.
    const secret = 'correct-horse-battery-staple';
    const app = new App({ appId: 'billing', url: 'http://x' })
      .signingKey(secret)
      .function(fn('a').onEvent('e').run(() => null));
    expect(JSON.stringify(app.manifest())).not.toContain(secret);
  });

  it('carries the defaults, so an operator can see what they were', () => {
    const [first] = build().manifest().functions;
    expect(first!.retries).toEqual({
      max_attempts: 4,
      backoff: 'exponential',
      initial: 'PT10S',
      max: 'PT1H',
      jitter: true,
    });
    expect(first!.timeouts).toEqual({ attempt: 'PT60S', run: 'P30D' });
  });
});

describe('cron triggers', () => {
  it('requires a zone rather than defaulting to UTC', () => {
    const f = fn('nightly').onCron('0 3 * * *', 'Europe/London').run(() => null);
    const config = f.config();
    expect(config.triggers[0]).toEqual({
      type: 'cron',
      cron: '0 3 * * *',
      tz: 'Europe/London',
    });
  });

  it('carries every catch-up option next to the schedule it applies to', () => {
    const f = fn('billing')
      .onCron('0 3 * * *', 'Europe/London', {
        catchup: 'all',
        catchupLimit: 7,
        misfireWindow: 'PT12H',
        singleton: 'billing',
      })
      .run(() => null);
    expect(f.config().triggers[0]).toEqual({
      type: 'cron',
      cron: '0 3 * * *',
      tz: 'Europe/London',
      catchup: 'all',
      catchup_limit: 7,
      misfire_window: 'PT12H',
      singleton: true,
      run_key: 'billing',
    });
  });
});

describe('lint', () => {
  it('names a function with no trigger, which would simply never run', () => {
    const app = new App({ appId: 'a', url: 'u' })
      .signingKey('k')
      .function(fn('orphan').run(() => null));
    expect(app.lint().join(' ')).toContain('no triggers');
  });

  it('names a function with no handler, whose attempts would all 404', () => {
    const app = new App({ appId: 'a', url: 'u' }).signingKey('k').function(fn('empty').onEvent('e'));
    expect(app.lint().join(' ')).toContain('no handler');
  });

  it('names a singleton with no key, where singleton has no effect', () => {
    const app = new App({ appId: 'a', url: 'u' })
      .signingKey('k')
      .function(fn('s').onEvent('e').singleton().run(() => null));
    expect(app.lint().join(' ')).toContain('singleton but has no key');
  });

  it('names an attempt timeout longer than the run timeout', () => {
    const app = new App({ appId: 'a', url: 'u' })
      .signingKey('k')
      .function(
        fn('t').onEvent('e').timeouts({ attempt: 'PT2H', run: 'PT1H' }).run(() => null),
      );
    expect(app.lint().join(' ')).toContain('longer than its run timeout');
  });

  it('names an app with no key that is not in dev mode', () => {
    // A missing key must fail closed; dev mode is the explicit opt-out.
    const app = new App({ appId: 'a', url: 'u' }).function(fn('f').onEvent('e').run(() => null));
    expect(app.lint().join(' ')).toContain('no signing key');
    const dev = new App({ appId: 'a', url: 'u', devMode: true }).function(
      fn('f').onEvent('e').run(() => null),
    );
    expect(dev.lint().join(' ')).not.toContain('no signing key');
  });

  it('is clean for a well-formed app', () => {
    expect(build().lint()).toEqual([]);
  });
});
