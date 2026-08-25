import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/**
 * The TypeScript half of ADR-006's crate-boundary guard.
 *
 * `stepd-proto` has a CI lane grepping `cargo tree` for a runtime or an I/O
 * dependency, because one added by accident would break no test. The same is
 * true here and more easily done: `import 'node:fs'` in this package would work
 * perfectly, pass every test, and quietly make the protocol binding unusable on
 * the edge runtimes an SDK is supposed to reach.
 */

const srcDir = fileURLToPath(new URL('../src', import.meta.url));
const pkg = JSON.parse(
  readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8'),
) as { dependencies?: Record<string, string> };

function sources(): Array<{ name: string; text: string }> {
  return readdirSync(srcDir)
    .filter((f) => f.endsWith('.ts'))
    .map((name) => ({ name, text: readFileSync(`${srcDir}/${name}`, 'utf8') }));
}

describe('@stepd/protocol has no runtime and no I/O', () => {
  it('has sources to check', () => {
    expect(sources().length).toBeGreaterThanOrEqual(5);
  });

  it.each(sources())('$name imports nothing from Node', ({ text }) => {
    expect(text).not.toMatch(/from\s+['"]node:/);
    expect(text).not.toMatch(/require\(['"]node:/);
  });

  it.each(sources())('$name reaches for no I/O global', ({ text }) => {
    // `fetch` in particular: the moment the protocol binding can make a request,
    // it stops being a description of the wire and becomes a client.
    for (const forbidden of ['fetch(', 'XMLHttpRequest', 'WebSocket', 'localStorage']) {
      expect(text).not.toContain(forbidden);
    }
  });

  it.each(sources())('$name uses no timer, so it cannot be time-dependent', ({ text }) => {
    // `verify` takes `now` as an argument rather than reading a clock. A
    // signature check that consults `Date.now()` itself cannot be tested at a
    // boundary, and every one of its tests becomes flaky near midnight.
    for (const forbidden of ['setTimeout', 'setInterval', 'Date.now()', 'new Date(']) {
      expect(text).not.toContain(forbidden);
    }
  });

  it('declares exactly one dependency', () => {
    // `@noble/hashes` is load-bearing rather than a convenience: `stepHash` must
    // be synchronous, because `ctx.step()` claims its occurrence when called
    // rather than when awaited, and WebCrypto's `subtle.digest` is async.
    expect(Object.keys(pkg.dependencies ?? {})).toEqual(['@noble/hashes']);
  });
});
