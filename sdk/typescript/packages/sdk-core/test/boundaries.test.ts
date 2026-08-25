import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/**
 * The same guard `@stepd/protocol` and ADR-006 carry, for the same reason.
 *
 * This package holds the logic whose defects corrupt silently, and it is
 * testable exhaustively *because* it has no I/O, no HTTP and no clock of its
 * own. An `import 'node:fs'` or a `Date.now()` here would work perfectly, pass
 * every other test, and quietly make the machinery untestable at its boundaries
 * and unusable off Node.
 */

const srcDir = fileURLToPath(new URL('../src', import.meta.url));
const pkg = JSON.parse(
  readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8'),
) as { dependencies?: Record<string, string> };

/**
 * Comments are stripped before scanning.
 *
 * Not a nicety: the first version of this guard failed on `json.ts`, whose doc
 * comment explains the hazard using `new Date()` as the example. A guard that
 * fires on documentation of the thing it forbids is a guard someone eventually
 * turns off.
 */
function stripComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/.*$/gm, '$1');
}

function sources(): Array<{ name: string; text: string }> {
  return readdirSync(srcDir)
    .filter((f) => f.endsWith('.ts'))
    .map((name) => ({ name, text: stripComments(readFileSync(`${srcDir}/${name}`, 'utf8')) }));
}

describe('@stepd/sdk-core has no I/O and no framework', () => {
  it('has sources to check', () => {
    expect(sources().length).toBeGreaterThanOrEqual(6);
  });

  it.each(sources())('$name imports nothing from Node', ({ text }) => {
    expect(text).not.toMatch(/from\s+['"]node:/);
    expect(text).not.toMatch(/require\(['"]node:/);
  });

  it.each(sources())('$name reaches for no I/O global', ({ text }) => {
    for (const forbidden of ['fetch(', 'XMLHttpRequest', 'WebSocket', 'process.env']) {
      expect(text).not.toContain(forbidden);
    }
  });

  it.each(sources())('$name reads no clock of its own', ({ text, name }) => {
    // `sleep` needs an instant, and takes it from `Ctx`'s injected `now`. A
    // module reaching for the real clock could not be tested at a boundary, and
    // `ctx.sleep` is exactly the op where the boundary is the interesting part.
    if (name === 'ctx.ts') {
      // The one permitted use: the default injected into the constructor.
      expect(text.match(/new Date\(\)/g) ?? []).toHaveLength(1);
      return;
    }
    expect(text).not.toContain('Date.now()');
    expect(text).not.toContain('new Date()');
  });

  it('depends on the protocol package and nothing else', () => {
    expect(Object.keys(pkg.dependencies ?? {})).toEqual(['@stepd/protocol']);
  });
});
