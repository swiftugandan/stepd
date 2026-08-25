import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { sign, stepHash, verify } from '../src/index.js';

/**
 * The other half of `spec/rust/crates/stepd-proto/tests/fixtures.rs`.
 *
 * Both bindings assert against one committed file rather than against each
 * other, so neither can be "corrected" to match the other's bug. If this file
 * and that one both pass, the two implementations agree with the specification's
 * artefact — which is a different and stronger claim than agreeing with each
 * other.
 */

interface HashCase {
  why: string;
  function_id: string;
  step_id: string;
  occurrence: number;
  hash: string;
}
interface SigCase {
  why: string;
  key: string;
  body: string;
  timestamp: number;
  nonce: string;
  header: string;
}

const fixtures = JSON.parse(
  readFileSync(fileURLToPath(new URL('../../fixtures/protocol.json', import.meta.url)), 'utf8'),
) as { step_hash: HashCase[]; signature: SigCase[] };

describe('step hash, against the committed vectors', () => {
  it('has vectors to check', () => {
    // A fixture file that failed to load, or was emptied, would make every
    // `it.each` below vanish and the suite pass with nothing run.
    expect(fixtures.step_hash.length).toBeGreaterThanOrEqual(12);
  });

  it.each(fixtures.step_hash)('$why — ($function_id, $step_id, $occurrence)', (c) => {
    expect(stepHash(c.function_id, c.step_id, c.occurrence)).toBe(c.hash);
  });
});

describe('request signature, against the committed vectors', () => {
  it('has vectors to check', () => {
    expect(fixtures.signature.length).toBeGreaterThanOrEqual(8);
  });

  it.each(fixtures.signature)('$why', (c) => {
    expect(sign(c.key, c.body, c.timestamp, c.nonce)).toBe(c.header);
  });

  it.each(fixtures.signature)('$why — and verifies', (c) => {
    const result = verify([c.key], c.header, c.body, c.timestamp);
    expect(result).toEqual({ ok: true, nonce: c.nonce, timestamp: c.timestamp });
  });

  it.each(fixtures.signature)('$why — and does not verify against a changed body', (c) => {
    const result = verify([c.key], c.header, `${c.body} `, c.timestamp);
    expect(result).toEqual({ ok: false, reason: 'mismatch' });
  });
});
