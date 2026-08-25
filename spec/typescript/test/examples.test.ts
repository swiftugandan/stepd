import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  decodeAttempt,
  validateEnvelope,
  type AppManifest,
  type AttemptResponse,
  type BlobReserveRequest,
  type BlobReserveResponse,
  type ConformanceManifest,
  type FunctionConfig,
  type ProblemBody,
} from '../src/index.js';

/**
 * The TypeScript half of `spec/rust/crates/stepd-proto/tests/examples.rs`.
 *
 * Committed examples are the documents. This file, the Rust crate, and
 * `spec/validate.py` all have to accept them. A pass here alone would only show
 * this decoder agrees with itself.
 */

const examplesDir = fileURLToPath(new URL('../../examples', import.meta.url));

function read(name: string): unknown {
  return JSON.parse(readFileSync(`${examplesDir}/${name}`, 'utf8'));
}

describe('committed attempt-request example', () => {
  it('decodes as the server sends it', () => {
    const r = decodeAttempt(read('attempt-request.example.json'));
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.attempt.fence).toBe(4);
    expect(r.attempt.run.lineage_id).toBe(r.attempt.run.id);
    expect(r.attempt.run.key).toBe('order:4711');
    expect(r.attempt.events[0]?.stepdkey).toBe('order:4711');
    expect(r.attempt.events[0]?.stepdidempotency).toBe('4711');
    expect('key' in (r.attempt.events[0] ?? {})).toBe(false);
  });
});

describe('committed attempt-response examples', () => {
  const names = readdirSync(examplesDir)
    .filter((n) => n.startsWith('attempt-response-') && n.endsWith('.example.json'))
    .sort();

  it('has the committed envelopes to check', () => {
    expect(names.length).toBeGreaterThanOrEqual(8);
  });

  it.each(names)('%s is a well-formed envelope', (name) => {
    const raw = read(name) as AttemptResponse;
    expect(validateEnvelope(raw)).toBeNull();
  });
});

describe('remaining wire examples', () => {
  it('app-manifest.example.json is an AppManifest', () => {
    const m = read('app-manifest.example.json') as AppManifest;
    expect(m.protocol).toBe('1');
    expect(m.app_id).toBe('billing');
    expect(m.functions).toHaveLength(1);
    expect(m.functions[0].triggers.length).toBeGreaterThan(0);
  });

  it('function-config-cron.example.json is a FunctionConfig', () => {
    const f = read('function-config-cron.example.json') as FunctionConfig;
    expect(f.id).toBe('nightly-billing');
    expect(f.triggers).toHaveLength(2);
    expect(f.triggers.every((t) => t.type === 'cron')).toBe(true);
  });

  it('blob-reserve examples parse as request and response', () => {
    const req = read('blob-reserve-request.example.json') as BlobReserveRequest;
    expect(req.size).toBe(184320);
    expect(req.sha256).toHaveLength(64);
    const res = read('blob-reserve-response.example.json') as BlobReserveResponse;
    expect(res.deduplicated).toBe(false);
    expect(res.upload_url).toBeDefined();
    const dedup = read('blob-reserve-response-dedup.example.json') as BlobReserveResponse;
    expect(dedup.deduplicated).toBe(true);
    expect(dedup.upload_url).toBeUndefined();
  });

  it('problem.example.json is a Problem Details body', () => {
    const p = read('problem.example.json') as ProblemBody;
    expect(p.status).toBe(400);
    expect(p.code).toBe('bad_cursor');
  });

  it('conformance-manifest.example.json is a ConformanceManifest', () => {
    const m = read('conformance-manifest.example.json') as ConformanceManifest;
    expect(m.protocol).toBe('1');
    expect(m.suites).toContain('memoization');
    expect(m.statically_prevented).toContain('offpath_claim');
  });
});
