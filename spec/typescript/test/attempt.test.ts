import { describe, expect, it } from 'vitest';
import { decodeAttempt } from '../src/index.js';

const base = {
  protocol: '1',
  attempt: 1,
  fence: 7,
  run: {
    id: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10',
    function_id: 'order-fulfilment',
    namespace: 'prod',
    started_at: '2026-08-19T10:00:00Z',
    lineage_id: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10',
  },
};

function ok(raw: unknown) {
  const r = decodeAttempt(raw);
  if (!r.ok) throw new Error(`expected a decode, got ${JSON.stringify(r.error)}`);
  return r.attempt;
}

describe('decodeAttempt', () => {
  it('reads the shape the server actually sends', () => {
    const a = ok(base);
    expect(a.fence).toBe(7);
    expect(a.run.function_id).toBe('order-fulfilment');
    expect(a.steps).toEqual({});
    expect(a.events).toEqual([]);
    expect(a.state_truncated).toBe(false);
    expect(a.run.cancelling).toBe(false);
  });

  it('rejects a string fence: the wire is an integer', () => {
    expect(decodeAttempt({ ...base, fence: '7' })).toEqual({
      ok: false,
      error: { kind: 'bad_field', field: 'fence', why: expect.any(String) },
    });
  });

  it('rejects a fence that is not an integer', () => {
    const r = decodeAttempt({ ...base, fence: 'seven' });
    expect(r).toEqual({
      ok: false,
      error: { kind: 'bad_field', field: 'fence', why: expect.any(String) },
    });
  });

  it('rejects the wrong protocol version rather than trying', () => {
    expect(decodeAttempt({ ...base, protocol: '2' })).toEqual({
      ok: false,
      error: { kind: 'bad_version', protocol: '2' },
    });
  });

  it('names the missing field', () => {
    const { function_id: _drop, ...run } = base.run;
    expect(decodeAttempt({ ...base, run })).toEqual({
      ok: false,
      error: { kind: 'missing', field: 'run.function_id' },
    });
  });

  it('names a missing lineage_id rather than guessing', () => {
    const { lineage_id: _drop, ...run } = base.run;
    expect(decodeAttempt({ ...base, run })).toEqual({
      ok: false,
      error: { kind: 'missing', field: 'run.lineage_id' },
    });
  });

  it('ignores unknown fields, per §11', () => {
    expect(ok({ ...base, invented_by_a_newer_minor: true }).attempt).toBe(1);
  });

  it('rejects a non-object', () => {
    for (const raw of [null, 'x', 42, []]) {
      expect(decodeAttempt(raw)).toEqual({ ok: false, error: { kind: 'not_an_object' } });
    }
  });

  it('carries state_truncated through, because ignoring it re-executes steps', () => {
    expect(ok({ ...base, state_truncated: true }).state_truncated).toBe(true);
  });

  it('keeps the CloudEvents extension names on events, not the Rust field names', () => {
    // Ingest reads `stepdkey`. A TS Event that used `key` would schema-validate
    // (additionalProperties) and the engine would ignore the field (§11).
    const a = ok({
      ...base,
      events: [
        {
          specversion: '1.0',
          id: 'evt_01',
          source: '/shop/checkout',
          type: 'order.created',
          data: { order_id: 4711 },
          stepdkey: 'order:4711',
          stepdidempotency: '4711',
        },
      ],
    });
    expect(a.events[0]?.stepdkey).toBe('order:4711');
    expect(a.events[0]?.stepdidempotency).toBe('4711');
    expect('key' in (a.events[0] ?? {})).toBe(false);
    expect('idempotency' in (a.events[0] ?? {})).toBe(false);
  });
});
