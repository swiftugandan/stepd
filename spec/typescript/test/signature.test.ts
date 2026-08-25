import { describe, expect, it } from 'vitest';
import { DEFAULT_TOLERANCE_SECS, sign, verify } from '../src/index.js';

const KEY = 'secret';
const BODY = '{"protocol":"1","ops":[]}';

describe('verify', () => {
  it('accepts a signature it just made, and returns the nonce', () => {
    const header = sign(KEY, BODY, 1000, 'abc');
    expect(verify([KEY], header, BODY, 1000)).toEqual({
      ok: true,
      nonce: 'abc',
      timestamp: 1000,
    });
  });

  it('rejects a body changed by one byte', () => {
    const header = sign(KEY, BODY, 1000, 'abc');
    expect(verify([KEY], header, `${BODY} `, 1000)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('rejects a swapped nonce, because the nonce is inside the MAC', () => {
    // Otherwise a captured body could be replayed with a fresh nonce and sail
    // past any seen-nonce cache.
    const header = sign(KEY, BODY, 1000, 'abc').replace('n=abc', 'n=xyz');
    expect(verify([KEY], header, BODY, 1000)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('rejects the wrong key', () => {
    const header = sign(KEY, BODY, 1000, 'abc');
    expect(verify(['other'], header, BODY, 1000)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('accepts any of several keys, so rotation needs no flag day', () => {
    const header = sign('new', BODY, 1000, 'abc');
    expect(verify(['old', 'new'], header, BODY, 1000).ok).toBe(true);
  });

  it('is symmetric about the tolerance window', () => {
    const header = sign(KEY, BODY, 1000, 'abc');
    expect(verify([KEY], header, BODY, 1000 + DEFAULT_TOLERANCE_SECS).ok).toBe(true);
    expect(verify([KEY], header, BODY, 1000 - DEFAULT_TOLERANCE_SECS).ok).toBe(true);
    // A timestamp far in the future is as suspicious as one far in the past;
    // accepting it would widen the replay window arbitrarily.
    expect(verify([KEY], header, BODY, 1000 + DEFAULT_TOLERANCE_SECS + 1)).toEqual({
      ok: false,
      reason: 'expired',
    });
    expect(verify([KEY], header, BODY, 1000 - DEFAULT_TOLERANCE_SECS - 1)).toEqual({
      ok: false,
      reason: 'expired',
    });
  });

  it('calls a header missing any component malformed, not mismatched', () => {
    // The distinction matters to whoever reads the log: malformed is a sender
    // that is not speaking this protocol, mismatch is one with the wrong key.
    for (const header of ['', 't=1', 'n=a,v1=ff', 't=1,v1=ff', 't=1,n=a', 'garbage']) {
      expect(verify([KEY], header, BODY, 1)).toEqual({ ok: false, reason: 'malformed' });
    }
  });

  it('calls a non-hex MAC malformed', () => {
    expect(verify([KEY], 't=1,n=a,v1=zz', BODY, 1)).toEqual({ ok: false, reason: 'malformed' });
    expect(verify([KEY], 't=1,n=a,v1=abc', BODY, 1)).toEqual({ ok: false, reason: 'malformed' });
  });

  it('ignores unknown components, per §11', () => {
    const header = `${sign(KEY, BODY, 1000, 'abc')},v2=future`;
    expect(verify([KEY], header, BODY, 1000).ok).toBe(true);
  });

  it('rejects a non-integer timestamp rather than coercing it', () => {
    expect(verify([KEY], 't=1.5,n=a,v1=ff', BODY, 1)).toEqual({ ok: false, reason: 'malformed' });
  });

  it('signs an empty body', () => {
    const header = sign(KEY, '', 1000, 'abc');
    expect(verify([KEY], header, '', 1000).ok).toBe(true);
  });

  it('accepts a raw key as bytes as well as a string', () => {
    const bytes = new TextEncoder().encode(KEY);
    const header = sign(bytes, BODY, 1000, 'abc');
    expect(verify([KEY], header, BODY, 1000).ok).toBe(true);
    expect(verify([bytes], header, BODY, 1000).ok).toBe(true);
  });
});
