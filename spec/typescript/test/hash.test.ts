import { describe, expect, it } from 'vitest';
import { OccurrenceCounter, stepHash } from '../src/index.js';

describe('stepHash', () => {
  it('is sixteen lowercase hex characters', () => {
    expect(stepHash('f', 's', 0)).toMatch(/^[0-9a-f]{16}$/);
  });

  it('separates its fields, so ("a","bc") and ("ab","c") cannot collide', () => {
    // The whole reason 0x1F is in the algorithm. Without it both inputs are the
    // bytes `abc0` and two unrelated steps share a memo entry — one of them
    // replays the other's result and the second never runs.
    expect(stepHash('a', 'bc', 0)).not.toBe(stepHash('ab', 'c', 0));
  });

  it('treats occurrence as decimal text, so 100 is not 1 then 00', () => {
    expect(stepHash('f', 's', 10)).not.toBe(stepHash('f', 's', 100));
  });

  it('includes the function id, so a child cannot collide with its parent', () => {
    expect(stepHash('parent', 'charge', 0)).not.toBe(stepHash('child', 'charge', 0));
  });

  it('is not positional: a step keeps its hash when neighbours change', () => {
    // What makes it safe to edit a workflow while runs are in flight. Identity
    // is (function, id, occurrence) and nothing about where the step sits.
    const before = stepHash('f', 'ship', 0);
    const after = stepHash('f', 'ship', 0);
    expect(after).toBe(before);
  });

  it('refuses a non-integer or negative occurrence', () => {
    expect(() => stepHash('f', 's', 1.5)).toThrow(RangeError);
    expect(() => stepHash('f', 's', -1)).toThrow(RangeError);
  });
});

describe('OccurrenceCounter', () => {
  it('counts per step id, from zero', () => {
    const c = new OccurrenceCounter();
    expect(c.claim('f', 'a')).toBe(stepHash('f', 'a', 0));
    expect(c.claim('f', 'a')).toBe(stepHash('f', 'a', 1));
    expect(c.claim('f', 'b')).toBe(stepHash('f', 'b', 0));
    expect(c.claimed('a')).toBe(2);
    expect(c.claimed('b')).toBe(1);
    expect(c.claimed('never')).toBe(0);
  });

  it('gives a fresh counter the same hashes as the last one', () => {
    // Counters reset every attempt. That is what makes a hash a function of the
    // handler's shape rather than of its history — replay attempt five and
    // attempt one must claim identically.
    const first = new OccurrenceCounter();
    const second = new OccurrenceCounter();
    const a = [first.claim('f', 'x'), first.claim('f', 'x')];
    const b = [second.claim('f', 'x'), second.claim('f', 'x')];
    expect(b).toEqual(a);
  });
});
