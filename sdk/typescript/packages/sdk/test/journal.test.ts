import type { RecordedStep } from '@stepd/protocol';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { JournalSource } from '../src/index.js';

const step = (id: string): RecordedStep => ({ id, op: 'step', status: 'completed', data: id });

afterEach(() => {
  vi.unstubAllGlobals();
});

function pages(...responses: Array<{ steps: Record<string, RecordedStep>; next: string | null }>) {
  const urls: string[] = [];
  let i = 0;
  vi.stubGlobal('fetch', async (input: string | URL) => {
    urls.push(String(input));
    return Response.json(responses[i++] ?? { steps: {}, next: null });
  });
  return urls;
}

describe('§8.6 paging', () => {
  it('refuses when it has nowhere to page from', async () => {
    // The only alternative to fetching. Replaying against a partial journal
    // re-executes every step the app could not see, the run still completes, and
    // nothing errors — so "proceed on what we hold" is never an option.
    const j = new JournalSource();
    expect(j.configured).toBe(false);
    await expect(j.fetchRemaining('run-1', {})).rejects.toThrow(/§8.6/);
  });

  it('pages from the highest hash already held', async () => {
    // The server orders by hash and sends the lowest n, so the cursor is the
    // highest one in hand. An unordered boundary would drop or repeat steps
    // between requests, and either corrupts a replay silently.
    const urls = pages(
      { steps: { cccc: step('c') }, next: 'cccc' },
      { steps: { dddd: step('d') }, next: null },
    );

    const j = new JournalSource();
    j.configure('http://stepd/', 'tok');
    const merged = await j.fetchRemaining('run-1', { aaaa: step('a'), bbbb: step('b') });

    expect(Object.keys(merged).sort()).toEqual(['aaaa', 'bbbb', 'cccc', 'dddd']);
    expect(urls[0]).toContain('after=bbbb');
    expect(urls[0]).toContain('limit=500');
    expect(urls[1]).toContain('after=cccc');
  });

  it('sends no cursor when it holds nothing', async () => {
    const urls = pages({ steps: { aaaa: step('a') }, next: null });
    const j = new JournalSource();
    j.configure('http://stepd', 'tok');
    await j.fetchRemaining('run-1', {});
    expect(urls[0]).not.toContain('after=');
  });

  it('authenticates every page', async () => {
    const seen: Array<string | null> = [];
    vi.stubGlobal('fetch', async (_input: string | URL, init: RequestInit = {}) => {
      seen.push(new Headers(init.headers).get('authorization'));
      return Response.json({ steps: {}, next: null });
    });
    const j = new JournalSource();
    j.configure('http://stepd', 'tok');
    await j.fetchRemaining('run-1', {});
    expect(seen).toEqual(['Bearer tok']);
  });

  it('refuses a repeated cursor rather than looping forever', async () => {
    // A server echoing the same `next` would otherwise be an infinite loop that
    // looks like a hang rather than a fault.
    pages(
      { steps: { cccc: step('c') }, next: 'cccc' },
      { steps: { cccc: step('c') }, next: 'cccc' },
    );
    const j = new JournalSource();
    j.configure('http://stepd', 'tok');
    await expect(j.fetchRemaining('run-1', {})).rejects.toThrow(/repeated the page cursor/);
  });

  it('surfaces a server error rather than replaying a partial journal', async () => {
    vi.stubGlobal('fetch', async () => new Response('nope', { status: 403 }));
    const j = new JournalSource();
    j.configure('http://stepd', 'tok');
    await expect(j.fetchRemaining('run-1', {})).rejects.toThrow(/403/);
  });
});
