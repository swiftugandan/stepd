import type { RecordedStep } from '@stepd/protocol';

/**
 * Paging a truncated journal (§8.6).
 *
 * A run whose journal outgrows the server's inline ceiling arrives with
 * `state_truncated` and only part of its steps. Replaying against that partial
 * journal is the worst available outcome: every step the app cannot see
 * re-executes, the run still completes, and nothing anywhere errors. It is the
 * same failure mode as an unstable step hash, reached by a different route.
 *
 * So there are exactly two acceptable behaviours — fetch the rest before
 * replaying, or fail the attempt non-retryably. An SDK with nowhere to fetch
 * from must still refuse.
 */
export class JournalSource {
  #endpoint: { base: string; token: string } | undefined;

  /** Point it at a stepd server. The token needs the operator role. */
  configure(baseUrl: string, token: string): void {
    this.#endpoint = { base: baseUrl.replace(/\/+$/, ''), token };
  }

  get configured(): boolean {
    return this.#endpoint !== undefined;
  }

  /**
   * Fetch every step the attempt did not carry, merged with what it did.
   *
   * The server orders by hash and sends the lowest `n`, so the cursor is the
   * highest hash already held. An unordered page boundary would drop or repeat
   * steps between requests, and either one corrupts a replay silently.
   */
  async fetchRemaining(
    runId: string,
    have: Record<string, RecordedStep>,
  ): Promise<Record<string, RecordedStep>> {
    const endpoint = this.#endpoint;
    if (endpoint === undefined) {
      throw new Error(
        'the attempt journal was truncated and this app has no server address to page it ' +
          'from; pass `journal` to the App (protocol §8.6)',
      );
    }

    const merged: Record<string, RecordedStep> = { ...have };
    let cursor: string | undefined = Object.keys(have).sort().at(-1);

    // Bounded, so a server that always answers `next` cannot spin here forever.
    // The engine's per-run ceiling is 10 000 steps and the page size is 500, so
    // 400 pages is far past any real journal.
    for (let page = 0; page < 400; page++) {
      const url = new URL(`${endpoint.base}/v1/runs/${runId}/steps`);
      url.searchParams.set('limit', '500');
      if (cursor !== undefined) url.searchParams.set('after', cursor);

      const res = await fetch(url, { headers: { authorization: `Bearer ${endpoint.token}` } });
      if (!res.ok) {
        throw new Error(`the server answered ${res.status} paging the journal from ${url.href}`);
      }
      const body = (await res.json()) as {
        steps?: Record<string, RecordedStep>;
        next?: string | null;
      };
      Object.assign(merged, body.steps ?? {});

      const next = body.next ?? undefined;
      if (next === undefined) return merged;
      // The cursor must advance. A server echoing the same `next` would
      // otherwise be an infinite loop that looks like a hang rather than a fault.
      if (next === cursor) {
        throw new Error(`the server repeated the page cursor while paging ${url.href}`);
      }
      cursor = next;
    }
    throw new Error(`gave up paging the journal for run ${runId} after 400 pages`);
  }
}
