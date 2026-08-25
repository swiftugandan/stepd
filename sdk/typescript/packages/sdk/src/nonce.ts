import { DEFAULT_TOLERANCE_SECS } from '@stepd/protocol';

/**
 * Bounded cache of recently-seen nonces.
 *
 * A timestamp window alone permits replay of a captured body for the width of
 * that window, so a receiver has to remember what it has already accepted.
 *
 * Two properties are load-bearing and neither is obvious:
 *
 * * **Entries are inserted only after the MAC verifies.** The caller does that
 *   ordering; this class cannot enforce it. Inserting an unverified nonce would
 *   let an attacker poison the cache with values they never had a signature for
 *   and lock out the legitimate sender's next request.
 * * **It forgets what can no longer be replayed.** Anything older than the
 *   window has an expired signature, so keeping it buys nothing and the cache
 *   would grow without bound.
 *
 * Per-replica is sufficient: fencing covers a replay that reaches a different
 * one.
 */
export class NonceCache {
  readonly #seen = new Map<string, number>();

  constructor(
    private readonly windowSecs: number = DEFAULT_TOLERANCE_SECS,
    /** Hard ceiling, in case a sender floods with distinct nonces. */
    private readonly capacity: number = 100_000,
  ) {}

  /** Record `nonce`, or report that it has been seen inside the window. */
  checkAndInsert(nonce: string, nowUnix: number): boolean {
    this.#evict(nowUnix);
    if (this.#seen.has(nonce)) return false;
    if (this.#seen.size >= this.capacity) {
      // Oldest first. Map iterates in insertion order, which is close enough to
      // time order for a cache whose entries all expire on the same schedule.
      const oldest = this.#seen.keys().next();
      if (!oldest.done) this.#seen.delete(oldest.value);
    }
    this.#seen.set(nonce, nowUnix);
    return true;
  }

  get size(): number {
    return this.#seen.size;
  }

  #evict(nowUnix: number): void {
    for (const [nonce, at] of this.#seen) {
      if (nowUnix - at > this.windowSecs) this.#seen.delete(nonce);
      // Insertion order is time order, so the first entry still inside the
      // window means every later one is too.
      else break;
    }
  }
}
