# ADR-011: Durable run inbox and the early-signal guarantee

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

A run suspends on `wait_event`. The event it is waiting for can arrive before the handler
ever reaches that line — a child finishing early, an approval clicked while the run is still
charging a card, a `signal` from a sibling run. A naive implementation matches events only
against waits that are already registered, so the event is dropped and the run parks until
its timeout, or forever if none was set. Gap A1, severity S1: nothing errors, nothing is
logged, and the run simply never finishes.

The subtler half is a transaction race rather than an ordering one. `commit_ops` registers a
wait; delivery inserts an event and looks for a wait to resolve. If those two transactions
interleave — delivery checks for waits *after* `commit_ops` has read the inbox but *before*
it has inserted the wait row — each concludes the other side is absent. The event sits
unconsumed and the run sits parked. `reference/force_race.sh` constructs exactly that
interleaving with a `pg_sleep` in the middle of the wait registration.

Forced-interleaving testing found that stepd was already surviving this race — **for a reason
nobody had written down**. The foreign key `run_inbox.run_id -> runs.id` makes an inbox
`INSERT` take `FOR KEY SHARE` on the run row, which happens to conflict with the `FOR UPDATE`
that `commit_ops` holds. Correct behaviour, wrong reason. Dropping the FK, deferring it,
partitioning `run_inbox`, or weakening the lock in `commit_ops` would each have reopened an
R1 race with no test failing anywhere. This is the origin of the project's standing risk 0:
passing tests do not tell you *why* they pass.

## Decision

**Every run has a durable inbox, and the run row is the explicit, documented serialisation
point for everything that resolves a wait.**

1. `run_inbox` buffers events directed at a run regardless of whether a wait is registered.
   `wait_event` with the default `since: run_start` matches anything received since the run
   began, so arrival order stops mattering.
2. `commit_ops` checks the inbox **first, in the same transaction** that would otherwise
   register the wait (migration `0006_engine_complete.sql`, the `wait_event` branch). On a
   match the step is recorded `completed` immediately and the run never suspends.
3. Both `deliver_to_inbox` and `commit_ops` take `SELECT … FROM runs WHERE id = … FOR UPDATE`
   **as their first statement**. Not because the lock is needed for the row's data, but
   because it is the mutual exclusion. Migration `0003_signal_race.sql` introduced this and
   says so in a `COMMENT ON FUNCTION` that survives into `pg_proc`.
4. The property is asserted **structurally**, not only behaviourally. `test_invariants.sql`
   reads `prosrc` and fails the build if the lock is missing, or if it appears after the
   `INSERT INTO run_inbox`. A behavioural test cannot notice a lock that quietly stopped
   being taken; a structural one can.
5. Consumption rules: FIFO by receipt, one entry per wait, sender-deduplicated on
   `(sender_run_id, sender_step_hash)` so a retried `signal` never double-delivers, bounded
   at `inbox_depth` with the oldest dropped and an `inbox_overflow` counter incremented —
   because an overflow that leaves no trace destroys the only evidence it happened.
6. `signal` ops are recorded into `signal_outbox` and delivered by `drain_signals`, never
   inline. Delivering inline would take the target's run lock while holding the sender's, so
   two runs signalling each other deadlock. Deferring removes the class of bug instead of
   relying on PostgreSQL's deadlock detector to tidy up.

## Consequences

### What this makes easy
* Handlers need no defensive ordering. "Send the approval, then start the run" and "start the
  run, then send the approval" behave identically.
* A wait that can already be satisfied resolves inside the commit and the run is requeued at
  once, so the common case costs no suspension and no timer.
* Retried senders are free: at-least-once signal delivery is safe because the inbox dedupes.

### What this makes hard
* Every path that resolves a wait must take the same lock in the same place. `commit_ops`,
  `deliver_to_inbox` and `resolve_child_result` all do, and each is guarded by a structural
  assertion. A fourth path added without the lock would be correct only by luck.
* The lock is on the run row, so all wait-resolving work for one run is serialised. That is
  intentional and it is the cost of the guarantee.

### What we accept
* The inbox is bounded, so a run signalled faster than it consumes **does** lose events. The
  oldest are dropped, the counter is bumped, and operators are expected to alert on it. An
  unbounded inbox would trade a visible drop for an invisible outage.
* `since_registration` remains available and reopens the race for that one wait by design. It
  is correct only when an earlier event of the same type genuinely must not match.
* Signal delivery is asynchronous, so a signal is visible to the target only after
  `drain_signals` runs. A deadlock-free relay was judged worth that latency.

## Alternatives considered

| Option | Why not |
|---|---|
| Keep relying on the `run_inbox` foreign key's incidental lock | It worked, but nothing recorded that it was load-bearing. Any schema change touching the FK reopens an S1 race silently. `reference/prove_fix.sh` drops the FK entirely and re-runs the forced interleaving to prove the explicit lock is what holds. |
| `SERIALIZABLE` isolation on both paths | Pushes the problem onto serialisation failures the whole engine would have to retry, on every commit, for one race. Also degrades badly under the dispatcher's concurrency. |
| Session-scoped advisory locks | Breaks under pgbouncer transaction mode (gap C1). `test_invariants.sql` check 3 fails the build if `pg_advisory` appears anywhere in the schema. |
| Register the wait, then re-check the inbox in a second statement | Still racy without the lock, and a retry loop makes the window smaller rather than closing it. |
| Match events only at ingest, no per-run buffer | Loses anything that arrives while the run is between attempts, which is most of a long run's life. |

## Verification

* `rust/migrations/0003_signal_race.sql` — the fix and the finding that motivated it, in the
  header comment and in `COMMENT ON FUNCTION deliver_to_inbox`.
* `rust/tests/sql/test_invariants.sql` — checks 1 and 2 assert the lock is present and first
  in `deliver_to_inbox` and present in `commit_ops`; check 3 forbids advisory locks; check 5
  asserts the sender-dedupe index; check 11 asserts signals are relayed and that `commit_ops`
  never calls `deliver_to_inbox` while holding its own run lock; check 14 asserts inbox
  overflow bumps a counter.
* `rust/tests/sql/test_engine.sql` — the `R1: early signal` block asserts the wait resolves
  immediately from the inbox, the run never suspends, no wait row is parked and the entry is
  consumed exactly once; `R1: late signal`, `R1: FIFO + one entry per wait` and
  `R1: sender dedupe` cover the remaining consumption rules.
* `rust/tests/sql/test_engine_ops.sql` — the `signal` block asserts a signal to a run already
  parked on a matching wait wakes it (the defect migration 0006 replaced), and that a second
  `drain_signals` leaves exactly one inbox entry.
* `reference/force_race.sh` and `reference/prove_fix.sh` — the forced interleaving, and the
  same interleaving with the foreign key dropped so only the explicit lock can protect it.
* `reference/simulation.py` — property P3, "no lost signal": a run parked on a wait while a
  matching unconsumed inbox entry exists is a violation, checked at every step and at
  quiescence.
