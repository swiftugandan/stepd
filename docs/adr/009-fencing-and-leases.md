# ADR-009: Fencing tokens and lease-based work claiming

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Recorded as **Proposed** in PRD §14; it is now implemented in the schema and the engine and
covered by live-database, in-memory and simulation tests, so the status is raised on that
evidence.

stepd runs as several replicas against one Postgres. A replica claims a run, calls the app,
and commits the ops it gets back. Between claim and commit anything can happen: the replica
is paused by the kernel, partitioned from the database, GC'd by its orchestrator, or simply
slow because the app it is calling is slow.

The dangerous case is not death but the *slow* replica presumed dead. Another worker takes
the run over and drives it forward — new steps recorded, timers scheduled, events emitted —
and then the first replica wakes holding a response describing a world three steps in the
past, and commits it. Nothing about that response is malformed. Applying it interleaves two
attempts' journals for one run, silently: no error, no alert, just a run whose recorded
history never happened.

A lease alone does not fix this. A lease with an expiry says who *should* be working, not
whether a write arriving now came from the current holder; any check that reads the lease
and then writes races the reclaim that happened in between.

## Decision

**Every run carries a monotonic `fence_token`, and claiming bumps it.**
`runs.fence_token bigint NOT NULL DEFAULT 0` (`engine/rust/migrations/0001_initial.sql`).
`claim_runs_ns` (`engine/rust/migrations/0006_engine_complete.sql`) selects claimable queue rows
with `FOR UPDATE SKIP LOCKED`, marks them `claimed_by`/`claimed_until`, and in the same
statement sets `fence_token = fence_token + 1`, `attempt_no = attempt_no + 1`,
`lease_owner`, `lease_until` and `status = 'running'` — one statement, so there is no window
in which a run is leased but unfenced. The token travels to the app in the `stepd-fence`
header (protocol §2.1, §7.3).

**The fence is checked inside `commit_ops`, under the run row lock, before anything is
written.** The function's first statement is `SELECT … FROM runs WHERE id = p_run_id FOR
UPDATE`; the very next checks are `no_such_run`, then `fence_token <> p_fence →
'stale_fence'`, then terminal status. Order is the point: reading the fence outside the lock
makes the check advisory rather than binding, so `tests/sql/test_invariants.sql` fails the
build if `FOR UPDATE` appears after the fence comparison in the function source.

**A stale attempt writes nothing at all.** `commit_ops` returns before its first insert, so
there is no partial journal to reconcile; `CommitOutcome::StaleFence` reaches the dispatcher,
which counts it and moves on.

**Lease expiry is a separate convergence sweep.** `reclaim_expired_leases` releases queue
rows whose `claimed_until` has passed and returns their runs to `pending`. It lives in the
`Housekeeping` trait and runs in the `Housekeeper` loop rather than in `tick`, because
reclamation must keep working when no app is reachable at all — otherwise a total app outage
also stops the system healing itself. The reclaim does not need to bump the fence: the claim
already did, so the dead worker's response was dead the moment the run was re-dispatched.

**Claiming uses row-level locks only** — `FOR UPDATE SKIP LOCKED`, never session-scoped
advisory locks — so the engine stays safe behind a transaction-mode pooler (F-DL-1,
ADR-019), and claims are namespace-scoped, because a namespace-blind claim lets one tenant's
backlog starve every other however the caller sequences its calls (F-LP-5).

## Consequences

### What this makes easy
* Abrupt death is safe: a replica can be SIGKILLed mid-attempt and the worst outcome is one
  duplicated step execution, never a corrupted journal (twelve-factor IX).
* Lease duration becomes a tuning parameter, not a correctness parameter. Too short costs
  duplicated work; it cannot cost consistency.
* Fence monotonicity is one column and one comparison, which is why it can be asserted as a
  simulation invariant (P6) across thousands of fault-injected seeds.
* The dispatcher needs no notion of "am I still the owner?" — it commits and reads the
  answer.

### What this makes hard
* Every commit path must thread the fence, and a future store implementing
  `StateStore::commit` gets no compile-time reminder that skipping the check is
  catastrophic: the signature takes `fence: i64` and nothing forces its use. The invariant
  test guards the SQL; a second backend needs its own equivalent. Long attempts must also
  heartbeat (`Queue::heartbeat`), or they are re-dispatched under their own holder.

### What we accept
* **Fencing protects the record, not the side effect.** A superseded attempt's step may
  already have charged a card (protocol §7.1.1): the work happened twice, the record is
  written once, and steps must be idempotent. This is not exactly-once execution and must
  never be described as such.
* A lease expiring while its holder is alive and healthy causes real duplicate execution.
  The 60-second default is a guess about attempt latency, and getting it wrong is paid for
  in duplicated work — the failure mode this design chooses to have.
* `reclaim_expired_leases` returns runs to `pending` with no backoff, so a run whose attempts
  consistently outlive the lease is re-dispatched in a tight loop until quarantine (F-LP-6)
  catches it.

## Alternatives considered

| Option | Why not |
|---|---|
| Lease with expiry, no fence | The check-then-write window is exactly where the failure lives. A commit that verified `lease_owner` and then wrote would race the reclaim between the two statements. |
| Session-scoped advisory locks per run | Incompatible with transaction-mode pgbouncer: the lock is held by a session the pooler may hand to someone else (F-DL-1). Also invisible to the reclaim sweep. |
| Optimistic concurrency on a row version | That is a fence, spelled less clearly. The token is explicit because it also travels to the app and appears in the `409` contract. |
| Trust the worker to notice it lost the lease | Requires the worker to be running well enough to notice, which is precisely what is in doubt. |

## Verification

* `engine/rust/crates/stepd-store-postgres/tests/live.rs`, `a_superseded_attempt_cannot_commit`:
  claims a run, releases it, reclaims it as a second worker, asserts `second.fence >
  first.fence`, then commits at the *old* fence and asserts `CommitOutcome::StaleFence` —
  and separately that `steps_page` is empty, so a stale attempt wrote nothing at all. The
  same ops then commit at the new fence.
* Same file, `an_expired_lease_returns_its_run_to_the_queue`: a worker claims with a 1 ms
  lease and dies; nobody else can claim until `reclaim_expired_leases` runs, and the retaken
  lease carries a higher fence.
* `engine/rust/tests/sql/test_invariants.sql`, assertion 8: fails the build if
  `position('FOR UPDATE') > position('fence_token <> p_fence')` in the `commit_ops` source —
  the countermeasure to the project's own finding that correctness can rest on undocumented
  ordering.
* `engine/rust/crates/stepd-core/tests/engine.rs`, `stale_fence_response_is_discarded` and
  `duplicate_commit_records_a_step_once`: the same property against in-memory doubles.
* `engine/rust/crates/stepd-store-postgres/tests/simulation.rs`, property **P6** (fence
  monotonicity): a stale fence never mutates state, asserted under injected faults across
  the seed budget.
* `engine/rust/crates/stepd-core/src/dispatcher.rs`: `StaleFence` increments `stats.stale` and logs
  at debug — an expected outcome, not an error path.
