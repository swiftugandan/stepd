# ADR-019: Pooler-safe locking; no session-scoped advisory locks

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Most production Postgres deployments sit behind pgbouncer in transaction mode,
where a client connection is bound to a backend only for the duration of a
transaction and handed to someone else afterwards. Anything scoped to a *session*
therefore has no owner: `pg_advisory_lock`, `SET` outside `SET LOCAL`, temporary
tables, prepared statements, `LISTEN`/`NOTIFY`.

Session-scoped advisory locks are the natural way to write a work queue, which is
what makes this dangerous. `pg_advisory_lock(run_id)` reads as "hold this run
while I work on it" and behaves that way in development, where the engine talks
to Postgres directly. Behind a transaction-mode pooler it does not fail — it
*succeeds on the wrong connection*. Two workers acquire what each believes is the
same exclusive lock, both proceed, and the run executes twice with no error
anywhere. Gap C1 rates this S2 for exactly that reason: duplicate side effects in
production and a green suite in CI. The engine cannot dictate the topology — an
operator will put a pooler in front of it — and discovering afterwards that the
engine assumed session affinity is not a recoverable position.

## Decision

**The engine uses row-level and transaction-scoped locking only. No session-scoped
advisory lock appears anywhere, and no code assumes session state survives a
transaction.**

Concretely:

* **Claiming is `FOR UPDATE SKIP LOCKED`.** `claim_runs_ns` selects claimable
  queue rows, updates them and bumps the run's fence in one transaction.
  `SKIP LOCKED` is what makes concurrent workers correct without coordination: a
  contended row is passed over rather than waited on, so N workers progress at N
  times the rate instead of serialising.
* **Every other sweep uses the same pattern.** `reclaim_expired_leases`,
  `drain_signals` and `fire_due_timers` select their batch with
  `FOR UPDATE SKIP LOCKED` under a `LIMIT`, so no sweep holds locks for as long
  as a backlog demands.
* **Serialisation between the commit path and signal delivery is a row lock on
  `runs`,** taken as the first statement of both `commit_ops` and
  `deliver_to_inbox`. That is the lock closing the lost-signal race, and being
  transaction-scoped it releases correctly however the connection is pooled.
* **Exclusivity that must not rest on a lock at all is an index:** keyed
  exclusivity is the partial unique index `runs_singleton_key`, a database
  invariant rather than something application logic must remember.
* **Signals are relayed, never delivered inline.** Inline delivery takes the
  target's row lock while holding the sender's, so two runs signalling each other
  deadlock; `signal_outbox` plus `drain_signals` removes the class of bug rather
  than relying on the deadlock detector. **And the absence of advisory locks is
  asserted structurally**, not left to review: a test scans `pg_proc` for
  `pg_advisory` and fails the build if any function contains it.

## Consequences

### What this makes easy
* The engine deploys behind pgbouncer, pgcat or RDS Proxy in transaction mode
  with no configuration and no caveats, which is how most operators will run it.
* Horizontal scaling is adding workers. There is no leader election, no lock
  server, and no coordination protocol to get wrong.
* A worker that dies holds nothing: its transaction aborts, its row locks
  release, the lease sweep requeues the run, and the fence bumped at claim time
  stops its late response committing.

### What this makes hard
* Any future feature that wants "hold this across several transactions" has no
  primitive available and must be expressed as durable state — a lease column
  with an expiry — instead. That is more code, and it is the code that survives a
  crash.
* `LISTEN`/`NOTIFY` is unavailable, so dispatch polls. The idle poll interval is
  a latency floor when the queue is empty.

### What we accept
* Polling costs a query per idle tick per worker — cheap, not free, and scaling
  with worker count rather than with work.
* The structural test greps function source text, so it would not catch an
  advisory lock taken from Rust rather than from a SQL function. That is one more
  reason the op commit lives in SQL rather than in the store crate.

## Alternatives considered

| Option | Why not |
|---|---|
| `pg_advisory_lock` per run | Silently unsound behind a transaction-mode pooler: the lock is held by a backend the next transaction may not get, so two workers can both "hold" it and the run executes twice with no error. |
| `pg_advisory_xact_lock` | Transaction-scoped and so pooler-safe, but it locks an arbitrary integer rather than the row: nothing structural ties it to the run, and nothing enforces that it is taken. `FOR UPDATE` on the row does both. |
| Document "do not use a pooler" | Operators use poolers. A constraint the deployment will violate is a defect with a paper trail, not a mitigation. |
| `FOR UPDATE` without `SKIP LOCKED` | Workers queue behind the same head row; throughput collapses to one worker's while every worker still holds a connection. |
| External lock service (etcd, Redis) | A second store to operate, a second failure mode, and split-brain against the database that holds the truth. |
| Leader election, single dispatcher | Reintroduces the coordination this design deletes, and makes the dispatcher a single point of failure and a throughput ceiling. |

## Verification

* `engine/rust/migrations/0002_engine.sql` — `claim_runs` claims with `FOR UPDATE SKIP
  LOCKED`; the header comment states "no advisory locks, so this is safe behind a
  transaction-mode pooler (F-DL-1)". `commit_ops` takes `FOR UPDATE` on the run
  row.
* `engine/rust/migrations/0006_engine_complete.sql` — `claim_runs_ns` (namespace-scoped,
  `FOR UPDATE SKIP LOCKED`), `reclaim_expired_leases`, `drain_signals` and
  `fire_due_timers` all use the same pattern with a bounded `LIMIT`;
  `commit_ops` and `resolve_child_result` take `FOR UPDATE` on the run and parent
  rows respectively. The migration header records why signals are relayed rather
  than delivered inline: the lock-ordering deadlock between two runs signalling
  each other.
* `engine/rust/tests/sql/test_invariants.sql` — the structural assertions. Check 3 fails
  the build if any `public` function's `prosrc` matches `%pg_advisory%`
  ("advisory lock found; breaks transaction-mode pooling"). Check 15 asserts
  `claim_runs_ns` still contains `SKIP LOCKED` and still filters by namespace.
  Checks 1, 2 and 8 assert the run-row lock is present in `deliver_to_inbox` and
  `commit_ops`, taken *before* the inbox insert and *before* the fence check.
  Check 11 asserts `commit_ops` still relays through `signal_outbox` and never
  calls `deliver_to_inbox` inline. `engine/rust/tests/sql/test_engine.sql` repeats the
  advisory-lock scan as assertion R2.
* `engine/rust/crates/stepd-cli/src/doctor.rs` — `structural_invariants` runs the same
  three checks against a live database, so a schema migrated by hand or restored
  from an older dump is caught before it silently loses a signal; a further check
  reports a detected pgbouncer-style pooler and states that it is supported.
  `engine/rust/crates/stepd-store-postgres/src/lib.rs` records the constraint as a
  standing rule for the crate.
* **Not verified:** PRD F-DL-1 requires a CI lane running the full battery through
  pgbouncer in transaction mode. There is no CI configuration in this repository
  at all, so the pooler lane does not exist. What is proven is structural — no
  advisory lock can be added without failing `test_invariants.sql` — not
  empirical.
