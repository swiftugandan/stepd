# stepd — durable workflow engine

**Archive packaged 2026-08-23.** Read this before running anything: it states
what is finished, what is not, and where the trustworthy artifacts are.

---

## What this is

An open-standards durable workflow engine: a single binary (server + console)
backed by Postgres, with a published wire protocol so any language can host
workflow code. Long-running business processes — order fulfilment, onboarding,
approvals, claims — survive crashes, deploys and month-long waits without
re-executing side effects.

Execution model: Inngest-style one-new-step-per-attempt with step-id
memoization, plus Restate-style keyed single-writer ordering, on Postgres.

## Layout

```
rust/        Rust workspace — INCOMPLETE, see below
spec/        Wire protocol, JSON Schemas, validator — complete
reference/   Python reference implementation — complete and tested end to end
docs/        BRD, PRD, gap register, SDK design, SDK prototype
```

## Status

| Component | State | Evidence |
|---|---|---|
| Protocol spec (rev 1.1) | Complete | 40 schema cases pass — `spec/validate.py` |
| Postgres schema + engine SQL | Complete, tested on PG16 | 25 behavioural + 6 structural assertions |
| Python reference engine | Complete | dispatcher, API and console tested end to end |
| Simulation harness | Complete | 120,000 seeds, 0 property violations |
| `stepd-proto` (Rust) | Complete | 25 unit tests pass |
| `stepd-core` (Rust) | Substantially complete | traits, dispatcher, policy, in-memory fakes |
| `stepd-store-postgres` | Substantially complete | ~900 lines, compiles |
| `stepd-sdk` | **Missing** | design in `docs/SDK-DESIGN-rust.md` |
| `stepd-server` | **Missing** | Python equivalent in `reference/api.py` |
| `stepd-cli` | **Missing** | — |

### Provenance caveat

The Rust workspace here is **not** the tree that was verified end to end during
development. A container recycle replaced it with a different implementation of
the same design before it could be archived. What is present compiles and its
unit tests pass, but it has not been run against a live database, and three
crates are absent.

`reference/` is the trustworthy executable artifact: it has been run end to end
against PostgreSQL 16, drives a five-step workflow to completion, and is what
the Rust port was derived from.

`rust/migrations/` originally contained only 0001 and 0002. Migrations 0003 and
0004 carry critical fixes and have been copied in from `reference/`.

## The two fixes not to lose

**0003 — explicit signal/wait serialization.** The lost-signal race was closed by
accident: a foreign key from `run_inbox` to `runs` happens to take a row lock
conflicting with the one `commit_ops` holds. Dropping or deferring that FK, or
partitioning the inbox, would silently reopen the race with no test failing.
`deliver_to_inbox` now takes the run row lock explicitly as its first statement.
Proven by dropping the FK entirely and re-running the forced interleaving
(`reference/prove_fix.sh`).

**0004 — idempotent ingest and wait upsert.** Ingest dedupe never fired: the
unique index included `received_at`. The cause is structural — `events` is
partitioned by `received_at`, and Postgres requires a unique index on a
partitioned table to include the partition key, so a global `UNIQUE (ns, idem)`
is inexpressible there. Dedupe moved to its own table. Separately,
`deliver_to_inbox` could consume an event without recording a result when the
pending step row was absent, losing the signal; it is now an UPSERT.

## Findings worth carrying forward

Every one came from a test failing, not from review.

1. **Correctness can rest on undocumented accidents.** See 0003. Structural
   invariant tests (`reference/test_invariants.sql`) are the countermeasure —
   they fail the build if the explicit lock is removed or moved.
2. **`continue_as_new` orphaned live children.** Found by the simulation harness
   via property P8 on its first run. The cascade rules covered cancellation and
   failure but not continuation, and the child's result would have been delivered
   into a journal that `continue_as_new` discards.
3. **Eager hash claiming makes concurrency safe by construction.** `ctx.step()`
   must claim the occurrence when *called*, not when its future is *polled*.
   Claiming at poll time ties the hash to scheduler order, so `join!` silently
   re-executes completed work. Demonstrated in `docs/sdk-prototype/`.
4. **A flaky test was a design defect.** The circuit breaker's probabilistic
   recovery ramp made recovery time impossible for operators to reason about and
   for tests to pin down. Replaced with a deterministic token budget.
5. **A breaker that closes only on observed successes never closes.** Once
   traffic stops it stays half-open, throttling the next burst long after the app
   recovered. Closure is now successes *or* a quiet period.
6. **Passing is not exercising.** `reference/coverage_check.py` showed cascade
   cancellation hit zero times across 500 green seeds — the suite had never
   tested a fix that had just been made.

## Running what works

```bash
# Protocol schemas — no database needed
cd spec && pip install jsonschema referencing && python3 validate.py

# Reference engine against a real Postgres
initdb -D /tmp/pgdata -A trust && pg_ctl -D /tmp/pgdata -o '-p 5433' start
createdb -p 5433 stepd
cd reference
psql -p 5433 -d stepd -f 001_initial.sql -f 002_engine.sql \
     -f 003_fix_signal_race.sql -f 004_fix_idempotency_and_wait_upsert.sql
psql -p 5433 -d stepd -f test_engine.sql -f test_invariants.sql
python3 test_dispatcher.py && python3 test_api.py && python3 test_console.py
python3 simulation.py 5000 && python3 coverage_check.py 400
python3 -c "import uvicorn, api; uvicorn.run(api.app, port=8099)"   # console at /

# Rust workspace — unit tests only, no database required
cd rust && cargo test --workspace
```

## Next steps, in order

1. Read `rust/crates/stepd-core` properly and reconcile against PRD §6.3. Its
   `testing.rs` in-memory fakes are a genuine improvement — they let the dispatch
   loop be tested with no database and no network at all.
2. Write `stepd-sdk` from `docs/SDK-DESIGN-rust.md`; the eager-claim mechanism in
   §2 is the part that must be right.
3. Write `stepd-server` and `stepd-cli` from `reference/api.py` and
   `reference/console.html`.
4. Point the simulation harness at the Rust engine instead of the Python model.
5. Write the ADRs indexed in the PRD, and the restore-hazard runbook — the
   document most likely to matter during a real incident.

## Licence

Apache-2.0 intended (see PRD open question 6; not yet formally applied).
