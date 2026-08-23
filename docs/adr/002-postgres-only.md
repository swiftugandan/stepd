# ADR-002: Postgres in all environments; no SQLite backend

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

The v0.1 shape assumed the usual arrangement: SQLite for local development and
small self-hosted deployments, Postgres for anything serious, with a storage
abstraction over both. It is a familiar arrangement and it is why the BRD 0.2
change log records "replaced SQLite-in-production assumption".

The problem is what the engine's correctness rests on. Op commit is one
transaction covering step results, inbox consumption, emitted events and the next
schedule. Work is claimed with `FOR UPDATE SKIP LOCKED`. Keyed exclusivity is a
partial unique index. Payloads are `jsonb`. The event log is range-partitioned by
`received_at`. None of these has a SQLite equivalent, and the substitutes —
a global write lock, a claimed-flag column, application-enforced key uniqueness —
have different concurrency behaviour, which is precisely the behaviour under test.

This project has already learned the specific way that goes wrong. An earlier
`stepd-store-postgres` reimplemented the op commit in Rust alongside the PL/pgSQL
one. Both worked. The two diverged in three ways that were live defects — signals
bypassing `deliver_to_inbox` and never waking a parked run, `wait_event` accepting
a `timeout` it never scheduled, and a cascade that orphaned descendants if it
crashed mid-way — and none of the structural invariant tests applied to the second
implementation, because they assert properties of the SQL functions. The reasoning
is written up in the module header of `rust/crates/stepd-store-postgres/src/lib.rs`.

A second *backend* is that same mistake with a larger surface.

## Decision

Postgres is the only supported backend, in development, CI and production (PRD
§6.4). `stepd-store-postgres` is the single implementation of every storage trait
in `stepd-core`, and `sqlx` is compiled with the `postgres` feature only. There is
no storage abstraction that a second engine could be slotted into, and no
lowest-common-denominator SQL.

Correctness-critical logic lives in SQL functions under `rust/migrations/`, not in
Rust. `stepd-store-postgres` marshals to JSON, calls the function and maps the
result. One reviewable correctness centre, guarded by
`rust/tests/sql/test_invariants.sql`, which fails the build if a future edit
removes a property no behavioural test would notice going missing.

## Consequences

### What this makes easy

* `SKIP LOCKED`, partial unique indexes, `jsonb` operators and partitioning are
  exercised locally exactly as in production. Parity is not a claim, it is the
  same engine.
* One implementation of `commit_ops` means the invariant tests cover the code that
  actually runs — the countermeasure to this project's own finding that
  "correctness can rest on undocumented accidents" applies to the real path.
* Engine-specific structural constraints surface early. Ingest dedupe never fired
  because the unique index included `received_at`; Postgres requires a unique index
  on a partitioned table to include the partition key, so a global `UNIQUE (ns, idem)`
  is inexpressible there and dedupe had to move to its own table. Under an
  abstraction layer that constraint is invisible until production.
* `stepd doctor` can ask engine-specific questions — pooler mode, partition lag,
  clock skew, stale leases — because it knows what it is talking to.

### What this makes hard

* Contributors need a running Postgres before they can run the full suite. The
  mitigation is real but partial: `stepd-core/src/testing.rs` provides in-memory
  fakes so the whole dispatch loop is testable with no database and no network, and
  the database-backed suites (`stepd-store-postgres/tests/live.rs`,
  `tests/simulation.rs`, `stepd-server/tests/end_to_end.rs`) skip with a printed
  notice when `STEPD_TEST_DATABASE_URL` is unset.
* No single-file, zero-dependency deployment. stepd cannot be embedded in an
  edge binary or shipped as a library, and never will be under this decision.
* CI needs a Postgres service container, and the pooler suite needs pgbouncer.

### What we accept

* A whole class of small self-hosted deployment is excluded. Someone who wants
  durable workflows for a hobby project must still operate a database.
* Tests that skip when the database is absent are tests that can silently stop
  running. The skip prints to stderr; nothing enforces that CI sets the variable.
* **`stepd dev` does not yet do what §6.4 promises.** The PRD says it "provisions
  Postgres transparently"; as built, `dev` in `rust/crates/stepd-cli/src/main.rs`
  takes a connection URL like every other command and only applies migrations,
  creates a namespace, mints a token and relaxes the egress policy. The parity
  argument holds either way — it is the same engine — but the developer ergonomics
  that justified removing SQLite are not delivered. PRD open question 7 (bundle the
  embedded Postgres, or require Docker) is still open and blocks this.

## Alternatives considered

| Option | Why not |
|---|---|
| SQLite for development, Postgres for production | The concurrency primitives under test — `SKIP LOCKED` claiming, row-lock serialisation of the signal race, partial unique indexes — have no SQLite equivalent, so the local suite would pass against semantics production never uses. |
| SQLite for small production deployments | Same divergence, now in front of users, and the failures it produces are lost signals and double-executed steps rather than errors. |
| A storage abstraction with two implementations | Two implementations of `commit_ops` is exactly the arrangement that produced three live defects inside this crate once already, with the invariant tests covering only one of them. |
| MySQL as a second backend | Same cost, and no partial indexes; the keyed exclusivity invariant would move from the database into application code. |
| Keep the commit logic in Rust for portability | Would move the correctness centre out from under `test_invariants.sql`, which is what makes the R1 races non-reopenable. |

## Verification

* `rust/Cargo.toml` declares `sqlx` with features `runtime-tokio, postgres, json,
  uuid, chrono, migrate` — no `sqlite`, so a second backend cannot be added without
  the dependency change being visible in review.
* `rust/crates/stepd-store-postgres/src/lib.rs` is the only implementation of
  `StateStore`, `Queue`, `TimerStore`, `EventLog` and `Housekeeping`; its module
  header records why the SQL stayed in SQL.
* `rust/tests/sql/test_invariants.sql` asserts the engine-specific properties
  directly: no advisory locks anywhere (pooler safety), `deliver_to_inbox` taking
  the run row lock as its *first* statement, `commit_ops` checking the fence under
  that lock, exactly one `commit_ops` overload, and the `runs_singleton_key`
  exclusivity index being present.
* `rust/crates/stepd-cli/src/doctor.rs` checks conditions only a Postgres
  deployment has: `pooler` (detects pgbouncer and its mode), `partitions` (next
  month's event partition exists), `clock` (skew against database time),
  `invariants`, `leases`.
* `rust/crates/stepd-store-postgres/tests/live.rs` and `tests/simulation.rs` run the
  behavioural and property suites against a real database, both gated on
  `STEPD_TEST_DATABASE_URL`; `no_property_is_violated_across_the_seed_budget` is the
  simulation entry point.
