# ADR-016: Cron misfire, catch-up and DST handling

| | |
|---|---|
| Status | Accepted, implemented |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

A cron trigger looks trivial until the server is down over a fire time, or the
clock moves. Then every question is a semantic one with no obvious answer, and
every engine answers it differently — which is exactly why it must be written
down rather than inherited from whichever scheduling library gets used.

The questions gap A5 identified: what happens to occurrences missed while the
server was down; how long a missed occurrence stays worth firing; what happens
when a fire arrives while the previous run of the same key is still going; and
what a schedule means on the two days a year when local time is not a bijection
with UTC. A scheduler that leaves these implicit makes the decisions anyway —
accidentally, differently in each code path — and the operator finds out which
one they got during an incident. Two of them are silent-corruption shaped rather
than merely surprising: `catchup: all` after a long outage fires thousands of
runs at once, which reads as a stampede rather than a scheduler bug; and an
ambiguous local time firing twice on the fall-back hour double-executes whatever
the workflow does, with no error raised anywhere.

## Decision

Cron semantics are as specified in `spec/PROTOCOL.md` §3.1, normatively:

* **Misfire** is governed by `catchup`: `one` (the default) fires exactly once on
  recovery however many occurrences were missed; `skip` fires nothing; `all`
  fires every missed occurrence, capped by `catchup_limit` (default 10). The
  default is `one` because the common intent behind a schedule is "this should
  have run recently", not "this must run N times".
* **Misfire window.** An occurrence older than `misfire_window` (default `PT1H`)
  is never caught up, whatever `catchup` says. The cap is on age, not only on
  count, because a count cap still permits a five-hour-old nightly report to fire
  into a business day where nobody expects it.
* **Overlap.** With `singleton: true` and a `run_key`, a fire whose key already
  has an active run is skipped and counted in a `cron_skipped` metric. Skipping
  silently would make a schedule that never keeps up look identical to one that
  is working. Without `singleton`, fires overlap freely.

  `run_key` is a **literal**, not a CEL expression, and this ADR originally said
  `key_expr`. A cron fire has no event to evaluate an expression against, so
  reusing `key_expr` would have given one field two meanings depending on what
  triggered the run. A server must reject `singleton: true` with no `run_key`:
  accepting it degrades silently into no overlap control, which is the one
  property the author asked for by writing `singleton`.
* **DST.** A nonexistent local time is skipped — 01:30 does not exist on a
  spring-forward day in `Europe/London`, and inventing a substitute is a decision
  the operator did not make. An ambiguous local time fires once, on the first
  (pre-transition) occurrence: firing on both is the double-execution hazard
  above, and firing on the second delays the run an hour for no stated reason.
* **Time zone data** comes from the IANA database, re-evaluated after a tzdata
  update — a government moving a transition date must not leave a process firing
  an hour out until the next restart. Fire times derive from **database time
  only**, never an app-supplied clock and never a replica's wall clock (F-DL-8),
  because a skewed replica trusting itself fires the whole fleet early.
* **Definition change.** Re-registering a function with a changed cron takes
  effect from the next occurrence; already-scheduled fires inside the misfire
  window are honoured.

## Consequences

### What this makes easy
* Recovery behaviour after an outage is predictable from the function config
  alone, without reading engine source.
* `catchup: skip` fits schedules whose work is worthless if late (a cache warm);
  `all` with a limit fits schedules whose occurrences each mean something (a
  billing tick).
* The skip metric turns "the schedule quietly stopped keeping up" into a number.

### What this makes hard
* `catchup: all` plus a long outage is still a burst. The limit bounds it; it
  does not smooth it. Timer jitter (F-LP-4) applies to the runs, not the fires.
* Singleton skip means a schedule can, legitimately, produce fewer runs than
  occurrences. Anyone reconciling counts must read the metric too.

### What we accept
* Skipping a nonexistent local time means a `0 1 * * *` schedule genuinely does
  not run one day a year in most European zones. That is correct and it will be
  reported as a bug.
* Catch-up fires carry the recovery time, not the intended occurrence time, in
  `started_at`; the occurrence is recorded separately. A handler reading the wall
  clock rather than its trigger will misdate its output.

## Alternatives considered

| Option | Why not |
|---|---|
| Fire every missed occurrence, always | A weekend outage of a per-minute schedule queues thousands of runs. The stampede lands precisely when the system is least healthy. |
| Never catch up (`skip` as the only policy) | Correct for cache warms, silently wrong for anything that accrues — a missed billing tick is money, not a skipped refresh. |
| Store schedules in UTC and ignore zones | "09:00 in Berlin" is a business requirement, not a formatting preference. UTC-only makes it drift by an hour twice a year. |
| Fire twice on the ambiguous hour | Double-executes the workflow with no error raised anywhere — the silent-corruption shape this project exists to avoid. |
| Session-scoped advisory lock to elect one scheduler | Breaks behind a transaction-mode pooler (ADR-019); overlap control belongs in the singleton key, already a database invariant. |
| Host clock in the scheduler process | A skewed replica fires the fleet's schedules early, invisibly until someone compares logs (F-DL-8). |

## Verification

**Implemented and verified.** `engine/rust/migrations/0009_cron.sql`,
`engine/rust/crates/stepd-core/src/cron.rs`, `engine/rust/crates/stepd-store-postgres/src/cron.rs`.

The implementation is split in two, deliberately. `stepd-core::cron` is pure —
parsing, next-occurrence computation, the catch-up decision, the sweep plan — and
has no database. The store executes plans and owns no policy. That is the same
division migration 006 was written to restore for the commit path, after this
project shipped two correctness centres with the structural tests guarding the
one that was not running.

| Claim | Evidence |
|---|---|
| Misfire policies `one` / `skip` / `all` behave as specified | `cron.rs` unit tests, incl. `catchup_skip_is_not_the_same_as_firing_nothing` |
| The misfire window beats the catch-up policy | `the_misfire_window_beats_the_catchup_policy` |
| The catch-up limit drops the **oldest**, not the newest | `the_catchup_limit_drops_the_oldest_not_the_newest`, `a_long_outage_on_a_frequent_schedule_still_fires_the_newest` |
| Spring-forward: the occurrence is skipped, not shifted | `a_nonexistent_local_time_is_skipped_not_shifted` |
| Fall-back: the occurrence fires once, pre-transition | `an_ambiguous_local_time_fires_once_on_the_first_occurrence` |
| Overlap under `singleton` is skipped and counted | `test_cron.sql`, `a_singleton_schedule_skips_rather_than_stacking_up` |
| An occurrence fires at most once, ever | PK on `cron_fires (schedule_id, occurrence_at)`; `two_schedulers_racing_one_occurrence_produce_one_run`; simulation property **P10** |
| Fire times derive from database time | `claim_due_cron` returns `now()`; `the claim hands back database time` |
| No advisory locks; claiming is `FOR UPDATE SKIP LOCKED` | structural invariants 3 and 19 |
| A definition change takes effect from the next occurrence | `re_registering_an_unchanged_schedule_does_not_postpone_it` |
| A withdrawn trigger stops firing | `a_withdrawn_trigger_stops_firing` |
| An unschedulable expression fails **registration** | `a_bad_cron_expression_is_refused_at_registration` |

Totals at the time of writing: 37 unit tests in `stepd-core::cron`, 47 SQL
assertions in `test_cron.sql`, four structural invariants (17–20), 13
live-database tests, three end-to-end tests, and simulation property P10 with a
positive control.

### What the implementation changed about this ADR

Three things were decided here and turned out to be underspecified. Each was
found by a test rather than by rereading the document.

1. **`key_expr` → `run_key`.** A cron fire has no event, so the expression had
   nothing to evaluate against. Recorded above; the protocol and the JSON Schema
   were both corrected, and the schema gained the `singleton` field it had been
   missing since it was written.

2. **Claiming must be namespace-scoped.** This ADR said "a housekeeper sweep
   claiming due rows with `FOR UPDATE SKIP LOCKED`" and said nothing about
   fairness. A blind claim ordered by `next_fire_at` is won by whoever is
   furthest behind: one tenant with a thousand overdue per-minute schedules fills
   every sweep and every other tenant's schedules stop firing — with nothing
   failing, no backlog anywhere an operator would look, and no component to point
   at. Found by two tests interfering in a shared database, which is the same way
   the identical defect was found in dispatch. `one_busy_namespace_cannot_starve_another`
   fails against the namespace-blind version and passes against this one.

3. **An unplannable schedule must be paused, not retried.** A due row is
   re-claimed on every sweep until its `next_fire_at` moves, and a schedule that
   cannot be planned is exactly one whose `next_fire_at` cannot be computed —
   so "log it and carry on" is a hot loop for as long as the row exists. The two
   ways to reach that state are a zone that disappeared from tzdata under a
   running server and a row edited by hand. The reason is written to
   `cron_schedules.last_error` and surfaced by `GET /v1/schedules` and by
   `stepd doctor`, because a paused schedule is a job that has silently stopped
   and "check the logs from whenever it happened" is not an answer at 3am.

### The restore interaction

A point-in-time restore rewinds `cron_schedules.next_fire_at` and removes
`cron_fires` rows, so occurrences already fired before the restore point become
eligible to fire again — the at-most-once guarantee is a database invariant, and
a restore rewrites the database. This is C3, the restore paradox, applied to
cron, and it is in `docs/runbooks/restore-hazard.md` rather than only here.

`trim_cron_fires` therefore floors its cutoff at the schedule's misfire window
rather than at the retention limit alone: deleting a ledger row inside the window
would produce the same double fire without a restore being involved.

### Still open

* `misfire_window` accepts weeks, days, hours, minutes and seconds. Months and
  years are refused rather than approximated (`iso8601_seconds`), because a month
  is not a fixed number of seconds and the difference only ever surfaces as an
  occurrence that silently did or did not get caught up.
* The `cron` conformance suite (protocol §12) is still specified and unwritten.
* Timer jitter (F-LP-4) applies to run dispatch, not to fires. `catchup: all`
  after a long outage is still a burst; the limit bounds it and does not smooth
  it, exactly as the consequences section says.
