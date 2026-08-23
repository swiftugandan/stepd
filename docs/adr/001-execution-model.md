# ADR-001: Execution model — step memoisation with per-attempt push

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

stepd must run business processes that survive crashes, deploys and month-long waits
without re-executing side effects, while the code defining them lives in the customer's
own service, in whatever language they use. That rules out a workflow VM owning the
process and forces a protocol.

Two families were available. Deterministic replay (Temporal-style) re-executes the
handler against an event history inside a sandboxed scheduler; it gives cheap replay
and demands that user code stay deterministic forever, including across dependency
upgrades the author did not make. Journalled memoisation (Inngest-style) records the
result of each *named* unit of work and hands the journal back on the next call.

Ordering is the remaining requirement: business processes are keyed, `order:4711`
must not be processed by two runs at once, and an engine that permits that has not
solved the problem it exists to solve. Restate answers this with a keyed
single-writer. The apps themselves stay stateless, holding no run state between
calls (PRD §6.5, factor VI).

## Decision

Journalled memoisation with per-attempt push, keyed single-writer ordering, on
Postgres. Four parts, all normative in `spec/PROTOCOL.md`:

**One new step per attempt (§1).** The server POSTs an `AttemptRequest` carrying the
run's completed and terminally-failed steps. The SDK re-executes the handler from the
top; a step whose hash is in that map returns its recorded value without running — and
without constructing the work at all. The first step without a result becomes an op,
the handler yields, the server commits and schedules the next attempt.

**Identity is the step id, not the position (§6).** A step is
`sha256(function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ occurrence)[0..8]`, implemented in
`rust/crates/stepd-proto/src/hash.rs`; occurrence is a per-`step_id` counter reset every
attempt. Because identity is not positional, adding, removing or reordering steps around
an in-flight one leaves its result addressable.

**Occurrence is claimed eagerly, in program order (§6.1).** The counter is claimed when
the step function is *called*, never when its future is polled. Claiming lazily ties the
hash to scheduler order, so `join!` silently produces different hashes on different
attempts and completed work re-executes with nothing erroring.

**Keyed single-writer plus fencing.** At most one non-terminal run exists per
`(ns, fn_id, key)`, enforced by the partial unique index `runs_singleton_key` in
`rust/migrations/0001_initial.sql` rather than by application logic. Each attempt carries
a monotonic fence token; a response bearing a stale fence is discarded (§7.3).

## Consequences

### What this makes easy

* Changing workflow code while runs are in flight. The versioning table in §6 is a
  consequence of the hash, not a feature bolted on afterwards.
* Writing an SDK in any language: a hash function, a map lookup and an early return.
  No sandboxed scheduler, no determinism contract.
* Testing the engine with no database and no network — `stepd-core` is generic over its
  traits and `stepd-core/src/testing.rs` supplies in-memory fakes.
* Sleeps and waits consume no app compute: a run parked on a seven-day approval holds one
  row, not a process.

### What this makes hard

* Latency and write amplification: every step is a round trip plus a commit, so a
  fifty-step workflow is fifty transactions. Parallel batches (§5.2) recover some of it; a
  tight loop of trivial steps is the wrong shape here. Run state also grows with the
  journal, because every attempt ships it — which is why `continue_as_new` exists and why
  unbounded loops MUST use it.
* "Code outside a step may execute many times" is a real footgun that fails quietly
  rather than loudly. Loops that fan out need explicit discriminators
  (`format!("charge-{id}")`); leaning on the occurrence counter inside a parallel
  group is an error.

### What we accept

* **At-least-once step execution.** A closure may run and the process may die before the
  commit lands. The contract is "the work happened twice, the record was written once";
  non-idempotent effects must derive a key from `run.id` + `hash` (§7.2).
* **Renaming a step id re-executes it.** The old result is orphaned and the side effect
  repeats. SDKs warn about orphans; nothing can prevent it.
* **The occurrence counter is guarded, not eliminated** — by eager claiming, by rejecting
  duplicate ids in a parallel group, and by binding the counter to the sequential pass.
* A point-in-time restore rewinds the journal and side effects replay (PRD F-DL-5) — a
  semantic limit of durable execution, to be documented before an incident.

## Alternatives considered

| Option | Why not |
|---|---|
| Deterministic replay VM (Temporal-style) | Imposes a determinism contract on user code that must survive every dependency upgrade, and needs a language-specific sandboxed scheduler per SDK — which contradicts a published protocol any language can implement. |
| Positional step identity (nth step in the history) | Inserting or removing a step shifts every later step's identity, so a routine code change re-executes completed work. |
| In-memory per-run actor holding state between steps | Makes the app stateful, so a deploy or an eviction loses in-flight runs; contradicts PRD §6.5 factor VI. |
| Pull-based workers claiming attempts | Deferred, not rejected. `Transport` is a trait (ADR-008), so pull is an alternative implementation rather than a protocol change. |

## Verification

* `rust/crates/stepd-core/tests/engine.rs` drives the real dispatch loop against in-memory
  components: `workflow_runs_end_to_end_and_each_step_executes_once` asserts each
  side-effecting counter is exactly 1 across a multi-attempt run,
  `duplicate_commit_records_a_step_once` asserts first-write-wins,
  `stale_fence_response_is_discarded` asserts a superseded attempt writes nothing, and
  `keyed_runs_are_mutually_exclusive` asserts a second active run on the same key is
  refused and that the key frees on termination.
* `rust/crates/stepd-sdk-core/src/tests.rs` covers the SDK half.
  `eager_claiming_makes_poll_order_irrelevant` builds futures in program order `a, b, a`,
  polls them in reverse, and asserts the hashes are unchanged;
  `naive_counter_under_reordering_is_demonstrably_broken` is its control, showing the lazy
  scheme *does* change under the same reordering — so the property is a measurement, not an
  article of faith. Identity rules are covered by
  `a_memoized_step_never_constructs_its_future`,
  `inserting_a_step_before_completed_ones_does_not_re_execute_them`,
  `renaming_a_step_orphans_its_result` and `loop_occurrences_are_stable_across_attempts`;
  `a_crash_at_any_point_yields_the_same_outcome` replays every crash prefix and asserts the
  outcome and the execution count are unchanged. `stepd-proto/src/hash.rs` holds the hash's
  own tests, including `separator_prevents_component_confusion` — without the `0x1F`
  separator, `("a","bc")` and `("ab","c")` hash identical bytes.
* `rust/crates/stepd-store-postgres/tests/live.rs` repeats the properties against a real
  database: `a_five_step_workflow_runs_to_completion`,
  `only_one_run_per_key_is_active_at_a_time`, `a_superseded_attempt_cannot_commit`.
  `rust/tests/sql/test_invariants.sql` fails the build if `runs_singleton_key` disappears
  or if `commit_ops` stops checking the fence under the run row lock.
