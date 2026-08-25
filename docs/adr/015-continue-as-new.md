# ADR-015: `continue_as_new` and lineage

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

A subscription poller, a monitor, a long-lived agent loop: these run for months and execute
steps every cycle. Every step result is retained for replay, so a run that loops forever grows
its journal forever. Every attempt ships that journal to the app, every replay walks it, and
eventually the run exceeds the inline-state limit or the `steps_per_run` ceiling and dies for a
reason unrelated to the business logic. Gap A6, severity S2.

`continue_as_new` closes the current run and starts a successor with the same function, key and
lineage, and an **empty** journal. Discarding the journal is the entire point: it is how an
unbounded loop keeps run state finite.

That discard is also what made this decision dangerous. Gap **A8**, and the reason this ADR
exists in its current form:

> **Found by the simulation harness, property P8, on its first run — not by review.**

The cascade rules (§7.5, ADR-014) enumerated cancellation, failure and timeout. Nobody had
asked what happens to a live non-detached child when its parent *continues*. The answer was:
the child keeps running, finishes, and delivers its result into a journal the successor has
already discarded — while the cascade rules, which cover only terminal transitions, never
touch it. An orphaned live run and a silently dropped result, from a design that had been read
by several people and had passing tests. This is finding 2 in the README and the strongest
single argument in the project for deterministic simulation over review.

## Decision

**`continue_as_new` is a v1 op that preserves key and lineage, discards the journal, and is
rejected outright while a non-detached child is live.**

Implemented in the `continue_as_new` branch of `commit_ops`
(`engine/rust/migrations/0006_engine_complete.sql`):

1. **Live-children check first.** Any non-detached child not in a terminal state fails the run
   with `continue_as_new_with_live_children` (non-retryable) before anything is written. No
   successor is created. Detached children are unaffected — their lifecycle is independent in
   both directions, so there is no journal for their result to be lost from.
2. **Chain limit.** `chain_position + 1 > engine_limit('chain_length')` (default 100 000) fails
   with `chain_limit_exceeded`, so a loop that continues forever is bounded like every other
   runaway (F-LP-9).
3. **Predecessor terminal, then successor inserted.** The order is mandatory, not stylistic:
   the partial unique index `runs_singleton_key` enforces one active run per
   `(ns, fn_id, key)`, so inserting the successor first makes the predecessor collide with its
   own continuation.
4. **Lineage preserved, `chain_position` incremented, key inherited.** Keyed ordering is
   therefore unbroken across the transition — no foreign run can slip in between predecessor
   and successor, which is what makes the chain equivalent to one long run.
5. **Alone in the envelope**, and always a halt on the SDK side: there is no "already done"
   case, because the successor is a different run with a different id.

The live-children rule is a *defensive invariant*. Under today's blocking `invoke` semantics a
conforming handler cannot reach the state at all — it is suspended waiting on the child. The
rule is written to stop a future non-blocking invoke, or a hand-rolled envelope, from
reintroducing the hazard silently.

## Consequences

### What this makes easy
* Genuinely unbounded workflows with bounded state. A cursor loop can run for years.
* Replay stays cheap: the successor starts with nothing to walk.
* Keyed exclusivity holds throughout, so a "restart the loop" operation cannot produce two live
  runs on one key, or a gap where there are none.

### What this makes hard
* Anything the successor needs must be in `input` (or copied via `carry`). The journal is gone,
  so a step result the handler relied on reading is simply not there.
* Observability spans several run ids. The console must present a lineage rather than a run,
  and "show me this workflow" is a query over `lineage_id`, not a primary key lookup.
* Every future terminal-ish transition must be checked against the cascade rules; A8 was exactly
  this omission and the shape of the mistake will recur.

### What we accept
* **The op fails the run rather than waiting.** A handler that reaches `continue_as_new` with a
  live child gets a non-retryable failure with the reason recorded on the run. Waiting for the
  child would be friendlier and would also mean the engine silently held a run in a state the
  protocol does not name. A loud failure is the correct trade for an S2 hazard.
* Predecessor and successor are separate rows, so an observer polling by run id sees the
  predecessor complete and must follow `lineage_id` to find the successor. Anything holding a
  bare run id across a continuation is looking at a run that will never move again.
* A detached child that outlives the transition has nowhere to deliver a result. That is
  already true of detached children generally — they never resolve a parent step — but the
  asymmetry with tracked children is real and has to be taught.
* The chain limit is a ceiling, not a policy: a workflow reaching 100 000 continuations is
  almost certainly wrong, but the engine cannot tell which, so it fails them all alike.

## Alternatives considered

| Option | Why not |
|---|---|
| Cancel live children as part of the continuation | Silently destroys work the handler started and expects to consume. A cancellation the developer did not ask for is worse than a failure they can see. |
| Re-parent live children onto the successor | The child's `parent_step_hash` names a journal entry the successor does not have and can never claim, so its result would resolve nothing. Re-parenting looks correct and quietly is not. |
| Let the continuation proceed and drop the child's result | The exact defect P8 found, made deliberate. |
| Automatic journal truncation instead of an explicit op | The engine cannot know which results the handler still replays; truncating one that is still read re-executes its step. The developer knows; the engine does not. |
| No `continue_as_new`; raise the step limit | Moves the wall without removing it, and makes every long run pay full replay cost until it hits the new wall. |
| Keep the journal in the successor | Removes the only reason to continue at all. |

## Verification

* `engine/rust/migrations/0006_engine_complete.sql`, `continue_as_new` branch — the live-children
  check, with the comment naming its provenance: "Found by simulation property P8, not by
  review". Also the ordering comment explaining why the predecessor must reach a terminal state
  before the successor row is inserted.
* `engine/rust/tests/sql/test_engine_ops.sql`, `continue_as_new` blocks — the successor keeps the key,
  `chain_position` advances to 1, `input` is carried, the successor's journal is empty, the
  successor is dispatchable, and **exactly one active run exists on the key throughout the
  transition** (never two, never zero). The second block invokes a tracked child and then
  attempts `continue_as_new`: the result is `failed:continue_as_new_with_live_children`, the
  code is recorded on the run, and no successor exists in the lineage.
* Same file, `fencing` block — a superseded attempt attempting `continue_as_new` returns
  `stale_fence` and creates no successor, so a lineage cannot fork from a replaced attempt.
* `sdk/rust/crates/stepd-sdk-core/src/tests.rs` — `continue_as_new_always_halts`: there is no
  memoised path, because the successor is a different run.
* `engine/rust/migrations/0005_step_op_continue_as_new.sql` — the enum value is added in its own
  `-- no-transaction` migration, because PostgreSQL will not let a new enum value be *used* in
  the transaction that adds it. Folding it into 0006 would fail on a fresh database and pass on
  an already-migrated one: a migration bug that only appears on the deployment that matters.
* `reference/simulation.py` — property P7, "no foreign run interleaves on a key across
  `continue_as_new`", and property P8, which found A8 in the first place.
