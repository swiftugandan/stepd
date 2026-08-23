# ADR-023: Remove the join policies; record every member's outcome instead

| | |
|---|---|
| Status | Accepted, implemented |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Protocol §5.2.1 offered three join policies on a parallel batch — `all`, `any`
and `all_settled` — with `all_settled` the default. The engine implemented one
of them. `commit_ops` took `p_join text DEFAULT 'all_settled'` and never read it;
the Rust store bound the literal `"all_settled"` into every call; no other
implementation existed.

So an app that asked for `any` got `all_settled` and was told nothing. That is
worse than the feature being absent: absent, a developer writes the fan-out by
hand and it works. Present-and-ignored, they write `join: "any"`, watch the fast
provider return, and assume the slow one was cancelled. Nothing cancels it. The
charge goes through twice.

Two options: implement the policies, or delete them.

### Why they cannot be implemented as specified

Both policies are defined in terms of **cancelling siblings**, and in this
execution model there is usually nothing left to cancel.

An SDK claims every hash in a group before polling any member (§6.1 rule 2,
eager claiming — the property the whole design rests on), then runs every body,
then emits one envelope. By the time a server sees the batch, every member has
already executed. `all` would cancel work that already ran. `any` would discard
results that already exist, which is a durable execution engine deliberately
forgetting what it did — the one thing it is for.

The policies are only meaningful for members that are still *in flight* when the
batch commits: `invoke` children and `wait_event` timers. That is a real and
useful pattern (race two carriers, take the first quote). It is also a much
narrower feature than an envelope flag whose meaning silently changes with what
happens to be in the batch, and it belongs to the §7.5 cascade rather than here.

### What deleting them exposed

Removing the policies meant writing down what a batch actually does, and then
asserting it. The sentence that replaced them is:

> **Every op runs to a terminal state. No op cancels a sibling. Every outcome is
> recorded.**

The third clause was false.

`run_steps.status` has had a `failed` value since migration 001. `RecordedStep`
carries `status` and `error` on the wire. The SDK's memo path already turns a
recorded `failed` step back into the error the body raised. **Nothing wrote
one.** The only producer of `step_status = 'failed'` was `resolve_child_result`,
for a failed child *run*; a step whose own body raised left no row at all.

Inside a parallel group it was worse, and that is how it was found. `join.rs`
polls every member to completion before deciding the group's outcome, and its
comment says why:

> Returning early on the first failure would leave siblings that had already
> executed unrecorded, which is the one thing a durable engine must never do —
> the work happened and nothing remembers it.

The layer above threw them away anyway. `PassOutcome::Error` carried no ops, and
`run_pass` drained the pending buffer only on the yield path. A group of three
where one member raised committed **nothing**: two bodies had run and returned,
and the journal knew about neither. The comment defended the hazard at one layer
and the next line reopened it.

The conformance case that catches this could not have been written before the
deletion, because §12's `parallel` suite asserted only the happy path — which is
another small argument for deleting rather than implementing: the specification
became something that could be checked.

## Decision

### 1. The policies are removed, not implemented

`join` is deleted from `AttemptResponse`, from the schema, from the examples and
from `commit_ops`'s signature. §5.2.1 states the single non-configurable rule
quoted above.

`join` becomes a **retired field** (§11): a server MUST reject an envelope
carrying one, naming it, rather than ignoring it. §11's ordinary rule — ignore
unknown fields — is right for a field a newer minor added, about which the
receiver has no opinion. A retired field is different: it had a meaning, an app
may still be sending it expecting that meaning, and being ignored is the exact
state it was removed for being in. It must not survive its own removal.

### 2. A terminal step failure is an outcome, and outcomes are recorded

`Op::Step` gains an optional `error`. Present, it records the step as `failed`
with that error; absent, `completed` as before. The journal now says what
happened to a step whose body raised — in a group and on the ordinary sequential
path alike, where previously a failed run showed a run-level error and an empty
space where the step should have been.

**Only non-retryable failures.** A retryable failure is not an outcome, it is a
pause. Recording one would memoise it: the next attempt would replay the error
instead of the closure, and the retry the app asked for would never run. This is
stated in §5.2.2, enforced in the SDK, and asserted in both directions.

### 3. `error` may ride at the end of a batch

An envelope may carry recording ops followed by an `error` op; the server applies
them in order. This is what makes clause three true for a group whose members
mostly succeeded: all three outcomes reach the journal, and *then* the run fails.

The alternative was to keep `error` exclusive and force the SDK to choose between
dropping the ops (losing executed work) and dropping the error (losing the reason
the run stopped). Neither is acceptable, so neither is required.

Two positional rules keep the combination unambiguous:

* **`error` last.** Otherwise the server fails the run and then goes on recording
  steps into it. Position is the only thing that says which happened first.
* **A retryable `error` travels alone.** Retrying is a decision about whether to
  re-execute and belongs to the dispatcher; committing is a decision about what
  already happened and belongs to the store. The dispatcher does not commit, so
  an envelope asking for both has no single owner. An SDK holding recorded ops
  and a retryable failure emits the ops alone and re-raises next attempt, where
  the work is memoised, nothing new is recorded, and the error goes on its own.
  This terminates because each attempt records strictly less than the last.

`done` and `continue_as_new` stay exclusive, for the opposite reason: recording a
step ends the pass, so a handler cannot both record a step and return. Ops
alongside them mean a swallowed step result, and that must be reported rather
than committed.

### 4. `run_pass` drains once, and every arm must decide

The buffer is now taken immediately after the handler returns, before the match.
Every arm has to say what it does with the pass's recorded work; none can drop it
by omission. The original defect was exactly an omission — the yield arm looked
and the others did not — and this is the shape of change that stops it recurring
rather than fixing this instance of it.

## Consequences

### What this makes easy

* A failed step is visible in the console, with its error, at the position it
  occupied. Previously an operator saw a failed run and a gap.
* A fan-out where one member fails behaves the way the code reads: the siblings'
  results are durable, the failure is surfaced, and a retry does not re-run the
  members that succeeded.
* The specification lost a feature it did not have. There is one fewer place
  where reading the protocol and reading the engine give different answers.

### What this makes hard

* A racing fan-out (`any`) now has no protocol-level expression at all. An app
  that wants one writes it with `invoke` children and a `wait_event`, which is
  more code, and honest about the fact that the losers keep running.
* `Op::Step` has two shapes. A future op-kind that can also fail terminally will
  want the same treatment, and the pressure to add a generic `status` field to
  every op should be resisted until a second case actually exists.

### What we accept

* **The old envelope is still parseable.** An app sending `join` gets a specific
  refusal, not silence — but it does get a refusal, and a deployed app pinned to
  a pre-1.2 SDK will fail on upgrade. That is the intended trade: this is a rev
  1.2 change to a protocol at major version 1, and the alternative is continuing
  to accept a field whose documented meaning never happened.
* **One extra attempt for a retryable failure inside a group.** The pass that
  recorded siblings commits them and comes back; the failure is raised on the
  next attempt. The engine already pays a round trip per step, so this is in
  keeping, and it buys a rule with no shared ownership between dispatcher and
  store.

## Alternatives considered

| Option | Why not |
|---|---|
| Implement `all` and `any` as specified | Both cancel siblings; eager claiming means every body has already run when the batch arrives. `all` cancels finished work, `any` discards results the engine is supposed to remember. |
| Keep the field, document it as advisory | An advisory field that changes nothing is what was already there. The failure mode is a developer trusting it. |
| Ignore a retired `join` per §11 | §11's ignore rule is for fields a receiver has no opinion about. This one has a documented meaning the receiver will not honour; being ignored is the state it was removed for. |
| Keep `error` exclusive, drop the pass's ops | Loses executed work. This was the bug. |
| Keep `error` exclusive, emit ops and discard the error | Loses the reason the run stopped, and relies on rediscovering it next attempt — which a non-deterministic handler may not do. |
| Record retryable failures as failed steps too | Memoises them. The retry would replay the error instead of the body, so the retry policy would silently stop working. |
| Let the dispatcher commit ops and then apply backoff | Splits one envelope across two transactions with no owner for the gap. The SDK deferring costs one round trip and keeps the rule in one place. |
| Add a generic `status` to every op kind | Speculative. `step` is the only op whose body the app runs; the others resolve server-side and already have terminal states. |

## Verification

| Claim | Evidence |
|---|---|
| A group with one fatal member records all three outcomes | `a_group_with_one_fatal_member_still_records_the_others` (stepd-sdk-core) |
| A retryable member defers its error and commits its siblings | `a_retryable_member_defers_the_error_and_commits_its_siblings` |
| A retryable failure records nothing about itself | `a_step_failure_propagates_with_its_retryability` |
| A fatal error may ride at the end of a batch | `a_fatal_error_may_ride_at_the_end_of_a_batch` (stepd-proto) |
| An error before the end is refused | `an_error_before_the_end_is_refused` |
| A retryable error may not travel with recorded ops | `a_retryable_error_may_not_travel_with_recorded_ops` |
| A retired `join` is refused, not ignored | `an_envelope_carrying_a_retired_field_is_refused_not_ignored`; `spec/validate.py` |
| The engine records the failed member and keeps its error | `tests/sql/test_engine_ops.sql`, six assertions |
| `commit_ops` still records failed outcomes | structural invariant 26, positive control: migrations through 012 only |
| No join-policy parameter returns | structural invariant 27, positive control: a probe function taking `p_join` |
| End to end, through a real server and app | conformance case `conf-parallel-partial` (§12.2), suite `parallel` |

### Positive controls

Both new structural invariants were run against a deliberately wrong schema
before being trusted. Invariant 26 was checked against a database migrated
through 012 only, where `commit_ops` still hardcodes `'completed'`; invariant 27
against a database carrying a one-line function declared with a `p_join`
parameter. Both raised. An invariant that has never failed is a comment.
