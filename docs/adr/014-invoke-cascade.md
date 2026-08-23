# ADR-014: Invoke-tree cascade, limits and cycle prevention

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

`invoke` starts another function as a child run and suspends the parent until it resolves. A
workflow of any size therefore has a *tree*, not a run. Gap A4, severity S2, recorded that the
protocol said nothing about how terminal state propagates through that tree: what happens to
grandchildren when the root is cancelled, whether a detached child dies with its parent,
whether a child's failure fails the parent, what bounds the tree at all, and what happens when
a run invokes something that ends up invoking it back.

Every one has a quiet failure mode. An unbounded tree exhausts the database rather than
reporting a limit. A cancellation reaching only one level leaves grandchildren running against
a parent that no longer exists — they finish, write results into a journal nobody reads, and
their side effects happen anyway. A run that invokes an ancestor holding the same key deadlocks
on keyed ordering: the child cannot start while the parent holds the key, and the parent waits
on the child. That does not error; it hangs, in a way that looks like a slow dependency.

## Decision

**A full propagation table, enforced limits held in a table rather than in literals, cycle
rejection at commit time, and a cascade implemented as one recursive SQL statement.**

Propagation (protocol §7.5), implemented in `rust/migrations/0006_engine_complete.sql`:

| Event | Effect |
|---|---|
| Parent cancelled, timed out, or failed | All non-detached descendants cancelled; each runs its own `on_cancel` path |
| Child fails | The parent's `invoke` step resolves `failed` and retries under the **parent's** policy for that step; the parent is not failed |
| Child times out | The child is cancelled, the parent's step resolves `timed_out` |
| Parent continues as new | Rejected while a non-detached child is live (ADR-015) |
| Detached child | Unaffected by any parent transition, in either direction, from creation |

Limits, read from `engine_limits` so an operator can raise one during an incident without a
deploy, and so the value in force is visible when reading a failed run:
`invoke_depth` 10, `invoke_fanout` 1 000 live non-detached children per run, plus
`steps_per_run`, `chain_length` and `inbox_depth`. Each violation calls `fail_run`, which
records a specific non-retryable code (`invoke_depth_exceeded`, `invoke_fanout_exceeded`,
`invoke_cycle`) and cascades.

Cycle rejection is a recursive walk up `parent_run_id` at commit time, matching an ancestor
with the same `fn_id` **and** the same non-null key. Rejecting is strictly better than
deadlocking, because a deadlock here is invisible until someone notices a run that has been
"running" for a week.

**`cascade_cancel` is one recursive statement, and that is the load-bearing part.** A
`WITH RECURSIVE tree … , cancelled AS (UPDATE …), dequeued AS (DELETE …)` cancels every
non-detached descendant and removes them from the queue in a single statement, so it either
happens completely or not at all. The protocol requires the cascade itself to be durable and
resumable; a plpgsql loop that recursed level by level and committed as it went could
half-finish, and a server dying at level three would leave levels four and below running with
no ancestor and nothing to notice them. Rolling back and retrying the whole cascade is the
only shape that satisfies the requirement without a resumption record of its own.

Two supporting decisions:

* `resolve_child_result` takes `FOR UPDATE` on the **parent** row — the same serialisation
  point as `commit_ops` and `deliver_to_inbox` (ADR-011) — so a child finishing while the
  parent is mid-commit cannot interleave with the parent registering further ops.
* `resolve_finished_children` sweeps children that reached a terminal state while their parent's
  step is still `pending` — the recovery path for a server dying between committing the child's
  `done` and resolving the parent, which the inline call cannot cover.

## Consequences

### What this makes easy
* Cancelling a run cancels the work it caused, once, with no per-level bookkeeping. Console and
  API both call `cancel_run`, so the UI has no privileged path.
* A detached child is a genuine escape hatch: its subtree is excluded at every level, including
  grandchildren reached *through* it.
* Every limit breach produces a named, non-retryable error the developer can act on, rather
  than a resource exhaustion an operator has to diagnose.

### What this makes hard
* Every new terminal transition must remember to cascade. `continue_as_new` is precisely the
  one that did not (ADR-015), and it was found by simulation rather than by review.
* A very wide tree makes the cascade one long statement holding row locks — bounded by
  `invoke_fanout` × `invoke_depth`, but that bound is large.

### What we accept
* The cascade cancels; it does not compensate. A descendant's already-executed side effects
  stay executed unless that function declares an `on_cancel` path.
* `invoke_fanout` counts *live non-detached children of one run*, not descendants in the whole
  tree. A wide-and-deep tree can hold far more than 1 000 live runs; `invoke_depth` is the only
  bound on the product.
* Cycle detection is same-key-ancestor only. `a → b → a` on *different* keys is legal and is
  bounded solely by depth 10 — the loop terminates, but the diagnostic an operator gets is
  `invoke_depth_exceeded`, which does not say "this workflow recurses".
* Cancellation of a descendant that has an attempt in flight follows §7.1.1: the SDK will not
  drop a running step future, so the effect may complete after the run is marked cancelled.

## Alternatives considered

| Option | Why not |
|---|---|
| Recursive plpgsql cancelling one level per call | Can half-finish. A crash mid-cascade orphans every level below the one it reached, and orphans are invisible: they keep running and keep writing. |
| A cascade worker driven from a queue table | Durable and resumable, but adds a second correctness centre and a window in which descendants of a cancelled run are still dispatchable. The single statement has neither. |
| No limits, rely on operator alerting | A runaway invoke tree exhausts connections and disk before an alert is actioned, and the failure surfaces as a database incident rather than as a workflow error naming the function. |
| Limits as SQL literals | Cannot be raised during an incident without a deploy, and the value in force is not visible when reading the run that hit it. |
| Detect cycles by depth alone | Depth catches it eventually, but a same-key ancestor deadlocks *before* the depth limit is reached: the child never starts, so the depth never grows. |
| Let a failed child fail the parent automatically | Removes the parent's ability to retry the child under its own policy, or to compensate. The parent's `invoke` step is a step; steps fail and get retried. |

## Verification

* `rust/migrations/0006_engine_complete.sql` — `cascade_cancel`, whose header states why it is
  a single statement, and whose `COMMENT ON FUNCTION` records that detached children are
  excluded at every level "including grandchildren reached through a detached parent".
* `rust/tests/sql/test_invariants.sql` — check 13 fails the build if `cascade_cancel` stops
  being a single `RECURSIVE` statement or stops excluding detached children; check 12 asserts
  `resolve_child_result` locks the parent row.
* `rust/tests/sql/test_engine_ops.sql`, `cascade` blocks — an `a → b → c` tree with a detached
  `d`: cancelling `a` cancels the child and the grandchild, leaves `d` pending, and removes all
  three cancelled runs from dispatch. A second block builds `a → (detached) b → c` and asserts
  the subtree *below* a detached child survives with it.
* Same file, `invoke` blocks — the parent suspends and is not dispatchable while the child
  runs; the child's output resolves the parent's step and requeues it; a failed child resolves
  the step as `failed` but leaves the parent `pending`; a detached invoke resolves immediately
  and the child survives the parent reaching a terminal state.
* Same file, `limits` and `fencing` blocks — `invoke_depth` lowered to 3 and exceeded yields
  `failed:invoke_depth_exceeded`, a same-key self-invoke yields `failed:invoke_cycle`, and a
  superseded attempt creates no child run at all.
* `reference/simulation.py` — property P8, "no non-detached descendant outlives a terminal
  parent", checked after every simulated step and at quiescence.
