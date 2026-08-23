# ADR-022: The cancellation compensation phase

| | |
|---|---|
| Status | Accepted, implemented |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Protocol §7.4 promised: on cancel, a function that declares `on_cancel` receives
one more attempt with `run.cancelling: true`, and runs only its compensation
path.

Every part of that existed except the part that makes it run.

* `stepd-proto` carried `cancelling` on the run context.
* The SDK exposed it as `ctx.run().cancelling`.
* The store computed it in `load_attempt`, with a comment explaining §7.4.
* And `cancel_run` **deleted the queue row**, so the run was never dispatched
  again and the flag could never be true.

Nothing errored. A workflow cancelled mid-flight simply never ran its
compensation: the refund was not issued, the reservation was not released, the
partner was not told. The run showed `cancelled` — which is what the operator
asked for, and exactly what it looks like when it worked.

This is the same shape as the cron scheduler, and it was found the same way: by
writing something that asked whether the promise was kept. The conformance suite's
`cancel` case is the first thing in the project's history to check whether the
compensation path executed.

`on_cancel` did not exist in the function config schema either, so there was also
no way for a function to declare that it had one.

## Decision

A run being cancelled enters a **compensation phase**, marked by a durable
`runs.compensating` flag, and is dispatched until its compensation path returns.

### Why a flag rather than a `cancelling` run status

A status looked right and is wrong in practice. Compensation needs *several*
attempts: one new step per attempt means a three-step undo takes three. So the
run cycles `pending → running → pending` like any other, and a status that had to
survive that churn would have to be re-asserted on every transition — through a
trigger on every path, or by threading it through every `UPDATE` in `commit_ops`.

A boolean that nothing else writes is a smaller thing to be right about. It is
durable rather than derived because it must survive a crash: a server that dies
between the cancel and the compensation attempt must still know, on restart, that
the run owes an undo.

### The phase keeps the business key

A compensating run is `pending` or `running`, both of which are inside
`runs_singleton_key`'s predicate, so it keeps its key for free.

That is load-bearing rather than incidental. Releasing the key when cancel was
requested would let the next run for that key start **while the previous one is
still issuing refunds** — two runs interleaved on one key, which is the exact
failure keyed ordering exists to prevent, and which would appear only when a
cancel raced an event. Structural invariant 23 fails the build if `cancel_run`
stops returning the run to a status the index covers.

### However it ends, the run is `cancelled`

A compensation path that returns `done` did not make the run succeed. One that
fails did not make the run *fail* either: it was cancelled, and a compensation
that threw is a fact about the compensation, recorded in the run's `error`.

An operator counting cancelled runs during an incident should not have to know
which of them had an undo that went wrong.

This is enforced by a trigger — invisible control flow, which owes an
explanation. `commit_ops` writes the terminal status in two places, and the
alternative is re-issuing all eight hundred of its lines in this migration with
those two branches changed. That is precisely the duplication migration 006 was
written to remove, and it would leave the next reader diffing two copies to find
out which one runs. The rule is one sentence, and structural invariant 22 fails
the build if the trigger disappears.

### The attempt is conditional on `on_cancel`

§7.4 conditions it on the function declaring a compensation path, so `on_cancel`
is now a boolean in the function config, set by `Function::on_cancel()` in the
SDK and read by `cancel_run` from the registered config.

Compensating unconditionally would hand a normal-looking attempt to a handler
that has no compensation path and rely on it noticing `cancelling` — which works
in the SDK that was tested and not in the one that was not.

The cascade consults each descendant's own declaration, so a tree of children
each get their phase without the parent having to know the shape of the tree.

## Consequences

### What this makes easy

* A workflow that reserves, charges or dispatches can undo it on cancellation,
  which is what durable execution is for.
* An operator cancelling a stuck run gets the undo without doing anything extra.

### What this makes hard

* A cancel is no longer instantaneous. The run reaches `cancelled` when its
  compensation path returns, not when the request is accepted — so an operator
  cancelling a thousand runs sees them settle over seconds rather than at once.
* A compensation path is deliberately restricted to steps. Sleeping, waiting for
  an event or invoking a child from it would leave a cancelled run parked
  indefinitely, holding its key, with no operator action that could clear it.

### What we accept

* A handler that declares `on_cancel` and ignores `ctx.run().cancelling` will
  re-run its normal path. The protocol says the SDK runs only the compensation
  path; the Rust SDK exposes the flag and does not yet enforce it. That is a gap,
  and it is in `docs/GAPS.md` rather than here.
* An app that is down when a run is cancelled delays the compensation until it
  returns. This is correct — the undo must happen, and happening late is better
  than not happening — but it means `cancelled` counts lag an app outage.

## Alternatives considered

| Option | Why not |
|---|---|
| Leave it: cancel means stop | Contradicts §7.4, which is in the published protocol, and silently drops the undo that a cancelled workflow most needs. |
| A `cancelling` run status | Must survive `pending → running → pending` across several attempts, so every transition has to re-assert it. More surface to be wrong on than one boolean. |
| Re-issue `commit_ops` with the two branches changed | Two copies of the commit path is the hazard migration 006 exists to remove; the next reader has to diff them to find which runs. |
| Compensate unconditionally | Hands a normal-looking attempt to handlers that have no compensation path and relies on every SDK noticing a flag. |
| One single compensation attempt, as §7.4 first read | Not implementable alongside memoisation: one new step per attempt means a multi-step undo cannot fit in one dispatch. The protocol wording was corrected. |
| Let the compensation outcome decide the run's status | A cancelled run reporting `completed` is a lie, and reporting `failed` hides which failures were undos. |

## Verification

Migration `rust/migrations/0010_compensation.sql`.

| Claim | Evidence |
|---|---|
| A function with no `on_cancel` finishes immediately, as before | `test_engine_ops.sql` |
| A function declaring `on_cancel` is queued for the phase | `test_engine_ops.sql`; structural invariant 21 |
| Compensation steps commit and memoize normally | `test_engine_ops.sql` |
| Several attempts, one new step each | `test_engine_ops.sql` |
| A compensation returning `done` ends the run `cancelled` | `test_engine_ops.sql`; structural invariant 22 |
| A compensation that fails still ends the run `cancelled`, with the error recorded | `test_engine_ops.sql` |
| The key is held throughout the phase | `test_engine_ops.sql`; structural invariant 23 |
| A cascaded child gets its own phase | `test_engine_ops.sql` |
| The path executes exactly once, end to end over HTTP | conformance `cancel` |

Each of the three structural invariants has a positive control: the check was run
against a deliberately broken definition and fired.
