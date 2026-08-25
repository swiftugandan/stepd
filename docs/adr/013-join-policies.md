# ADR-013: Batch join policies and sibling cancellation

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

An SDK that discovers several independent ops in one replay pass returns them together
(protocol §5.2). The server commits the batch atomically, but the ops then resolve
*independently*: a `step` is already resolved on commit, while `sleep`, `wait_event` and
`invoke` resolve minutes or days later, each with its own retry policy.

That leaves the question recorded as gap A3, severity S2: what happens when one member of a
batch fails and the others do not? Every plausible answer is defensible and they contradict
each other. Fail the run, and a handler that wanted to fall back on a stale cache never gets
the chance. Ignore the failure, and a handler that assumed all-or-nothing reads a value it
never checked. Leaving it undefined means two SDKs implementing the same protocol disagree
about the same workflow, which is worse than either answer.

There is a second, sharper hazard inside the SDK's own group combinator. If a group returns as
soon as the first member fails, siblings that had *already executed* have their results dropped
on the floor. Their effects happened — the invoice was fetched, the card was charged — and
nothing recorded them, so the next attempt repeats them. That is the one outcome a durable
engine must never produce, and it produces no error at all.

## Decision

**Three join policies, carried once on the envelope, with `all_settled` as the default; and
the SDK's group combinator polls every member to completion before deciding anything.**

| `join` | Next attempt scheduled when | Failed ops |
|---|---|---|
| `all_settled` (default) | Every op has reached a terminal state | Surfaced to the handler as failed step results; user code decides |
| `all` | Every op completed successfully | The first terminal failure cancels the siblings and fails the run |
| `any` | The first op completes successfully | Siblings are cancelled, their results discarded |

Rules that follow: under `all_settled` a failed step in the map does **not** by itself fail the
run — the handler must read it and decide explicitly; sibling cancellation under `all` and
`any` uses the §7.5 cascade, including for in-flight `invoke` children, so there is one
cancellation mechanism rather than two (ADR-014); retrying one op does not re-dispatch the
whole batch; and a batch carries exactly one policy, mixing being rejected.

In `sdk/rust/crates/stepd-sdk-core/src/join.rs`, `group_outcome` implements the `all_settled` shape
on the handler side. A genuine failure outranks a yield, so the handler sees the error rather
than being replayed into the same failing step forever; a yield with no failure becomes
`Halt::Yield(0)`, because ops accumulate on the `Ctx` rather than travelling in the halt value
— which is what lets one envelope hold every member.

The ordering is what matters. `JoinAll` drives every member to `Ready` first, and each
member's op is pushed onto the `Ctx` as that member resolves — so by the time `group_outcome`
sees a failure, everything that executed is already in the pending envelope. A combinator
written the obvious way, returning at the first failing member, would abandon siblings that
had already run and never emit their ops: the work happened, nothing recorded it, and the next
attempt does it again with no error raised anywhere.

The single short-circuit is `Halt::Fatal`, which aborts the envelope entirely. That is safe
only because the sole source of `Fatal` inside a group is the id-uniqueness check, which runs
*before any member is polled*. `a_duplicate_id_is_rejected_before_any_member_runs` exists to
hold that line: it asserts zero member closures executed.

`ctx.join` additionally enforces unique ids within a group: two members sharing an id would get
occurrences 0 and 1 in declaration order — stable, but almost never what the developer meant,
and silently numbering them makes a real bug look like it works until the loop's length changes.

## Consequences

### What this makes easy
* Partial-failure handling becomes ordinary Rust: read the failed sibling's result, decide,
  carry on. No special mode, no separate API.
* A five-way fan-out costs one attempt, not five: the whole group emits in one envelope.
* `all` and `any` express fail-fast and race semantics without the handler hand-rolling
  cancellation, which it cannot do correctly for a child run anyway.

### What this makes hard
* Under `all_settled` a run waits for the slowest member even when an early failure has made
  the rest pointless — the price of never discarding a recorded result.
* `any` needs a cancellation path for every op type, including `invoke` children that have
  descendants of their own.

### What we accept
* **Cancellation is not compensation.** Under `all` and `any` a sibling that had already
  executed its effect keeps that effect; only its *result* is discarded. A step that must be
  undone needs an `on_cancel` path (protocol §7.4), and stepd will not invent one.
* `all_settled` puts the decision on the developer. A handler that reads a step result without
  checking whether it failed will proceed on a value that does not exist. `stepd lint` and the
  test harness's assertions are the mitigation; the type system is not (a failed step surfaces
  as `Err`, so `?` propagates it, but `.unwrap_or_default()` does not).
* **`all` and `any` are specified but not yet enforced by the engine.** `commit_ops` in
  `engine/rust/migrations/0006_engine_complete.sql` accepts `p_join text DEFAULT 'all_settled'` and
  never reads it; the wake rule it implements is `all_settled` and only that — a run is
  requeued when no `run_steps` row for it is still `pending`. An SDK sending `join: "any"`
  today gets `all_settled` behaviour with no error. This is a known gap, not a subtlety, and
  it must close before the conformance suite can claim §5.2.1.

## Alternatives considered

| Option | Why not |
|---|---|
| Fail the run on any sibling failure (only `all`) | Removes the most common real pattern: fetch three enrichment sources, proceed with whichever returned. |
| `all_settled` only | Leaves no way to express a race, and forces every handler to hand-roll cancellation of an in-flight child run — which it cannot do correctly from user code. |
| Per-op policy rather than per-envelope | The policy governs when the *batch* resolves; a per-op field would let one envelope express contradictory wake conditions. |
| Return from the group on first failure | Siblings that already executed would go unrecorded. Their effects happened and nothing remembers them, so the next attempt repeats them — silent duplicate execution, no error anywhere. |
| Cancel siblings inside the SDK | The SDK cannot cancel a child run; only the engine can, and only the engine can do it durably. |

## Verification

* `sdk/rust/crates/stepd-sdk-core/src/join.rs` — `group_outcome`, whose doc comment states the
  rule: returning early on first failure "would leave siblings that had already executed
  unrecorded, which is the one thing a durable engine must never do". `JoinAll::poll` drives
  every member to `Ready` before the outcome is computed.
* `sdk/rust/crates/stepd-sdk-core/src/tests.rs` —
  `a_duplicate_id_is_rejected_before_any_member_runs` (zero closures executed, so the one
  short-circuiting path cannot lose work), `a_duplicate_id_inside_a_parallel_group_is_fatal`
  (`protocol_violation`, non-retryable),
  `a_parallel_group_emits_one_envelope_and_each_member_runs_once` (two attempts, not one per
  member), and `join_all_handles_a_fan_out_with_discriminated_ids`.
* `engine/rust/tests/sql/test_engine_ops.sql` — the `parallel batch` block commits a `sleep` and a
  `wait_event` in one envelope, delivers the event, and asserts the run stays parked while the
  sibling is pending, then resumes only once every op has settled. This is the engine-side
  `all_settled` wake rule.
* `engine/rust/migrations/0006_engine_complete.sql` — `deliver_to_inbox` returns without requeueing
  while any `run_steps` row is still `pending`: "a run that waited and slept in one parallel
  batch must not wake with the sleep still pending".
