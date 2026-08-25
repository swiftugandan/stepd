# ADR-012: Eager occurrence claiming and structured parallelism

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Step identity is `hash = sha256(function_id ‖ step_id ‖ occurrence)[0..8]`, where occurrence is
a per-`step_id` counter reset at the start of every attempt (protocol §6). The counter is what
makes steps inside loops work: `charge` encountered three times is `charge#0`, `charge#1`,
`charge#2`.

The hash is also the memo key. If a step's hash differs between attempt *n* and attempt *n+1*,
the server holds a result the handler never asks for and the handler asks for a result the
server does not have. The step re-executes — the payment is taken twice — and the old result
is orphaned. **Nothing errors.** The run completes, the console shows green, and the only trace
is an orphaned-hash count nobody was reading. Protocol §6.1 names this the single most
dangerous failure mode in the design; gap A2 rates it S1 precisely because it corrupts quietly
rather than failing.

Counter assignment by execution order breaks the moment a handler runs steps concurrently: two
tasks racing to claim occurrences 0 and 1 will swap between attempts. Prototyping
(`docs/SDK-DESIGN-rust.md` §2, `docs/sdk-prototype/`) then established a sharper rule than the
protocol's prose. "Program order" is not a statement about *where in the source* the claim
appears; what matters is **when** it happens:

| Approach | `join!` behaviour |
|---|---|
| Lazy — claim inside the returned future, at first poll | Occurrence follows *scheduler* order. Two joined `ctx.step` calls get different hashes on different attempts. Silent corruption. |
| Eager — claim when `ctx.step(…)` is *called* | Occurrence follows *declaration* order. Poll order is irrelevant. Safe by construction. |

## Decision

**`ctx.step` is a synchronous function, not an `async fn`. It claims the occurrence, consults
the memo, and returns a future whose hash is already fixed.**

This is the load-bearing sentence of the whole SDK, and it is documented as such in the
module docs of `sdk/rust/crates/stepd-sdk-core/src/lib.rs`.

Consequences that follow directly: `join!`, `select!`, `FuturesUnordered` and hand-rolled
polling are all safe without the user importing anything from stepd, because every hash was
fixed before any of them ran — the hazard is removed rather than detected; a memoised step
never constructs its future, so replaying forty recorded steps calls no closure and allocates
nothing; and `sleep`, `signal`, `continue_as_new`, `wait_event` and `invoke` all claim the same
way, the last two in `IntoFuture::into_future` so a builder never awaited never claims.

Layered defences cover what eager claiming alone cannot:

* **`Ctx` is `!Send + !Sync`** (a `RefCell` plus `PhantomData<Rc<()>>`).
  `tokio::spawn(async move { ctx.step(…) })` fails to *compile*. The earliest and cheapest
  place to catch the mistake is the type system.
* **Pass token.** `Ctx::claim` compares the caller's `PassToken` against the pass's own and
  returns `Halt::Fatal` on mismatch — for a `Ctx` smuggled through an `Rc`, a scoped thread,
  or FFI. The SDK never emits a guessed hash, because a guessed hash re-executes recorded
  work.
* **`ctx.join` / `ctx.join_all`** for structured parallelism: repeated ids inside one group are
  rejected as `ambiguous_step_id` *before any member is polled*, and all members emit in one
  envelope so a five-way fan-out costs one round trip rather than five.

## Consequences

### What this makes easy
* Concurrency in user code needs no stepd-specific combinator to be correct — otherwise a user
  is one forgotten import away from silent corruption.
* Replay is free for recorded steps: no closure construction, no allocation, no I/O.
* Adding or renaming a step has published semantics rather than emergent ones.

### What this makes hard
* `ctx.step` cannot be an `async fn`, so its signature carries an explicit `StepFuture` and a
  lifetime. Handlers need the `Handler<'a, T>` trait with a `for<'a>` bound (and the `wf!`
  macro for closures) because a plain `Fn` bound cannot express "the returned future borrows
  the context it was given".
* Steps cannot be created from spawned tasks at all — deliberate, but it means a CPU-bound step
  blocks its siblings inside the one handler task.

### What we accept
* **`step` claims at construction; `wait_event` and `invoke` claim at `.await`.** For ordinary
  code the two coincide, since a builder is awaited where it is written. A builder held in a
  variable and awaited later, out of declaration order, would claim out of order — and the
  pass token does not catch it, because it is still on the sequential path. `stepd lint`
  (F-DX-6) is the intended countermeasure; today it is developer discipline.
* Step ids remain developer-supplied strings. Renaming one orphans its result and re-executes
  its side effect. No mechanism removes that; diagnostics only make it visible.
* A memoised value that no longer decodes into the handler's type is a non-retryable error
  naming the step, never a silent re-execution — the side effect already happened, so
  re-running the closure is the worse of the two failures.

## Alternatives considered

| Option | Why not |
|---|---|
| Claim lazily at first poll, detect reordering at runtime | Detection needs a previous attempt to compare against, so the first corrupting attempt is undetectable — and it reports the problem after the work has re-run. |
| Derive the id from file and line, or from the closure's type | Breaks under any refactor that moves code, which is precisely when developers most need identity to be stable. |
| Require the developer to pass the occurrence explicitly | Pushes the hardest invariant in the system onto every user, in every loop, forever. |
| Ban concurrency in handlers entirely | Removes the hazard and most of the value: parallel fan-out is a primary reason to use a workflow engine. |
| Only offer `ctx.join`, no bare futures | Users reach for `futures::join!` anyway. A design that is correct only when the right import is used is not correct. |

## Verification

All in `sdk/rust/crates/stepd-sdk-core/src/tests.rs` unless noted.

* `eager_claiming_makes_poll_order_irrelevant` — futures created in order `a, b, a` and polled
  in reverse produce identical hashes. This is the property the design exists for.
* `naive_counter_under_reordering_is_demonstrably_broken` — a deliberate control implementing
  the lazy scheme and asserting it *does* change under reordering. Included so the property
  above is a measurement rather than an assertion of faith; if it ever passes trivially, the
  control is broken and the test says so in its failure message.
* `a_memoized_step_never_constructs_its_future` — the replay-cost claim, tested not assumed.
* `loop_occurrences_are_stable_across_attempts`, `hash_sequence_is_identical_across_many_replays`
  and `hash_is_scoped_by_function_id` — counter, sequence and scoping stability.
* `a_duplicate_id_is_rejected_before_any_member_runs` — asserts zero member closures executed;
  rejecting after a member ran would leave a side effect nothing recorded.
* `a_parallel_group_emits_one_envelope_and_each_member_runs_once`,
  `join_all_handles_a_fan_out_with_discriminated_ids` — one envelope, two attempts, each member
  executed once.
* `inserting_a_step_before_completed_ones_does_not_re_execute_them`,
  `renaming_a_step_orphans_its_result`, `a_swallowed_halt_is_a_loud_error_not_a_silent_success`,
  and `a_crash_at_any_point_yields_the_same_outcome` — seeded crash interleavings converging on
  one outcome, steps re-executed but never mis-recorded.
* `reference/simulation.py` — properties P4 (hash stability across replays) and P2 (a step hash
  recorded at most once).
