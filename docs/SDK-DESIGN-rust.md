# stepd Rust SDK — Design

| | |
|---|---|
| Version | 0.1 (draft) |
| Date | 2026-08-22 |
| Implements | stepd SDK Protocol v1, rev 1.1 |
| Status | Mechanisms prototyped and property-tested (see §9) |

The SDK is the subtlest code in the project. Two mechanisms carry all the risk:
**short-circuit control flow** (yielding an op from the middle of a handler) and
**occurrence assignment** (stable step hashes across replays, safe under concurrency).
Both are R1 — a defect corrupts silently rather than erroring.

This document specifies both, and records a design finding from prototyping that changes
the recommended approach.

---

## 1. Surface

```rust
stepd::function("order-fulfilment")
    .on_event("order.created")
    .key("'order:' + string(event.data.order_id)")
    .singleton()
    .run(|ctx, ev: OrderCreated| async move {
        let charge = ctx.step("charge", || async {
            gateway.charge(ev.total).await
        }).await?;

        ctx.sleep("cooldown", Duration::from_secs(86_400)).await?;

        let approval = ctx.wait_event::<Approval>("approval", "order.approved")
            .timeout(Duration::from_days(7))
            .prompt("Approve refund", json!({ "amount": ev.total }))
            .await?;

        let (invoice, risk) = ctx.join((
            ctx.step("fetch-invoice", || async { billing.invoice(&ev).await }),
            ctx.step("score-risk",    || async { risk_api.score(&ev).await }),
        )).await?;

        Ok(Receipt { charge, approval, invoice, risk })
    });
```

Design constraints on the surface:

* A step is visually distinct from ordinary code, because the memoization boundary is
  semantically load-bearing. `ctx.step(...)` reads as a boundary; a bare `.await` does not.
* Step ids are explicit strings, never derived from function names or line numbers — the
  hash must survive refactoring that moves code around.
* The handler returns `Result<T, StepError>`; `?` propagates both real errors and the
  short-circuit signal (§3).

## 2. Occurrence assignment — claim eagerly

### 2.1 The finding

The protocol (§6.1) requires occurrence to be assigned "in program order at the point of
the call". Prototyping showed **where** the claim happens matters more than any runtime
guard:

| Approach | `join!` behaviour |
|---|---|
| **Lazy** — claim inside the returned future, at first poll | Occurrence follows **poll order**. Two `ctx.step("a", …)` calls joined together get different hashes depending on the scheduler. Silent corruption. |
| **Eager** — claim when `ctx.step(...)` is *called*, before the future is returned | Occurrence follows **declaration order**. Poll order is irrelevant. Safe by construction. |

Measured directly (`examples/eager_claim.rs`): with eager claiming, futures created as
`a`, `b`, `a` and then polled in reverse still hash to `a#0`, `b#0`, `a#1`. With lazy
claiming, reversing the poll order swaps the occurrences.

### 2.2 Consequence

**`ctx.step()` is not an `async fn`.** It is a synchronous function that claims the hash,
consults the memo, and returns a future:

```rust
impl Ctx {
    pub fn step<'a, T, F, Fut>(&'a self, id: &'a str, f: F) -> StepFuture<'a, T>
    where F: FnOnce() -> Fut, Fut: Future<Output = Result<T, StepError>>,
    {
        // --- happens NOW, synchronously, in program order ---
        let hash = self.claim(id);              // occurrence assigned here
        match self.memo.get(&hash) {
            Some(v) => StepFuture::Memoized(decode(v)),   // closure never constructed
            None    => StepFuture::Execute { hash, fut: f() },
        }
    }
}
```

This makes the dangerous case impossible rather than detectable: `join!`, `select!`,
`FuturesUnordered` and hand-rolled polling all become safe, because every hash was fixed
before any of them ran.

It also means a memoized step **never constructs its future**, so replay does no work and
allocates nothing for steps already recorded.

### 2.3 Defence in depth

Eager claiming handles same-task concurrency. Two further guards cover the rest:

1. **`Ctx` is `!Send + !Sync`** (it owns a `RefCell`). `tokio::spawn(async move { ctx.step(…) })`
   therefore fails to *compile*. The type system rejects cross-task claiming outright — the
   best possible outcome, since it is caught before the code ever runs.
2. **Pass-token check** as a runtime backstop for what the types cannot see (scoped threads,
   a `Ctx` smuggled through an `Rc`, FFI). A claim carrying a foreign token is
   `Halt::Fatal`, never a guessed hash. Verified under 1 600 concurrent claim attempts:
   every one rejected, zero succeeded.
3. **`ctx.join(...)`** for structured parallelism, which additionally enforces unique ids
   within the group (`ambiguous_step_id`) and emits all members in one envelope.

Layered: types reject what they can, eager claiming makes the rest order-independent, and
the token check catches the residue.

## 3. Short-circuit control flow

When a handler reaches unmemoized work, it must stop and return an op. Options considered:

| Mechanism | Verdict |
|---|---|
| Panic + `catch_unwind` | Rejected. Unwinding across an async boundary is fragile, `panic = "abort"` breaks it, and it corrupts user `Drop` semantics. |
| Generators / `Stream` | Rejected for v1. Ergonomically poor without stable generator syntax; forces a non-idiomatic handler shape. |
| A future that never completes | Rejected. Leaks the task and gives the runtime no way to return the op. |
| **`Err` variant propagated by `?`** | **Chosen.** Idiomatic, zero-cost, works identically in sync and async, composes with user error handling. |

```rust
pub enum StepError {
    /// Real failure from user code.
    Failed { retryable: bool, source: BoxError },
    /// Control flow, not failure: the handler reached unmemoized work.
    Halt(Halt),
}

pub enum Halt {
    Yield(Vec<Op>),     // emit these ops, expect another attempt
    Fatal(String),      // a protocol rule was violated; non-retryable
}
```

The handler signature is `Result<T, StepError>`, so `?` propagates a halt to the top of the
handler where the transport converts it into an `AttemptResponse`.

**The hazard this creates** and its mitigation: user code that swallows errors
(`let _ = ctx.step(...)`, `.unwrap_or_default()`, a bare `match` arm) also swallows the
halt, and the handler continues in an undefined state. Countermeasures:

* `StepError::Halt` carries `#[must_use]` and the SDK's error type deliberately does **not**
  implement `From<StepError>` for common user error types, so absorbing it requires
  conscious effort.
* The `Ctx` records that a halt was issued. If a handler returns `Ok` after a halt was
  raised, the SDK fails the attempt non-retryably with `swallowed_halt` and names the step —
  turning a silent corruption into a loud, specific error.
* `stepd lint` (F-DX-6) flags `let _ =` and `.ok()` applied to a step result.

## 4. Replay pass

Each attempt is one pass:

1. Decode `AttemptRequest`; verify signature and nonce; check the fence.
2. Build `Ctx` with the memo map, a fresh pass token and empty counters.
3. Invoke the handler.
4. On `Ok(v)` → `done` op. On `Halt::Yield(ops)` → those ops. On `Halt::Fatal` or
   `Failed { retryable: false }` → `error` op, non-retryable.
5. Attach `diagnostics`: orphaned hashes, replay duration, steps replayed.

Replay is cheap by construction: memoized steps return without constructing their futures,
and blob values are lazy handles (§6).

## 5. Deadline discipline

Protocol §7.1.1 forbids dropping a running step future to meet the attempt deadline.
Implementation:

* The SDK never wraps a step future in `tokio::time::timeout`. Dropping it mid-await leaves
  the external effect indeterminate *and* loses the result that would have made it durable.
* Instead the SDK monitors the deadline and, when it expires, **abandons the HTTP response**
  while letting the step run to completion. The server sees no response, treats the attempt
  as `unknown`, and retries. The step re-executes; it is recorded once.
* At start-up the SDK warns if any declared step timeout exceeds the attempt deadline.
* `ctx.idempotency_key()` returns a stable `run_id + step_hash` for use with provider-side
  idempotency, since at-least-once execution is the contract.

## 6. Payloads

* Values under the inline threshold are plain `serde_json`.
* Managed blobs are a lazy handle: `Blob` derefs to nothing until `.bytes()`, `.stream()` or
  `.range(..)` is called. Replaying forty blob-bearing steps fetches nothing.
* Uploads use the two-phase reservation and go **directly** to the blob store, never through
  the stepd server.
* `ExternalRef` is a transparent passthrough type — the SDK never resolves it.

## 7. Testing surface (F-DX-1)

The same machinery that drives a real attempt drives the test harness, so tests exercise
production paths:

```rust
let t = stepd::test::harness(order_fulfilment);
t.given_event(order_created(4711));
t.expect_step("charge").returning(json!({"tx": "ch_1"}));
t.advance_clock(Duration::from_days(1));          // 24h sleep resolves instantly
t.send_event(approval(4711));                      // arrives BEFORE the wait registers
let out = t.run_to_completion()?;

t.assert_step_executed_once("charge");
t.assert_step_not_re_executed_after_replay("charge");
assert_eq!(out.shipped, true);
```

Notable: the harness can deliver an event *before* the handler reaches its `wait_event`,
which is how a user tests their own code against the early-signal semantics.

## 8. Crate layout

```
stepd-sdk           facade: function builder, Ctx, test harness
stepd-sdk-core      claim/memo/halt machinery — no I/O, no runtime  ← the R1 code
stepd-sdk-axum      mount as an axum router
stepd-sdk-lambda    mount as a Lambda handler
```

`stepd-sdk-core` depends only on `stepd-proto`. It is synchronous and runtime-agnostic,
which is what makes the R1 logic exhaustively testable without scheduling noise.

## 9. Verification status

A working prototype of §2 and §3 exists and passes 15 property tests plus an adversarial
suite. Assertions that now hold in code, not just in prose:

| Property | Test |
|---|---|
| Each step executes exactly once across all attempts | `each_step_executes_exactly_once_across_all_attempts` |
| Loop occurrences stable across replays | `loop_occurrences_are_stable_across_attempts` |
| Hash scoped by function id (no child-run collision) | `hash_is_scoped_by_function_id` |
| Off-path claim is fatal, never guessed | `claiming_an_occurrence_off_the_sequential_path_is_fatal` |
| Duplicate id in a parallel group is fatal | `duplicate_id_inside_a_parallel_group_is_fatal` |
| Parallel members hash by declaration order, emit in one envelope | `parallel_group_hashes_are_stable_and_each_member_runs_once` |
| Inserting a step before completed ones does not re-execute them | `inserting_a_step_before_completed_ones_does_not_re_execute_them` |
| Renaming a step orphans its result (F-DX-4 diagnostic) | `renaming_a_step_orphans_its_result` |
| `wait_event` defaults to `run_start` | `wait_event_defaults_to_run_start_window` |
| 1 600 concurrent off-path claims: all rejected, none succeeded | `concurrent_claims_are_all_rejected_never_guessed` |
| Hash sequence identical across 50 replays | `hash_sequence_identical_across_many_replays` |
| 500 seeded crash interleavings: identical result, 4 hashes recorded, steps re-execute but never mis-record | `crash_at_any_point_yields_identical_outcome` |
| Naive counter under reordering demonstrably breaks | `naive_counter_under_threads_produces_unstable_hashes` |
| Eager claiming makes `join!` order-independent; lazy does not | `examples/eager_claim.rs` |

The last two are deliberately included: they demonstrate that the hazard is real and that
the chosen design removes it, rather than asserting it on authority.

## 10. Open questions

1. Should `ctx.step` require the closure to be `UnwindSafe`, so a panic inside a step can be
   converted to a retryable error rather than killing the worker?
2. Typed step results (`ctx.step::<Invoice>(...)`) versus `serde_json::Value` — typing is
   nicer but a schema change between versions makes an old memoized value fail to decode.
   Proposal: decode failure on a memoized value is a non-retryable error naming the step,
   never a silent re-execution.
3. Whether `ctx.join` should support heterogeneous tuple types in v1 (needs a macro) or
   ship with a homogeneous `Vec` first.
4. Whether to expose the pass token publicly for advanced users writing their own
   combinators, or keep it private and accept that the `join` set is closed.
