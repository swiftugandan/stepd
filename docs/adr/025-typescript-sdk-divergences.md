# ADR-025: Where the TypeScript SDK deliberately differs from the Rust one, and why each difference is safer

| | |
|---|---|
| Status | Accepted, implemented |
| Date | 2026-08-25 |
| Supersedes | — |

## Context

The TypeScript SDK is a port. [ADR-024](024-language-trees.md) records the tree it
lives in; this one records where it does **not** follow `stepd-sdk`, because a
port that diverges silently is worse than either a faithful copy or an
independent implementation. A reader comparing the two needs to know which
differences are decisions and which are drift.

The forcing constraint is that Rust's strongest defence does not exist here.
`stepd-sdk-core` makes `Ctx` `!Send` and `!Sync`, so `tokio::spawn`-ing a task
that claims a step **fails to compile** — the earliest and cheapest place to
catch the mistake ([ADR-012](012-eager-occurrence-claiming.md)). TypeScript has
no equivalent. Everything below follows from having to replace a compile error
with something else, or from JavaScript making a hazard likelier than Rust does.

None of these is a preference. Each one is a case where copying Rust exactly
would have produced a defect that corrupts quietly.

## Decision

### 1. Step results are projected through JSON on first execution too

Rust returns the value the closure produced on the attempt that runs it, and the
JSON projection on every attempt after. The TypeScript SDK round-trips on both.

The asymmetry is a trap. `ctx.step('t', () => new Date())` hands back a `Date`
the first time and an ISO string ever after, so a handler calling `.getTime()`
works until the first retry — and a retry is exactly the moment nobody is
watching. Paying one `JSON.parse(JSON.stringify(..))` per step makes attempt one
behave like attempt forty.

This is a divergence from Rust rather than a fix to it only because changing it
there is a breaking change to a shipped API.

### 2. A step body may not create steps

Rust permits a nested `ctx.step` — the closure borrows `&ctx` and it compiles.
TypeScript refuses it, non-retryably.

A nested claim happens only on the attempts where the outer body actually runs.
On an attempt that replays the outer step from the memo, the body is never
called, so the inner claim never happens and every later occurrence of that id
shifts. That is the same class of defect as an unstable hash, reached by a
different route, and nothing about it errors.

`Ctx` tracks a step-body depth and refuses a claim made at depth > 0.

### 3. Three runtime guards replace `!Send`

They are not belt-and-braces here; they are the only line of defence.

| Guard | What it catches |
|---|---|
| Pass sealing | A claim after the pass ended — a timer, a floating promise, a `Ctx` captured in a closure that outlived its attempt |
| Step-body depth | Decision 2 |
| Repeated id in one parallel group | A fan-out loop with no discriminator |

The first is what makes `conf-offpath` implementable at all. Rust declares
`statically_prevented: ["offpath_claim"]` and omits the function; TypeScript
declares nothing of the sort, registers the function, and fails it at run time —
which is what §12.1's closed enum exists to force.

### 4. Suspension is a thrown sentinel, and swallowing it matters more

Rust propagates `Err(Halt)` with `?`. TypeScript throws a `Halt`, which is safe
across `await` in a way Rust unwinding across an async boundary is not — the
Rust design doc rejects `panic!` for exactly that reason and it does not apply.

The cost is that `catch` is far more idiomatic in JavaScript than
`unwrap_or_default()` is in Rust. Two shapes swallow a `Halt` without looking
like a mistake:

* `try { await ctx.step(..) } catch {}`
* `await Promise.allSettled([a, b])` — which never rejects, so it absorbs the
  `Halt` a recorded step raises and the handler runs on to its return

Both are caught by the `swallowed_halt` check, which is the same check Rust has
and carries more weight here.

A related consequence: a `Halt` raised **inside** a step body is rethrown
unchanged rather than passed through the "anything thrown is an application
failure" path. Without that, a protocol violation became an ordinary retryable
error and the engine retried a program that could not succeed.

### 5. `ctx.parallel(...)`, not `join`/`join3`/`joinN`

Rust needs an arity family because its tuples are heterogeneous and typed.
TypeScript expresses the same thing with one variadic function and a mapped
return type. The semantics are identical: hashes assigned up front, duplicate
ids refused before any body runs, every member settled before the group
resolves, a failure surfaced rather than cancelling siblings.

### 6. Options objects, not builders, for `waitEvent` and `invoke`

Rust returns a `WaitBuilder`/`InvokeBuilder` so that the claim can happen at
construction while options are still being set. TypeScript takes an options
object and claims in the same call, which removes a class of confusion the
builders create: there is no object to hold un-awaited, so "when does this
claim?" has one answer.

### 7. An arbitrary thrown value is retryable

Rust handlers return `StepResult`, so there is no third case. TypeScript
handlers can throw anything — a `TypeError`, a string, an `AbortError`. These
become **retryable** failures, matching §5.1's default for an `error` op.

A bug that fails identically every time then burns the retry budget and is caught
by the engine's poison-pill grouping, which is the machinery built for that.
Guessing "terminal" would turn one transient exception into a permanently failed
run. An author who knows better says so with `fatal(..)`.

### 8. The harness counts attempts from 1

Rust's `testing::harness` reports `2` on a workflow's first attempt
([#31](https://github.com/swiftugandan/stepd/issues/31)). The TypeScript harness
does not reproduce it and has a test pinning the first attempt to 1.

This is a divergence from Rust's *behaviour* and agreement with the protocol,
which is the only reading of "the same number in a test as in production" that
holds.

## Consequences

### What this makes easy

* A reader comparing the two SDKs can tell a decision from drift.
* Every guard here is testable, and each has a test that fails without it.
* The Rust SDK gets two defect reports it would not otherwise have had (#31, and
  the dead `Inner.group` check noted below).

### What this makes hard

* Two SDKs that are *nearly* the same are harder to keep in step than two that
  are identical or plainly different. Nothing detects drift automatically beyond
  the shared fixtures in `spec/fixtures/protocol.json`, which cover only the
  hash and the signature.
* Decision 1 costs a JSON round trip per step. Measured against a network round
  trip per step it is nothing, but it is not free.
* Decision 2 forbids a program Rust accepts, so a workflow ported from Rust may
  need restructuring. This is intended.

### What we accept

* **These differences make the two SDKs less comparable as evidence.** Two
  implementations that behaved identically would at least fail identically. This
  one is closer to the protocol in several places, which is better for its users
  and slightly worse for cross-checking.
* **This is still a port, and it is not the evidence
  [#6](https://github.com/swiftugandan/stepd/issues/6) asks for.** It was written
  by reading `stepd-sdk`, so the two can share a misreading of the specification
  the same way the conformance battery and the Rust SDK can. Reaching level 2
  shows the §12.2 contract is implementable twice against one server. It does not
  show `spec/PROTOCOL.md` is sufficient on its own.
* **The list will grow.** Nothing forces a future divergence to be recorded here,
  and a divergence that is not recorded is drift.

## Alternatives considered

| Option | Why not |
|---|---|
| Copy Rust exactly, including its asymmetries | Decisions 1 and 2 would each ship a silent-corruption path into a new SDK, knowingly. Fidelity to an implementation is not a goal; fidelity to the protocol is. |
| Fix the Rust SDK to match instead | Decision 1 is a breaking change to a shipped API, and decision 2 forbids programs that currently compile. Worth proposing separately; not worth blocking a second SDK on. |
| Leave the differences in commit messages | Which is where they were before this ADR. A reader comparing the two SDKs does not read four commit messages, and by the fourth divergence the pattern needed a home. |
| Write the divergences into each SDK's README | Half of them are only meaningful as a comparison, and a README that explains itself by reference to another language's implementation is the wrong document. |

## Verification

| Claim | Evidence |
|---|---|
| JSON projection on first execution | `replays a value through JSON, so attempt one matches attempt forty` (`sdk-core/test/memo.test.ts`) |
| A nested claim is refused, non-retryably, recording nothing | `reports a nested claim as a protocol violation, not a retryable error` (`sdk-core/test/claiming.test.ts`) |
| A claim after the pass is refused | `refuses a claim after the pass has ended`; and end-to-end as `conf-offpath` |
| `Promise.allSettled` over steps is caught | `catches Promise.allSettled over steps as a swallowed halt` (`sdk-core/test/parallel.test.ts`) |
| A duplicate id is refused before any body runs | `rejects a repeated id before any member body runs` — asserts no effect was recorded |
| An arbitrary throw is retryable | `treats an arbitrary thrown error as retryable, per §5.1` (`sdk-core/test/pass.test.ts`) |
| The harness reports attempt 1 first | `starts at 1, as the protocol counts it` (`sdk/test/testing.test.ts`) |
| Both SDKs agree on the two algorithms that corrupt silently | `spec/fixtures/protocol.json`, asserted by `stepd-proto/tests/fixtures.rs` and `spec/typescript/test/fixtures.test.ts` |
| The whole thing satisfies the protocol | `stepd conformance --app …`: 28/28 cases, **CONFORMANT AT LEVEL 2**, in `tier 2 · integration` |

### What the port found in the Rust SDK

Three things, none of which the Rust tests would have surfaced:

1. **`testing::harness` reports attempt 2 first** ([#31](https://github.com/swiftugandan/stepd/issues/31)).
   Every handler branching on `ctx.attempt()` behaves differently under test than
   in production, in the direction that hides bugs: first-attempt behaviour is
   never exercised.
2. **`Inner.group` is dead.** It is initialised to `None` and never set, so the
   duplicate-id check inside `Ctx::claim` cannot fire. The real check is in
   `JoinAll::new`, so the behaviour is right — but the code reads as a guard and
   is not one.
3. **A 400 from an app is retried rather than failing the run**
   ([#30](https://github.com/swiftugandan/stepd/issues/30)) — found by making the
   `truncation` suite bite, which is a defect in the engine rather than the SDK.
