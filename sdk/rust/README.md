# stepd — the Rust SDK

What a workflow author imports. Two crates, no engine, no database, no server.

```
crates/stepd-sdk-core/    the replay machinery — no async, no I/O
crates/stepd-sdk/         Function, Ctx, the axum adapter, the test harness
```

The root [`README.md`](../../README.md) says what the project is;
[`docs/SDK-DESIGN-rust.md`](../../docs/SDK-DESIGN-rust.md) covers the two
mechanisms that carry all of this SDK's risk.

## Why this is its own workspace

Because "a workflow author's process contains no engine" should fail a build
rather than appear in a diagram. This workspace's whole dependency graph is:

```
  stepd-sdk
      └── stepd-sdk-core
              └── stepd-proto        ../../spec/rust
```

`cd sdk/rust && cargo build` resolves with only `spec/rust` beside it. CI also
greps the tree for engine crates, because a build succeeding says nothing about
what it pulled in — see [ADR-024](../../docs/adr/024-language-trees.md).

`stepd-proto` is a path dependency on `spec/rust`, not a member here. It is the
published wire contract, shared with the engine and with every other SDK; owning
it from one side would make it that side's type definitions rather than a
contract.

## Running the tests

No database, no server, no `STEPD_TEST_DATABASE_URL` — that is the point of the
split. Seconds, not minutes:

```bash
cargo test --workspace
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
```

64 as of this writing: 32 in `stepd-sdk-core` for the replay machinery, 30 in
`stepd-sdk` covering the builders, the signature path, the blob client and the
in-process workflow harness, and 2 doctests.

Two of them are the ones to read first, in `stepd-sdk-core/src/tests.rs`.
`eager_claiming_makes_poll_order_irrelevant` creates step futures `a, b, a` and
polls them in reverse, asserting the hashes are unchanged.
`naive_counter_under_reordering_is_demonstrably_broken` implements the lazy
scheme as a deliberate control and asserts that it *does* break — if that test
ever passes, the control is broken and the first test is proving nothing.

Requires Rust **1.85**, for `Waker::noop`: it is what lets
`stepd_sdk::testing::harness` run a workflow to completion inside an ordinary
`#[test]`, with no async runtime. The moment testing a workflow needs ceremony,
fewer workflows get tested.

## Large journals

A run whose journal outgrows the server's inline ceiling arrives with
`state_truncated` and only part of its steps. This SDK pages the rest before
replaying (§8.6), which needs a server address:

```rust
let app = App::new("billing", url)
    .signing_key(key)
    .journal_source("https://stepd.internal", operator_token)
    .function(/* … */);
```

Without it such an attempt fails non-retryably, naming the method. That is the
only safe alternative: replaying against a partial journal re-executes every step
the app could not see, the run still completes, and nothing errors — the same
failure mode as an unstable step hash, reached by a different route.

Only runs that reach the ceiling are affected, so an app whose runs stay small
never needs it.

## Conformance

This SDK is what the protocol §12 battery drives, through the reference app in
`engine/rust/crates/stepd-conformance`. It reaches **level 2**.

That is weaker evidence than it looks, and the project says so in its own
[README](../../README.md): the battery and this SDK were written together, so
they can agree on a shared misreading of the specification and no number of
green ticks would surface it. The strong evidence would be an independent
implementation passing, and it does not exist yet —
[#6](https://github.com/swiftugandan/stepd/issues/6).
