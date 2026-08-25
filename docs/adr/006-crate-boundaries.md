# ADR-006: Crate boundaries and the no-backend-dependency rule for `stepd-core`

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

stepd ships one binary but is not one program. It is an engine, a wire protocol, a
Postgres implementation of that engine's storage, an HTTP transport, an expression
interpreter, an SDK and a CLI. Two commitments make the split load-bearing rather than
cosmetic.

The first is BR-17: storage, queue, timer, blob and expression concerns sit behind
replaceable interfaces, so that changing a backing service is a new crate rather than an
engine change. The second is the open protocol. `spec/PROTOCOL.md` is a published artefact
and Lin, the platform engineer in PRD §2, is expected to write a Python SDK against it. If
the only usable Rust expression of the protocol arrives welded to our engine — an async
runtime, a connection pool, a transport — then "open protocol" means "read the Markdown and
start from nothing", and the spec drifts from the implementation within a release.

The failure mode being designed against is not a build error. It is the quiet one: the
engine reaches into `sqlx` for one query that would be awkward to express through a trait,
that query grows a second and a third, and eighteen months later swapping the store is a
rewrite that nobody proposes because the estimate is unbelievable.

## Decision

Nine crates, with dependency arrows pointing inward (`engine/rust/Cargo.toml`):

| Crate | Depends on | Role |
|---|---|---|
| `stepd-proto` | serde, sha2, hmac, hex, subtle, chrono, uuid | Wire types, step hash, signature. No I/O, no runtime. |
| `stepd-sdk-core` | `stepd-proto` | Handler context, eager occurrence claiming. |
| `stepd-core` | `stepd-proto` | Engine: dispatch loop, retry, breaker, fairness. Generic over the traits. |
| `stepd-store-postgres` | `stepd-core`, `sqlx` | Every storage trait. |
| `stepd-transport-http` | `stepd-core`, `reqwest` | Signed push transport. |
| `stepd-expr-cel` | `stepd-core` | `ExprEngine`. |
| `stepd-sdk` | `stepd-proto`, `stepd-sdk-core`, `axum` | Rust SDK. |
| `stepd-server` / `stepd-cli` | all of the above | Wiring. |

Two rules follow, and they are the whole ADR:

1. **`stepd-proto` has no async runtime, no I/O and no database.** It is a pure data and
   algorithm crate. A third party implementing an alternative server or SDK can depend on
   it for the two things every implementation must agree on byte-for-byte — the step hash
   and the HMAC — without inheriting our concurrency model.
2. **`stepd-core` names no concrete backend.** Every component reaches it as a trait defined
   in `stepd-core/src/traits.rs`: `StateStore`, `Queue`, `TimerStore`, `EventLog`,
   `Transport`, `BlobStore`, `ExprEngine`, `Housekeeping`. Backends depend on `stepd-core`,
   never the reverse. `stepd-server` is the only crate that knows `PostgresStore`,
   `HttpTransport` and `CelEngine` exist.

Where a piece of contract could plausibly live on either side of the seam, it lives on the
engine side. `CommitOutcome::from_sql` in `traits.rs` parses the string that the SQL
`commit_ops` returns, and it sits next to the enum rather than in the Postgres crate,
because two backends spelling `stale_fence` differently is a silent divergence.

## Consequences

### What this makes easy
* The dispatch loop is tested with no database and no network at all.
  `stepd-core/src/testing.rs` provides in-memory doubles for every interface behind a
  `testing` feature, and `stepd-core/tests/engine.rs` drives fourteen behavioural cases —
  quarantine, circuit opening, namespace rotation, cascade — through the real `Dispatcher`.
* A second store is a new crate implementing known traits, not a patch to the engine. The
  in-memory doubles double as an executable specification of what the engine expects.
* The protocol crate is small enough to read in a sitting: 1 007 lines across five files.

### What this makes hard
* Anything the engine wants that no trait exposes needs a trait change, which touches every
  implementation. `steps_page` exists because state truncation could not be expressed
  otherwise; adding it was a four-crate change.
* Trait objects and `async_trait` cost a boxed future per call on the dispatch path.
* The SDK cannot reuse the engine's helpers, because it must not depend on `stepd-core`.
  `stepd-sdk-core` exists to hold what both sides of the protocol need.

### What we accept
* The CI enforcement described in PRD §6.1 — `cargo-deny` bans plus a dependency-graph
  assertion — **does not yet exist**: `engine/rust/.github/workflows/` is empty. The rule currently
  holds by construction of the manifests and is checkable by hand with `cargo tree`. Until
  the lint lands, the guarantee is a convention, and conventions decay.
* The shipped graph differs from the PRD §6.1 table in two places, and the tree is right:
  `stepd-transport-http` and `stepd-expr-cel` depend on `stepd-core`, not `stepd-proto`,
  because the traits they implement (`Transport`, `ExprEngine`) and the error type they
  return live in `stepd-core`. `stepd-sdk` additionally depends on `stepd-sdk-core`. PRD
  §6.1 should be amended rather than the crates.
* `stepd-core` does depend on `tokio` (`time`, `rt`, `macros`, `sync`) and `rand`. The rule
  is *no concrete backend*, not *no runtime*; the engine owns a loop, so it owns a clock.

## Alternatives considered

| Option | Why not |
|---|---|
| One crate, modules instead of crates | Module boundaries are advisory. Nothing stops the engine importing `sqlx::PgPool`, and nothing would have caught the first time it did. |
| Traits defined in each backend crate | Inverts the dependency: the engine would import the store to name its interface, which is the coupling this ADR exists to prevent. |
| `stepd-proto` depends on `tokio` for streaming bodies | Forces every alternative implementation onto our runtime. A Python or Go SDK author does not care; a Rust one on `async-std` or `smol` is excluded from the crate that defines the protocol. |
| Generic parameters everywhere, no trait objects | Monomorphises the whole engine per backend combination and makes `Dispatcher` unnameable in the server's state. The object-safe traits keep `Arc<dyn StateStore>` available where it is needed. |
| Publish only the JSON Schemas, no Rust crate | Leaves the step hash and the MAC as prose. Those are the two places where an implementation is either byte-identical or silently wrong. |

## Verification

* `engine/rust/Cargo.toml` lists the nine members; `spec/rust/crates/stepd-proto/Cargo.toml` has no
  `tokio`, `reqwest`, `sqlx` or `async-trait` entry, and `grep -rn "tokio\|reqwest\|sqlx"
  spec/rust/crates/stepd-proto/src/` matches only the doc comment asserting their absence.
* `engine/rust/crates/stepd-core/Cargo.toml` names `stepd-proto` and nothing else from the
  workspace. `stepd-store-postgres`, `stepd-transport-http` and `stepd-expr-cel` each name
  `stepd-core`, confirming the arrows point inward.
* `engine/rust/crates/stepd-core/src/lib.rs` states the rule in its module docs; `traits.rs`
  restates it above the trait definitions.
* `engine/rust/crates/stepd-core/tests/engine.rs` runs the real dispatch loop against
  `stepd-core/src/testing.rs` doubles — the payoff for defining the traits, and the evidence
  that the engine has no hidden backend requirement.
