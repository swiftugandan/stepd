# ADR-024: Role at the top, language underneath — and the protocol owned by neither implementation

| | |
|---|---|
| Status | Accepted, implemented |
| Date | 2026-08-25 |
| Supersedes | — |

## Context

Everything Rust lived in one workspace under `rust/`. That was right while there
was one implementation, and it stopped being right the moment a second language
was planned: a TypeScript SDK ([#6](https://github.com/swiftugandan/stepd/issues/6),
BRD §Community, PRD M5) has nowhere obvious to go, and `typescript/` beside
`rust/` would make the top level a list of languages while the thing a reader
actually wants to find — the protocol, the SDK, the server — stays invisible.

The first attempt at this was to move `rust/` to `sdk/rust/`. That is wrong for
a reason worth recording, because it is the kind of wrong that survives review:
`rust/` holds `stepd-server`, `stepd-store-postgres`, `stepd-cli`, the console,
the migrations and the SQL suites. `stepd-sdk` is two of its eleven crates.
Filing the whole engine under `sdk/` would name the product after one of its
parts, and a reader looking for the dispatch loop would not look there.

Underneath the naming question is a real structural one that had never been
tested. `rust/README.md` has always claimed that "the SDK branch reaches `proto`
without going through `core` at all, because a workflow author's process has no
engine in it". In one workspace that is a diagram. Nothing failed if it stopped
being true; ADR-006's `cargo tree` guards cover `stepd-proto` and `stepd-core`,
but nothing asserted that the SDK's graph excludes the store, the server or the
transport.

And `stepd-proto` sits under both. It is the published wire contract — ADR-006
made it dependency-free specifically so "a Python or Go SDK author" is
unencumbered, and ADR-003 chose CloudEvents for the same reason. A contract that
lives inside one of its consumers is that consumer's type definitions with a
comment attached.

## Decision

### 1. The top level is role; language is the directory underneath

```
spec/     PROTOCOL.md, schemas/, examples/, validate.py     rust/  typescript/
sdk/      what a workflow author imports                    rust/  typescript/
engine/   ingest, storage, dispatch, console, CLI           rust/
docs/     BRD, PRD, gap register, ADRs, runbooks
reference/  the Python model, kept independent
```

A reader looking for the wire format opens `spec/`. A workflow author opens
`sdk/`. An operator opens `engine/`. None of them has to know which language
answered first, and adding a language adds a directory rather than a debate.

### 2. Three Cargo workspaces, not one

`spec/rust` depends on nothing. `sdk/rust` depends on `spec/rust` and stops.
`engine/rust` depends on both, by path.

This is the part that carries weight. `cd sdk/rust && cargo build` succeeds with
only `spec/rust` beside it, so "a workflow author's process contains no engine"
is now a build failure rather than a sentence in a README. CI additionally greps
the SDK workspace's `cargo tree` for engine crates, because a build succeeding
says nothing about what it pulled in.

### 3. `stepd-proto` goes to `spec/rust`, beside the specification it binds

Not into `sdk/rust`, even though the SDK is its most demanding consumer, and not
left in `engine/rust`, even though the server is its largest one. It is the
contract both implement against, and it sits with `PROTOCOL.md` and `schemas/`
so that neither implementation owns it.

The same rule applies per language: the TypeScript protocol package will be
`spec/typescript`, not a package inside the TS SDK.

### 4. The engine's two edges into the SDK stay, and are named

`engine/rust` depends on `sdk/rust` in exactly two places, and neither is the
server's runtime graph:

* `stepd-conformance` depends on `stepd-sdk` because the §12 battery's bundled
  reference app is written with it. This is the crate that is *supposed* to see
  both sides — it stands up a real server and drives a real SDK app through it.
* `stepd-server` depends on `stepd-sdk` under `[dev-dependencies]`, for
  `tests/end_to_end.rs`.

The direction reads correctly — the engine consumes the SDK for testing, not the
reverse — so the edges are kept rather than engineered around.

### 5. One dependency policy at the root

`deny.toml` moves to the repository root and all three workspaces run
`cargo deny --config ../../deny.toml check`. What this project redistributes is a
project-wide decision. Three copies would be three lists drifting apart, and the
one that mattered would be whichever the reviewer did not open.

## Consequences

### What this makes easy

* Adding a language is adding a directory. `sdk/typescript` and
  `spec/typescript` have obvious homes, and neither needs a discussion first.
* The SDK's independence is checked on every push, by two mechanisms that fail
  differently: the workspace failing to resolve, and the tree grep.
* A workflow author's feedback loop no longer waits on the engine.
  `cd sdk/rust && cargo test --workspace` is 64 tests in under a second, with no
  database — so those suites moved from CI's tier 2 to tier 1.
* A third party reading `spec/` finds the prose, the schemas and a Rust binding
  of them in one place, none of which is inside an implementation.

### What this makes hard

* Three `[workspace.dependencies]` tables instead of one. Shared crate versions
  can now drift between trees, and nothing detects it until an API disagrees.
* Three `Cargo.lock` files. A `cargo update` is three commands.
* `cargo test --workspace` no longer means "everything". `CLAUDE.md` says so
  explicitly, because the failure mode is a local run that looked green.
* The Docker build must copy three trees in their repository arrangement,
  because the path dependencies are relative. A flattened copy fails to resolve.
* Roughly 230 path references across 39 files had to move at once — every ADR,
  the runbooks, `spec/PROTOCOL.md`, `ci.yml`, the `Dockerfile`. A stale path in
  a document is not caught by any build.

### What we accept

* **The `engine/rust` → `sdk/rust` edge means the trees are not fully
  independent in the other direction.** `cd engine/rust && cargo test` compiles
  the SDK. That is correct — the conformance battery must — but it means only
  one of the two independence claims is checkable, and it is the one that
  matters.
* **This restructure is not evidence for
  [#6](https://github.com/swiftugandan/stepd/issues/6).** Making room for a
  second SDK is not the same as having one. The claim that any language can host
  workflow code stays untested until an independent implementation reaches
  level 2, and `sdk/` having a `typescript/` slot in it does not change that.
* **The naming is a judgement, not a derivation.** `engine/` could as reasonably
  have been `server/`. What is *not* a judgement is that the whole tree could not
  stay named `sdk/`, and that the protocol could not live inside either
  implementation.
* **`reference/` was left where it is.** It is a Python model of the engine and
  by this scheme belongs at `engine/python/`, but it is deliberately an
  independent model rather than a second implementation, and moving it would
  imply a parity it does not claim.

## Alternatives considered

| Option | Why not |
|---|---|
| `rust/` and `typescript/` at the top level | Zero churn, and it was the cheapest answer. But it makes the top level a list of languages, so a reader wanting the wire format has to know it is in Rust — and `spec/` would still have been separate, so the scheme was already inconsistent with itself. |
| `sdk/rust/` holding the whole existing tree | Names the server, the migrations, the console and the CLI "sdk". The engine becomes unfindable, and the one honest thing a directory name does is stop being true. |
| `impl/rust/`, `impl/typescript/` | Accurate, and it groups them. But it puts the protocol *and* the SDK *and* the server behind one jargon word, which answers the language question and none of the others. |
| One workspace at the repo root spanning both trees | One `Cargo.lock`, smallest CI change. But `sdk/rust` would not be independently buildable, so the separation would be a directory convention — exactly the thing this ADR exists to stop it being. |
| `stepd-proto` stays in `engine/rust` | The SDK would path-depend into the engine tree, so "the SDK needs no engine on disk" becomes false in the most literal sense. |
| `stepd-proto` moves to `sdk/rust` | The server would depend on a crate filed under `sdk/`, which reads backwards, and the contract would be owned by one of its two consumers. |
| Publish `stepd-proto` to crates.io and depend on it by version | Removes the cross-tree path dependency entirely. Rejected for now: it makes every protocol change a release, at a point where the protocol and the engine still change together. Worth revisiting when the wire format is stable. |

## Verification

| Claim | Evidence |
|---|---|
| The SDK's graph contains no engine | CI lane `tier 1 · crate boundaries`, step *the SDK's dependency graph contains no engine* — `cargo build --workspace --locked` in `sdk/rust`, then `cargo tree` grepped for `stepd-(core\|store-postgres\|server\|transport-http\|expr-cel\|blobs-s3\|conformance\|cli)` |
| `stepd-proto` gained no runtime or I/O | Same lane, step *stepd-proto has no runtime, no I/O, no database*, now run in `spec/rust` (ADR-006's guard, relocated) |
| Nothing regressed in the engine | `cargo test --workspace` in `engine/rust`: 254 passed, 22 suites, against PostgreSQL 16 |
| The SDK still passes the protocol battery | `cargo test -p stepd-conformance --test battery -- --nocapture`: 28/28 cases, `CONFORMANT AT LEVEL 2` |
| The commit path is unchanged | The four SQL suites against a hand-migrated database: 193 `PASS` assertions, including `test_invariants.sql` |
| The SDK and protocol suites need nothing running | `cargo test --workspace` in `sdk/rust` (64) and `spec/rust` (36 + 1 doctest), with no `STEPD_TEST_DATABASE_URL` set |
| All three workspaces are formatted and clippy-clean | CI lane `tier 1 · unit (≤10s)`, which now loops over `spec/rust sdk/rust engine/rust` |
| One licence policy covers all three | CI step *licences and advisories*, looping `cargo deny --config ../../deny.toml check` |
| The image still builds from three trees | `docker build .` at the repository root: exit 0, and `docker run --rm <image> --help` lists all nine subcommands |
| The schemas still validate | `.venv/bin/python validate.py` in `spec/`: `ALL PASS` |
