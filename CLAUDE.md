# CLAUDE.md

Durable workflow engine. Three Cargo workspaces — `spec/rust` (the wire
contract), `sdk/rust` (what a workflow author imports), `engine/rust` (the
server) — plus the protocol itself in `spec/`.
Read [`README.md`](README.md) for status before changing anything, and the
[`gap`-labelled issues](https://github.com/swiftugandan/stepd/issues?q=is%3Aissue+is%3Aopen+label%3Agap)
for the known gaps — they live in the tracker, not in the README.
[`engine/rust/README.md`](engine/rust/README.md) has the crate graph and the test layout.

## Commands

**Which workspace you are in matters.** `cargo test --workspace` in `engine/rust`
does not compile the SDK's own tests, and vice versa. CI runs all three; a local
run that covered one is not a green run.

```bash
# the engine — needs STEPD_TEST_DATABASE_URL for the full suite (see below)
cd engine/rust
cargo test --workspace
cargo run -p stepd-cli -- dev       # server + console, migrations, a token
cargo run -p stepd-cli -- doctor    # non-zero exit on anything critical

# the SDK and the protocol — no database, no server, seconds
(cd sdk/rust  && cargo test --workspace)
(cd spec/rust && cargo test --workspace)

# all three, as CI does
for w in spec/rust sdk/rust engine/rust; do
  (cd "$w" && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings)
done
```

**Do not make `sdk/rust` depend on the engine.** It is a separate workspace so
that `cd sdk/rust && cargo build` failing is what tells you the SDK has grown an
engine dependency; CI also greps its dependency tree. `spec/rust` holds
`stepd-proto` because a contract owned by one of its consumers stops being a
contract — see [ADR-024](docs/adr/024-language-trees.md).

Protocol schemas, from `spec/` — needs a recent `jsonschema` in a virtualenv,
because the system package is older, lacks the `registry=` argument the
validator uses, and shadows a plain `pip install`:

```bash
python3 -m venv .venv && .venv/bin/pip install 'jsonschema>=4.18' referencing
.venv/bin/python validate.py
```

## Postgres

No local install; use a container.

```bash
podman run -d --name stepd-pg -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_USER=postgres -p 5433:5432 postgres:16
export STEPD_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/stepd_rust"
```

**The SQL suites and the Rust tests need separate databases.** `psql -f
migrations/*.sql` and `stepd migrate` keep different ideas of what has been
applied; both against one database leaves sqlx starting from the first migration
against objects that already exist.

**Without `STEPD_TEST_DATABASE_URL` the database tests skip loudly.** A green run
that skipped them is not a green run.

## Rules that exist because something broke

- **Per-instance state never goes in a `static`.** In `stepd-conformance` it goes
  in `AppState`. Separate components have made this mistake independently, each
  time by two tests interfering through shared mutable state, and nothing in the
  build catches the next one.
- **`ctx.step()` claims when called, not when polled.** Claiming at poll time
  ties the hash to scheduler order and `join!` silently re-executes completed
  work. Same rule for `wait_event` and `invoke`. See
  [`docs/SDK-DESIGN-rust.md`](docs/SDK-DESIGN-rust.md) and the
  [eager occurrence claiming](docs/adr/012-eager-occurrence-claiming.md) ADR.
- **The commit path lives in SQL.** `engine/rust/tests/sql/test_invariants.sql` fails the
  build if a serialization point moves. Don't reimplement commit logic in the
  Rust store — a second correctness centre is where live defects hid while the
  tests guarded the first.
- **Don't overstate in docs or comments.** A comment describing a property the
  code lacks is worse than none: it stops the next reader checking.

## Finding work

The [issue tracker](https://github.com/swiftugandan/stepd/issues) is the live
list; `gap`-labelled issues carry the register's severities. `README.md`'s "Next
steps" and `docs/GAPS.md` are snapshots taken when someone last edited them, so
read the tracker too and trust it where they disagree.

```bash
gh issue list --state open
gh run list --limit 5 --json databaseId,status,conclusion,displayTitle
gh run view <id> --json jobs -q '.jobs[] | "\(.conclusion)\t\(.name)"'
gh run view <id> --log-failed
```

CI runs on every push to `main`, so start there: a red lane means a claim the
README cites as evidence is not currently being produced, which outranks new
work. Check whether an issue already exists for a failure before filing one, and
say which issue a change closes.

## Where to read

| | |
|---|---|
| [`README.md`](README.md) | Status, how to run it, and how to write a workflow |
| [`engine/rust/README.md`](engine/rust/README.md) | Crate graph, test layout, running the server |
| [`spec/PROTOCOL.md`](spec/PROTOCOL.md) | The wire protocol |
| [`docs/adr/`](docs/adr/) | ADRs; the silent-corruption ones are eager occurrence claiming, the durable run inbox and pooler-safe locking |
| [`docs/runbooks/restore-hazard.md`](docs/runbooks/restore-hazard.md) | Read before you need it: PITR re-executes side effects |
| [`sdk/typescript/README.md`](sdk/typescript/README.md) | The TypeScript SDK; `docs/SDK-DESIGN-typescript.md` and [ADR-025](docs/adr/025-typescript-sdk-divergences.md) for its design and its deliberate differences from the Rust one |
| [`docs/GAPS.md`](docs/GAPS.md) | Gap register — note it records spec resolutions, not always code |
| [`docs/RECONCILIATION.md`](docs/RECONCILIATION.md) | The defects found and fixed, and §7 — the findings worth carrying forward |
