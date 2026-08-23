# CLAUDE.md

Durable workflow engine: Rust workspace in `rust/`, wire protocol in `spec/`.
Read [`README.md`](README.md) for status and the honest gaps before changing
anything; [`rust/README.md`](rust/README.md) for the crate graph and the test
layout.

## Commands

All from `rust/` unless stated.

```bash
cargo build --workspace
cargo test --workspace          # needs STEPD_TEST_DATABASE_URL (see below)
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
cargo run -p stepd-cli -- dev       # server + console, migrations, a token
cargo run -p stepd-cli -- doctor    # 13 checks, non-zero on anything critical
```

Protocol schemas, from `spec/` — needs `jsonschema>=4.18` in a venv, because the
system package is older and shadows it (issue #12):

```bash
python3 -m venv .venv && .venv/bin/pip install 'jsonschema>=4.18' referencing
.venv/bin/python validate.py        # 54 cases
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
applied; both against one database leaves sqlx starting from migration 1 against
objects that already exist.

**Without `STEPD_TEST_DATABASE_URL` the database tests skip loudly.** A green run
that skipped them is not a green run.

## Rules that exist because something broke

- **Per-instance state never goes in a `static`.** In `stepd-conformance` it goes
  in `AppState`. Three components have now made this mistake independently
  (README findings 2 and 13); nothing in the build catches a fourth (issue #8).
- **`ctx.step()` claims when called, not when polled.** Claiming at poll time
  ties the hash to scheduler order and `join!` silently re-executes completed
  work. Same rule for `wait_event` and `invoke`. See
  [`docs/SDK-DESIGN-rust.md`](docs/SDK-DESIGN-rust.md) and ADR-012.
- **The commit path lives in SQL.** `tests/sql/test_invariants.sql` fails the
  build if a serialization point moves. Don't reimplement commit logic in the
  Rust store — that is how three live defects hid (ADR-011, ADR-019).
- **Don't overstate in docs or comments.** A comment describing a property the
  code lacks is worse than none (README finding 9).

## Where to read

| | |
|---|---|
| [`README.md`](README.md) | Status, honest gaps, the 17 findings |
| [`rust/README.md`](rust/README.md) | Crate graph, test layout, running the server |
| [`spec/PROTOCOL.md`](spec/PROTOCOL.md) | The wire protocol |
| [`docs/adr/`](docs/adr/) | 22 ADRs; 011, 012 and 019 are the silent-corruption ones |
| [`docs/runbooks/restore-hazard.md`](docs/runbooks/restore-hazard.md) | Read before you need it: PITR re-executes side effects |
| [`docs/GAPS.md`](docs/GAPS.md) | Gap register — note it records spec resolutions, not always code |

Open gaps are tracked as [issues](https://github.com/swiftugandan/stepd/issues),
labelled `gap` with the register's S1–S4 severities.
