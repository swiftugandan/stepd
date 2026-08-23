# stepd — the Rust implementation

Ten crates, thirteen migrations and four SQL suites. The root
[`README.md`](../README.md) says what the project is and where it is honest
about not being finished; this file is for working inside `rust/`.

---

## The crate graph

Dependency order, bottom first. Nothing below depends on anything above it, and
that is the property worth preserving when adding code — `stepd-proto` and
`stepd-sdk-core` in particular are async-free and I/O-free on purpose.

```
  stepd-cli                          the `stepd` binary
      └── stepd-conformance          the §12 battery and the reference app
              └── stepd-server       ingest/management API, console, loops
                      │
      ┌───────────────┼───────────────┬──────────────┐
  stepd-store-    stepd-transport-  stepd-expr-   stepd-sdk
   postgres           http             cel            └── stepd-sdk-core
      └───────────────┴───────────────┘                       │
                  stepd-core                                  │
                      └───────────────────────────────────────┴── stepd-proto
```

Read it bottom-up. `stepd-proto` is the root everything shares; `stepd-core`
sits above it holding the engine logic; the store, transport and expression
crates are interchangeable implementations of `core`'s traits; the SDK branch
reaches `proto` without going through `core` at all, because a workflow author's
process has no engine in it.

| Crate | Lines | Tests | What it owns |
|---|--:|--:|---|
| `stepd-proto` | 1,370 | 36 | The wire contract. **This is the crate a third party implements against** — no I/O, no runtime, so it can be read as a specification. |
| `stepd-core` | 3,887 | 65 | The engine, generic over storage and transport traits. In-memory fakes for every interface, so the logic is testable without a database. |
| `stepd-store-postgres` | 1,760 | 39 | The Postgres store, plus the simulation harness. Pooler-safe: row-level locking only, never session-scoped advisory locks. |
| `stepd-expr-cel` | 1,056 | 16 | A deliberately partial CEL subset, explicit about what it refuses rather than silently accepting. |
| `stepd-transport-http` | 531 | 10 | Signed HTTP push, and the egress policy that stops an app-supplied URL reaching cloud metadata. |
| `stepd-sdk-core` | 2,529 | 32 | The replay machinery. No async, which is what makes the dangerous logic exhaustively testable without scheduling noise. |
| `stepd-sdk` | 2,298 | 29 | What a workflow author touches: `Function`, `Ctx`, the axum adapter, and the in-process test harness. |
| `stepd-server` | 3,153 | 45 | Ingest, management and read API, the operations console, dispatch and convergence loops. |
| `stepd-cli` | 908 | 4 | `serve` · `migrate` · `doctor` · `dev` · `token` · `namespace` · `run` · `limits` · `conformance` |
| `stepd-conformance` | 3,084 | 12 | The protocol §12 battery, and the reference app it drives. |

**291 tests, whole workspace green** — verified against PostgreSQL 16.14 on
2026-08-23. Of those, 39 need a live database and 17 are the end-to-end lane
that drives a real SDK app over a real socket.

Requires Rust **1.85** — `Waker::noop`, which is what lets a workflow test run
with no async runtime at all.

## Layout

```
crates/            the ten crates above
migrations/        0001..0013, applied by `stepd migrate` or by psql — not both
tests/sql/         four suites that run against a hand-migrated database
```

The migrations carry the engine itself, not just the schema: the commit path
lives in SQL functions, and `tests/sql/test_invariants.sql` fails the build if
a serialization point moves. That file is a countermeasure with a history —
see finding 2 in the root README for what it once failed to point at.

## Building

```bash
cargo build --workspace
```

## Running the tests

There is one thing to get right here, and it is the reason this section is
longer than it looks like it needs to be.

**The Rust tests and the SQL suites need different databases.** `psql -f
migrations/*.sql` and `stepd migrate` keep separate ideas of what has been
applied; run both against one database and sqlx starts from migration 1 against
objects that already exist. `migrate` detects this and says so rather than
failing obscurely, but two databases is the clean answer.

A container is the shortest path to both:

```bash
podman run -d --name stepd-pg \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=postgres \
  -p 5433:5432 postgres:16
podman exec stepd-pg createdb -U postgres stepd_sql
podman exec stepd-pg createdb -U postgres stepd_rust
```

### The SQL suites — migrated by hand

```bash
SQLDB="postgres://postgres@127.0.0.1:5433/stepd_sql"
for f in migrations/*.sql; do psql "$SQLDB" -v ON_ERROR_STOP=1 -q -f "$f"; done
psql "$SQLDB" -v ON_ERROR_STOP=1 -q \
  -f tests/sql/test_engine.sql -f tests/sql/test_engine_ops.sql \
  -f tests/sql/test_cron.sql -f tests/sql/test_invariants.sql
```

| Suite | Assertions |
|---|--:|
| `test_engine.sql` | 25 |
| `test_engine_ops.sql` | 94 |
| `test_cron.sql` | 47 |
| `test_invariants.sql` | 27 structural |

166 behavioural and 27 structural. They print `PASS` per assertion and stop on
the first failure.

### The Rust tests — migrated by sqlx

```bash
export STEPD_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/stepd_rust"
cargo test --workspace
```

**291 passed, 0 failed**, in about four minutes — most of it the conformance
battery and the simulation harness.

**Without `STEPD_TEST_DATABASE_URL` the database-backed tests skip loudly.**
That is deliberate: a database test that silently passes when it did not run is
worse than no test, because the green tick then lies about the thing most
likely to break.

Longer-running lanes, kept out of the default run because seeds are a
parameter rather than a rewrite:

```bash
STEPD_SIM_SEEDS=250 cargo test --release -p stepd-store-postgres --test simulation
cargo test -p stepd-conformance --test battery -- --nocapture
```

### What the battery isolates, and why

Each battery gets its own database, namespace, port and app instance. That is
not tidiness. Blob collection is server-wide by design — an unreferenced blob is
unreferenced whatever namespace it is in — so two batteries sharing a database
have one deleting the other's in-flight reservations.

The reference app's own state is isolated on the same principle: its blob client
and effect log live in an `AppState` owned by the instance, cloned into each step
closure, rather than in a `static`. They were `static` until 2026-08-23, and the
four concurrent batteries shared them — a blob client configured by whichever
battery started last, and an effect log any battery's `reset` wiped for all of
them. The `blobs` suite failed with `no_such_run` and the runner reported level 1
instead of level 2, on a multi-core machine only.

That was the project's finding 13 arriving a third time: two tests interfering
through shared mutable state. Nothing generalised the lesson after the first two,
so the reference app made the same choice from scratch. If you add state to that
app, put it in `AppState` — an instance owns it, the process does not.

## Running the server

```bash
cargo run -p stepd-cli -- dev      # migrations, a namespace, a token, a console
cargo run -p stepd-cli -- doctor   # thirteen checks, non-zero exit on anything critical
```

`dev` prints a console URL with a token in it and configures nothing for
production. For a real deployment set `STEPD_DATABASE_URL` and
`STEPD_SIGNING_KEY`, then `stepd migrate` and `stepd serve` — `serve` refuses to
start without a signing key rather than sending every attempt unsigned.

Managed blobs additionally need `STEPD_BLOB_SIGNING_KEY`, `STEPD_BLOB_ROOT` and
`STEPD_BLOB_BASE_URL`. Without the key the two transfer endpoints answer 501 and
say why: a capability signed with a default key verifies for anyone who guesses
it, so there is no default.

Everything `Config::from_env` reads: `STEPD_DATABASE_URL` · `STEPD_BIND` ·
`STEPD_SIGNING_KEY` · `STEPD_SIGNING_KEY_PREVIOUS` · `STEPD_WORKER` ·
`STEPD_BATCH` · `STEPD_LEASE_SECONDS` · `STEPD_ATTEMPT_TIMEOUT_SECONDS` ·
`STEPD_IDLE_POLL_MS` · `STEPD_TIMER_JITTER_SECONDS` · `STEPD_MAX_CONNECTIONS` ·
`STEPD_LOG` · `STEPD_LOG_JSON` · `STEPD_ALLOW_LOOPBACK_EGRESS` ·
`STEPD_ALLOW_PRIVATE_EGRESS` · `STEPD_EGRESS_ALLOWLIST` · `STEPD_BLOB_SIGNING_KEY` ·
`STEPD_BLOB_ROOT` · `STEPD_BLOB_BASE_URL` · `STEPD_BLOB_MAX_SIZE` ·
`STEPD_BLOB_RESERVATION_TTL_HOURS`

### Driving a real workflow through it

```bash
cargo run -p stepd-sdk --example order_demo -- http://127.0.0.1:8080 <token>
```

A real SDK app on a real socket: it registers through `PUT /v1/apps`, ingests an
event, waits for the handler to park on its `wait_event`, approves it through
`resolve-wait`, and reports what the run produced and how many times each step
body actually executed. It is the shortest path to seeing the guarantee the
project exists to provide, rather than reading an assertion about it.

Two shapes it exists to document, because the end-to-end test reads Postgres
directly and so never meets them: `GET /v1/runs` returns `{"items": [...]}`, and
`GET /v1/runs/{id}/steps` returns `steps` as a **map keyed by step hash**, with
the step id inside the value rather than as the key.

## Where the risk is concentrated

Two mechanisms carry most of it, and both have a document because reading the
code against the design is what found the defect in them:

* **Eager occurrence claiming.** `ctx.step()` claims when *called*, not when its
  future is *polled*. Claiming at poll time ties the hash to scheduler order, so
  `join!` silently re-executes completed work. `docs/SDK-DESIGN-rust.md` and
  ADR-012.
* **The commit path.** It lives in SQL, and the Rust store once grew four hundred
  lines that reimplemented it and called none of it — two correctness centres,
  with the tests guarding the one that did not run. ADR-011 and ADR-019.

`docs/adr/` has twenty-two ADRs; 011, 012 and 019 are the silent-corruption
ones. `docs/runbooks/restore-hazard.md` is the one to read before it is needed:
a point-in-time restore re-executes side effects and re-fires cron occurrences.
