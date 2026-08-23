# stepd — the Rust implementation

The crates, the migrations and the SQL suites. The root
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

| Crate | What it owns |
|---|---|
| `stepd-proto` | The wire contract. **This is the crate a third party implements against** — no I/O, no runtime, so it can be read as a specification. |
| `stepd-core` | The engine, generic over storage and transport traits. In-memory fakes for every interface, so the logic is testable without a database. |
| `stepd-store-postgres` | The Postgres store, plus the simulation harness. Pooler-safe: row-level locking only, never session-scoped advisory locks. |
| `stepd-expr-cel` | A deliberately partial CEL subset, explicit about what it refuses rather than silently accepting. |
| `stepd-transport-http` | Signed HTTP push, and the egress policy that stops an app-supplied URL reaching cloud metadata. |
| `stepd-sdk-core` | The replay machinery. No async, which is what makes the dangerous logic exhaustively testable without scheduling noise. |
| `stepd-sdk` | What a workflow author touches: `Function`, `Ctx`, the axum adapter, and the in-process test harness. |
| `stepd-server` | Ingest, management and read API, the operations console, dispatch and convergence loops. |
| `stepd-cli` | `serve` · `migrate` · `doctor` · `dev` · `token` · `namespace` · `run` · `limits` · `conformance` |
| `stepd-conformance` | The protocol §12 battery, and the reference app it drives. |

Every crate carries unit tests against in-memory fakes. On top of those sit the
lanes that need a live database: the store's own tests, the simulation harness,
and the end-to-end lane that drives a real SDK app over a real socket.

Requires Rust **1.85** — `Waker::noop`, which is what lets a workflow test run
with no async runtime at all.

## Layout

```
crates/            the crates above
migrations/        applied by `stepd migrate` or by psql — never both
tests/sql/         suites that run against a hand-migrated database
```

The migrations carry the engine itself, not just the schema: the commit path
lives in SQL functions, and `tests/sql/test_invariants.sql` fails the build if
a serialization point moves. That file is a countermeasure with a
history: it once asserted properties of the SQL while the Rust store had grown
its own reimplementation of the commit that called none of it, so the tests
guarded a correctness centre that was not the one running.

## Building

```bash
cargo build --workspace
```

## Running the tests

There is one thing to get right here, and it is the reason this section is
longer than it looks like it needs to be.

**The Rust tests and the SQL suites need different databases.** `psql -f
migrations/*.sql` and `stepd migrate` keep separate ideas of what has been
applied; run both against one database and sqlx starts from the first migration against
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

| Suite | What it asserts |
|---|---|
| `test_engine.sql` | Core engine behaviour: claiming, commit, memoisation |
| `test_engine_ops.sql` | Every op — invoke, cascade, continue, limits, protocol errors |
| `test_cron.sql` | Schedule planning, catch-up, misfire, singleton, fairness |
| `test_invariants.sql` | **Structural**: fails the build if a serialization point moves |

They print `PASS` per assertion and stop on the first failure.

### The Rust tests — migrated by sqlx

```bash
export STEPD_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/stepd_rust"
cargo test --workspace
```

Expect it to take a few minutes; most of the wall-clock is the conformance
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
closure, rather than in a `static`. They were `static` until recently, and the
concurrent batteries shared them — a blob client configured by whichever battery
started last, and an effect log any battery's `reset` wiped for all of them. The
`blobs` suite failed with `no_such_run` and the runner reported level 1 instead
of level 2, on a multi-core machine only.

That was two tests interfering through shared mutable state, which the root
README's findings record happening twice before — in the dispatcher, then in the
cron sweep. Nothing generalised the lesson after those, so the reference app made
the same choice from scratch. If you add state to that app, put it in `AppState`
— an instance owns it, the process does not.

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
  `join!` silently re-executes completed work. See
  [`docs/SDK-DESIGN-rust.md`](../docs/SDK-DESIGN-rust.md) and the
  [eager occurrence claiming](../docs/adr/012-eager-occurrence-claiming.md) ADR.
* **The commit path.** It lives in SQL, and the Rust store once grew four hundred
  lines that reimplemented it and called none of it — two correctness centres,
  with the tests guarding the one that did not run. See the
  [durable run inbox](../docs/adr/011-durable-run-inbox.md) and
  [pooler-safe locking](../docs/adr/019-pooler-safe-locking.md) ADRs.

[`docs/adr/`](../docs/adr/) holds the decision records; the silent-corruption
ones are eager occurrence claiming, the durable run inbox and pooler-safe
locking. `docs/runbooks/restore-hazard.md` is the one to read before it is needed:
a point-in-time restore re-executes side effects and re-fires cron occurrences.
