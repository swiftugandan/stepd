# stepd — durable workflow engine

**Last verified 2026-08-23** against PostgreSQL 16. Read this before running
anything: it states what is finished and where the trustworthy artifacts are.
What is *not* finished is in the [`gap`-labelled issues][gaps].

---

## What this is

An open-standards durable workflow engine: a single binary (server + console)
backed by Postgres, with a published wire protocol so any language can host
workflow code. Long-running business processes — order fulfilment, onboarding,
approvals, claims — survive crashes, deploys and month-long waits without
re-executing side effects.

Execution model: Inngest-style one-new-step-per-attempt with step-id
memoisation, plus Restate-style keyed single-writer ordering, on Postgres.

## Layout

Role at the top, language underneath.

```
spec/           The protocol: PROTOCOL.md, JSON Schemas, validator
  rust/           stepd-proto — the wire contract as a crate
sdk/            What a workflow author imports
  rust/           stepd-sdk, stepd-sdk-core
engine/         The server: ingest, storage, dispatch, console, CLI
  rust/           eight crates, migrations, SQL test suites
docs/           BRD, PRD, gap register, SDK design, ADRs, runbooks
reference/      Python reference implementation — kept as an independent model
```

Three Cargo workspaces, not one. `spec/rust` depends on nothing;
`sdk/rust` depends on `spec/rust` and stops there; `engine/rust` depends on both.
That last edge runs in the direction it reads: the engine implements the
protocol, and consumes the SDK in exactly two places — the conformance
battery's bundled reference app, and the server's end-to-end test, which is a
dev-dependency.

The split is what makes "a workflow author's process contains no engine" a
checkable claim rather than a diagram. `cd sdk/rust && cargo build` succeeds
with only `spec/rust` beside it, and CI greps the SDK's dependency tree for
engine crates ([ADR-024](docs/adr/024-language-trees.md)).

## Status

| Component | State | Evidence |
|---|---|---|
| Protocol spec (rev 1.2) | Complete | 54 schema cases — `spec/validate.py` |
| Postgres schema + engine SQL | Complete | 166 behavioural + 27 structural assertions |
| `stepd-proto` | Complete | 36 tests |
| `stepd-core` | Complete | 72 tests, in-memory fakes for every interface |
| `stepd-store-postgres` | Complete | 16 unit + 25 live + the simulation harness |
| `stepd-expr-cel` | Complete (documented subset) | 16 tests |
| `stepd-transport-http` | Complete | 10 tests |
| `stepd-sdk-core` | Complete | 32 tests — the R1 machinery |
| `stepd-sdk` | Complete | 31 tests, including the workflow test harness and §8.6 journal paging |
| `stepd-server` | Complete | 42 unit + 19 end-to-end |
| `stepd-blobs-s3` | Complete | 8 offline + 6 live; the live ones need `STEPD_TEST_S3_*` |
| `stepd-cli` | Complete | `serve` · `migrate` · `doctor` · `dev` · `token` · `run` · `limits` · `conformance` |
| Cron scheduler | Complete | 37 unit + 47 SQL + 13 live + 3 e2e; simulation property P10 |
| Cancellation compensation | Complete | migration 010; conformance `cancel` |
| Managed blobs | Complete — filesystem (relay) and S3 (presigned) | two-phase upload, `Range`, dedupe, reference tracking; the S3 suites need `STEPD_TEST_S3_*` and no CI lane sets it ([#25](https://github.com/swiftugandan/stepd/issues/25)) |
| Subject erasure | **Schema only** | [#1](https://github.com/swiftugandan/stepd/issues/1) |
| Conformance suite | Complete | 19 suites, 28 cases; the reference app reaches **level 2** |

**A five-step workflow runs from an ingested event to a completed run**, through
the real dispatcher, a real SDK app on a real socket, over signed HTTP, against
PostgreSQL 16 — `engine/rust/crates/stepd-server/tests/end_to_end.rs`.

## Known gaps

Every gap between what this claims and what it implements is an open issue,
labelled [`gap`][gaps] and carrying the register's severity — **S1** silent
corruption, **S2** outage or data loss, **S3** operational pain, **S4** adoption
drag. That tracker is the live list. `docs/GAPS.md` records the same material
with its resolutions, but it is a snapshot and loses where the two disagree.

They live there rather than in a section here because a list in a README is
updated when someone remembers to. The version of the status table above that
this replaced overstated in the opposite direction, and that is how four defects
sat undetected.

[`docs/RECONCILIATION.md`](docs/RECONCILIATION.md) is the other half of the
record: the seventeen defects found and fixed, and §7, the findings worth
carrying forward from them.

Two CI lanes are red as of 2026-08-25, so two of the evidence claims above are
not currently being produced automatically: `tier 3 · through pgbouncer`
([#28](https://github.com/swiftugandan/stepd/issues/28)) and `tier 4 · soak`
([#27](https://github.com/swiftugandan/stepd/issues/27)).

[gaps]: https://github.com/swiftugandan/stepd/issues?q=is%3Aissue+is%3Aopen+label%3Agap

## Running it

```bash
# ---- protocol schemas, no database
cd spec && pip install jsonschema referencing && python3 validate.py

# ---- two databases: one migrated by hand for the SQL suites, one migrated by
# ---- sqlx for the Rust tests. See the note below on why not to mix them.
initdb -D /tmp/pgdata -A trust -U postgres
pg_ctl -D /tmp/pgdata -o '-p 5433' -l /tmp/pg.log start
createdb -p 5433 -U postgres stepd_sql
createdb -p 5433 -U postgres stepd_rust

# ---- the engine SQL: behaviour and structural invariants
cd engine/rust
SQLDB="postgres://postgres@127.0.0.1:5433/stepd_sql"
for f in migrations/*.sql; do psql "$SQLDB" -v ON_ERROR_STOP=1 -q -f "$f"; done
psql "$SQLDB" -v ON_ERROR_STOP=1 -q \
  -f tests/sql/test_engine.sql -f tests/sql/test_engine_ops.sql \
  -f tests/sql/test_cron.sql -f tests/sql/test_invariants.sql

# ---- everything, including live-database and end-to-end
export STEPD_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/stepd_rust"
cargo test --workspace

# ---- simulation against the real engine (seeds are a parameter, not a rewrite)
STEPD_SIM_SEEDS=250 cargo test --release -p stepd-store-postgres --test simulation

# ---- the reference implementation, kept as an independent model
cd ../reference && python3 simulation.py 5000 && python3 coverage_check.py 400
```

### Conformance

```bash
# Against your own SDK, once it serves the §12.1 endpoints:
cargo run -p stepd-cli -- conformance \
  --app http://127.0.0.1:9944 \
  --app-configure-url http://127.0.0.1:9944/_conformance/configure \
  --database-url postgres://postgres@127.0.0.1:5433/stepd_conf
```

`--app-configure-url` is what the `blobs` and `truncation` suites need. Both
require the app to call back into the server, and neither the server's address
nor a token exists until after the app is already running — the runner binds an
ephemeral port and mints the token itself. So it posts them to that URL once it
is serving. Leave the flag off and those two suites are unreachable, which is
what they were for every app this runner did not start in-process — that is to
say, every app not written in Rust.

The report says what it checked **and what it did not**. A suite the app did not
declare and a suite this runner has not implemented are different lines, and
either one stops it certifying the level containing it — a conformance tool that
certifies around its own gaps is worse than none, because the whole point is that
the verdict can be trusted without reading its source.

`stepd conformance` tests an **app**. Turning it round to test a second server
implementation would need the mirror battery — a fixed app that reports what it
was sent — which is specified nowhere and does not exist (protocol §12.4,
[#10](https://github.com/swiftugandan/stepd/issues/10)).

The battery also runs against the bundled reference app in CI:

```bash
cargo test -p stepd-conformance --test battery -- --nocapture
```

That is weaker evidence than it looks, and worth being clear about: the suite and
the Rust SDK were written together, so they can agree on a shared misreading of
the specification. What it establishes is that every assertion is reachable, that
the §12.2 contract is implementable, and that an SDK change breaking a protocol
guarantee fails a build.

Without `STEPD_TEST_DATABASE_URL` the database-backed tests **skip loudly**. A
database test that silently passes when it did not run is worse than no test,
because the green tick is then a lie about the thing most likely to break.

> **Apply the migrations one way or the other, not both.** `psql -f migrations/*.sql`
> and `stepd migrate` keep separate ideas of what has been applied, so doing both
> leaves sqlx starting from migration 1 against objects that already exist.
> `migrate` detects this and says so rather than failing on `type "run_status"
> already exists`, but the cleanest answer is a fresh database. The SQL suites
> above and `cargo test` therefore want **different databases**.

### Running the server

```bash
cd engine/rust
cargo run -p stepd-cli -- dev            # migrations, a namespace, a token, a console
cargo run -p stepd-cli -- doctor         # thirteen checks; non-zero exit on anything critical
```

`stepd dev` prints a console URL with a token in it. For production, set
`STEPD_DATABASE_URL` and `STEPD_SIGNING_KEY`, then run `stepd migrate` and
`stepd serve` — `serve` refuses to start without a signing key rather than
sending every attempt unsigned.

Managed blobs need `STEPD_BLOB_SIGNING_KEY`. Without it `POST /v1/blobs:reserve`
answers 501 and says why: a capability signed with a default key verifies for
anyone who guesses it, and the failure would be silent, so there is no default.

`STEPD_BLOB_BACKEND` chooses where the bytes live. It is read case- and
whitespace-insensitively; anything other than `fs` or `s3` warns and falls back
to `fs` rather than guessing silently.

| | |
|---|---|
| `fs` *(default)* | `STEPD_BLOB_ROOT` (default `/var/lib/stepd/blobs`) and `STEPD_BLOB_BASE_URL`. A filesystem cannot presign, so §8.3.2's compatibility relay is mounted and every payload byte crosses this process — warned once at start-up and again on every relayed upload. This is what makes `stepd dev` work with no cloud account. |
| `s3` | `STEPD_BLOB_S3_ENDPOINT`, `STEPD_BLOB_S3_BUCKET`, `STEPD_BLOB_S3_ACCESS_KEY`, `STEPD_BLOB_S3_SECRET_KEY`, plus optional `STEPD_BLOB_S3_REGION` (default `us-east-1`) and `STEPD_BLOB_S3_PATH_STYLE`. `serve` refuses to start if any of the four required ones is missing, naming them: a half-configured S3 backend would hand apps upload URLs pointing at nothing. The relay route is not mounted, and `doctor` gains a fourteenth check that probes the bucket with these credentials. |

`STEPD_BLOB_MAX_SIZE` and `STEPD_BLOB_RESERVATION_TTL_HOURS` (default 24) apply
to either. `STEPD_BLOB_ROOT` and `STEPD_BLOB_BASE_URL` do nothing on `s3`; the
`STEPD_BLOB_S3_*` variables do nothing on `fs`.

The `s3` backend's safety rests on the object store rejecting a presigned PUT
whose body does not match the `x-amz-checksum-sha256` bound into its signature —
not every S3-compatible server does. [`docs/blob-backends.md`](docs/blob-backends.md)
records what MinIO and RustFS were actually observed to do, and the caveats on
each. Read it before pointing this at a third server.

### Running with Docker

`Dockerfile` and `compose.yaml` at the root stand up Postgres, apply the
migrations and serve, with the console on `:8080`.

```bash
cp .env.example .env      # set POSTGRES_PASSWORD and STEPD_SIGNING_KEY
docker compose up -d --build
docker compose --profile bootstrap run --rm bootstrap   # a namespace and a token
docker compose --profile ops run --rm doctor
```

`bootstrap` prints the console URL with a token in it, once — only the hash is
stored. Verified end to end on 2026-08-24: image built, migrations applied,
`/v1/health` answering, the console served, `doctor` at 13 checks and 0
critical, and SIGTERM draining both loops in under a second.

Four things about the file are load-bearing rather than stylistic:

* **Migrations are their own one-shot service, not `serve --migrate`.** It is a
  release step that must run against a database whose server will not start,
  and it means replicas of `serve` do not race to apply the same migration.
  `serve` waits on `service_completed_successfully`.
* **The egress flags are passed as bare names.** `Config::from_env` reads them
  with `env::var(..).is_ok()` — presence, not value — so
  `STEPD_ALLOW_PRIVATE_EGRESS=0` *enables* private egress. The bare form makes
  an unset variable arrive absent instead of empty. An app on the compose
  network has an RFC 1918 address, so it needs this set; the policy fails
  closed and metadata addresses stay denied regardless.
* **No pgbouncer.** [#15](https://github.com/swiftugandan/stepd/issues/15) —
  sqlx's migrator takes a session-scoped advisory lock that never releases
  through a transaction-mode pooler. Putting one in front of this would
  reproduce that hang on the first `up`. `doctor`'s pooler check is what tells
  you whether the connection you have is safe.
* **`stop_grace_period` exceeds the attempt timeout.** SIGTERM makes the server
  drain in-flight attempts; killing it mid-drain abandons leases that then have
  to expire, so every deploy would delay the runs it interrupted.

The build image is `rust:1-bookworm`, matching what CI resolves
`@stable` to — **not** the `rust-version = "1.85"` the workspace declares,
which no longer builds ([#21](https://github.com/swiftugandan/stepd/issues/21)).

### Writing a workflow

```rust
use stepd_sdk::prelude::*;

async fn order_fulfilment(ctx: &Ctx) -> StepResult<Receipt> {
    let tx: String = ctx.step("charge", || async { gateway.charge().await }).await?;
    ctx.sleep("cooldown", chrono::Duration::days(1)).await?;
    let approval: Option<Approval> = ctx.wait_event("approval", "order.approved")
        .timeout(chrono::Duration::days(7))
        .await?;
    Ok(Receipt { tx, approval })
}
```

…and test it with no database, no server and no HTTP:

```rust
let mut t = stepd_sdk::testing::harness(order_fulfilment);
t.send_event("order.approved", json!({ "by": "priya" }));  // BEFORE the wait
let out = t.run_to_completion()?;
t.assert_step_executed_once("charge");
```

That test delivers the event *before* the handler reaches its `wait_event`, and
the run still resolves. Being able to write the early-signal case in three lines
is the difference between a developer trusting the guarantee and hoping for it.

### Storing a large payload

```rust
let blobs = Blobs::new(stepd_url, token);

let receipt: Blob = ctx.step("receipt", || async {
    blobs.put(ctx.run().id, &pdf).content_type("application/pdf").await
}).await?;

// …later, in another step, on another attempt, possibly a week later:
let header = blobs.read_range(&receipt, 0, 1023).await?;
```

The app reserves, uploads to the URL it was handed, and returns a reference. On
the `s3` backend that URL addresses the object store directly, signed with the
store's own credentials, so the bytes never pass through the server on the way
in; on the default `fs` backend it points
back here and they do — that is §8.3.2's compatibility relay, and it is why the
backend is a configuration choice rather than a detail. Either way reading is
lazy and the reference is a value, so replaying a run with forty blob-bearing
steps decodes forty references and downloads nothing.

Uploading inside a step is what records the reference in the journal, which is
what keeps the bytes alive. Content addressing means a retry that re-reserves the
same digest skips the upload entirely.

### Scheduling a workflow

```rust
Function::new("nightly-billing")
    .on_cron_with(
        "0 3 * * *",
        "Europe/London",               // not UTC: "3am in London" is the requirement
        CronOptions::default()
            .catchup_all(7)            // each occurrence is money, so fire them all
            .misfire_window("PT12H")   // …but nothing older than half a day
            .singleton("billing"),     // and never two at once
    )
    .run(nightly_billing)
```

Every option beyond the expression answers a question that only comes up when the
server was down over a fire time — which is exactly when nobody wants to find out
what the default was. The defaults are `catchup: one` within `PT1H`, overlapping
freely: right for "this should have run recently", wrong for a billing tick.

An expression that cannot be scheduled fails **registration**, with the field
named. `GET /v1/schedules` shows the next fire, the last fire, the 24-hour fired
and skipped counts, and — if the sweep gave up on a schedule — why.

## Documentation

| | |
|---|---|
| `spec/PROTOCOL.md` | The wire protocol. The thing a third party implements against — §12 now says what they must expose for the battery to run. |
| `docs/adr/` | Twenty-four ADRs. Start with 011, 012 and 019 — the silent-corruption ones; 024 is why the tree is laid out as it is. ADR-016 records what building cron taught about ADR-016; 021 and 022 what the conformance suite found. |
| `docs/runbooks/restore-hazard.md` | **Read before you need it.** A point-in-time restore re-executes side effects — and re-fires cron occurrences, which is section 3a. |
| `docs/runbooks/` | Stuck runs, backlog, poison pills, upgrades. |
| `docs/RECONCILIATION.md` | What was wrong with the archived tree, what was done, and §7 — the findings worth carrying forward. |
| `docs/GAPS.md` | The gap register, with a resolution against each gap. A snapshot; the [issue tracker](https://github.com/swiftugandan/stepd/issues) is the live list. |
| `docs/blob-backends.md` | Which S3-compatible servers were observed to enforce a signed upload checksum, and what each one's rejection actually looked like. Read before choosing a store for `STEPD_BLOB_BACKEND=s3`. |
| `docs/SDK-DESIGN-rust.md` | The two mechanisms that carry all the SDK's risk. |

## Licence

Apache-2.0. Applied in `0d01e75`; the text is in `LICENSE` at the root.
