# stepd — durable workflow engine

**Last verified 2026-08-23** against PostgreSQL 16. Read this before running
anything: it states what is finished, what is not, and where the trustworthy
artifacts are.

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

```
spec/        Wire protocol, JSON Schemas, validator
rust/        The implementation: eleven crates, migrations, SQL test suites
docs/        BRD, PRD, gap register, SDK design, ADRs, runbooks
reference/   Python reference implementation — kept as an independent model
```

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
| `stepd-sdk` | Complete | 30 tests, including the workflow test harness |
| `stepd-server` | Complete | 42 unit + 19 end-to-end |
| `stepd-blobs-s3` | Complete | 8 offline + 6 live; the live ones need `STEPD_TEST_S3_*` |
| `stepd-cli` | Complete | `serve` · `migrate` · `doctor` · `dev` · `token` · `run` · `limits` · `conformance` |
| Cron scheduler | Complete | 37 unit + 47 SQL + 13 live + 3 e2e; simulation property P10 |
| Cancellation compensation | Complete | migration 010; conformance `cancel` |
| Managed blobs | Complete — filesystem (relay) and S3 (presigned) | two-phase upload, `Range`, dedupe, reference tracking; the S3 suites need `STEPD_TEST_S3_*` and nothing automatic sets it |
| Subject erasure | **Schema only** | see below |
| Conformance suite | Complete | 19 suites, 28 cases; the reference app reaches **level 2** |

**A five-step workflow runs from an ingested event to a completed run**, through
the real dispatcher, a real SDK app on a real socket, over signed HTTP, against
PostgreSQL 16 — `rust/crates/stepd-server/tests/end_to_end.rs`.

## Honest gaps

Listed here rather than buried, because the previous version of this table
overstated the opposite way and that is how four defects sat undetected.

* **Subject erasure is schema only.** `subject_index` and `erasures` exist; no
  code reads `subject_key`. `docs/adr/020-subject-erasure.md` is `Proposed`.
* **Circuit-breaker state is not in the API.** Breakers are per-replica and in
  memory. `/v1/functions` reports observable failure counts rather than a value
  that would be confidently wrong.
* **BR-19 on the S3 path is proved by tests nothing runs automatically.**
  `STEPD_BLOB_BACKEND=s3` presigns the upload with `x-amz-checksum-sha256` and
  `content-length` bound into the SigV4 signature, so the object store rejects
  mismatched bytes itself and the server issues only `HeadObject` and
  `DeleteObject` against it — it never transfers an object.
  `no_object_bytes_reach_the_server_on_the_s3_path`
  (`stepd-server/tests/end_to_end.rs`) drives a real run whose step result
  carries a 256 KiB `$blob` and asserts that everything crossing the server's
  own socket, both directions, stayed under 32 KiB; `stepd-blobs-s3/tests/live.rs`
  checks the store's enforcement directly. Both skip loudly without
  `STEPD_TEST_S3_*`, and no lane in `.github/workflows/ci.yml` sets it —
  `grep -c STEPD_TEST_S3 .github/workflows/ci.yml` is 0. (The end-to-end one
  also needs `STEPD_TEST_DATABASE_URL`, which CI *does* set at `ci.yml:108`, so
  `tier 2 · integration` compiles and runs that test on every push — and it
  skips, for want of the S3 variables.) So this is evidence that exists and
  passes locally, and no evidence that is produced automatically.
  `docs/blob-backends.md` records what
  MinIO `RELEASE.2025-09-07T16-13-09Z` and RustFS `v1.0.0-beta.12` actually did
  when probed — both reject a presigned PUT whose body does not match its signed
  checksum, but RustFS is a beta release and reports that rejection under the
  wrong header name (`Content-Md5`), so its error text is not a basis for any
  claim about which header it checked.
* **One narrow regression in the S3 backend would escape every test.** If
  `S3Backend::stored` kept its `HeadObject` and kept refusing an object the
  store reports no checksum for, and merely *added* a `GetObject` beside them,
  nothing would go red — and the payload would be crossing the wire between the
  server and the object store again.
  `a_committed_object_reports_its_digest_without_transferring_it` measures the
  answer and not the transfer, and says so in its own comment; and
  `no_object_bytes_reach_the_server_on_the_s3_path` counts bytes on the server's
  client-facing socket, which server-to-store traffic never crosses. The
  neighbouring regressions *are* caught. Replacing the `HeadObject` with a
  GET-and-hash fails
  `an_object_the_store_reports_no_checksum_for_is_an_error_not_a_fallback`,
  because that test's object has no checksum and the replacement would answer
  for it instead of erroring. And `stored` returning `None` — which would route
  `commit_blob` into its read-and-hash arm — fails the same test, and on a real
  deployment fails the commit loudly anyway, because
  `PostgresBlobStore::get_bytes` errors on a store built with `relay: None` and
  that is how `Server::build` builds the S3 one. So: one specific shape, not a
  class, recorded because it is the one change that could put payload bytes back
  onto the control plane without a red build.
* **A `$blob` that arrives outside an attempt envelope is still never
  verified.** §8.3.2 requires the server to check `size` and `sha256` before a
  blob becomes readable. `Dispatcher::verify_blobs` does that for an envelope's
  ops — which is where the `invoke` and `continue_as_new` run inputs live — and
  its emitted events. A `$blob` in an event ingested through `POST /v1/events`,
  which is what becomes a run's input, or in a signal payload sent to
  `POST /v1/runs/{id}/resolve-wait`, reaches no `commit_blob` call anywhere:
  neither `api.rs` nor `ingest.rs` mentions blobs at all, and `commit_blob` has
  exactly two callers on the serving path — the dispatcher and the relay
  endpoint. On a presigning backend such a row therefore stays `reserved`, and
  the collector takes its bytes once the reservation window
  (`STEPD_BLOB_RESERVATION_TTL_HOURS`, default 24) passes. §8.3.4, the
  *reference* obligation, is a separate matter and is covered on every path —
  `runs_record_blob_refs` in `migrations/0011_blob_refs.sql` fires on
  `INSERT OR UPDATE OF input, output ON runs`, so an ingested event's blob does
  get its `blob_refs` row. It is verification, not reference counting, that has
  one entry point.
* **`commit_blob` does no namespace check**, and neither does `attach_read_urls`
  or `presign_read` — all three look a blob up by id alone. Pre-existing, but
  op-commit is the first place an app-supplied blob id drives a
  `reserved → committed` transition on a row the app may not own. Substituting
  content is a separate matter and remains impossible: the digest is fixed at
  reservation and is what the commit checks against.
* **The SDK does not enforce the compensation path.** A handler that declares
  `on_cancel` and ignores `ctx.run().cancelling` will re-run its normal work.
  The protocol says the SDK runs only the compensation path; the Rust SDK exposes
  the flag and trusts the handler.

`docs/RECONCILIATION.md` has the full list, and the seventeen defects found and
fixed — nine in the tree that shipped in this archive, three while building the
cron scheduler, three the conformance suite found on its first run, one more
found by asking what kept a blob's bytes alive, and one found by deleting a
feature and having to write down what was left.

## The findings worth carrying forward

Every one came from a test failing, or from reading code against the design it
claimed to implement — none from review in the abstract.

1. **Correctness can rest on undocumented accidents.** The lost-signal race was
   originally closed only by a foreign key's incidental row lock. Fixed by making
   the serialization explicit, with a structural test that fails the build if it
   moves.

2. **A countermeasure can point at the wrong thing.** Those structural tests
   asserted properties of the SQL functions — and the Rust store had grown its
   own four hundred lines of application SQL that reimplemented the commit and
   called none of them. Two correctness centres; the tests guarded one; the other
   was the one that ran. Three live defects were sitting in it.

3. **`continue_as_new` orphaned live children.** Found by the simulation harness
   via property P8 on its first run. The cascade rules covered cancellation and
   failure but not continuation.

4. **Eager hash claiming makes concurrency safe by construction.** `ctx.step()`
   must claim the occurrence when *called*, not when its future is *polled*.
   Claiming at poll time ties the hash to scheduler order, so `join!` silently
   re-executes completed work.

5. **…and a rule obeyed in one place is not obeyed.** `ctx.step` claimed eagerly;
   `wait_event` and `invoke` claimed at `.await`. The project's own headline
   hazard, reintroduced through a side door, found by reading the code against
   its design document while writing ADR-012.

6. **A flaky test was a design defect.** The circuit breaker's probabilistic
   recovery ramp made recovery time impossible for operators to reason about and
   for tests to pin down. Replaced with a deterministic token budget.

7. **A breaker that closes only on observed successes never closes.** Once
   traffic stops it stays half-open, throttling the next burst long after the app
   recovered. Closure is now successes *or* a quiet period.

8. **Passing is not exercising.** `reference/coverage_check.py` showed cascade
   cancellation hit zero times across 500 green seeds — the suite had never
   tested a fix that had just been made.

9. **Documentation that overstates is a defect.** The transport's module comment
   claimed resolve-then-connect DNS pinning that the code did not perform. A
   comment describing a security property the code lacks is worse than no
   comment: it stops the next reader from checking.

10. **Plumbing a feature end to end is not implementing it.** Cancellation
    compensation had a field on the wire type, an accessor in the SDK, a computed
    value in the store and a comment citing the specification — and `cancel_run`
    deleted the queue row, so the run was never dispatched again and the flag
    could never be true. The undo silently did not happen and the run looked
    exactly as it does when it worked. Three times now the missing piece has been
    one line in the one place that would make the thing run, and every time
    everything around it read as finished.

11. **Ask what keeps a thing alive, not just what creates it.** `blob_refs`,
    `add_ref` and `blob_ids` were all written, tested and never called — so the
    collector was entitled to delete the bytes behind every `$blob` in a live
    run's journal from the moment they were committed. The run would fail on its
    next replay with a missing object, hours after the collection that caused it,
    with nothing connecting the two. The reference is now recorded by a trigger
    in the same transaction as the row that carries it, because any gap at all is
    a window where a crash makes live data look like garbage.

12. **A settled design document is not a specification.** ADR-016 answered every
    question anyone had thought to ask about cron and was still underspecified in
    three places, each found by a test rather than by rereading it: it said
    `key_expr` where a cron fire has no event to evaluate one against; it said
    nothing about fairness, and a namespace-blind claim ordered by `next_fire_at`
    lets one tenant's backlog silently stop everyone else's schedules; and it did
    not say what to do with a schedule that cannot be planned, where "log it and
    carry on" is a hot loop because the row stays due forever.

13. **The same defect arrives twice by the same route.** The cron sweep's
    starvation bug is the dispatcher's, and it was found the same way — two tests
    interfering in a shared database. `tick_namespace` exists because of the
    first one. Nothing generalised the lesson into a rule, so the second
    component made the same choice from scratch. Structural invariant 19 now
    asserts it for cron, next to invariant 15 which asserts it for dispatch.

14. **A truncated timestamp is a lost distinction.** The run input rendered its
    cron occurrence to whole seconds, so two occurrences inside one second became
    indistinguishable to the handler — and a point-in-time restore produces
    exactly that, because it rewinds `next_fire_at` to an arbitrary instant. The
    simulation harness found it on its first run with property P10, reporting two
    runs for one occurrence that were in fact two occurrences it could no longer
    tell apart.

15. **A conformance suite has to specify what it observes.** Protocol §12 listed
    nineteen suites and never said what an implementation must expose, so the
    claim "a third party can implement this" had nothing behind it. Worse, the
    headline guarantee is untestable from server state: the journal after one
    execution of a step body and after two is byte-identical, so the app itself
    has to report what it ran.

16. **A tool that certifies must be able to say what it did not check.** The
    failure specific to a conformance runner is reporting LEVEL 2 when four of
    its suites were never written — a result indistinguishable from a real pass.
    So "not implemented by this runner" is a first-class outcome here, printed as
    loudly as a failure, and it bars the level it belongs to. The runner's own
    gaps are also evaluated *before* the app's declarations, because otherwise a
    suite nobody declares is a hole that only shows up as somebody else's.

17. **A comment can defend a hazard that the next layer reopens.** `join.rs`
    polls every member of a parallel group to completion before deciding its
    outcome, and says why in five lines: returning early would leave siblings
    that had already executed unrecorded. The layer above discarded them anyway
    — `PassOutcome::Error` carried no ops, so a group where one member raised
    committed nothing at all. Two step bodies had run and returned and the
    journal knew about neither. Deleting the join policies is what surfaced it:
    the sentence that replaced them ends "every outcome is recorded", and
    writing that down meant checking whether anything did.

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
cd rust
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
  --database-url postgres://postgres@127.0.0.1:5433/stepd_conf
```

The report says what it checked **and what it did not**. A suite the app did not
declare and a suite this runner has not implemented are different lines, and
either one stops it certifying the level containing it — a conformance tool that
certifies around its own gaps is worse than none, because the whole point is that
the verdict can be trusted without reading its source.

`stepd conformance` tests an **app**. Turning it round to test a second server
implementation would need the mirror battery — a fixed app that reports what it
was sent — which is specified nowhere and does not exist (protocol §12.4).

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
cd rust
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
| `docs/adr/` | Twenty-three ADRs. Start with 011, 012 and 019 — the silent-corruption ones. ADR-016 records what building cron taught about ADR-016; 021 and 022 what the conformance suite found. |
| `docs/runbooks/restore-hazard.md` | **Read before you need it.** A point-in-time restore re-executes side effects — and re-fires cron occurrences, which is section 3a. |
| `docs/runbooks/` | Stuck runs, backlog, poison pills, upgrades. |
| `docs/RECONCILIATION.md` | What was wrong with the archived tree, and what was done. |
| `docs/GAPS.md` | The gap register. |
| `docs/blob-backends.md` | Which S3-compatible servers were observed to enforce a signed upload checksum, and what each one's rejection actually looked like. Read before choosing a store for `STEPD_BLOB_BACKEND=s3`. |
| `docs/SDK-DESIGN-rust.md` | The two mechanisms that carry all the SDK's risk. |

## Next steps, in order

1. **Implement subject erasure**, and resolve its tension with the append-only
   journal and with backups — stated in ADR-020, not yet decided.
2. **Persist circuit-breaker state**, so an operator can see it during an
   incident.
3. **Get a second SDK written against the spec alone.** The conformance battery
   exists now, and passing it against the reference app is weak evidence: the
   suite and the Rust SDK were written together and can share a misreading. An
   independent implementation reaching level 2 is the claim the protocol is
   actually making.
4. **Get the CI lanes green, and add one for the S3 backend.**
   `.github/workflows/ci.yml` runs on every push; as of run `32688345824` four
   of six lanes pass and two do not — `tier 3 · through pgbouncer (F-DL-1)`,
   which is the lane that turns "we only use row-level locks" from an assertion
   into evidence, and `tier 4 · soak (nightly)`. Nothing sets `STEPD_TEST_S3_*`
   in any lane, so the managed-blob claim in *Honest gaps* above has no
   automatic evidence behind it either.
5. **Rehearse the restore runbook.** It is the document most likely to matter, a
   procedure nobody has practised takes hours and produces its decisions under
   pressure — and it just grew a cron section that has never been walked
   through.

## Licence

Apache-2.0. Applied in `0d01e75`; the text is in `LICENSE` at the root.
