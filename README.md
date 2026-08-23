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
rust/        The implementation: nine crates, migrations, SQL test suites
docs/        BRD, PRD, gap register, SDK design, ADRs, runbooks
reference/   Python reference implementation — kept as an independent model
```

## Status

| Component | State | Evidence |
|---|---|---|
| Protocol spec (rev 1.2) | Complete | 54 schema cases — `spec/validate.py` |
| Postgres schema + engine SQL | Complete | 166 behavioural + 27 structural assertions |
| `stepd-proto` | Complete | 36 tests |
| `stepd-core` | Complete | 65 tests, in-memory fakes for every interface |
| `stepd-store-postgres` | Complete | 11 unit + 23 live + the simulation harness |
| `stepd-expr-cel` | Complete (documented subset) | 16 tests |
| `stepd-transport-http` | Complete | 10 tests |
| `stepd-sdk-core` | Complete | 32 tests — the R1 machinery |
| `stepd-sdk` | Complete | 29 tests, including the workflow test harness |
| `stepd-server` | Complete | 28 unit + 17 end-to-end |
| `stepd-cli` | Complete | `serve` · `migrate` · `doctor` · `dev` · `token` · `run` · `limits` · `conformance` |
| Cron scheduler | Complete | 37 unit + 47 SQL + 13 live + 3 e2e; simulation property P10 |
| Cancellation compensation | Complete | migration 010; conformance `cancel` |
| Managed blobs | Complete | two-phase upload, `Range`, dedupe, reference tracking |
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
* **The blob relay is the fallback path, not the fast one.** The bundled
  filesystem store cannot presign, so §8.3.2's compatibility relay applies and
  bytes go through the server. It warns every time. An S3-backed store
  implementing the same trait removes that hop without anything above it
  changing — but nobody has written one.
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

Managed blobs need `STEPD_BLOB_SIGNING_KEY`, `STEPD_BLOB_ROOT` and
`STEPD_BLOB_BASE_URL`. Without the key the two transfer endpoints answer 501 and
say why: a capability signed with a default key verifies for anyone who guesses
it, and the failure would be silent, so there is no default.

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

Bytes never pass through the server on the way in: the app reserves, uploads
directly, and hands back a reference. Reading is lazy and the reference is a
value — replaying a run with forty blob-bearing steps decodes forty references
and downloads nothing.

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
| `docs/adr/` | Twenty-two ADRs. Start with 011, 012 and 019 — the silent-corruption ones. ADR-016 records what building cron taught about ADR-016; 021 and 022 what the conformance suite found. |
| `docs/runbooks/restore-hazard.md` | **Read before you need it.** A point-in-time restore re-executes side effects — and re-fires cron occurrences, which is section 3a. |
| `docs/runbooks/` | Stuck runs, backlog, poison pills, upgrades. |
| `docs/RECONCILIATION.md` | What was wrong with the archived tree, and what was done. |
| `docs/GAPS.md` | The gap register. |
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
4. **Run the CI lanes.** `.github/workflows/ci.yml` exists and has never
   executed; the pgbouncer lane in particular turns "we only use row-level
   locks" from an assertion into evidence.
5. **Rehearse the restore runbook.** It is the document most likely to matter, a
   procedure nobody has practised takes hours and produces its decisions under
   pressure — and it just grew a cron section that has never been walked
   through.

## Licence

Apache-2.0 intended (see PRD open question 6; not yet formally applied).
