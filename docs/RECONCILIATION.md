# Reconciliation: the Rust tree against the design

| | |
|---|---|
| Date | 2026-08-23 |
| Scope | The whole Rust workspace, reconciled against PRD §6.3, the protocol spec and `reference/` |
| Outcome | Seventeen defects found and fixed; four crates written; the cron scheduler, the conformance battery and managed blobs built; the engine runs against a live database |

This is the record of what the archived Rust tree actually was, what was wrong
with it, and what was done. It exists because the README's provenance caveat was
right to be suspicious, and because the specific things that were wrong are more
useful than the fact that something was.

---

## 1. The finding behind most of the others

The README's first "finding worth carrying forward" reads:

> **Correctness can rest on undocumented accidents.** Structural invariant tests
> are the countermeasure — they fail the build if the explicit lock is removed
> or moved.

The countermeasure was in place and pointed at the wrong thing. `test_invariants.sql`
asserts properties of the **SQL functions** — that `deliver_to_inbox` takes the
run row lock as its first statement, that `commit_ops` takes a matching lock. But
`stepd-store-postgres` did not call those functions. It had grown roughly four
hundred lines of application-level SQL that reimplemented the commit: recording
steps, creating child runs, consuming inbox entries, cascading cancellation.

So there were two correctness centres. The invariant tests guarded one of them.
The other was the one that ran.

Three of the defects below follow directly from that, and they had all been
sitting in the tree undetected because nothing tested the path they were on.

**What was done.** Migration `0006_engine_complete.sql` moves every op into
`commit_ops`, and `stepd-store-postgres` becomes a marshalling adapter: build the
JSON, call the function, map the result. Ten further structural invariants were
added, including one asserting there is exactly **one** `commit_ops` — because an
overload is either an ambiguity error or, worse, a silent dispatch to an older
body that drops `invoke`, `signal` and `continue_as_new` without erroring.

---

## 2. Defects found

Ordered by how quietly they failed.

### 2.1 `signal` never woke a parked run — S2, silent

The Rust store inserted `signal` ops straight into `run_inbox`, bypassing
`deliver_to_inbox`. A signal sent to a run **already parked on a matching wait**
was therefore buffered and never resolved it. The run slept until its timeout, or
forever if it had none.

Nothing errored. The signal was durably recorded, the sender's step completed,
the console showed a healthy run in `waiting`.

*Fixed* by routing signals through a durable relay (`signal_outbox`) drained by
`drain_signals`, which calls `deliver_to_inbox` and therefore takes the documented
serialization lock. Delivery is deferred rather than inline because inline
delivery takes a second run row lock while holding the sender's, and two runs
signalling each other deadlock. The relay removes the class of bug instead of
relying on PostgreSQL's deadlock detector to clean up after it.

*Proven by* `a_signal_wakes_a_run_already_parked_on_the_wait` (live database) and
the `signal` block of `test_engine_ops.sql`.

### 2.2 `wait_event` scheduled no timer — S2, silent

`timeout` was accepted on the op and discarded. A run could wait for an event
that never came, indefinitely, while the console displayed a timeout that was
never going to fire.

*Fixed* in `commit_ops`: a wait with `timeout_at` inserts a `wait_timeout` timer,
and `fire_due_timers` resolves both the wait row and the step. `deliver_to_inbox`
defuses the timer when the wait resolves, so a resolved step cannot later be
overwritten by its own timeout.

*Proven by* the `wait timeout` and `wait timeout race` blocks of
`test_engine_ops.sql`.

### 2.3 Cascade cancellation committed per level — S2

`cascade_cancel` was a recursive `async fn` in Rust that walked one level per
statement. A server that died mid-cascade left every descendant below the point
it reached alive, with a terminal ancestor. Protocol §7.5 requires the cascade
itself to be durable and resumable; a loop that commits as it goes is neither.

*Fixed* as a single `WITH RECURSIVE` statement. It cannot half-finish: either the
transaction commits and the whole subtree is cancelled, or it rolls back and the
cascade is retried whole.

*Proven by* the `cascade` and `detached subtree` blocks of `test_engine_ops.sql`,
plus structural invariant 13, which fails the build if it stops being one
recursive statement.

### 2.4 `rust/migrations/0002_engine.sql` was a botched concatenation

The file contained 002, 003 and 004 spliced together — including a `COMMIT;`
mid-file followed by another `BEGIN;` — *and* 0003 and 0004 also existed as
separate files. sqlx wraps each migration in a transaction, so the embedded
`COMMIT` would have ended sqlx's transaction before it recorded the migration as
applied.

*Fixed*: 0002 restored to its own content, `BEGIN`/`COMMIT` stripped from every
migration (sqlx owns the transaction), 0003 and 0004 left as the separate
migrations they always were.

### 2.5 The SDK claimed occurrences lazily for `wait_event` and `invoke` — S1, silent

This one is the project's own headline hazard, reintroduced through a side door.

`ctx.step` claimed eagerly, exactly as `SDK-DESIGN-rust.md` §2 requires. But
`wait_event` and `invoke` returned builders that claimed inside `IntoFuture` —
that is, at `.await`. Two waits sharing a step id, awaited out of declaration
order, therefore swapped their occurrences and their hashes. Completed work would
re-execute, and nothing would error.

Found while writing ADR-012, by reading the code against the design doc it was
supposed to implement.

*Fixed*: both builders claim at construction. The cost is stated in a test — an
abandoned builder still consumes its occurrence — so it is a decision rather than
a surprise.

*Proven by* `a_wait_claims_at_the_call_site_not_at_the_await` and
`an_invoke_claims_at_the_call_site_too`, which assert that the *second-declared*
op keeps occurrence 1 whichever is awaited first.

### 2.6 The egress policy did not pin the address it approved — S2

`HttpTransport::deliver` resolved the app URL through `EgressPolicy::resolve`,
checked the address, and then handed the **hostname** to `reqwest` — which
resolved it again. The second answer can be the metadata address after the first
was innocuous. The module documentation claimed resolve-then-connect pinning that
the code did not perform, which is worse than not claiming it.

*Fixed*: clients are built per approved `(host, port, ip)` with
`reqwest::ClientBuilder::resolve`, so the connection goes to the address the
policy actually approved. The cache is bounded, because it is keyed by
tenant-supplied hostnames.

*Proven by* `a_refused_url_never_produces_a_client` and
`an_approved_url_yields_a_client_pinned_to_that_address`.

### 2.7 Quarantine counted attempts, not identical failures — S3

`DispatchConfig::quarantine_after` documented itself as "identical consecutive
failures" and compared an attempt count. A run failing a different way each time
— a run that deserves a human — was removed from dispatch exactly like a poison
pill, and the variety that would have explained it was hidden.

Compounding it, `error_signature` held two incompatible formats: `ErrorBody::signature()`
hashed `code ‖ 0x1F ‖ message` to 16 hex characters, `fail_run` hashed the code
alone to 64. A run failed by a protocol rule and a run failed by its app could
never group together — and the DLQ view exists to group them.

Including the message also defeated grouping outright, because messages routinely
embed a run id or an order number.

*Fixed* in migration 0008: one `error_signature` definition, over the code when
there is one; `record_failure` returns consecutive identical failures, reset when
the signature changes; the dispatcher quarantines on that.

*Proven by* `quarantine_needs_the_same_failure_repeatedly_not_merely_many_failures`
and `the_signature_ignores_a_message_that_embeds_an_identifier`.

### 2.8 One attempt at a time per replica — S3

`tick_namespace` claimed a batch of sixteen leases and awaited `drive` on each in
turn. Per-replica attempt concurrency was therefore one, however large `batch`.
Worse, with slow attempts the last lease in a batch could expire before it was
ever dispatched: the run would be reclaimed, re-dispatched and its step
re-executed, under entirely ordinary load, with nothing failing to say so.

*Fixed*: leases are driven concurrently, and one failure no longer abandons the
rest of the batch — the others hold leases, and dropping out would leave them to
expire and re-execute.

Separately, the default lease was **equal** to the default attempt timeout, so an
app using its whole budget raced its own lease. The lease default is now 150 s
against a 60 s attempt timeout, and `Config::validate` warns when an operator
configures them the other way.

### 2.9 `key_hash_current` held a hash of the app id — S3

`NOT NULL` forced registration to write *something*, and what it wrote was
`SHA-256(app_id)`: a value shaped exactly like a key digest, verifying nothing.
An operator asking "is a signing key configured for this app?" got a confident
wrong answer.

*Fixed* in migration 0007: the column is nullable, `NULL` means "this server
holds no key for this app", and registration records the digest of the key it
will actually sign with — plus a warning when there is none.

---

### 2.10 Cron claiming would have starved every namespace but the busiest — S3, silent

**Found by:** two live tests interfering in a shared database. `sweep` visited
every namespace with work, so one test's schedules were fired and counted by
another test's sweep.

The test artefact was trivial to fix. The defect underneath it was not: a claim
written as `ORDER BY next_fire_at LIMIT n` with no namespace filter is won by
whoever is furthest behind. One tenant with a thousand overdue per-minute
schedules fills every sweep, and every other tenant's schedules stop firing —
with nothing failing, no backlog anywhere an operator would look, and no
component to point at.

This is the *same defect* as the dispatcher's, found the *same way*.
`Dispatcher::tick_namespace` exists because of the first one. Nothing turned that
into a rule, so the second component made the same choice from scratch.

**Fixed:** `claim_due_cron` takes a namespace; `cron_active_namespaces()` and a
per-replica rotation cursor mirror `active_namespaces()` and the dispatcher's.
Structural invariant 19 now asserts the namespace filter, next to invariant 15
which asserts the dispatcher's. `one_busy_namespace_cannot_starve_another` fails
against the blind version and passes against this one.

### 2.11 An unplannable schedule would have spun the sweep — S2

**Found by:** writing the planner and asking what happens when `Schedule::parse`
fails on a row that is already stored.

A due row is re-claimed on every sweep until `next_fire_at` moves, and a schedule
that cannot be planned is exactly one whose `next_fire_at` cannot be computed. So
the obvious handling — log the error and carry on — is a hot loop for as long as
the row exists. The two ways to reach that state are a zone that disappeared from
tzdata under a running server, and a row edited by hand.

**Fixed:** the planner returns `unschedulable` with a reason; the store pauses the
schedule, writes the reason to `cron_schedules.last_error`, and bumps
`cron_unschedulable`. `stepd doctor` reports it as **critical** and
`GET /v1/schedules` shows it, because a paused schedule is a job that has
silently stopped and "read the logs from whenever it happened" is not an answer
at 3am. `POST /v1/schedules/{id}/resume` re-parses before clearing the flag, so
resuming a still-broken schedule fails loudly instead of pausing again next pass.

### 2.12 The run input truncated its cron occurrence to whole seconds — S3, silent

**Found by:** the simulation harness, property P10, on its first run — reporting
two runs for one occurrence.

They were not two runs for one occurrence. They were two occurrences the run
input could no longer tell apart: `to_char(..., 'HH24:MI:SS')` discarded
sub-second precision, and the occurrence in the run input is the handler's only
view of which fire it is (`started_at` is the recovery time, not what the
schedule meant — ADR-016 accepts that explicitly).

Sub-second occurrences are not hypothetical. A point-in-time restore rewinds
`next_fire_at` to an arbitrary instant; so does an operator repairing a schedule
by hand. The simulation's fault model includes exactly that rewind, which is why
it found this and no unit test did.

**Fixed:** `to_jsonb(p_occurrence)`, which is lossless and RFC 3339. The SQL test
now compares the value as an instant rather than as text, so a future precision
change does not read as a regression.

---

### 2.13 The cancellation compensation attempt never happened — S1, silent

**Found by:** the conformance suite's `cancel` case, on its first run. Nothing
before it had ever asked whether the compensation path executed.

Protocol §7.4 promises a cancelled run one more dispatch so the handler can undo
what it did. `stepd-proto` carried `cancelling` on the run context; the SDK
exposed it as `ctx.run().cancelling`; the store computed it in `load_attempt`,
with a comment citing the specification. And `cancel_run` deleted the queue row,
so the run was never dispatched again and the flag could never be true.

Nothing errored. The refund was not issued, the reservation was not released, the
partner was not told — and the run showed `cancelled`, which is what the operator
asked for and exactly what it looks like when it worked. `on_cancel` did not
exist in the function config schema either, so there was no way for a function to
declare it had a compensation path at all.

This is the same shape as the cron scheduler, and it is the second time the
missing piece has been one line in the one place that would make the feature run.

**Fixed:** migration 010 and ADR-022. A durable `runs.compensating` flag, the
phase queued by `cancel_run` when the function declares `on_cancel`, the business
key held throughout, and a terminal state of `cancelled` however the compensation
path ends. Three structural invariants (21–23), each with a positive control.

### 2.14 `ctx.attempt()` did not exist — S3

**Found by:** writing the conformance app, which needs to force a second attempt
deterministically rather than by racing a timeout.

Protocol §4 puts the attempt number on every `AttemptRequest`. The SDK received
it and used it internally as the pass token, and no handler could read it — so
"log which retry this is", "escalate on the last attempt" and "behave differently
the first time through" were unavailable for no reason.

**Fixed:** `Ctx::attempt()`, with the attempt stored as its own field rather than
sharing the pass token's. They are different concepts that today carry the same
value, and one field would mean a future change to pass numbering silently
changed what a handler is told about its retries.

### 2.15 A permanent failure could not carry a code — S3

**Found by:** the conformance suite's `errors` case, which sat in `Pending` for
ninety seconds because a deliberately terminal error was being retried.

`StepError::coded` was always retryable and `StepError::fatal` took no code. So
the one case where a machine-readable code is most useful — a permanent,
classifiable failure like "this customer does not exist" — could not have one.
Callers reached for `coded` because they wanted the code and silently got a retry
loop against a condition that would never change.

**Fixed:** `StepError::fatal_coded`, and `coded`'s documentation now says which
one it is and why retryable is the right default for it.

---

### 2.16 Nothing kept a blob's bytes alive — S1, silent

**Found by:** wiring the blob endpoints and asking what stopped the collector
deleting what an app had just uploaded.

Protocol §8.3.4: "The server maintains a reference from every step result, run
input and emitted event that contains a `$blob`." The `blob_refs` table existed.
`BlobStore::add_ref` existed and was tested. `blob_ids`, which walks a value and
finds them, existed and was tested. Nothing called any of it.

The collector deletes committed blobs with no references. So the bytes behind
every `$blob` in a live run's journal were collectable from the moment they were
committed. The run would fail on its next replay with a missing object — hours
after the collection that caused it, with nothing connecting the two.

**Fixed:** migration 011. Triggers record references in the **same transaction**
as the row that carries them: any gap is a window in which a crash leaves a
referenced blob unreferenced, and a collector cannot tell that apart from
garbage. A trigger rather than a call in `commit_ops` also covers every path that
records a step, including ones that do not exist yet — the difference between
"the commit function is careful" and "a recorded reference is always tracked".

The walk is recursive, and structural invariant 25 fails the build if it stops
being: a step result is arbitrary JSON, a blob nested inside an array inside an
object is the ordinary shape, and a top-level-only walk passes every test written
with a flat payload while losing data on the first realistic one.

---

### 2.17 A parallel group with one failed member committed nothing — S1, silent

**Found by:** deleting the `all` and `any` join policies, and having to write
down what a batch actually does. The replacement sentence ends "every outcome is
recorded". Nothing recorded them.

Two separate holes, one behind the other.

`run_steps.status` has had a `failed` value since migration 001. `RecordedStep`
carries `status` and `error` on the wire. The SDK's memo path already turns a
recorded `failed` step back into the error the body raised. The only thing that
ever wrote such a row was `resolve_child_result`, for a failed child *run* — a
step whose own body raised left **no row at all**. A failed run showed a
run-level error and a gap where the step should have been.

Behind that, the worse one. `stepd-sdk-core/src/join.rs` polls every member of a
group to completion before deciding the group's outcome, and spends five lines
explaining why: returning early on the first failure "would leave siblings that
had already executed unrecorded, which is the one thing a durable engine must
never do". It is correct, and the layer above discarded them anyway.
`PassOutcome::Error` carried no ops, and `run_pass` drained the pending buffer
only on the yield arm. A group of three where one member raised committed
nothing: two bodies had run and returned, and the journal knew about neither. On
the retry they ran again.

Nothing errored. The run failed with the failing member's message, which is
exactly what it looks like when it worked.

**Fixed:** `Op::Step` gained an optional `error`; migration 013 records such an
op as `failed` with its error rather than `completed`. `error` stopped being an
exclusive op and now rides at the end of a batch, so the outcomes and the failure
commit in one transaction. `run_pass` drains the buffer once, before the match,
so every arm has to say what it does with the pass's recorded work — the defect
was an omission, and an arm that omits it no longer compiles.

A retryable failure is deliberately *not* recorded: retrying re-executes, and a
recorded step is memoised, so recording one would replay the error forever and
the retry would silently stop working. An SDK holding recorded ops and a
retryable failure emits the ops alone and re-raises next attempt. ADR-023.

Structural invariant 26 fails the build if `commit_ops` stops reading the error
off a step op; 27 fails it if a join-policy parameter reappears. Both were run
against deliberately wrong schemas before being trusted.

---

## 3. Divergences from PRD §6.3, and why

The implemented traits are not the PRD's traits. Where they differ, the
implementation is generally sharper, and the PRD should be amended rather than
the code.

| PRD §6.3 | Implemented | Why |
|---|---|---|
| `Queue::claim(worker, max)` | `claim(namespace, worker, max, lease)` | A namespace-blind claim lets one tenant's backlog starve every other, whatever order the caller makes its calls in. Fair dispatch is not expressible without the parameter (F-LP-5). |
| `StateStore::create_run -> RunId` | `-> Option<RunId>` | `None` is keyed exclusivity refusing a second active run on a key. That is the singleton guarantee working, not an error, and modelling it as one makes every caller handle a false alarm. |
| `TimerStore::{schedule, cancel, due}` | `fire_due` | Timers are scheduled inside `commit_ops`, in the same transaction as the op that needs them. A separate `schedule` call would be a second write that can fail after the first succeeded. |
| `EventLog::match_waits` | `correlate_event` in SQL | Matching and delivering in one transaction is what keeps the inbox check atomic with wait registration. Returning matches to application code to deliver reopens the window. |
| — | `Housekeeping` trait | Timers, relayed signals, finished children and expired leases must converge when *no app is reachable at all*. Folding them into dispatch means a total app outage also stops the system healing itself. |
| — | `CommitOutcome::Rejected(code)` | The store fails the run inside the commit transaction when an envelope breaks a rule, so the caller must not write again. Carrying the code lets the console say which rule without re-reading the run. |
| `StateStore::steps_page` | implemented | Protocol §8.6. Absent, a run at the step ceiling can only fail or be silently truncated, and silent truncation makes the SDK re-execute steps whose results merely were not sent. |

The `BlobStore` and `ExprEngine` traits from §6.3 were absent and are now present.

---

## 4. What was built

| Crate | State | Evidence |
|---|---|---|
| `stepd-sdk-core` | New | 30 tests: eager claiming, halt control flow, crash interleavings, decode safety |
| `stepd-sdk` | New | 25 tests: function builder, lint, signatures, replay defence, test harness |
| `stepd-expr-cel` | New | 16 tests, including every expression that appears in the protocol spec |
| `stepd-transport-http` | New | 10 tests, all of the egress policy |
| `stepd-server` | New | 24 unit + 15 end-to-end against a live database and a real SDK app |
| `stepd-cli` | New | `serve`, `migrate`, `doctor`, `dev`, `token`, `namespace`, `run`, `limits` |
| `stepd-store-postgres` | Rewritten as an adapter | 11 unit + 23 live-database + a simulation harness |
| `stepd-core` | Extended | `Housekeeping`, `BlobStore`, `ExprEngine`, `CronStore`, the pure `cron` engine and planner, `steps_page`, concurrent dispatch |
| `stepd-proto` | Signature fix; `iso8601_seconds` moved in from the SDK | 29 tests |
| `stepd-conformance` | New | The protocol §12 battery, the reference app, and a report that names what it did not check |
| `stepd-sdk` (blobs) | New module | The app side of §8.3: two-phase upload, content addressing, lazy read, `Range` |

---

## 5. What is still not built

Stated plainly, because a status table that overstates is how the last one went
wrong.

| Thing | State |
|---|---|
| Subject erasure (F-SEC-5) | **Schema only.** `subject_index` and `erasures` exist; no code reads `subject_key`. ADR-020 is `Proposed`. |
| Join policies `all` / `any` | **Specified, not enforced.** `commit_ops` accepts `p_join` and does not read it. Every batch behaves as `all_settled`. |
| Circuit-breaker state in the API | **Deliberately absent.** Breakers are per-replica and in memory; `/v1/functions` now reports observable failure counts instead of a value that would be confidently wrong. |
| `stepd dev` embedded Postgres | Not implemented; `dev` takes a connection URL. |
| Console SSE (F-UI-3) | Not implemented; the console polls. |
| Threat model, benchmark harness | Not started. |
| Conformance suite (`stepd conformance`) | **Built.** 18 of 19 suites, 25 cases. `blobs` cannot run while the blob routes are absent, and the runner reports that rather than certifying around it. |

---

## 6. Test inventory

| Layer | Count | Needs a database |
|---|---|---|
| Protocol schema cases (`spec/validate.py`) | 54 | no |
| Rust unit tests (`cargo test --workspace --lib`) | 200 | no |
| Engine tests against in-memory fakes (`stepd-core/tests/engine.rs`) | 19 | no |
| SQL behavioural assertions (`test_engine.sql`, `test_engine_ops.sql`, `test_cron.sql`) | 25 + 94 + 47 | yes |
| SQL structural invariants (`test_invariants.sql`) | 27 | yes |
| Live-database integration (`live.rs`, `cron_live.rs`) | 10 + 13 | yes |
| End-to-end, real SDK over real HTTP (`end_to_end.rs`) | 17 | yes |
| Conformance battery against the reference app | 28 cases, 19 suites, level 2 | yes |
| Rust simulation, real engine | 250 seeds × 120 steps, 10 properties, 12 fault types | yes |
| Python reference suite | unchanged | yes |

The database-backed tests **skip loudly** without `STEPD_TEST_DATABASE_URL`. A
database test that silently passes when it did not run is worse than no test,
because the green tick is then a lie about the thing most likely to break.
