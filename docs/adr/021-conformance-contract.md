# ADR-021: What a conformance suite has to specify, and what it may not conclude

| | |
|---|---|
| Status | Accepted, implemented |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

The protocol's central business claim is that a third party can implement it —
BRD §Community names "a second SDK produced by a contributor using only the spec
and conformance suite" as a success condition, and PRD P3 makes "an external SDK
reaches conformance level 2 using only the spec" the gate for the ecosystem
phase.

Protocol §12 listed nineteen suites and what each asserts. It did not say what an
implementation must **expose** for any of it to be runnable. Nothing named the
functions the battery drives, nothing said how a runner observes behaviour, and
nothing let an implementation declare how much of the protocol it supports.

That gap is not a documentation shortfall. It means the headline claim had
nothing behind it: an SDK author in another language could read the whole
specification and still not know what to build, and no two runners would agree on
what "conformant" meant.

Three further problems surfaced while writing it, each of which changed the
design rather than the prose.

## Decision

### 1. The battery observes two channels, and the app must provide one of them

A runner sees server state — run status, the journal, the queue. That is
authoritative for what was **recorded** and it is not enough, because the
headline guarantee is about what was **executed**.

"A recorded step is never re-executed" is invisible in server state. The journal
after one execution of a step body and after two is byte-identical. Without a
second channel, the suite that guards the guarantee the entire project exists for
cannot be written at all.

So §12.1 requires the app under test to expose an **effect log**: an ordered,
per-run record of every time a step *body* ran, at
`GET {app_url}/_conformance/effects?run={id}`. Every interesting assertion in the
battery compares the two channels.

The endpoint takes no authentication and exposes execution detail, so the
specification requires it to be absent outside conformance mode, and requires an
SDK offering a conformance app to make it a separate binary or an explicit
opt-in — never a default route on the app that serves production traffic.

### 2. A runner must not certify around its own gaps

A conformance tool exists to be pointed at somebody else's implementation and
produce a claim about it. Its characteristic failure is reporting LEVEL 2 when
four of its suites were never written — a result that reads identically to a real
pass, and which makes the tool worse than nothing, because its whole value is
that the verdict can be trusted without reading its source.

So an outcome is one of five, not two: **passed**, **passed by construction**,
**failed**, **not declared by the app**, and **not implemented by this runner**.
The last two are unknowns. Both are printed as loudly as a failure and both bar
certification of the level containing them.

The two unknowns are kept apart because they mean opposite things. "Not declared"
is a smaller implementation reporting itself accurately, and must not fail a
build — failing it would push implementers towards declaring suites they have not
written, which is the one thing the manifest exists to prevent. "Not implemented"
is the runner's own hole and does fail the build.

The runner's gaps are also checked **before** the app's declarations. The other
order hides them: a suite this battery has not implemented would show as "not
declared" whenever no app happened to declare it, and the tool's hole becomes
invisible exactly when nothing else would reveal it.

### 3. A hazard prevented by construction is not a hazard undemonstrated

`determinism` requires that "a step created off the sequential path fails
non-retryably". The Rust SDK's context type is `!Send` and `!Sync`, so a handler
that claims a step from a spawned task **does not compile** — and no runtime
assertion can be made about a program that does not exist.

Demanding a runtime failure would score the strongest possible defence below a
weaker one. That is a test rewarding the wrong thing, and an implementer who
noticed would be right to weaken their SDK to pass it.

So a manifest may declare `statically_prevented: ["offpath_claim"]`, omit the
corresponding function, and have the case recorded as satisfied **by
construction** — labelled distinctly, because it is a claim the runner relayed
rather than one it checked. The enum is closed: a runner must never be able to be
told to skip an assertion by a value it does not recognise.

### 4. It tests an app, and says so

The battery drives an app through a known-good server, so a failure is
attributable to the app. It cannot be turned round. A second implementation of
the **server** is equally within the protocol's claim and would need the mirror
image — a fixed app that reports what it was sent — which is specified nowhere
and does not exist.

That limit is in the specification (§12.4), in the runner's module documentation
and in the CLI's help text, because a tool whose scope is stated only in the
specification will be used outside it.

## Consequences

### What this makes easy

* An SDK author in another language has a checklist: twenty-four named
  functions, three endpoints, one manifest.
* A partial implementation is reportable. Declaring seven suites and passing them
  is "conformant at level 1", not twelve failures.
* The tool's own coverage is visible in its output rather than in its source.

### What this makes hard

* The reference app must be maintained alongside the specification. A function
  the battery drives that no reference implementation exercises is a suite that
  has never run.
* Adding a suite means adding a function to the contract, which is a protocol
  change rather than a runner change.

### What we accept

* **Passing against the reference app is weak evidence.** The battery and the
  Rust SDK were written together and can agree on a shared misreading of the
  specification; no number of green ticks would surface that. What a green run
  establishes is that every assertion is reachable, that the §12.2 contract is
  implementable, and that an SDK change breaking a protocol guarantee fails a
  build. The strong evidence is an independent implementation passing, and it
  does not exist yet.
* The `blobs` suite was specified and unrunnable when this ADR was written: the
  server's blob HTTP endpoints were not in the router, so the runner reported it
  as **not implemented** and refused to certify level 2 over it. The integration
  test asserted that state, and failed the moment the endpoints were wired —
  which is exactly what it was written to do. The suite now runs and the
  reference app reaches level 2. A test that had quietly passed through both
  states would have said nothing about either.

## Alternatives considered

| Option | Why not |
|---|---|
| Observe memoisation from server state alone | Impossible. The journal is identical whether a step body ran once or twice, so the project's headline guarantee would be the one thing the suite could not test. |
| A single pass/fail per suite | Cannot express "the app does not claim this" or "I did not check this", so a runner with gaps produces a verdict indistinguishable from a real pass. |
| Fail the build on undeclared suites | Punishes accurate self-reporting and rewards declaring suites you have not written — the opposite of what the manifest is for. |
| Require a runtime failure for off-path claims | Scores an SDK that makes the hazard unrepresentable below one that merely detects it. An implementer who noticed would weaken their SDK to pass. |
| Let apps declare arbitrary hazards as prevented | An open set is an instruction to the runner to skip assertions, written by the thing under test. |
| Test the server with the same battery | Different subject. A failure would be attributable to either side, and neither could be cleared. |

## Verification

`engine/rust/crates/stepd-conformance`, driven by
`engine/rust/crates/stepd-conformance/tests/battery.rs` and by `stepd conformance`.

| Claim | Evidence |
|---|---|
| A full pass certifies level 2 | `a_full_pass_certifies_level_two` |
| An unimplemented suite bars its level and fails the build | `an_unimplemented_suite_bars_the_level_it_belongs_to` |
| An undeclared suite bars certification but not the build | `an_undeclared_suite_bars_certification_but_not_the_build` |
| A suite with no cases is not a pass | `a_suite_with_no_cases_is_not_a_pass` |
| Prevention by construction certifies but prints differently | `a_hazard_prevented_by_construction_certifies_but_prints_differently` |
| The runner has no unimplemented suites left, by its own data | `every_suite_in_the_protocol_is_driven_by_this_runner` |
| Every declared suite actually runs a case | `every_declared_suite_actually_ran_a_case` |
| The reference app reaches level 2 | `the_reference_app_passes_every_case_the_runner_can_drive` |
| The conformance manifest schema rejects unknown suites and hazards | `spec/validate.py`, four negative cases |
| An app in another process can reach the callback-dependent suites | `an_app_configured_over_http_reaches_the_suites_that_need_a_callback` |
| …and that test is not vacuous | `without_any_callback_the_suites_that_need_one_fail` |
| A failed configuration stops the run and names itself | `a_configure_url_that_does_not_answer_stops_the_run` |
| A fixed API bind is honoured | `a_fixed_api_bind_is_honoured` |

### What §12.1 left out (found 2026-08-25)

The surface this ADR fixed was incomplete, and the omission had the same shape as
the defects below: specified, plumbed, and unreachable by one line.

§12.1 said what an app must *expose*. It never said how an app learns what it
must *call* — and two suites need that. `blobs` reserves against
`POST /v1/blobs:reserve` and `truncation` pages `GET /v1/runs/{id}/steps`, both
bearer-authenticated. The runner binds its API to an ephemeral port and mints the
token itself, so neither value exists before the app under test is already
running: whoever launched the app could not have supplied them, which is what
`Options::on_ready`'s own comment assumed they had.

The consequence was invisible because the only app anyone ran was the bundled
one, which the battery starts in-process and configures through a closure. For
every other app — which is to say every app not written in Rust — two of the
nineteen suites could not pass, whatever its SDK did. An honest implementation
would have declared them, failed them, and had no way to tell whether the fault
was its own.

`POST /_conformance/configure` closes it, and `stepd conformance
--app-configure-url` drives it. The reference app serves the route despite not
needing it, because an endpoint no implementation has ever served is a
specification nobody has tested.

The general finding, which is §7 material: **a contract that says only what one
side must expose is half a contract.** This one was written from the runner's
point of view — what it needs to observe — and the direction it forgot was the
one where the app is the caller.

### What the suite found

Four defects, all in the shape this project keeps finding: a feature specified,
plumbed end to end, and unreachable by one line. The first three came from its
first run; the fourth from closing its last gap.

1. **The cancellation compensation attempt never happened.** Protocol §7.4
   promises a cancelled run one more dispatch so the handler can undo what it
   did. The wire type carried `cancelling`, the SDK read it, the store computed
   it — and `cancel_run` deleted the queue row, so the run was never dispatched
   again and the flag could never be true. Nothing errored; the refund simply did
   not happen, and the run showed `cancelled`, which is exactly what it looks
   like when it worked. Fixed in migration 010; recorded in ADR-022.

2. **`ctx.attempt()` did not exist.** Protocol §4 puts the attempt number on
   every request, the SDK received it and used it internally as a pass token, and
   no handler could read it. "Log which retry this is" and "escalate on the last
   attempt" were unavailable for no reason.

3. **`StepError::coded` was always retryable and there was no fatal
   equivalent.** The one case where a machine-readable code is most useful — a
   permanent, classifiable failure — could not have one, so callers reached for
   `coded` to get the code and silently got a retry loop against a condition that
   would never change. `fatal_coded` added.

4. **Nothing kept a blob's bytes alive.** Wiring the endpoints so the `blobs`
   suite could run meant asking what stopped the collector deleting what an app
   had just uploaded. `blob_refs`, `add_ref` and `blob_ids` all existed, were all
   tested, and nothing called any of them — so every `$blob` in a live run's
   journal was collectable the moment it was committed. Fixed in migration 011;
   recorded as gap A13.

None of the four would have been found by a test written against the
implementation. They were found by writing down what the protocol promises and
then asking whether anything did it.
