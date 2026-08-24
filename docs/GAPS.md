# stepd — Gap Register

| | |
|---|---|
| Version | 1.0 |
| Date | 2026-08-22 |
| Status | Every gap below carries a resolution. Work that is still open is named in two places: inside the resolution cells — A15's "Still open", D7's threat model and disclosure process — and in the *Artifacts still to produce* table, which still lists the threat model, the risk-zone ownership map, CONTRIBUTING/SECURITY.md/CoC, the absent `paths:` filter and the unrehearsed runbooks. This file is a snapshot, statuses last re-checked 2026-08-24; the [issue tracker](https://github.com/swiftugandan/stepd/issues) is the live list and wins where the two disagree |

Every gap identified in the implementation readiness review, with where it is now resolved
and what remains genuinely open. Severity is the cost of discovering it late:
**S1** silent corruption · **S2** outage or data loss · **S3** operational pain · **S4** adoption drag.

---

## A. Correctness semantics

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| A1 | Signal arrives before the run reaches `wait_event` — event lost, run waits forever | S1 | Durable per-run **inbox**; `wait_event` checks it in the commit transaction; `since: run_start` default opens the window at run start. FIFO consumption, one entry per wait, sender-deduplicated, bounded with an overflow metric. | Protocol §7.6; `run_inbox` table; conformance `early_signal` |
| A2 | Occurrence counters race under concurrent steps — hashes differ between attempts, completed work silently re-executes | S1 | Occurrence assigned in **program order at the call site**, sequential pass only. Prototyping established the sharper rule: the claim must happen when the step fn is *called*, not when its future is *polled* — which makes `join!` safe by construction rather than merely detectable. Layered defence: `Ctx` is `!Send`/`!Sync` so cross-task claims fail to compile; structured `ctx.join` assigns hashes up front and rejects duplicate ids; a pass-token check catches the residue. Verified: 1 600 concurrent off-path claims, all rejected. | Protocol §6.1; `SDK-DESIGN-rust.md` §2; conformance `determinism` (level-1 gate) |
| A3 | Partial failure inside a parallel batch undefined | S2 | One non-configurable rule: every op runs to a terminal state, no op cancels a sibling, every outcome is recorded. The `all`/`any` join policies were removed rather than implemented — both are defined by cancelling siblings, and eager claiming means every body has already run when the batch arrives (ADR-023). Fixing the rule meant fixing the engine: a member that raised left no journal row, and its successful siblings were discarded with it. | Protocol §5.2.1–5.2.2; ADR-023; conformance `parallel` |
| A4 | Cascade through the invoke tree unspecified (timeouts, cancellation, detached children, grandchildren) | S2 | Full propagation table; depth limit 10, fan-out 1 000, cycle rejection on same-key ancestors; cascade is itself durable and resumable. | Protocol §7.5; conformance `cascade` |
| A5 | Cron misfires, catch-up, overlap, DST | S3 | `catchup` (`one`/`skip`/`all`), `catchup_limit`, `misfire_window`; singleton skip with metric; spring-forward skipped, fall-back fires once; IANA tzdata; database time only. **Implemented.** Building it found three further gaps the specification had not reached: the overlap key had to be a literal (`run_key`) because a cron fire has no event to evaluate `key_expr` against; claiming had to be namespace-scoped, or one tenant's backlog silently starves every other tenant's schedules (A9); and an unplannable schedule has to be *paused*, since a due row is re-claimed every sweep and "log and continue" is a hot loop (A10). | Protocol §3.1; ADR-016; `migrations/0009_cron.sql`; conformance `cron` |
| A9 | A namespace-blind claim ordered by due time is won by whoever is furthest behind — one tenant's overdue schedules fill every sweep and every other tenant's stop firing, with nothing failing anywhere | S3 | **Found by two tests interfering, not by review — and it is the dispatcher's A-tier defect arriving a second time by the same route.** Claiming is namespace-scoped with a rotation cursor, mirroring `claim_runs_ns`. Structural invariant 19 asserts the filter next to invariant 15, which asserts the dispatcher's. | `claim_due_cron`, `cron_active_namespaces`; `test_invariants.sql` |
| A10 | A schedule whose expression or zone no longer resolves stays due forever, so the sweep re-claims it every pass | S2 | Paused, with the reason in `cron_schedules.last_error`, a `cron_unschedulable` counter, a **critical** `stepd doctor` finding and a `GET /v1/schedules` field. `POST /v1/schedules/{id}/resume` re-parses before clearing the flag. | `stepd-core::cron::plan`; `pause_cron_schedule` |
| A6 | `continue_as_new` treated as optional — unbounded run state for long loops | S2 | Promoted to a v1 op. Successor keeps key and lineage, starts with empty state, no run interleaves on the key; chain length capped. | Protocol §5.1; `runs.lineage_id`, `chain_position` |
| A8 | `continue_as_new` orphaned in-flight children — the cascade rules covered cancellation and failure but not continuation, so a child's result could be delivered into a discarded journal | S2 | **Found by simulation (P8), not by review.** The op is now rejected with `continue_as_new_with_live_children` when a non-detached child is live; detached children are unaffected. | Protocol §5.1, §7.5; `reference/simulation.py` |
| A11 | The cancellation compensation attempt was specified, plumbed end to end, and never dispatched — `cancel_run` deleted the queue row, so `run.cancelling` could never be true and a cancelled workflow's undo silently did not happen | S1 | **Found by the conformance suite on its first run**, which was the first thing to ask whether the path executed. A durable `runs.compensating` flag; the phase queued when the function declares `on_cancel`; the business key held throughout, because releasing it lets the next run start while this one is still issuing refunds; terminal state `cancelled` however the path ends. | Protocol §7.4; ADR-022; `migrations/0010_compensation.sql`; structural invariants 21–23 |
| A12 | Protocol §12 listed nineteen conformance suites and never said what an implementation must expose, so "a third party can implement this" was unverifiable — and memoisation is untestable from server state, since the journal after one execution of a step body and after two is identical | S2 | The app under test exposes a conformance manifest, an effect log reporting what it actually executed, and a reset endpoint. Twenty-three named functions. A runner may not certify a level containing a suite the app did not declare or the runner did not implement, and reports the two differently. | Protocol §12.1–12.4; ADR-021; `stepd-conformance` |
| A13 | `blob_refs`, `add_ref` and `blob_ids` all existed and nothing called any of them, so the collector was free to delete the bytes behind every `$blob` in a live run's journal from the moment they were committed — and the run would fail on its next replay, hours after the collection that caused it | S1 | **Found by wiring the blob endpoints and asking what kept the bytes alive.** A trigger records references in the same transaction as the row that carries them, so any path that records a step is covered, including ones that do not exist yet. The walk is recursive: a blob nested inside an array inside an object is the ordinary payload shape, and a top-level-only walk loses it silently. | Protocol §8.3.4; `migrations/0011_blob_refs.sql`; structural invariants 24–25 |
| A14 | BR-19 requires bulk data never to pass through the orchestrator, and the only blob backend that shipped could not presign — so §8.3.2's compatibility relay applied to every managed-blob transfer and control-plane throughput was a function of payload size after all | S3 | A `BlobBackend` seam separates the Postgres-resident index (the row, per-namespace dedupe, `blob_refs` recorded by trigger, collection) from bytes and URL minting, and raw transfer moved to a separate `RelayBytes` trait so a backend that presigns is *unable* to answer it rather than merely not asked to. `stepd-blobs-s3` presigns a `PutObject` with `x-amz-checksum-sha256` and `content-length` bound into the SigV4 signature, so the object store rejects mismatched bytes itself and the server verifies by `HeadObject` — it issues only HEAD and DELETE against the store and never transfers an object. The relay route mounts only where `can_presign()` is false. **The evidence is local only:** the end-to-end test and `stepd-blobs-s3`'s live suite both skip without `STEPD_TEST_S3_*`, and no lane in `.github/workflows/ci.yml` sets it, so BR-19 on this path is proven by tests that pass locally and by nothing that runs automatically. Which object stores actually enforce the signed checksum is recorded rather than assumed. | BR-19; ADR-010; `stepd-blobs-s3`; `Server::router`; `docs/blob-backends.md` |
| A15 | On a backend that presigns, nothing verified a `$blob` at all: `commit_blob`'s only caller was the relay endpoint, which such a backend does not mount — so the row stayed `reserved`, the reservation sweep deleted the bytes regardless of `blob_refs`, and a live run failed on a later attempt hours after the collection that caused it | S1 | **Found by asking what commits a blob when the bytes never reach the server** — A13's question one layer up, and the same silent shape: the reference read back perfectly well, because read URLs are minted without consulting blob state. `Dispatcher::commit` now verifies every `$blob` an envelope's ops and emitted events carry, immediately before `store.commit`; a mismatch fails the run non-retryably and records no ops, an unreachable store backs off instead. **Still open:** §8.3.2's verify obligation binds every path a `$blob` reaches the server by, and only the envelope path enforces it — one in an ingested event or a `resolve-wait` payload still reaches no `commit_blob`. (§8.3.4, *reference* maintenance, is a different clause and is covered everywhere, by `runs_record_blob_refs`.) Also: `commit_blob` performs no namespace check; and one narrow regression that would move digest verification back onto the control plane escapes every test — an `S3Backend::stored` that keeps its `HeadObject` and keeps erroring for a checksumless object, and merely *adds* a `GetObject` beside them, which `a_committed_object_reports_its_digest_without_transferring_it` cannot see because it measures the answer, not the transfer. (Both neighbouring shapes are caught: *replacing* the `HeadObject` with a GET-and-hash, and `stored` → `None`, each fail `an_object_the_store_reports_no_checksum_for_is_an_error_not_a_fallback`, and `None` additionally fails the commit through `get_bytes` erroring on the `relay: None` store `Server::build` gives S3.) | Protocol §8.3.2; `Dispatcher::verify_blobs`; `migrations/0011_blob_refs.sql`; `stepd-core/tests/engine.rs` |
| A7 | Rust future dropped mid-step leaves indeterminate external effect | S1 | SDK MUST NOT drop a running step future to meet a deadline; abandon the response instead. Server treats no-response as `unknown`, not failed, and retries. Attempt timeout must exceed slowest step; SDK warns otherwise. | Protocol §7.1.1; conformance `abandonment` |

## B. Load and failure containment

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| B1 | Thundering herd onto a recovering app | S2 | Per-app dispatch ceiling, circuit breaker with half-open probing, gradual recovery ramp. Implemented and tested; a first version that closed only on observed successes stayed half-open forever once traffic stopped, so closure is now success-or-quiet-period (F-LP-2b). | F-LP-1…3, F-LP-2b; `app_health`; `reference/dispatcher.py` |
| B2 | Timer fan-out — millions of runs waking at midnight | S2 | Jitter window (default ±60 s, 0 for exact-time needs). | F-LP-4 |
| B3 | Namespace starvation; priority without fairness | S3 | Weighted fair queuing across namespaces/functions; priority operates within a fairness class. | F-LP-5 |
| B4 | Poison pills consume dispatch capacity forever | S3 | Quarantine on repeated identical failure signature; DLQ view with bulk retry/cancel/export. | F-LP-6…7 |
| B5 | Ingest accepts work the system cannot drain | S3 | Backpressure `429` with `Retry-After` above namespace queue thresholds; per-namespace quotas. | F-LP-8, F-LP-10 |
| B6 | Runaway runs (steps, fan-out, chain) unbounded | S2 | Hard limits with specific non-retryable errors. | F-LP-9; protocol §5.1, §7.5 |

## C. Data layer, recovery, upgrades

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| C1 | pgbouncer transaction mode breaks session-scoped advisory locks | S2 | Engine uses only row-level `FOR UPDATE SKIP LOCKED` and transaction-scoped locks; no session state assumed; a CI lane runs the whole suite through pgbouncer. | F-DL-1 |
| C2 | Autovacuum and partition maintenance on hot tables | S3 | Tuned settings shipped in migrations; time-partitioned `events`/`runs` with automated create/detach; retention as batched rate-limited deletes or partition drops. | F-DL-2…3 |
| C3 | **PITR restore rewinds the journal — committed steps un-commit and side effects re-execute** | S2 | Documented prominently as a semantic limit of durable execution; restored runs marked `restored_at` and visible in the console; bulk-cancel offered. Must not be discovered during an incident. | F-DL-5 |
| C4 | Upgrading stepd with in-flight runs | S2 | Expand/contract migrations only; N-1/N interoperate; mixed-version cluster supported; upgrade test in the release gate. | F-DL-6; §10.2 |
| C5 | No operational self-diagnosis | S3 | `stepd doctor` for pooler misconfiguration, clock skew, partition lag, orphaned blobs, stuck leases, unreachable apps. | F-DL-7 |
| C6 | Replica clock skew corrupting scheduling | S2 | All scheduling from database time; skewed replicas refuse to claim work and alert. | F-DL-8 |
| C7 | Backup/restore untested | S2 | Documented strategy plus a rehearsed restore runbook; RTO/RPO stated. | F-DL-4; NFR |

## D. Security and privacy

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| D1 | SSRF — server fetches app-supplied URLs, reaching cloud metadata | S2 | Egress policy: deny link-local/metadata/loopback/private unless allowlisted; resolve-then-connect against rebinding; zero redirects; size cap; per-namespace allowlist required for multi-tenant. | Protocol §9; F-SEC-2 |
| D2 | Namespace authorisation deferred to v1.1 although it is a security boundary | S2 | Promoted to v1. Tokens carry role × namespace; isolation enforced at the query layer, not by response filtering. | F-SEC-1; `tokens` table |
| D3 | Signature replay within the 300 s window | S3 | `stepd-nonce` covered by the MAC plus a replay cache, both directions. | Protocol §9; conformance `signature` |
| D4 | Console XSS from arbitrary payload JSON and `$ref` URIs | S2 | Escaped-by-default rendering; URI scheme allowlist; `javascript:`/`data:`/`file:` never linked; strict CSP, no inline scripts; XSS corpus in the security test layer. | F-SEC-4 |
| D5 | No GDPR/subject erasure path | S2 | `subject_expr` per function, `subject_index`, asynchronous resumable erasure API across events, inputs, step results and blobs, recorded in the audit log. | F-SEC-5; `subject_index`, `erasures` |
| D6 | Payload redaction and encryption unspecified | S3 | Namespace redaction policy applied before persistence; pluggable KMS hook with per-namespace rotatable keys. | F-SEC-6…7 |
| D7 | No threat model or disclosure process | S3 | Both required deliverables before v1. | F-SEC-10 |

## E. Developer experience

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| E1 | **No way to unit-test a workflow** | S4 | Test framework in the SDK: in-process mock server, op-sequence assertions, injected results/failures/events, virtual clock, "did not re-execute" assertions. Treated as adoption-critical, not a nicety. | F-DX-1 |
| E2 | Push model breaks laptop development behind NAT | S4 | `stepd dev --tunnel`; purely local development needs no tunnel. | F-DX-3 |
| E3 | "Why did my step re-run?" has no answer in the UI | S4 | Orphaned-hash diagnostics surfaced per run with previous/current ids; replay debugger reports first divergence. | F-DX-2, F-DX-4 |
| E4 | Hazards only discoverable in production | S4 | `stepd lint`: duplicate ids in a parallel group, side effects outside `ctx.run`, attempt timeout shorter than step timeout, unbounded loops without `continue_as_new`. | F-DX-6 |
| E5 | Error messages without context | S4 | Errors name function, step, attempt, rule violated, and link to the spec section. | F-DX-5 |
| E6 | Documentation unscoped | S4 | Quickstart, concepts, migration guides from Temporal/Inngest, ops runbook, reference app. | F-DX-8 |

## F. Verification

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| F1 | No technique capable of finding interleaving bugs | S1 | **Deterministic simulation testing** built and running: 11 fault types combined by swarm testing, nine properties, positive controls, and a coverage tool that reports never-exercised paths. Found a real specification gap (A8) on its first run. | PRD §10.3; `reference/simulation.py` |
| F2 | Test strategy absent | S2 | Eleven layers specified and mapped to risk zones and tiers. | PRD §10.4 |
| F3 | No release gates | S2 | Risk-differentiated gates: R1 findings block release absolutely; R4/R5 may ship as documented known issues. | PRD §10.7 |
| F4 | Uniform testing effort — everything tested equally, so the suite is both slow and thin where it matters | S2 | Five risk zones scored by severity × likelihood × **detectability**; depth on silent-corruption paths, breadth on safety, speed elsewhere. Seed budget weighted 70/30; mutation gating only on R1 code. | PRD §10.1, §10.3 |
| F5 | No feedback-latency discipline — a slow suite gets bypassed, which is worse than a smaller one | S3 | Five tiers with **enforced** budgets (10 s / 5 min / 30 min / nightly / release) and diff-driven selective execution keyed to declared risk-zone ownership. A suite that outgrows its tier is refactored, not tolerated. | PRD §10.2 |
| F6 | No stated limits — untested areas discovered by accident, and seed budgets growing without payoff | S3 | Explicit not-tested list; coverage-guided seed selection with a stop rule; seeds-per-finding tracked as an efficiency metric. | PRD §10.5, §10.3 |
| F7 | Flaky tests train the team to ignore failures | S2 | Zero flakiness budget: same-day quarantine, fix or delete. Simulation makes this tractable — a failure carries a seed, so "flaky" usually means a real rare interleaving. | PRD §10.6 |

## G. Planning

| # | Gap | Sev | Resolution | Where |
|---|---|---|---|---|
| G1 | Plan expressed as a calendar, when the binding constraints are verification compute, external review and production exposure — none of which track build effort | S3 | Milestones re-expressed as dependency-ordered phases with condition gates. Elapsed time is set by the soak, the seed budget, independent spec review and design-partner exposure, not by implementation. | PRD §11.1; BRD §10 |
| G2 | v1 surface larger than it needs to be | S3 | Irreducible core (protocol, engine, store, Rust SDK, simulation harness, read-only console) versus a named deferral list, each behind an existing seam. Justified by correctness surface and API commitment, not capacity. | PRD §11.3 |

---

## Artifacts still to produce

Statuses re-checked against the tree on 2026-08-24. The rows below were written
against a Python prototype at `engine-schema/`, a path that exists neither in this
repository nor anywhere in its history; every file they cited by it — `dispatcher.py`,
`api.py`, `console.html`, `simulation.py`, `openapi.json` — is present under
`reference/`. The citations were repointed there rather than dropped, since what they
record having been tested is still what those files test. Where the Rust tree has since
superseded the prototype, the row says so.

| Artifact | Status | Blocking |
|---|---|---|
| Dispatch loop (claim → deliver → commit, load protection) | **Implemented and tested end to end** (`reference/dispatcher.py`, 19 assertions, full workflow completes) | M1 |
| OpenAPI 3.1 management/read API | **Implemented and tested** (`reference/api.py`, 30 assertions, 11 endpoints, `reference/openapi.json`); adversarial namespace-isolation tests included | M1 |
| Postgres DDL + migrations | **Written, applied and tested against live PostgreSQL 16** (`reference/`, and since superseded by `rust/migrations/`): 25 behavioural + 6 structural assertions, 12-worker concurrency stress, 120 randomised signal races, forced-interleaving test | M1 |
| Rust SDK design doc (short-circuit control flow — the subtlest code in the project) | **Written, and the SDK built from it**: `docs/SDK-DESIGN-rust.md` and `docs/sdk-prototype/` (property and adversarial tests, an `eager_claim` example), then `stepd-sdk-core` (32 tests) and `stepd-sdk` (30) | M1 |
| Simulation harness design | **Built**, twice: `reference/simulation.py` against the model and `rust/crates/stepd-store-postgres/tests/simulation.rs` (5 tests) against the real engine; `reference/coverage_check.py` reports never-exercised paths | M0.5 |
| Risk-zone ownership map (drives selective test execution) | Specified, not built | M0.5 |
| Tiered CI configuration with enforced time budgets | **Tiers built, budgets not enforced.** `.github/workflows/ci.yml` has six lanes across tiers 1–4; three name a time budget (`≤10s`, `≤5min`, `≤30min`), one names a cadence (`nightly`), and two — `tier 1 · crate boundaries` and `tier 3 · through pgbouncer` — name neither. Nothing holds a lane to its budget: the `timeout-minutes` guards are 10/10/20/30/40/360, each far above the budget beside it, so they catch a hang and not a lane that has outgrown its tier. PRD §10.2's diff-driven selective execution is absent too — no lane carries a `paths:` filter | M0.5 |
| ADRs 001–023 | **Written** — 23 files under `docs/adr/` | M0.5 |
| Console IA and wireframes | **Built and tested** (`reference/console.html`, 17 assertions incl. content-safety audit and the operator flow; the shipped console is `rust/crates/stepd-server/assets/console.html`) | M3 |
| Threat model | Not started | M4 |
| Operations runbooks (incl. restore hazard) | **Written**: `restore-hazard`, `stuck-run`, `backlog`, `poison-pill`, `upgrade` under `docs/runbooks/`. None has been rehearsed | M4 |
| Benchmark harness | **Built for the reference implementation** (`reference/bench.py`, against the PRD's ≥1000 commits/s NFR). Nothing benchmarks the Rust engine — no `benches/` anywhere in `rust/` | M4 |
| Licence, CONTRIBUTING, SECURITY.md, CoC | Licence **applied** (Apache-2.0, `LICENSE` at the root, commit `0d01e75`). The other three do not exist | Public release |

## Remaining open questions

Ten, listed in PRD §13. None are correctness-blocking; all are product or deployment
choices that can be made during M1–M3 without rework, because each sits behind a seam.

## Standing risks that cannot be designed away

0. **Correctness can rest on undocumented accidents.** Found in practice: the lost-signal
   race was closed only by a foreign key's incidental row lock. Fixed by making the
   serialization explicit and adding a structural test, but the general lesson stands —
   passing tests do not tell you *why* they pass, and a system can be correct for a reason
   nobody wrote down. Structural invariant tests are the countermeasure.
1. **Durable execution has a restore paradox** (C3). Any point-in-time restore re-executes
   side effects. Mitigated by documentation and markers, not eliminated.
2. **At-least-once step execution is the contract** (A7). Non-idempotent steps against
   providers with no idempotency mechanism will occasionally double-execute. Mitigated by
   `ctx.idempotency_key()` and reserve/confirm guidance.
3. **Step-id renaming orphans results.** Safe versioning depends on developer discipline;
   `stepd lint` and diagnostics reduce but do not remove the hazard.
4. **The market is well funded and moving.** Openness, Rust and Postgres-only operations are
   the differentiators; they are worth little if correctness is not demonstrably better.
5. **Trust accrues on its own schedule.** No amount of implementation speed shortens the
   period of production exposure needed before a team will route payments through this. The
   artifacts that buy trust early — a spec someone else can implement, a simulation suite
   with stated properties, an honest account of the restore hazard — are therefore worth
   more than additional features.
