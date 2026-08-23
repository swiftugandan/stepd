# Business Requirements Document
## Durable Workflow Engine (working name: **stepd**)

| | |
|---|---|
| Version | 0.5 (draft) |
| Date | 2026-08-22 |
| Status | For review |
| Owner | Product / Engineering lead |
| Changes in 0.2 | Named the product `stepd`; added modularity and dev/prod-parity requirements (BR-15…BR-18); replaced SQLite-in-production assumption; split deploy config from code-defined config; added SDK protocol as a shipped artefact |
| Changes in 0.3 | Added payload/media requirements (BR-19, BR-20): bulk data never traverses the control plane; large media stays application-owned |
| Changes in 0.4 | Closed the implementation-readiness gap register (`GAPS.md`). Added BR-21…BR-26 covering silent-corruption resistance, blast-radius containment, recoverability, tenant isolation, privacy rights and testability. Recut v1 scope for realistic delivery capacity |
| Changes in 0.5 | Added BR-27: quality assurance effort allocated by risk, with enforced feedback-time budgets |

---

## 1. Executive summary

Long-running business processes — order fulfilment, onboarding, approvals, billing cycles, claims — are still built from queues, cron jobs, idempotency keys and hand-rolled saga code. Durable execution platforms (Temporal, Restate, Inngest, DBOS, Cloudflare Workflows) have proven that a "journal + replay" runtime removes most of that boilerplate, but each is either operationally heavy, proprietary in its wire format, tied to one cloud, or restrictively licensed.

**stepd** is an open-standards, Rust-based durable workflow engine: a single binary (server + web UI) backed by Postgres, with a published SDK protocol so any language can host workflow code, and a modern web console for operating runs. It targets teams who want Temporal-grade reliability for business workflows without running a cluster, and without lock-in.

## 2. Business objectives

| # | Objective | Success measure |
|---|---|---|
| O1 | Make multi-step, long-running business processes reliable by default | 99.99% of runs complete or land in a visible terminal state; zero lost runs across restarts/deploys |
| O2 | Eliminate bespoke orchestration code | Reference app implements a 10-step saga with <30% of the code of a queue-based baseline |
| O3 | Zero-to-running in minutes | New user runs first workflow locally in <10 min; in production with only Postgres in <1 day |
| O4 | No lock-in | Wire protocol, event format, API and export formats are all open standards or published specs |
| O5 | Operable by non-authors | On-call engineers can diagnose and remediate a stuck run from the UI without reading code |

## 3. Background and market context

* **Category is established.** Temporal ($5B valuation), Restate, Inngest, DBOS, Trigger.dev, Cloudflare Workflows and AWS Lambda Durable Functions all ship the same core primitive: persist completed step boundaries, resume after failure without repeating side effects.
* **Gaps observed:**
  * *Operational weight* — Temporal requires a dedicated cluster and worker fleet.
  * *Proprietary surfaces* — most vendors define their own event and protocol formats; portability between them is nil.
  * *Licensing* — Inngest (SSPL), Restate server (BSL) limit self-hosting/redistribution for some buyers.
  * *Versioning* — industry consensus that versioning long-running workflows causes more incidents than scheduling failures; few products solve it simply.
  * *Rust ecosystem* — no Rust-native, Postgres-backed engine with a first-class Rust SDK and UI.
* **Tailwinds:** AI-agent workloads (multi-step, retry-heavy, human-in-the-loop) are driving new demand for durable execution; CloudEvents, OpenTelemetry and OpenAPI are now default expectations in platform tooling.

## 4. Scope

### 4.1 In scope (v1)
* Durable execution of code-defined workflows (steps, sleeps, waits, invokes) with at-least-once step execution and exactly-once step *effects* via memoization.
* Event-driven triggering (CloudEvents), cron triggers, manual triggers.
* Keyed ordering (single in-flight run per business key) and basic flow control (concurrency, rate limit).
* Open SDK protocol over HTTP, published as a versioned specification with JSON Schemas; reference Rust SDK; protocol conformance test suite.
* Postgres persistence in all environments; `stepd dev` provisions an embedded ephemeral instance so local development exercises production code paths.
* Web console: runs, steps, events, functions, manual remediation.
* OpenAPI management/read API; OpenTelemetry tracing.
* Single-binary distribution; container image.

### 4.2 Out of scope (v1)
* Declarative DSL front-ends (Serverless Workflow, BPMN) — planned as compilers to the same IR later.
* Own replicated log / multi-region active-active.
* WASM-hosted steps, OCI container steps.
* Hosted SaaS offering, billing, multi-tenant isolation beyond namespaces.
* Visual workflow designer.

## 5. Stakeholders

| Role | Interest |
|---|---|
| Backend engineers (primary users) | Simple SDK, local dev loop, predictable semantics |
| Platform / SRE | Single binary, Postgres only, metrics, runbooks, upgrade safety |
| Product / ops staff | Console to see where a customer's process is and nudge it |
| Security / compliance | Audit trail, data retention, auth integration, licensing |
| Executive sponsor | Reduced incident rate, engineering velocity, strategic optionality |

## 6. Business requirements

| ID | Requirement | Priority |
|---|---|---|
| BR-1 | A workflow run must survive process crashes, deploys and infrastructure restarts, resuming from the last completed step | Must |
| BR-2 | A step's side effect must not be re-executed once recorded, even under retries or duplicate delivery | Must |
| BR-3 | Runs must be able to sleep or wait for external events for days to months at no compute cost | Must |
| BR-4 | At most one run may be active per business key when the function is configured as keyed | Must |
| BR-5 | Workflow code may be changed while runs are in flight without corrupting them | Must |
| BR-6 | All inbound/outbound events use CloudEvents; the SDK protocol and management API are published under an open licence | Must |
| BR-7 | Operators can view, cancel, retry, and manually resolve waits for any run from a web UI | Must |
| BR-8 | The whole system runs locally with one command and no external services | Must |
| BR-9 | Production deployment requires only the binary and a Postgres database | Must |
| BR-10 | Every run and step emits OpenTelemetry traces linkable from the UI | Should |
| BR-11 | Workflow code may be written in any language by implementing the protocol; Rust SDK is reference | Should |
| BR-12 | Data retention per namespace is configurable; payloads can be redacted/encrypted | Should |
| BR-13 | Throughput of at least 1,000 step completions/sec on a single mid-size Postgres | Should |
| BR-14 | Role-based access to the console (viewer / operator / admin) | Could (v1.1) |
| BR-15 | Local development must exercise the same database engine and code paths as production | Must |
| BR-16 | Deployment configuration (endpoints, credentials, namespace) must come from the environment, never from code-defined function definitions | Must |
| BR-17 | Storage, queue, timer, blob and expression concerns must sit behind replaceable interfaces so that no component change requires an engine change | Must |
| BR-18 | The wire protocol must be transport-agnostic, so an alternative delivery mode can be added without a protocol version bump | Should |
| BR-19 | Workflow payloads of any size — including images, documents and video — must be supported without bulk data passing through the orchestrator, and without the orchestrator's cost or capacity depending on payload size | Must |
| BR-20 | Data the organisation already stores must remain under its own control and lifecycle; the product must not require copying it into the orchestrator to use it in a workflow | Must |
| BR-21 | Failure modes that corrupt data silently must be prevented by the system, not by user discipline; each must be covered by a normative rule, a conformance test and a simulation property | Must |
| BR-22 | A failing application, a hot tenant or a repeatedly failing workflow must degrade only itself and never the wider service | Must |
| BR-23 | The service must be recoverable and upgradable without invalidating in-flight work, and any recovery action that re-executes side effects must be documented and visibly marked, never silent | Must |
| BR-24 | Tenant isolation must be a structural property enforced at the data layer from v1, not a filtering behaviour added later | Must |
| BR-25 | The organisation must be able to erase an individual's data across all stored workflow state on request | Must |
| BR-26 | Workflows must be testable by their authors without deploying infrastructure, including time-dependent behaviour | Must |
| BR-27 | Assurance effort must be allocated in proportion to risk — weighted toward failures that are severe and hard to detect — and the development feedback loop must stay fast enough that the regime is never bypassed | Must |

## 7. Constraints and assumptions

* Implementation language: Rust (server, SDK, CLI). UI: TypeScript/React, embedded in the binary.
* Architecture: engine logic is generic over storage, queue, timer, blob and expression traits; Postgres is the only implementation shipped in v1.
* Persistence: Postgres 14+ in every environment, including local development (embedded/ephemeral instance). No Redis/Kafka dependency in v1.
* Licence: Apache-2.0 for server, SDKs, protocol and UI (to be confirmed by legal).
* Execution model: push — the server calls the application's HTTP endpoint. A pull-worker mode is a possible later addition.
* Assumes applications can expose an HTTPS endpoint reachable by the server (or run the server alongside the app).

## 8. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Per-step HTTP round-trip overhead on very step-heavy workflows | Latency, cost | Protocol designed to allow a streamed/batched v2; step count guidance |
| Function-state growth for long runs | Storage, replay time | Payload offloading, state compaction, `continue_as_new` equivalent |
| Competing with funded incumbents | Adoption | Differentiate on openness, Rust, Postgres-only ops, UI quality |
| Postgres becomes the bottleneck | Scale ceiling | Partition tables by namespace/time; document scaling envelope; pluggable store trait |
| Media-heavy workloads turn the control plane into a data plane | Cost, capacity, latency | Tiered payloads with direct-to-store upload; external references for large media; control-plane purity asserted by benchmark |
| Protocol churn breaks SDKs | Trust | Semantic versioning of protocol; conformance suite; compatibility window |
| Dev/prod divergence hides defects until production | Reliability, trust | Same database engine in all environments; parity asserted in CI |
| Trait abstractions become leaky or premature | Velocity | Traits derived from one real implementation; second implementation deferred until a concrete need |
| Silent correctness bugs surface only in production, destroying trust irrecoverably | Existential | Deterministic simulation testing from the foundations phase with stated properties, weighted toward the hardest-to-detect failures; conformance gates; no release with a known silent-corruption path |
| Scope exceeds delivery capacity, producing a large untrustworthy system | Existential | v1 recut to an irreducible core with an explicit deferral list; each deferral sits behind an existing interface |
| Operators discover the point-in-time-restore side-effect hazard during an incident | Reputational | Documented as a semantic property of durable execution; restored runs marked in the console; rehearsed runbook |

## 9. Success criteria and KPIs

* **Reliability:** no lost or duplicated step effects in chaos tests (kill -9 server/app mid-step, DB failover), and zero violations of the silent-corruption properties across the release simulation budget.
* **Assurance efficiency:** escaped defects traced to the tier that should have caught them, with the fast feedback loop held inside its time budget.
* **Adoption:** internal reference app migrated; 3 design-partner teams running in production, measured from v1 availability rather than from a fixed date.
* **Time-to-first-workflow:** median under 10 minutes from download (a user-experience measure, unaffected by build velocity).
* **Ops:** P1 incidents attributable to orchestration reduced ≥50% in reference app.
* **Community:** second-language SDK (TypeScript or Python) produced by a contributor using only the spec and conformance suite.

## 10. Phasing

Phases are ordered by dependency, not calendar. Each gate is a *condition*, since the
binding constraints on this project are verification, external review and real-world
exposure rather than implementation effort.

| Phase | Contents | Gate to exit |
|---|---|---|
| P0 – Spec | Protocol specification, schemas, examples, validation harness | Schemas validate; spec reviewed by external implementers |
| P0.5 – Foundations | Crate skeleton, component interfaces, simulation harness, CI lanes | Simulation properties hold across the target seed budget |
| P1 – Core | Engine, store, SDK, dev tooling, read-only console | Chaos and simulation gates green; reference workflow runs end to end |
| P2 – Hardening | Coordination, load protection, security, retention, benchmarks, documentation | Soak, upgrade, security and performance gates green |
| P3 – Ecosystem | Second SDK by a contributor, DSL front-ends, alternative step hosting | An external SDK reaches conformance level 2 using only the spec |

**What does not compress.** Elapsed time is dominated by things that are not implementation:
the soak test takes as long as the soak; simulation coverage is bounded by compute, not
authoring speed; external spec review, usability testing and design-partner production
exposure run on other people's clocks. Plan around those, not around build effort.

## 11. Open questions

1. Push-only or also pull-worker mode in v1?
2. Licence choice (Apache-2.0 vs. MIT) and CLA policy.
3. Namespace model: single DB shared by namespaces, or DB-per-namespace?
4. Should keyed ordering be per function or allow cross-function keys (Restate-style virtual objects)?
5. Hosted offering — is it on the roadmap at all?
