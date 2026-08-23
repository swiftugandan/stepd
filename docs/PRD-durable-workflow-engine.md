# Product Requirements Document
## stepd — Durable Workflow Engine, v1

| | |
|---|---|
| Version | 0.5 (draft) |
| Date | 2026-08-22 |
| Parent | BRD-durable-workflow-engine v0.5 |
| Companion | stepd SDK Protocol v1 (`stepd-spec/PROTOCOL.md`) |
| Status | For review |

### Changes in 0.2
1. **Dev/prod parity** — SQLite removed as a supported backend; `stepd dev` runs an embedded ephemeral Postgres (§4.8, §6).
2. **Config separation** — deployment config (URL, keys, namespace) moved out of the code-defined manifest into the environment (§4.9).
3. **Log discipline** — protocol `logs` demoted to a capped diagnostic channel; real logs go to stdout/OTLP from the app (§4.9, §5).
4. **Modularity** — component traits and crate boundaries specified (§6); `signal` op added; expression engine and transport are now pluggable.
5. Protocol details aligned with the published spec: `signal` op, `prompt` block on `wait_event`, `meta` on ops, fencing semantics.

### Changes in 0.3
6. **Payload tiering** — three tiers (inline / managed blob / external reference) with a hard rule that bulk data never traverses the server (§4.10, §5, §7).
7. **Direct upload** — two-phase presigned reservation replaces uploading through the orchestrator; content-addressed deduplication by digest.
8. **Blob lifecycle** — `blob_refs` reference counting added; bytes survive until refcount zero *and* retention elapsed.
9. **Media handling** — video and large media use external references; console renders previews by `content_type`; engine never interprets payload bytes.

### Changes in 0.4 — gap closure
Closes the gap register (`GAPS.md`). Material additions:
10. **Correctness** — early-signal inbox (lost-signal race), occurrence determinism under concurrency, batch join policies, invoke-tree cascade, `continue_as_new`, abandoned-step semantics, cron misfire/DST rules. All specified normatively in the protocol.
11. **Load protection** (§4.11) — app-side circuit breaking and ramp, timer jitter, namespace fairness, poison-pill quarantine and DLQ.
12. **Data layer realities** (§4.12) — connection pooling constraints, autovacuum and partition maintenance, disaster recovery and the restore/idempotency hazard, zero-downtime upgrades.
13. **Security** (§4.13) — namespace authorisation promoted to v1, SSRF egress policy, signature replay defence, console content safety, subject erasure for GDPR.
14. **Developer experience** (§4.14) — workflow test framework, dev tunnel, replay diagnostics.
15. **Verification** (§10) — deterministic simulation testing added as an M0.5 deliverable.
16. **Scope realism** (§11) — v1 recut by dependency and correctness surface; explicit deferral list.

### Changes in 0.5
17. **Risk-based QA regime** (§10) — five risk zones scored by severity × likelihood × *detectability*; five test tiers with enforced feedback-time budgets; diff-driven selective execution; simulation seed budget weighted 70/30 toward silent-corruption scenarios with swarm testing, shrinking, coverage-guided selection and a stop rule; mutation gating on R1 code only; an explicit not-tested list; zero flakiness budget; risk-differentiated release gates.

---

## 1. Product overview

stepd is a single-binary durable execution server plus SDKs and a web console. Applications register **functions** (workflow handlers) that react to **events**, crons or direct calls. The server drives each **run** step-by-step by calling the application over an open HTTP protocol, persisting each step result so the run resumes from the last checkpoint after any failure.

Execution model (chosen in design phase): Inngest-style *one-new-step-per-invocation with step-ID memoization*, plus Restate-style *keyed single-writer ordering*, on Postgres.

The **app** is a stateless 12-factor process: it holds no run state between attempts, receives all state in the request, and is disposable. The **server** is deliberately stateful — it is a backing service, and the 12 factors apply to the applications that attach to it, not to it.

## 2. Personas

* **Dana, backend engineer** — writes workflows in Rust; wants a tiny API, a fast local loop, and no surprises about what re-runs.
* **Omar, SRE** — deploys and upgrades stepd; needs metrics, a scaling envelope, and runbooks.
* **Priya, support/ops** — never reads code; needs to find "order 4711", see why it's stuck, and retry or cancel it.
* **Lin, platform engineer** — wants to write a Python SDK against the spec.

## 3. Core concepts (glossary)

| Term | Definition |
|---|---|
| Namespace | Isolation boundary for functions, events, runs, keys and retention. |
| App | A deployed service that hosts functions; identified by a URL and a signing key. |
| Function | A named, versioned handler with triggers and flow-control config. |
| Trigger | Event match expression, cron schedule, or `invoke` only. |
| Event | A CloudEvents 1.0 envelope; `type`, `source`, `id`, `time`, `data`, plus stepd extensions (`stepdkey`, `stepdidempotency`). |
| Run | One execution of a function for one trigger. Has a status and a state map. |
| Step | A unit of work inside a run, identified by `hash(step_id, occurrence)`. Result is memoized. |
| Op | A control instruction the SDK returns to the server: `step`, `sleep`, `wait_event`, `invoke`, `signal`, `done`, `error`. Events are emitted via the `emit` array alongside any op. |
| Key | Optional business key (e.g. `order:4711`). At most one active run per `(function, key)`. |
| Attempt | One HTTP call from server to app for a run. |
| Inbox | Per-run durable buffer of events directed at that run, consumed by `wait_event`. Prevents the lost-signal race. |
| Lineage | The chain of runs linked by `continue_as_new`; shares a `lineage_id` and the key. |
| Quarantine | State for a run failing identically and repeatedly; stops consuming dispatch capacity pending human action. |
| Detached child | A run started by `invoke` with `detach: true`; independent of the parent's lifecycle in both directions. |

## 4. Functional requirements

### 4.1 Events
| ID | Requirement |
|---|---|
| F-EV-1 | `POST /v1/events` accepts a single CloudEvent or batch (JSON, structured mode). Returns event ids. |
| F-EV-2 | Events are persisted (append-only) before acknowledgement. |
| F-EV-3 | Idempotent ingest via `stepdidempotency` extension or `id`+`source` uniqueness within a configurable window. The dedupe index MUST live outside the time-partitioned `events` table, because PostgreSQL requires a unique index on a partitioned table to include the partition key, which makes a global uniqueness constraint inexpressible there. |
| F-EV-4 | Events match functions by `type` exact match plus optional CEL expression over `data`. |
| F-EV-5 | Events are retained per namespace retention policy and are replayable into functions from UI/API. |

### 4.2 Functions and apps
| ID | Requirement |
|---|---|
| F-FN-1 | Apps register by `PUT /v1/apps` or by the server calling the app's `/.well-known/stepd` discovery endpoint; registration payload is validated against a published JSON Schema. |
| F-FN-2 | Function config: `id`, `version`, `triggers[]`, `key_expr` (CEL), `concurrency`, `rate_limit`, `retries`, `timeouts`, `cancel_on[]`. |
| F-FN-3 | Re-registering with a new version does not affect in-flight runs; runs always memoize by step id so code changes are tolerated. |
| F-FN-4 | Functions can be paused/resumed (stop dispatch, keep accepting events). |

### 4.3 Execution
| ID | Requirement |
|---|---|
| F-EX-1 | For each attempt the server POSTs to the app: run id, trigger event(s), current step state, and the op expected; the app replays memoized steps and returns exactly one new op (or `done`/`error`). |
| F-EX-2 | `step` op: server stores `{hash → result}` atomically with scheduling the next attempt. |
| F-EX-3 | `sleep` op: durable timer; next attempt scheduled at wake time; no compute consumed while sleeping. |
| F-EX-4 | `wait_event` op: run is suspended until an event matches `type` + CEL expression (may reference run state) or timeout elapses. |
| F-EX-5 | `invoke` op: start a child run and suspend until it completes; result memoized as a step. |
| F-EX-6 | `emit` array: events published durably in the same transaction as the ops (outbox semantics). |
| F-EX-6b | `signal` op: deliver an event directly to another run by id or key, without round-tripping through ingest. |
| F-EX-7 | Parallel steps: SDK may return multiple `step` ops in one response; server runs them concurrently and memoizes each. |
| F-EX-8 | Step failure retries with exponential backoff up to `retries`; exhausted → run `failed` with `on_failure` hook optional. |
| F-EX-9 | Non-retryable error class halts immediately. |
| F-EX-10 | Keyed functions: server guarantees one active run per key; others queue FIFO. |
| F-EX-11 | Concurrency and rate limits enforced at dispatch; excess queued, visible in UI. |
| F-EX-12 | Cancellation: API/UI/`cancel_on` event → run `cancelled`; an optional `on_cancel` step is invoked. |
| F-EX-13 | Run-level timeout and per-attempt timeout configurable. |
| F-EX-14 | Step outputs are tiered by size: inline JSON below the threshold, managed `$blob` reference in the mid range, application-owned `$ref` above the ceiling (§4.10). |
| F-EX-15 | Each attempt carries a fencing token; responses bearing a stale token are rejected with `409` and their ops discarded. |
| F-EX-16 | `wait_event` may carry an optional `prompt` block (title, detail, actions) that renders as a pending decision in the console. |
| F-EX-17 | Ops may carry `meta` (model, tool, token counts, cost) for display and aggregation; the engine treats it as opaque. |

### 4.4 SDK (Rust reference)
| ID | Requirement |
|---|---|
| F-SDK-1 | `stepd::function("id").on_event("order.created").key("data.order_id").run(|ctx, ev| async { ... })` |
| F-SDK-2 | `ctx.run("charge", || async {...})` returns memoized value or short-circuits the handler with a `step` op. |
| F-SDK-3 | `ctx.sleep("cooldown", Duration)`, `ctx.sleep_until`, `ctx.wait_event("approval", "order.approved", expr, timeout)`, `ctx.invoke("fn", input)`, `ctx.signal(target, ev)`, `ctx.emit(ev)` (queued into the `emit` array, committed with the next op). |
| F-SDK-4 | Step ids inside loops are disambiguated by occurrence counter automatically. |
| F-SDK-5 | Handler mounts as an `axum` router and as an AWS Lambda handler. |
| F-SDK-6 | Request signature verification (HMAC-SHA256 over body + timestamp). |
| F-SDK-7 | Structured `ctx.logger` that suppresses duplicate logs during replay. |
| F-SDK-8 | Strongly typed event payloads via `serde`; optional JSON Schema generation via `schemars`. |
| F-SDK-9 | Manifest assembled at start-up from code-defined functions plus environment bindings (F-CFG-4); the SDK refuses to start if a function definition contains a URL or credential. |
| F-SDK-10 | `ctx.idempotency_key()` returns a stable `run_id + step_hash` value for use with non-idempotent external APIs. |
| F-SDK-11 | Depends only on `stepd-proto`; no database, queue or server dependency. |

### 4.5 Protocol (open spec)
| ID | Requirement |
|---|---|
| F-PR-1 | Spec document with versioned message schemas (JSON Schema), published in repo under Apache-2.0. |
| F-PR-2 | Protocol version negotiated via header; server supports N and N-1. |
| F-PR-3 | Conformance test suite runnable against any SDK implementation (`stepd conformance --app http://…`). |
| F-PR-4 | All timestamps RFC 3339; all ids UUIDv7; all payloads JSON. |

### 4.6 Web console
| ID | Requirement |
|---|---|
| F-UI-1 | Runs list: filter by namespace, function, status, key, date range; free-text search on run id / event id / key. |
| F-UI-2 | Run detail: status, trigger event, timeline of steps (name, attempts, duration, input/output JSON viewer, error), sleeps and waits with expected wake/expiry, child runs, OTel trace link. |
| F-UI-3 | Live updates via SSE on run detail and runs list. |
| F-UI-4 | Actions: cancel run, retry from failed step, retry whole run, resolve a `wait_event` with a synthetic event, replay event. Each action is an audited command through the public API. |
| F-UI-5 | Events explorer: list/search CloudEvents, view payload, see triggered runs. |
| F-UI-6 | Functions page: config, versions, app health (last discovery, last successful attempt), pause/resume. |
| F-UI-7 | Queue view: backlog and in-flight per function and key; throttled counts. |
| F-UI-8 | Auth: bearer admin token v1; OIDC-ready. |
| F-UI-9 | Assets embedded in the server binary; served at `/`. |

### 4.7 Management API
| ID | Requirement |
|---|---|
| F-API-1 | OpenAPI 3.1 spec generated from code (`utoipa`); served at `/openapi.json`. |
| F-API-2 | Resources: namespaces, apps, functions, events, runs, steps, commands (cancel/retry/resolve), queue stats. |
| F-API-3 | Cursor pagination; consistent error envelope (RFC 9457 Problem Details). |

### 4.8 Operations
| ID | Requirement |
|---|---|
| F-OP-1 | `stepd dev` starts server + UI with zero config against an **embedded ephemeral Postgres** on a temp directory, torn down on exit. Identical schema, migrations and query paths as production (BR-15). |
| F-OP-2 | `stepd serve` reads all configuration from the environment (§4.9); migrations applied via `stepd migrate`, or automatically when `STEPD_AUTO_MIGRATE=true`. |
| F-OP-2b | CI runs the full test suite against the same Postgres major version used in production; a parity check fails the build if the dev and prod code paths diverge. |
| F-OP-3 | Prometheus metrics endpoint; OTel traces/metrics export (OTLP). |
| F-OP-4 | Retention jobs per namespace: events, completed runs, step payloads. |
| F-OP-5 | Horizontal scale: multiple server replicas share Postgres; work claimed via `FOR UPDATE SKIP LOCKED` leases with heartbeat. |
| F-OP-6 | Graceful shutdown on SIGTERM: stop claiming, finish or release in-flight attempts, release leases, exit within a configurable drain window (default 30 s). |
| F-OP-7 | Server writes structured JSON logs to stdout only; it never writes log files and never rotates logs. |
| F-OP-8 | All state lives in Postgres and the blob store; server replicas hold no durable local state and may be replaced at any time. |

### 4.9 Configuration and logging
| ID | Requirement |
|---|---|
| F-CFG-1 | All deployment configuration is read from environment variables. No config file is required, and none may contain secrets. |
| F-CFG-2 | Server variables: `STEPD_DATABASE_URL`, `STEPD_LISTEN`, `STEPD_BLOB_STORE`, `STEPD_ADMIN_TOKEN`, `STEPD_LOG_LEVEL`, `STEPD_OTLP_ENDPOINT`, `STEPD_AUTO_MIGRATE`, `STEPD_DRAIN_SECONDS`. |
| F-CFG-3 | App/SDK variables: `STEPD_SERVER_URL`, `STEPD_APP_URL`, `STEPD_APP_ID`, `STEPD_NAMESPACE`, `STEPD_SIGNING_KEY`, `STEPD_ENV`. |
| F-CFG-4 | The `AppManifest` is assembled at start-up from **code-defined** function definitions plus **environment-supplied** binding fields (`app_id`, `url`, `namespace`, `env`). Function definitions MUST NOT contain URLs, credentials or environment names (BR-16). |
| F-CFG-5 | Signing keys are supplied by the environment and support two active keys during rotation; keys are never persisted in the manifest or logged. |
| F-CFG-6 | Identical artefacts (binary, container image, app build) run in every environment; behaviour differs only by environment variables. |
| F-LOG-1 | Server and app treat logs as event streams written unbuffered to stdout in structured JSON; neither manages log files, routing or retention. |
| F-LOG-2 | The protocol `logs` array is a **capped diagnostic channel** for replay-aware messages surfaced in the console (default 500 entries, 8 KiB each, dropped beyond). It is explicitly not a logging transport; SDK documentation must say so. |
| F-LOG-3 | SDKs suppress log emission for steps replayed from memo, so a message logged inside a step appears once per execution, not once per attempt. |
| F-LOG-4 | Step payloads are not logged at default levels; per-namespace redaction policy governs what the console displays. |

### 4.10 Payloads, blobs and media
| ID | Requirement |
|---|---|
| F-BLB-1 | Three payload tiers with configurable thresholds: **inline** (< 1 MiB, stored in Postgres), **managed blob** (1–100 MiB, stored in the blob store, refcounted by stepd), **external reference** (> 100 MiB, application-owned; stepd stores no bytes). |
| F-BLB-2 | Bulk data MUST NOT traverse the stepd server. Managed blobs are uploaded by the app directly to the blob store via a presigned URL obtained from `POST /v1/blobs:reserve`. |
| F-BLB-3 | Two-phase upload: reserve (declare size, digest, content type) → direct PUT to store → reference returned in the step op. The server verifies size and digest before the blob is readable; a mismatch fails the commit with `blob_digest_mismatch`. |
| F-BLB-4 | Blobs are content-addressed per namespace: reserving an existing digest returns the existing id with no upload URL, and the app skips the upload. Deduplication never spans namespaces. |
| F-BLB-5 | Reference counting: every step result, run input and emitted event containing a `$blob` holds a reference. Bytes are deleted only when the count reaches zero **and** the retention window of all referencing runs has elapsed. |
| F-BLB-6 | Reserved-but-uncommitted blobs are garbage collected after a configurable window (default 24 h). |
| F-BLB-7 | SDKs fetch blobs lazily — replaying a step that returned a blob costs nothing until user code dereferences the value. Streaming reads and HTTP `Range` are supported so a step can read a header without pulling the whole object. |
| F-BLB-8 | Read URLs are presigned per attempt (default TTL 300 s), read-only, never logged, and never persisted by SDKs. Upload URLs are write-only and scoped to the reserved id, size and content type. |
| F-BLB-9 | External references (`$ref`) carry a URI plus optional digest, size, content type and opaque metadata. The server never fetches, validates, resolves or holds credentials for them. |
| F-BLB-10 | The engine never transcodes, resizes, decompresses, parses or otherwise interprets payload bytes in any tier. Media processing belongs in a step. |
| F-BLB-11 | Exceeding a tier limit is a non-retryable `payload_too_large` error naming the offending step id and suggesting the next tier. |
| F-BLB-12 | The console renders payloads by `content_type`: JSON viewer, image preview, video player, or a download link; previews for `$ref` render only if the operator's browser can already reach the URI. |
| F-BLB-13 | A relay endpoint (`PUT /v1/blobs/{id}/content`) exists only as a fallback for stores that cannot presign; the server emits a warning metric whenever it is used. |
| F-BLB-14 | `BlobStore` implementations ship for Postgres large objects (development) and S3-compatible stores (production), selected by `STEPD_BLOB_STORE` with no schema or code change. |

### 4.11 Load protection and failure containment
| ID | Requirement |
|---|---|
| F-LP-1 | Per-app dispatch concurrency ceiling, independent of function-level concurrency, so stepd cannot overwhelm a customer's application. |
| F-LP-2 | Circuit breaker per app endpoint: consecutive failures or timeouts open the circuit, dispatch pauses, and recovery uses half-open probing. Circuit state is visible in the console and exported as a metric. |
| F-LP-2b | A half-open circuit MUST close on consecutive successes **or** after a quiet period with no failures, whichever comes first, and MUST always admit at least a probe. A breaker whose closure depends solely on observing successes stays half-open indefinitely once traffic stops, throttling the next burst of work long after the app recovered. |
| F-LP-2c | Half-open admission MUST be a deterministic budget (e.g. a probe count that widens on success), never a probability. Probabilistic admission makes recovery time unreasonable-about for operators and any test of it inherently flaky. |
| F-LP-3 | Recovery ramp: when an app returns from an outage, dispatch rate increases gradually (default: 10% of ceiling, doubling every 30 s) rather than releasing the full backlog at once. |
| F-LP-4 | Timer fan-out spreading: timers due in the same instant are dispatched across a jitter window (default ±60 s, configurable per function, 0 for exact-time requirements) so that a million midnight wake-ups do not arrive simultaneously. |
| F-LP-5 | Namespace fairness: dispatch uses weighted fair queuing across namespaces and functions so that one tenant's backlog cannot starve another. Priority operates *within* a fairness class, not across. |
| F-LP-6 | Poison-pill quarantine: a run whose attempts fail identically N times (default 20) with the same error signature is moved to `quarantined`, stops consuming dispatch capacity, and is surfaced for bulk action. |
| F-LP-7 | Dead-letter queue: runs that exhaust retries or are quarantined are listed in a DLQ view supporting filter, inspect, bulk retry, bulk cancel and export. |
| F-LP-8 | Backpressure on ingest: when queue depth exceeds a namespace threshold, event ingest returns `429` with `Retry-After` rather than accepting work the system cannot drain. |
| F-LP-9 | Runaway protection: a run exceeding `max_steps`, `invoke_fanout` or chain length is failed non-retryably with a specific error, never allowed to grow unbounded. |
| F-LP-10 | Per-namespace resource quotas: max concurrent runs, max events/s, max stored bytes; exceeding them degrades that namespace only. |

### 4.12 Data layer, recovery and upgrades
| ID | Requirement |
|---|---|
| F-DL-1 | Connection pooling: the server manages its own pool and MUST document that **transaction-mode poolers (pgbouncer) are incompatible with session-scoped advisory locks**. The engine therefore uses row-level `FOR UPDATE SKIP LOCKED` and transaction-scoped locks only, so it remains pooler-safe; no session-scoped state is assumed. Verified by a CI suite that runs the full test battery through pgbouncer in transaction mode. |
| F-DL-2 | Hot-table maintenance: `queue`, `run_steps` and `events` have tuned autovacuum settings shipped in the migrations; `events` and completed `runs` are time-partitioned with automated partition creation and detachment. |
| F-DL-3 | Retention is executed as batched, rate-limited deletes (or partition drops) that cannot block dispatch; progress and lag are exported as metrics. |
| F-DL-4 | Backup and restore: documented backup strategy (continuous archiving) with a tested restore runbook. |
| F-DL-5 | **Restore hazard**: a point-in-time restore rewinds the journal, so steps recorded after the restore point are lost and their external side effects will re-execute on replay. The product MUST document this prominently, MUST mark restored runs with a `restored_at` marker visible in the console, and SHOULD offer bulk-cancel of runs whose steps were rewound. This is a semantic limit of durable execution, not a defect, and must not be discovered in an incident. |
| F-DL-6 | Zero-downtime upgrades: expand/contract migrations only; schema version N-1 and N interoperate; a mixed-version cluster is supported for the duration of a rolling deploy; in-flight runs are never invalidated by an upgrade. |
| F-DL-7 | `stepd doctor` diagnoses common operational faults: pooler misconfiguration, clock skew between replicas, partition lag, orphaned blobs, stuck leases, unreachable apps. |
| F-DL-8 | Clock discipline: all scheduling derives from database time, not replica wall clocks; replicas with skew beyond a threshold refuse to claim work and alert. |

### 4.13 Security and privacy
| ID | Requirement |
|---|---|
| F-SEC-1 | **Namespace authorisation is v1, not v1.1.** Every API call and console action is scoped to a namespace; a token grants a role (`viewer`, `operator`, `admin`) within one or more namespaces. Cross-namespace access is impossible by construction, not by filtering. |
| F-SEC-2 | Egress policy for every server-initiated fetch (app dispatch, discovery, relay): deny link-local, cloud metadata (169.254.169.254, fd00:ec2::/32), loopback and private ranges unless allowlisted; resolve-then-connect to defeat DNS rebinding; zero redirects; response size cap. Multi-tenant deployments require a per-namespace allowlist. |
| F-SEC-3 | Request signing includes a nonce with a replay cache over the timestamp window, in both directions. |
| F-SEC-4 | Console content safety: payload JSON, error strings, `$ref` URIs and `prompt` blocks are untrusted input. Rendering is escaped by default; URI schemes are allowlisted (`https`, `s3`, `gs`, `azure`); `javascript:`, `data:` and `file:` are never linked; a strict CSP with no inline scripts is served. |
| F-SEC-5 | Subject erasure: `DELETE /v1/namespaces/{ns}/subjects/{id}` removes or tombstones every event, run input, step result and blob associated with a subject key, across live and archived data, and records the erasure in the audit log. Functions declare a `subject_expr` so the system knows what identifies a subject. Erasure is asynchronous, resumable and reports completion. |
| F-SEC-6 | Field-level redaction: namespace policy declares JSON paths to redact before persistence; redacted values never reach the database, the console, logs or blobs. |
| F-SEC-7 | Encryption at rest for step payloads and blobs via a pluggable KMS hook; keys are per namespace and rotatable. |
| F-SEC-8 | Audit log covers every mutating command (cancel, retry, resolve wait, pause, erase, token issue) with actor, target, request id and timestamp; append-only and exportable. |
| F-SEC-9 | Secrets never appear in the manifest, the console, logs, traces or error messages; the SDK refuses to start if a function definition contains a credential-shaped value. |
| F-SEC-10 | Published threat model and a documented security disclosure process. |

### 4.14 Developer experience
| ID | Requirement |
|---|---|
| F-DX-1 | **Workflow test framework** shipped with the SDK: run a function in-process against a mock server with no database; assert the op sequence; inject step results, failures and events; advance a virtual clock so a 30-day sleep resolves instantly; assert that a given step did not re-execute. Adoption depends on this — a durable workflow that cannot be unit-tested will not be trusted with money. |
| F-DX-2 | Replay debugging: re-run a recorded run's journal against local code and report the first divergence, naming the step. |
| F-DX-3 | Dev tunnel: `stepd dev --tunnel` exposes a laptop app to a remote stepd server, so the push model does not break development behind NAT. Local-only development requires no tunnel. |
| F-DX-4 | Orphaned-step diagnostics: when a recorded hash is not encountered during replay, the console shows "this step's id changed or was removed" against the run, with the previous and current ids where derivable. This is the primary "why did my step re-run" question. |
| F-DX-5 | Error messages name the function, step id, attempt and the specific rule violated, and link to the relevant spec section. |
| F-DX-6 | `stepd lint` checks a manifest for hazards before deploy: duplicate step ids in a parallel group, steps with side effects outside `ctx.run` (best-effort static check), attempt timeouts shorter than declared step timeouts, unbounded loops without `continue_as_new`. |
| F-DX-7 | Local console at `stepd dev` shows runs live with sub-second refresh, including a step-by-step "what the server sent, what the SDK returned" protocol inspector. |
| F-DX-8 | Documentation set: quickstart, concept guide, migration guides from Temporal/Inngest, operations runbook, and a worked reference application. |

## 5. Non-functional requirements

| Area | Requirement |
|---|---|
| Durability | No acknowledged event or recorded step may be lost on crash of any server replica. |
| Consistency | Step result write and next-attempt schedule are one transaction. Outbox for emitted events. |
| Exactly-once effects | Same `(run, step hash)` never executes twice after a recorded result; duplicate attempts detected by attempt fencing token. |
| Performance | ≥1,000 step commits/s and ≥2,000 events/s on 4 vCPU Postgres; p95 dispatch latency <250 ms when unthrottled. *Measured on the reference implementation: ~2,650 commits/s sequential and ~7,500 batched on a single unpooled connection, so the target holds with headroom.* |
| Scale | 10M retained runs per namespace without UI degradation (indexes + partitioning). |
| Security | HMAC-signed requests to apps; TLS; secrets never logged; payload encryption at rest optional via KMS hook. |
| Portability | Linux x86-64/arm64 binaries; OCI image; no glibc-version pinning surprises (musl build). |
| Observability | Every run/step is a span; logs JSON; correlation ids in all API responses. |
| Compatibility | Protocol N-1 support; DB migrations backward compatible within a minor version. |
| Dev/prod parity | Same database engine, schema, migrations and query paths in development, CI and production. Divergence is a release blocker. |
| Modularity | No engine code may reference a concrete store, queue, blob backend or expression engine; all access is via the traits in §6.3. Enforced by a crate-dependency lint in CI. |
| Config | Zero secrets in the repository or in code-defined function definitions; all deploy config from the environment. |
| Statelessness | Any server replica may be killed at any moment with no data loss and no run stalling beyond the lease timeout. |
| Control-plane purity | Server memory and bandwidth are independent of payload size. A workflow moving 2 GB videos costs the server the same as one moving 2 KB records. Verified by benchmark. |
| Blast radius | A failing app, a hot namespace or a poison-pill run degrades only itself. No single tenant, function or run can exhaust dispatch capacity, connections or storage for others. |
| Silent-corruption resistance | The two failure modes that corrupt quietly — non-deterministic step hashing and lost signals — are each covered by a normative rule, a conformance suite and a simulation-test property. Neither may rely on user discipline alone. |
| Feedback latency | Pre-commit checks under 10 s, pull-request suite under 5 min, merge suite under 30 min. These are enforced budgets: a suite that outgrows its tier is refactored or moved down, never allowed to slow the loop. |
| Recovery | Documented and rehearsed RTO/RPO; a restore that rewinds the journal marks affected runs and is never silent. |
| Pooler compatibility | The engine works unmodified behind a transaction-mode connection pooler. Asserted in CI. |
| Authorisation | Namespace isolation is enforced at the query layer, not by response filtering; a token scoped to one namespace cannot observe the existence of another. |

## 6. Architecture and modularity

### 6.1 Crate boundaries

| Crate | Contains | May depend on |
|---|---|---|
| `stepd-proto` | Protocol types, JSON Schemas, hashing, signature verification. **No async runtime, no I/O, no database.** | `serde`, `sha2`, `schemars` |
| `stepd-core` | Engine: run lifecycle, op commit, memoization, retries, keyed ordering, flow control. Generic over the §6.3 traits. | `stepd-proto` |
| `stepd-store-postgres` | The v1 implementation of every storage trait. | `stepd-proto`, `sqlx` |
| `stepd-expr-cel` | `ExprEngine` implementation. | `stepd-proto`, `cel-interpreter` |
| `stepd-transport-http` | Push transport: signs, calls apps, parses responses. | `stepd-proto`, `reqwest` |
| `stepd-server` | Wiring, HTTP API, SSE, embedded console assets, CLI. | all of the above |
| `stepd-sdk` | Rust SDK: handler, memo table, short-circuit control flow, `axum`/Lambda adapters. | `stepd-proto` |
| `stepd-cli` | `dev`, `serve`, `migrate`, `conformance`. | `stepd-server` |

`stepd-proto` having no runtime dependency is the load-bearing constraint: it lets a third party build an alternative server or SDK against the spec without inheriting our engine.

**Enforcement:** a CI lint (`cargo-deny` bans + a dependency-graph assertion) fails the build if `stepd-core` gains a dependency on any concrete backend crate.

### 6.2 Component diagram

```
            events                    attempts
  clients ──────► [ ingest ] ──► [ runner ] ──► [ dispatcher ] ──► apps
                       │              │               │
                       ▼              ▼               ▼
                  EventLog       StateStore        Transport
                                  Queue
                                  TimerStore
                                  BlobStore
                                  ExprEngine
                       ▲
                  [ console / API ] ── read model, SSE
```

### 6.3 Component traits

Every trait is object-safe, async, and defined in `stepd-core`. Postgres implements all of them in v1.

```rust
/// Durable run and step state. The commit method is the correctness centre of the system.
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn create_run(&self, run: NewRun) -> Result<RunId>;
    async fn load_attempt(&self, run: RunId) -> Result<AttemptRequest>;
    /// Atomic: persist op results + emitted events + next schedule, guarded by the fence token.
    async fn commit(&self, run: RunId, fence: Fence, commit: OpCommit) -> Result<CommitOutcome>;
    async fn finish_run(&self, run: RunId, outcome: RunOutcome) -> Result<()>;
    async fn steps_page(&self, run: RunId, cursor: Option<Cursor>) -> Result<StepPage>;
}

/// Work admission and leasing. Owns concurrency, rate limiting, debounce, batching, priority.
#[async_trait]
pub trait Queue: Send + Sync {
    async fn enqueue(&self, item: QueueItem) -> Result<()>;
    async fn claim(&self, worker: WorkerId, max: usize) -> Result<Vec<Lease>>;
    async fn heartbeat(&self, lease: &Lease) -> Result<()>;
    async fn release(&self, lease: Lease, disposition: Disposition) -> Result<()>;
    async fn stats(&self, filter: QueueFilter) -> Result<QueueStats>;
}

/// Durable timers for sleeps, timeouts and scheduled events.
#[async_trait]
pub trait TimerStore: Send + Sync {
    async fn schedule(&self, timer: Timer) -> Result<TimerId>;
    async fn cancel(&self, id: TimerId) -> Result<()>;
    async fn due(&self, now: Timestamp, max: usize) -> Result<Vec<Timer>>;
}

/// Append-only event history and correlation for wait_event / cancel_on.
#[async_trait]
pub trait EventLog: Send + Sync {
    async fn append(&self, events: Vec<Event>) -> Result<Vec<EventId>>;
    async fn match_waits(&self, event: &Event) -> Result<Vec<WaitMatch>>;
    async fn query(&self, filter: EventFilter) -> Result<EventPage>;
}

/// Out-of-line payload storage.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Phase one: reserve an id and issue a write-scoped upload URL.
    /// Returns `Deduplicated(id)` when the digest already exists in the namespace.
    async fn reserve(&self, ns: &Namespace, spec: BlobSpec) -> Result<Reservation>;
    /// Phase two: verify declared size and digest, then mark readable.
    async fn commit(&self, id: BlobId) -> Result<BlobRef>;
    /// Read-scoped, short-lived URL. Minted per attempt; never persisted.
    async fn presign_read(&self, id: BlobId, ttl: Duration) -> Result<Url>;
    async fn add_ref(&self, id: BlobId, r: RefSite) -> Result<()>;
    async fn drop_refs(&self, run: RunId) -> Result<()>;
    /// Delete blobs with zero references whose retention window has elapsed,
    /// plus reservations that were never committed.
    async fn collect(&self, before: Timestamp) -> Result<CollectStats>;
    /// Fallback only, for stores that cannot presign. Emits a warning metric.
    async fn relay_put(&self, id: BlobId, bytes: ByteStream) -> Result<()>;
}

/// Expression evaluation for triggers, keys, waits and cancellation.
pub trait ExprEngine: Send + Sync {
    fn compile(&self, source: &str) -> Result<CompiledExpr>;
    fn eval(&self, expr: &CompiledExpr, bindings: &Bindings) -> Result<Value>;
}

/// Delivery of an attempt to an app. Push (HTTP) in v1; a pull-worker mode is
/// an alternative implementation, NOT a protocol change (BR-18).
#[async_trait]
pub trait Transport: Send + Sync {
    async fn deliver(&self, target: &AppBinding, req: AttemptRequest)
        -> Result<AttemptResponse, TransportError>;
}
```

### 6.4 Dev/prod parity

| Concern | Development | CI | Production |
|---|---|---|---|
| Database | Embedded ephemeral Postgres (same major version) | Postgres service container | Managed Postgres |
| Schema/migrations | Identical | Identical | Identical |
| Blob store | Postgres large objects or local MinIO | Same as dev | S3-compatible |
| Transport | HTTP to localhost | HTTP | HTTPS |
| Config source | `.env` file loaded into the environment | CI secrets | Orchestrator secrets |

SQLite is **not** a supported backend. `stepd dev` provisions Postgres transparently so that `SKIP LOCKED`, advisory locks, `jsonb` operators and partitioning are exercised locally exactly as in production.

### 6.5 Twelve-factor conformance

| Factor | How stepd satisfies it |
|---|---|
| I Codebase | One repo, many deploys; version-controlled schemas and protocol |
| II Dependencies | `Cargo.lock`; console assets vendored into the binary; no implicit system packages |
| III Config | Environment only (§4.9); manifests carry no credentials or endpoints |
| IV Backing services | Postgres, blob store and apps all attached by URL and swappable without code change |
| V Build/release/run | Immutable artefacts; release = artefact + env; `stepd migrate` is a separate release step |
| VI Processes | Apps are stateless and share-nothing; all run state travels in the attempt request |
| VII Port binding | Server and apps are self-contained HTTP listeners; no external web server required |
| VIII Concurrency | Scale out by adding server replicas and app instances; flow control is declarative |
| IX Disposability | Fast start; SIGTERM drains within a bounded window; leases and fencing make abrupt death safe |
| X Parity | §6.4 — same database engine everywhere; deploy gap measured in minutes |
| XI Logs | Unbuffered JSON to stdout; protocol `logs` is a capped diagnostic channel, not a log transport |
| XII Admin processes | `stepd migrate`, `stepd conformance`, retention jobs run as one-off commands against the same artefact |

The stepd **server** is intentionally stateful; it is a backing service (factor IV) for the applications that attach to it, and the factors are assessed against those applications and against the server's own operational surface.

## 7. Data model (logical)

Postgres is the only v1 implementation. Every table is reached through exactly one trait from §6.3 — noted in the right-hand column so ownership stays unambiguous.

```
namespaces(id, name, retention_cfg, redaction_cfg)                        -- StateStore

-- Deployment binding, supplied by the environment at app start-up (F-CFG-4).
-- Signing keys are stored hashed; the plaintext lives only in the app's environment.
app_bindings(id, ns, app_id, url, env, key_hash_current, key_hash_previous,
             last_seen, last_manifest_checksum)                           -- StateStore

-- Code-defined function definitions. Contains NO urls, credentials or env names.
functions(id, ns, app_binding_id, fn_id, version, config_json, paused,
          archived_at, UNIQUE(ns, fn_id, version))                        -- StateStore

events(id uuidv7, ns, type, source, time, key, idem, data jsonb,
       received_at)                        -- partitioned by month           EventLog

runs(id, ns, fn_id, fn_version, status, key, trigger_event_id, parent_run_id,
     parent_step_hash, detached, lineage_id, chain_position,
     started_at, ended_at, deadline, attempt_no, error_signature,
     quarantined_at, restored_at,
     lease_owner, lease_until, fence_token)                               -- StateStore
     -- status: pending|running|sleeping|waiting|completed|failed|cancelled|quarantined
     -- error_signature powers poison-pill detection (F-LP-6)
     -- restored_at marks runs rewound by a PITR restore (F-DL-5)

-- Durable per-run inbox. Solves the lost-signal race: an event directed at a run is
-- retained whether or not a wait is currently registered (protocol §7.6).
run_inbox(id, run_id, received_at, event jsonb, consumed_by_step_hash,
          sender_run_id, sender_step_hash,
          UNIQUE(run_id, sender_run_id, sender_step_hash))                -- EventLog
          -- index on (run_id, consumed_by_step_hash) WHERE consumed_by_step_hash IS NULL

run_steps(run_id, step_hash, step_id, occurrence, op, status,
          result jsonb | blob_ref, meta jsonb, attempts,
          started_at, ended_at, error,
          PRIMARY KEY(run_id, step_hash))                                 -- StateStore

timers(id, run_id, fire_at, step_hash, kind)   -- index on fire_at           TimerStore
waits(id, run_id, step_hash, event_type, expr_compiled, expires_at,
      prompt jsonb)                            -- index on (ns,event_type)   EventLog
queue(id, ns, fn_id, key, run_id, priority, available_at,
      claimed_by, claimed_until)                                          -- Queue
concurrency_slots(ns, scope_key, in_flight, limit)                        -- Queue
rate_buckets(ns, scope_key, tokens, refilled_at)                          -- Queue
keys(ns, fn_id, key, active_run_id, PRIMARY KEY(ns, fn_id, key))          -- Queue
outbox(id, run_id, event jsonb, published, published_at)                  -- EventLog
blobs(id, ns, storage_url, size, sha256, content_type, filename,
      state, reserved_at, committed_at,
      UNIQUE(ns, sha256))            -- content-addressed per namespace        BlobStore
blob_refs(blob_id, run_id, step_hash, PRIMARY KEY(blob_id, run_id, step_hash))
                                     -- refcount; GC when empty AND retained   BlobStore
commands_audit(id, ns, actor, command, target, at, request_id)            -- StateStore

-- Authorisation, namespace-scoped from v1 (F-SEC-1)
tokens(id, ns, role, token_hash, name, created_at, expires_at, revoked_at) -- StateStore

-- Subject erasure index (F-SEC-5): maps a subject key to everything referencing it
subject_index(ns, subject_key, entity_kind, entity_id,
              PRIMARY KEY(ns, subject_key, entity_kind, entity_id))       -- StateStore
erasures(id, ns, subject_key, requested_by, requested_at,
         state, progress_cursor, completed_at)                           -- StateStore

-- Dispatch health per app endpoint (F-LP-2, F-LP-3)
app_health(app_binding_id, circuit_state, consecutive_failures,
           opened_at, half_open_at, ramp_fraction)                       -- Queue
```

Notes:
* `app_bindings` is separated from `functions` so that the same function definitions can be registered from any environment without editing code (BR-16). Rebinding an app to a new URL does not touch function rows.
* `blobs` stores a `storage_url` rather than bytes, so the `BlobStore` implementation may be Postgres large objects (dev) or S3 (production) without a schema change. `state` is `reserved` → `committed`; reserved rows past their window are collected.
* `UNIQUE(ns, sha256)` gives content-addressed deduplication scoped per namespace, so a retried step or replayed event never duplicates bytes and a digest cannot probe another tenant's data.
* `blob_refs` is the reference count. A blob referenced by a parent run and a child run survives until both are collected; deleting one run never orphans bytes another still needs.
* External references (`$ref`) appear only inside `run_steps.result` as opaque JSON — there is no table for them, because stepd stores none of that data.
* `run_inbox` is the durability mechanism behind the early-signal guarantee. The unique constraint on `(run_id, sender_run_id, sender_step_hash)` deduplicates a retried sender, so a `signal` op that re-executes never double-delivers.
* No table uses session-scoped advisory locks; all claiming is row-level `FOR UPDATE SKIP LOCKED` so the schema is safe behind a transaction-mode pooler (F-DL-1).
* `run_steps.meta` holds the opaque `meta` block from ops (model, tokens, cost) for console display and aggregation.

## 8. Protocol

The wire protocol is specified separately and normatively in **stepd SDK Protocol v1** (`stepd-spec/PROTOCOL.md`), with JSON Schemas in `stepd-spec/schemas/` and a validation harness in `stepd-spec/validate.py`. This PRD does not restate it; the summary below exists only for orientation.

* **Server → app:** `POST {app_url}` with an `AttemptRequest` — run identity, trigger events, the map of recorded steps by hash, fence token and deadline.
* **App → server:** an `AttemptResponse` — an `ops` array (one op, or a parallel batch), an optional `emit` array of CloudEvents committed in the same transaction, capped `logs`, and `diagnostics`.
* **Ops:** `step`, `sleep`, `wait_event`, `invoke`, `signal`, `done`, `error`. `done` and `error` appear alone.
* **Step identity:** `sha256(function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ occurrence)` truncated to 16 hex characters.
* **Safety:** HMAC-SHA256 request signing with a 300 s window; per-attempt fencing tokens; `409` on a stale fence.

Protocol changes are governed by the spec's own versioning rules (§11 of the spec), not by this document's version.

## 9. User flows

1. **Author:** `cargo add stepd-sdk` → define function → `stepd dev` → send test event from UI → watch steps appear live.
2. **Deploy:** the same app artefact is deployed with `STEPD_SERVER_URL`, `STEPD_APP_URL`, `STEPD_NAMESPACE` and `STEPD_SIGNING_KEY` set by the orchestrator; the SDK assembles the manifest from code-defined functions plus those bindings and registers on start-up. No code or config file changes between staging and production.
3. **Operate a stuck run:** Priya searches key `order:4711` → sees run waiting for `order.approved` since 3 days → clicks *Resolve wait* → pastes JSON → run continues; action audited.
4. **Ship a code change:** Dana adds a new step between two existing ones; in-flight runs pick it up on next attempt because the new step hash is absent and existing hashes are memoized.
5. **Diagnose:** Omar sees dispatch-latency alert → Queue view shows one key with 2k backlog → pauses function, investigates, resumes.
6. **Swap a backing service:** the blob store moves from Postgres large objects to S3 by changing `STEPD_BLOB_STORE`; no engine code, schema or protocol change (factor IV).

## 10. Verification strategy

Correctness under crashes is the entire value proposition, so verification is a first-class
deliverable. But an exhaustive regime run against everything is a slow regime, and a slow
regime gets bypassed. Testing effort is therefore allocated by **risk**, and organised by
**feedback latency**, so that the fast loop stays fast and the heavy machinery is aimed only
where failure is expensive and hard to see.

### 10.1 Risk model

Risk = *severity if it fails* × *likelihood of a defect* × *how long it stays hidden*. The
third term dominates here: a bug that throws is cheap, a bug that silently records the wrong
thing is not.

| Zone | What | Sev | Detectability | Regime |
|---|---|---|---|---|
| **R1 Silent corruption** | Op commit transaction, step hashing and occurrence assignment, inbox matching and consumption, fencing, keyed exclusivity, `continue_as_new` chain handover | Catastrophic | Very low — corrupts quietly, surfaces weeks later as a customer's wrong outcome | Simulation with full property set, exhaustive interleaving search on the commit path, mutation testing, formal state-machine model. No change ships without a property covering it |
| **R2 Durability and money** | Retry policy, cascade, timers, blob commit and refcounting, outbox, migrations | High | Low–medium | Simulation subset, chaos, integration on real Postgres, upgrade tests |
| **R3 Isolation and safety** | Namespace authz, egress policy, signature and nonce, redaction, erasure, console rendering | High | Medium — usually a fast, loud failure once triggered | Targeted adversarial suites, fuzzing, dependency and container scanning |
| **R4 Capacity** | Flow control, fairness, circuit breaker, timer fan-out, queue throughput | Medium | High — visible in metrics | Benchmarks with regression thresholds, load tests at release |
| **R5 Surface** | Console, CLI, docs, error strings, OpenAPI shape | Low–medium | High — someone notices immediately | Component tests, a few end-to-end paths, usability sessions, docs examples executed in CI |

Rule of thumb: **spend depth on R1, breadth on R3, and speed everywhere else.** An R5 bug
costs an apology; an R1 bug costs the product.

### 10.2 Tiers by feedback latency

Every tier has a time budget. Exceeding the budget is treated as a defect in the test suite,
not an acceptable cost.

| Tier | Budget | Scope | Runs on |
|---|---|---|---|
| **T0 Pre-commit** | < 10 s | Types, lint, schema validation, unit and property tests for pure logic (hashing, backoff, CEL binding, occurrence assignment) | Every save/commit |
| **T1 Pull request** | < 5 min | T0 + component tests, contract tests against the spec examples, **risk-targeted simulation smoke** (fixed seed corpus incl. every historical regression seed), single-node integration on ephemeral Postgres | Every push |
| **T2 Merge** | < 30 min | T1 + full integration incl. the pgbouncer lane, conformance suite against the Rust SDK, security suites, benchmark smoke against thresholds | Every merge to main |
| **T3 Nightly** | hours | Deep simulation (large seed budget, weighted per §10.3), chaos matrix, upgrade N-1→N with in-flight runs of every op type, mutation testing on R1 code | Scheduled |
| **T4 Release** | days | Soak with retention, partition rollover and a rolling upgrade mid-soak; full load and fan-out benchmarks; usability sessions; external conformance run | Release candidate |

**Selective execution.** T1 chooses its simulation and integration subset from the diff: a
change touching R1 code pulls in the full R1 property set and a larger seed budget even
though it is a pull-request tier; a change touching only console code runs component tests
and skips simulation entirely. Risk zone is derived from file ownership declared in the
repository, so the mapping is explicit rather than inferred.

### 10.3 Simulation, allocated by risk

The engine runs against a seeded simulator: virtual clock, in-memory store implementing the
§6.3 traits, and an adversarial scheduler injecting crashes, reordering, duplicate delivery,
partial commits, clock skew, lease expiry and partitions. A failing seed reproduces the exact
interleaving, which is what makes ordering bugs tractable. This is the only technique that
reliably finds R1 defects, because they need a specific interleaving that conventional tests
reach by luck, if ever.

| Property | Statement | Zone |
|---|---|---|
| No lost effect | Every op accepted by the server is eventually reflected in run state | R1 |
| No duplicate record | A given step hash is recorded at most once, whatever the crash sequence | R1 |
| No lost signal | An event directed at a run is either consumed by a wait or explicitly discarded at termination — never silently dropped | R1 |
| Hash stability | The hash sequence for a given handler and input is identical across every replay | R1 |
| Keyed exclusivity | At most one active run per `(function, key)` at any instant, including across leader change | R1 |
| Fence monotonicity | A response bearing a stale fence never mutates state | R1 |
| Chain continuity | Across `continue_as_new`, no foreign run interleaves on the key | R1 |
| Cascade completeness | After a parent reaches terminal state, no non-detached descendant remains running | R2 |
| Termination | Every run reaches a terminal state or is explicitly blocked on a timer/wait — no stalls | R2 |

Efficiency measures, so that seeds buy findings rather than repetition:

* **Weighted seed budget.** Roughly 70% of nightly seeds target R1 scenarios (concurrent
  steps, signal races, crash-during-commit, leader change under load), 30% explore broadly.
* **Swarm testing** — each seed enables a random subset of features rather than all of them,
  which reaches unusual combinations far faster than uniform generation.
* **Shrinking.** A failing seed is automatically minimised to the shortest interleaving that
  still fails, before a human ever looks at it.
* **Regression corpus.** Every historical failing seed becomes a permanent fixed-seed case in
  T1 — the cheapest test in the suite is the bug you already found.
* **Coverage-guided seed selection.** Seeds that reach new state-machine transitions are
  retained and mutated; seeds that add no coverage are dropped, so the budget stops paying
  for repetition.
* **Stop rule.** If a nightly run finds nothing new across the budget for several
  consecutive runs, the budget moves to a less-explored zone rather than growing.

### 10.4 Other layers, sized to risk

| Layer | Approach | Tier | Zones |
|---|---|---|---|
| Unit / property | Pure logic in `stepd-proto`, `stepd-core`; property tests for hashing, backoff, CEL, occurrence | T0 | R1, R5 |
| Contract | Spec examples validated; SDK ↔ server conformance (protocol §12) | T1/T2 | R1, R2 |
| Integration | Real Postgres, real HTTP, multi-replica; **mandatory pgbouncer transaction-mode lane** (F-DL-1) | T1 subset, T2 full | R2 |
| Chaos | `kill -9` server and app mid-step; DB failover; disk-full; slow app; clock skew | T3 | R1, R2 |
| Mutation | Mutation score threshold enforced **only on R1 code**; elsewhere reported, not gated | T3 | R1 |
| Fuzzing | Protocol decoder, CEL expressions, blob refs, console payload rendering | T3 | R1, R3 |
| Security | Egress policy (metadata IP, DNS rebinding, redirects), signature replay, namespace isolation, XSS corpus, dependency scan | T2 | R3 |
| Performance | Step commit rate, dispatch latency, timer fan-out at 10M sleeping runs, control-plane purity with 2 GB payloads | T2 smoke, T4 full | R4 |
| Upgrade | N-1 → N mixed-version cluster with in-flight runs of every op type | T3 | R2 |
| Soak | 72 h continuous load with retention, partition rollover, rolling upgrade mid-soak | T4 | R2, R4 |
| Usability | Moderated tests of the Priya remediation and Dana first-workflow flows | T4 | R5 |

### 10.5 Deliberately not tested

Naming these keeps the suite honest and fast:

* Third-party library internals (Postgres, `sqlx`, `tokio`) — we test our use of them, not them.
* Exhaustive CRUD permutations on the read API; the OpenAPI contract plus a few paths suffice.
* Console pixel appearance; behaviour and escaping are tested, styling is reviewed.
* Every combination of flow-control settings; the interaction matrix is sampled, not enumerated.
* Performance of anything off the hot path.

### 10.6 Suite health

* **Flakiness budget: zero.** A test that fails intermittently is quarantined the same day
  and either fixed or deleted. A flaky suite trains people to ignore red, which costs more
  than the test was ever worth. Simulation makes this tractable: a failure carries a seed,
  so "flaky" almost always means "real bug with a rare interleaving".
* **Tier budgets are enforced in CI.** If T1 exceeds five minutes, tests move down a tier or
  get faster; the budget does not move.
* **Every escaped defect gets a post-mortem question**: which tier should have caught this,
  and what is the cheapest test that would have? The answer is usually a fixed seed or a
  property, not a new integration test.
* **Test code is reviewed as production code**, particularly simulation properties — a
  property that is subtly wrong is worse than no property, because it certifies false safety.

### 10.7 Release gates

No release ships with: a failing R1 property, a failing conformance suite, a mutation score
below threshold on R1 code, a known silent-corruption path, an unresolved security finding
in R3, a benchmark regression beyond threshold, or an undocumented operational hazard.

R4 and R5 findings may ship as documented known issues. R1 findings may not ship at all.

## 11. Milestones and scope realism

### 11.1 Sequencing, not scheduling

Milestones below are ordered by dependency and gated by conditions, not dates. Build effort
is not the limiting factor on this project; three other things are:

1. **Verification compute.** Simulation coverage is bounded by seeds executed, and the soak
   test takes as long as the soak. These set the floor on elapsed time to a trustworthy release.
2. **External input.** Spec review by independent implementers, usability testing with real
   operators, and a contributor building the second SDK from the spec alone — each of these
   is the actual test of the artifact, and none of them runs on our clock.
3. **Production exposure.** Confidence in a durable execution engine comes from months of
   other people's workloads hitting it, not from passing our own tests.

The ordering still matters even when implementation is cheap, because each milestone's exit
condition is what makes the next one meaningful: foundations before features, because
retrofitting simulatability means rewriting the engine; conformance before a second SDK,
because the spec is only proven when someone else implements it.

Scope is a separate question from speed. Even with unlimited implementation capacity, a
narrower v1 is better: fewer surfaces to keep correct across every future change, and less
committed API to regret. The deferral list in §11.3 stands on that reasoning, not on
capacity.

### 11.2 Milestones

| Milestone | Deliverables | Exit gate |
|---|---|---|
| M0 Spec ✅ | Protocol spec rev 1.1, JSON Schemas, examples, validation harness | 40 schema cases green; correctness gaps closed normatively |
| M0.5 Foundations | Crate split (§6.1), traits (§6.3), **simulation harness with the §10.3 properties**, risk-zone ownership map, tiered CI (T0–T3), dependency lint, embedded-Postgres dev harness, pgbouncer lane | All R1 properties green under the nightly seed budget; T0/T1 within budget; lint fails on a deliberate backend dependency |
| M1 Core | Events, functions, runs, `step`/`sleep`/`done`/`error`, Postgres store, leases + fencing, `stepd dev`/`serve`/`migrate` | Chaos: `kill -9` mid-step, no duplicate effects; R1 properties green |
| M2 Coordination | `wait_event` + **inbox**, `invoke` + cascade, `signal`, `continue_as_new`, keys, concurrency, retries, cancel, outbox | Saga reference app passes; conformance level 1 green; `early_signal` and `determinism` suites green |
| M3 Console + DX | Runs/steps/events/functions/queue/DLQ views, SSE, commands, pending-decision cards, **SDK test framework**, dev tunnel | Priya flow completed by a non-engineer; Dana writes and unit-tests a workflow without reading the spec |
| M4 Hardening | Load protection (§4.11), data-layer work (§4.12), security (§4.13), blob tiering + GC, metrics/OTLP, retention | ≥1k step commits/s; soak 72 h; upgrade test; control-plane purity benchmark; security tests green |
| M5 Ecosystem | Conformance levels 1–2 packaged, second SDK by a contributor, migration guides | Python SDK passes level 2 using only the spec |

### 11.3 Explicitly deferred from v1 (scope, not capacity)

Declarative DSL front-ends (Serverless Workflow, BPMN) · WASM and container step hosting ·
pull-worker transport · multi-region · visual designer · hosted SaaS · batching, debounce
and priority beyond simple concurrency and rate limits · field-level encryption (KMS hook
stubbed, not implemented) · second SDK maintained in-house.

Each deferred item has a named seam in §6.3 so that deferring costs a feature, not a rewrite.
These are deferred because a smaller v1 is easier to keep correct and commits less API we
would have to live with — not because they are expensive to build.

## 12. Metrics

* Lost-run count (must be 0); duplicate-effect count (must be 0).
* p50/p95 step dispatch latency; queue age per function.
* Time-to-first-workflow (telemetry opt-in in `stepd dev`).
* Console task success rate in usability tests.
* Dev/prod parity: count of code paths conditional on backend type (target: 0).
* Modularity: `stepd-core` dependencies on concrete backends (target: 0, enforced in CI).
* Bytes of payload data traversing the server per GB of workflow payload (target: ~0 outside the relay fallback).
* Blob relay-endpoint usage (target: 0 in production deployments).
* Orphaned blob bytes and uncollected reservations (target: 0 after each GC cycle).
* Simulation seeds executed per release and property violations found (target: violations 0).
* Inbox overflow events (target: 0; non-zero means a run is signalled faster than it consumes).
* Quarantined runs and DLQ depth, with median time to remediation.
* Circuit-breaker openings per app, and dispatch rejected by fairness or quota.
* Conformance level achieved by each SDK.
* Mean time to diagnose a stuck run in usability testing (proxy for F-DX-4 quality).
* Feedback latency per tier against budget (T0 <10 s, T1 <5 min, T2 <30 min).
* Escaped-defect rate by risk zone, and for each escape the tier that should have caught it.
* Mutation score on R1 code; simulation coverage of state-machine transitions.
* Flaky-test count (target: 0) and mean time to quarantine.
* Seeds executed per new finding — the efficiency measure that triggers the §10.3 stop rule.

## 13. Open questions / decisions pending

**Resolved in 0.4** (see `GAPS.md` for the full register)
* Lost-signal race → durable per-run inbox with `since: run_start` default.
* Occurrence determinism → program-order assignment plus a structured parallel construct; off-path assignment is a non-retryable error.
* Partial batch failure → `join` policies `all_settled` (default) / `all` / `any`.
* Invoke cascade, depth, fan-out, cycles → specified in protocol §7.5.
* `continue_as_new` → promoted from open question to a v1 op; mandatory for unbounded loops.
* Cron misfire, catch-up, DST → specified in protocol §3.1.
* Namespace authorisation → promoted to v1 (F-SEC-1), no longer v1.1.
* Pooler strategy → transaction-mode safe by construction; no session-scoped locks; asserted in CI.

**Still open**
1. Whether the managed-blob ceiling should be per-plan rather than per-namespace.
2. Whether the console proxies `$ref` previews for operators outside the network perimeter (leaning refuse — proxying makes the server a data plane).
3. Blob retention when the only referencing run is archived: collect immediately or hold for the namespace window?
4. Key scope: per function, or namespace-wide virtual-object semantics?
5. Namespace isolation: shared database with row-level scoping, or database-per-namespace for regulated tenants?
6. Licence (Apache-2.0 assumed) and CLA policy.
7. Whether `stepd dev`'s embedded Postgres ships bundled or requires Docker.
8. Whether the dev tunnel is self-hosted or a hosted convenience (the latter implies operating a service before there is a product).
9. Erasure semantics for a subject whose data is inside a *completed* run's journal: tombstone the fields or delete the run entirely?
10. Whether `join: any` should cancel siblings or let them complete detached.

## 14. Appendix: ADR index

| ADR | Subject | Status |
|---|---|---|
| ADR-001 | Execution model: step memoization with per-attempt push | Accepted |
| ADR-002 | Storage: Postgres in all environments; no SQLite backend | Accepted (0.2) |
| ADR-003 | Event envelope: CloudEvents 1.0 with `stepd*` extensions | Accepted |
| ADR-004 | Expression language: CEL behind an `ExprEngine` trait | Accepted (0.2) |
| ADR-005 | Console stack: React/TS embedded via `rust-embed`; OpenAPI + SSE | Accepted |
| ADR-006 | Crate boundaries and the no-backend-dependency rule for `stepd-core` | Accepted (0.2) |
| ADR-007 | Configuration from the environment; manifests carry no deploy config | Accepted (0.2) |
| ADR-008 | Transport as a trait; push in v1, pull deferred | Accepted (0.2) |
| ADR-009 | Fencing tokens and lease-based work claiming | Proposed |
| ADR-010 | Payload tiering, direct upload and blob lifecycle | Accepted (0.3) |
| ADR-011 | Durable run inbox and the early-signal guarantee | Accepted (0.4) |
| ADR-012 | Occurrence assignment in program order; structured parallelism | Accepted (0.4) |
| ADR-013 | Batch join policies and sibling cancellation | Accepted (0.4) |
| ADR-014 | Invoke-tree cascade, limits and cycle prevention | Accepted (0.4) |
| ADR-015 | `continue_as_new` and lineage | Accepted (0.4) |
| ADR-016 | Cron misfire, catch-up and DST handling | Accepted (0.4) |
| ADR-017 | Deterministic simulation testing as the primary correctness technique | Accepted (0.4) |
| ADR-018 | Namespace authorisation model | Accepted (0.4) |
| ADR-019 | Pooler-safe locking; no session-scoped advisory locks | Accepted (0.4) |
| ADR-020 | Subject erasure and redaction model | Proposed |
