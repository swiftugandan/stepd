# stepd SDK Protocol — Specification v1

| | |
|---|---|
| Protocol version | `1` |
| Spec revision | 1.2 |
| Status | Draft |
| Licence | Apache-2.0 |
| Media type | `application/json` |
| Companion schemas | `schemas/*.schema.json` |

The key words MUST, MUST NOT, REQUIRED, SHALL, SHOULD, SHOULD NOT, MAY and OPTIONAL are to be interpreted as described in RFC 2119 / RFC 8174.

---

## 1. Overview

stepd is an orchestrator. The **server** owns durable state; the **app** (an HTTP service hosting an SDK) owns the workflow code. The server drives execution by repeatedly calling the app. Each call is one **attempt**.

```
  ┌────────┐   1. attempt request (run state)      ┌────────┐
  │ server │ ────────────────────────────────────► │  app   │
  │        │ ◄──────────────────────────────────── │ (SDK)  │
  └────────┘   2. response: one or more ops        └────────┘
       │
       └── 3. commit ops durably, schedule next attempt
```

On each attempt the SDK re-executes the handler from the top. Steps whose results the server already holds are **memoized**: the SDK returns the stored value without running the closure. The first step without a stored result becomes a new op, and the handler yields. The server commits the op and schedules the next attempt. A run therefore advances one (or one parallel batch of) step per attempt until the handler returns.

### 1.1 Design consequences

* Code outside a step MAY execute many times. All side effects MUST be inside steps.
* Workflow code need not be deterministic across versions: identity is the **step id**, not execution order. Adding, removing or reordering steps between attempts is safe (§6).
* The app is stateless. All durable state travels in the attempt request or is referenced by it.
* stepd is a control plane. Payloads are tiered (§8.1) so that bulk data — images, documents, video — never passes through the server.
* Sleeps and waits consume no app compute.

## 2. Transport

* HTTP/1.1 or HTTP/2 over TLS. Plain HTTP MAY be used for loopback development only.
* Server → app: `POST {app_url}` with `Content-Type: application/json`.
* App → server (event ingest, discovery push): `POST {server_url}/v1/events`, `PUT /v1/apps`.
* Request and response bodies MUST be UTF-8 JSON.
* `Content-Encoding: gzip` SHOULD be supported in both directions.

### 2.1 Headers

| Header | Direction | Required | Meaning |
|---|---|---|---|
| `stepd-protocol` | both | yes | Protocol major version, e.g. `1` |
| `stepd-signature` | both | yes (prod) | `t=<unix_seconds>,v1=<hex hmac>` (§9) |
| `stepd-nonce` | both | yes (prod) | Unique per request, covered by the MAC; replay defence (§9) |
| `stepd-run-id` | s→a | yes | Convenience for logging/routing |
| `stepd-attempt` | s→a | yes | Attempt counter, from 1 |
| `stepd-fence` | s→a | yes | Fencing token for this attempt (§7.3) |
| `stepd-sdk` | a→s | yes | `<language>/<version>`, e.g. `rust/0.3.1` |
| `traceparent` | both | SHOULD | W3C Trace Context |

### 2.2 Status codes (app → server)

| Code | Meaning | Server behaviour |
|---|---|---|
| `200` | Body contains an op envelope | Commit per §5 |
| `400` | Malformed attempt request | Fail run, non-retryable |
| `401` | Signature invalid/expired | Fail attempt, alert; retry with backoff |
| `404` | Unknown function | Mark function unhealthy; retry with backoff |
| `409` | Fence token stale | Discard response (§7.3) |
| `429` | App-side throttle; honour `Retry-After` | Reschedule attempt |
| `5xx` | Transient app failure | Retry per policy |

A response body MAY accompany 4xx/5xx as a Problem Details object (RFC 9457).

## 3. Discovery and registration

Two mechanisms; a server MUST support both.

**Pull.** The server GETs `{app_url}/.well-known/stepd` and receives an `AppManifest`.
**Push.** The app calls `PUT {server_url}/v1/apps` with the same `AppManifest`.

```jsonc
{
  "protocol": "1",
  "app_id": "billing",
  "url": "https://billing.internal/stepd",
  "sdk": "rust/0.3.1",
  "checksum": "sha256:…",          // over the functions array; server skips no-op updates
  "functions": [ /* FunctionConfig */ ]
}
```

`FunctionConfig` (see `schemas/function-config.schema.json`):

```jsonc
{
  "id": "order-fulfilment",
  "version": "3",                     // opaque; informational only
  "name": "Order fulfilment",
  "triggers": [
    { "type": "event", "event": "order.created", "expr": "event.data.total > 0" },
    { "type": "cron",  "cron": "0 3 * * *", "tz": "Europe/London" },
    { "type": "invoke" }
  ],
  "key_expr": "'order:' + event.data.order_id",   // CEL → string; enables keyed ordering
  "concurrency": [ { "limit": 20 }, { "limit": 1, "key_expr": "event.data.tenant" } ],
  "rate_limit": { "limit": 100, "period": "PT1M" },
  "debounce":   { "period": "PT10S", "key_expr": "event.data.order_id" },
  "batch":      { "max_size": 50, "timeout": "PT5S" },
  "retries": { "max_attempts": 4, "backoff": "exponential", "initial": "PT10S", "max": "PT1H", "jitter": true },
  "timeouts": { "attempt": "PT60S", "run": "P30D", "start": "PT5M" },
  "cancel_on": [ { "event": "order.cancelled", "expr": "event.data.order_id == run.key_suffix" } ],
  "priority_expr": "event.data.vip ? 10 : 0",
  "idempotency_expr": "event.data.order_id"
}
```

Registration is idempotent by `(app_id, function.id)`. Removing a function from the manifest **archives** it: no new runs start; in-flight runs continue to be driven.

### 3.1 Cron trigger semantics

| Concern | Rule |
|---|---|
| Misfire (server down over a fire time) | Governed by `catchup`: `one` (default) fires once on recovery however many were missed; `skip` fires nothing; `all` fires every missed occurrence, capped by `catchup_limit` (default 10). |
| Misfire window | Occurrences older than `misfire_window` (default `PT1H`) are never caught up, whatever `catchup` says. |
| Overlap | With `singleton: true` and a `run_key`, a fire whose key already has an active run is **skipped** and counted in a `cron_skipped` metric. Without `singleton`, fires overlap freely. |
| DST — nonexistent local time | The occurrence is skipped (e.g. 01:30 on a spring-forward day in `Europe/London`). |
| DST — ambiguous local time | Fires once, on the **first** (pre-transition) occurrence. |
| Time zone data | Servers MUST use the IANA database and MUST re-evaluate schedules after a tzdata update. |
| Clock skew | Fire times derive from the server's monotonic scheduler backed by database time, never from an app-supplied clock. |
| Definition change | Re-registering a function with a changed cron takes effect from the next occurrence; already-scheduled fires within the misfire window are honoured. |

`catchup`, `catchup_limit`, `misfire_window`, `singleton` and `run_key` are
fields on the cron trigger.

`singleton` requires `run_key`, and `run_key` is a literal rather than a CEL
expression: a cron fire has no event to evaluate an expression against, and
giving `key_expr` one meaning for event-triggered runs and another for cron
fires would make the field's behaviour depend on what triggered it. Servers
MUST reject a cron trigger with `singleton: true` and no `run_key`, because
overlap control with nothing to be exclusive on degrades silently into no
overlap control at all — the operator asked for the one property that is then
missing.

Servers MUST record each occurrence's outcome, fired or skipped, durably enough
that the same occurrence cannot fire twice across a restart or a failover, and
MUST expose the skip count. A schedule that is quietly skipping every fire and
one that is keeping up are otherwise indistinguishable from the run history.

## 4. Attempt request (server → app)

`AttemptRequest` (`schemas/attempt-request.schema.json`):

```jsonc
{
  "protocol": "1",
  "attempt": 7,
  "fence": "01J8…",                       // opaque; echo in ingest calls made during the attempt
  "run": {
    "id": "01J8ZQ…",                      // UUIDv7
    "function_id": "order-fulfilment",
    "namespace": "prod",
    "key": "order:4711",                  // null if unkeyed
    "started_at": "2026-08-19T10:00:00Z",
    "parent": { "run_id": "01J8Z…", "step_hash": "3f2a…" },   // null if root
    "input": { }                          // for invoke-triggered runs
  },
  "events": [ { /* CloudEvent, structured mode */ } ],   // 1..n (n>1 only for batched triggers)
  "steps": {
    "3f2a91c4": { "id": "charge",   "op": "step",       "status": "completed", "data": { "tx": "ch_1" } },
    "a71bd0e2": { "id": "cooldown", "op": "sleep",      "status": "completed" },
    "c0ff3312": { "id": "approval", "op": "wait_event", "status": "completed", "data": { /* CloudEvent */ } },
    "9d1e77aa": { "id": "refund",   "op": "step",       "status": "failed",
                  "error": { "code": "gateway_down", "message": "…", "attempts": 3 } }
  },
  "state_truncated": false,               // true if `steps` is paginated (§8.3)
  "deadline": "2026-08-19T10:01:00Z"      // attempt timeout; SDK SHOULD abort after
}
```

Rules:

* `steps` contains **only completed or terminally failed** entries. Pending ops are never sent.
* Step `data` MAY be replaced by a blob reference (§8.2).
* The SDK MUST treat unknown fields as forward-compatible and ignore them.

## 5. Response: op envelope (app → server)

`AttemptResponse` (`schemas/attempt-response.schema.json`):

```jsonc
{
  "protocol": "1",
  "ops": [ /* 1..n Op objects; >1 only for parallel step discovery */ ],
  "emit": [ /* 0..n CloudEvents to publish transactionally with the ops */ ],
  "logs": [ { "level": "info", "message": "…", "at": "…", "step": "charge" } ]
}
```

### 5.1 Op types

Every op carries `id` (developer-supplied, stable) and `hash` (§6). `hash` MUST be computed by the SDK and MUST be reproducible.

#### `step` — record a unit of work that reached a terminal outcome
```jsonc
{ "op": "step", "id": "charge", "hash": "3f2a91c4", "data": { "tx": "ch_1" },
  "started_at": "…", "ended_at": "…", "meta": { "tokens_in": 812, "cost_usd": 0.014 } }
```
The SDK executed the closure during this attempt. The server MUST persist `data` and schedule the next attempt immediately.

A step whose body raised **non-retryably** is also an outcome, and carries `error`
instead of `data`:

```jsonc
{ "op": "step", "id": "charge", "hash": "3f2a91c4",
  "error": { "code": "card_declined", "message": "…" } }
```

The server MUST record it with status `failed` and MUST make it visible in the
journal and in `steps` on subsequent attempts, where the SDK replays it as the
error the body raised rather than re-executing it.

An SDK MUST NOT send `error` on a step op for a **retryable** failure. A retry
means re-executing the body, and a recorded step is memoised: the next attempt
would replay the error instead of the closure and the retry would never happen.
A retryable failure is reported with the `error` op (§5.2.2), which the server
does not commit.

#### `sleep` — durable timer
```jsonc
{ "op": "sleep", "id": "cooldown", "hash": "a71bd0e2", "until": "2026-09-01T00:00:00Z" }
```
Either `until` (RFC 3339) or `duration` (ISO 8601, e.g. `P7D`). Server schedules the next attempt at wake time. Result on replay is `null`.

#### `wait_event` — suspend for a matching event
```jsonc
{ "op": "wait_event", "id": "approval", "hash": "c0ff3312",
  "event": "order.approved",
  "expr": "event.data.order_id == run.key_suffix",
  "timeout": "P7D",
  "since": "run_start",
  "prompt": { "title": "Approve refund", "detail": { "amount": 4200 } } }
```
`prompt` is OPTIONAL, purely presentational, and surfaces the pending decision in the console. On match the event is memoized as the step result; on timeout the result is `null` with `"timed_out": true`.

`since` controls the **matching window** and defaults to `run_start` (see §7.6). Values:
`run_start` (match events received at or after the run began), `registration` (only events
after this op commits), or an RFC 3339 timestamp. `run_start` is the safe default and the
one that prevents the lost-signal race.

#### `invoke` — call another function as a child run
```jsonc
{ "op": "invoke", "id": "refund", "hash": "9d1e77aa",
  "function": "refunds/issue", "input": { "order": 4711 },
  "timeout": "PT1H", "detach": false }
```
`detach: true` fires and forgets (result memoized immediately as the child run id).

#### `signal` — send a signal to another run
```jsonc
{ "op": "signal", "id": "notify", "hash": "b2…", "target_run": "01J8…", "event": { /* CloudEvent */ } }
```

#### `continue_as_new` — close this run and start a successor

```jsonc
{ "op": "continue_as_new", "id": "next-cycle", "hash": "d4…",
  "input": { "cursor": 41200 }, "carry": ["subscription_id"] }
```

Closes the current run as `completed` and atomically creates a successor with the same
function, key and lineage. Journal state is **not** carried over: the successor starts with
an empty step map, which is the point — this is how unbounded loops keep run state finite.

* `input` becomes the successor's `run.input`.
* `carry` OPTIONALLY names step ids whose results are copied into the successor's input under
  `carried`, as a convenience.
* `lineage_id` is preserved across the chain; `chain_position` increments.
* The successor inherits the key, so keyed ordering is unbroken — no other run may slip in
  between predecessor and successor.
* Servers MUST enforce a maximum chain length (default 100 000) and fail with
  `chain_limit_exceeded`.
* MUST appear alone in the `ops` array.
* **Live children.** If any non-detached child run is still in flight, the server MUST
  reject the op with `continue_as_new_with_live_children` (non-retryable) rather than
  complete the predecessor. The child's result would otherwise be delivered into a journal
  that `continue_as_new` discards, and the child would be orphaned by the cascade rules in
  §7.5, which cover cancellation and failure but not continuation. Detached children are
  unaffected and do not block the op. Under blocking `invoke` semantics a conforming
  handler cannot reach this state, so the rule is a defensive invariant that keeps a future
  non-blocking invoke from reintroducing the hazard silently.

Runs SHOULD continue-as-new when a loop exceeds a few hundred steps or run state approaches
the inline-state limit. SDKs SHOULD warn when either threshold is crossed.

#### `done` — handler returned
```jsonc
{ "op": "done", "data": { "shipped": true } }
```

#### `error` — handler raised
```jsonc
{ "op": "error", "retryable": true, "step": "charge",
  "error": { "code": "gateway_down", "message": "…", "stack": "…", "retry_after": "PT30S" } }
```
`retryable: false` terminates the run immediately as `failed`.

### 5.2 Parallel ops

An SDK that discovers several independent steps in one pass MAY return them together:

```jsonc
{ "ops": [ { "op": "step", "id": "a", "hash": "…", "data": … },
           { "op": "invoke", "id": "b", "hash": "…", "function": "…" } ] }
```

Constraints:
* All ops in one envelope MUST have distinct hashes.
* `done` and `continue_as_new` MUST appear alone.
* `error` MUST be the last op in the envelope, and a **retryable** `error` MUST appear
  alone (§5.2.2).
* The server commits the batch atomically. Each op then resolves **independently** — a
  `step` op is already resolved on commit; `sleep`, `wait_event` and `invoke` resolve later.

#### 5.2.1 Partial failure within a batch

Ops in a batch are independent units, each with its own retry policy. A batch has
exactly one resolution rule, and it is not configurable:

**Every op runs to a terminal state. No op cancels a sibling. Every outcome is
recorded.**

Rules:
* The next attempt is scheduled when every op in the batch has reached a terminal
  state — completed, failed, timed out or cancelled.
* A failed op does not by itself fail the run. Its outcome is recorded like any
  other, and the handler decides what it means.
* An SDK MUST NOT hide a failed sibling's outcome from the handler. It may choose
  how to present one — propagating the first error is a reasonable ergonomic
  choice — but a batch member that failed must be discoverable, because a
  workflow that silently continues past a failed charge is the failure this
  specification exists to prevent.
* Retries of an individual op do not re-dispatch the whole batch.

> **Earlier revisions offered `all` and `any` join policies on the envelope.**
> They were removed rather than implemented (ADR-023). Both are defined in terms
> of *cancelling siblings*, and in this execution model there is usually nothing
> left to cancel: an SDK claims every hash in a group and runs every body before
> emitting the batch, so by the time a server sees a `join` field the work has
> already happened. `all` would cancel work that already ran; `any` would discard
> completed results, which is a durable execution engine deliberately forgetting
> what it did.
>
> The policies are only meaningful for members that are still in flight when the
> batch commits — `invoke` children and `wait_event` timers. If that pattern is
> wanted (racing two providers, say), it should be specified narrowly for those
> op kinds and implemented through the §7.5 cascade, not as a general envelope
> flag whose meaning changes with what is in the batch.
>
> `join` is a **retired field** (§11): servers MUST reject an envelope carrying
> one, naming it. Ignoring it — which §11's rule for genuinely unknown fields
> would otherwise require — is what would let an app believe it had requested a
> policy that never took effect.

#### 5.2.2 Recording work alongside a failure

A pass can record work and then fail. A parallel group in which one member raises
is the ordinary case: the other members' bodies have already run and returned.

An envelope therefore MAY carry recording ops followed by an `error` op. The
server MUST apply them in order — record every op, then apply the error — so that
the run fails with its journal complete.

An envelope that could not express this would force the SDK to drop one of the
two. Dropping the ops loses executed work, which is the failure this
specification exists to prevent; dropping the error loses the reason the run
stopped. Neither is acceptable, so neither is required.

Two rules keep the combination unambiguous:

* **`error` last.** An `error` before other ops would have the server fail the
  run and then go on recording steps into it. Position is the only thing that
  says which happened first.
* **A retryable `error` travels alone.** Retrying is a decision about *whether to
  re-execute*; committing is a decision about *what has already happened*. A
  server that retries does not commit the attempt, so an envelope asking for both
  has no coherent reading. An SDK holding recorded ops and a retryable failure
  MUST emit the ops on their own; the failure is raised again on the next
  attempt, where the recorded work is memoised, nothing new is recorded, and the
  error can be sent alone. This terminates, because each attempt records strictly
  less than the last.

`done` and `continue_as_new` stay exclusive for the opposite reason. Recording a
step ends the pass, so a handler cannot both record a step and return; ops
alongside them mean the handler swallowed a step result, and an SDK MUST report
that rather than commit a `done` for a run whose middle never happened.

## 6. Step identity, hashing and versioning

```
hash = base16( sha256( function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ decimal(occurrence) )[0..8] )
```

* `step_id` is the developer string. Occurrence is a per-`step_id` counter, starting at 0, incremented each time the SDK *encounters* that id during a single replay pass (this is what makes steps inside loops work).
* Counters MUST be reset at the start of every attempt.
* The server treats hashes as opaque keys. It never re-derives them.

### 6.1 Occurrence assignment under concurrency

Counter assignment by "execution order" is unsafe the moment a handler runs steps
concurrently: two tasks racing to claim occurrence 0 and 1 will swap between attempts, the
hashes will differ, and completed work will silently re-execute. **This is the single most
dangerous failure mode in the whole design**, because it corrupts quietly rather than
erroring.

Normative rules:

1. Occurrence MUST be assigned in **program order at the point of the call**, in a single
   sequential replay pass, before any concurrency is introduced. An SDK MUST NOT assign an
   occurrence from inside a concurrently scheduled task.

   For SDKs in languages with deferred execution (futures, promises, coroutines), this means
   the claim MUST happen when the step function is **called**, not when the returned
   future/promise is first **polled or awaited**. Claiming at poll time ties the occurrence
   to scheduler order, so combinators such as `join`/`Promise.all` silently produce different
   hashes on different attempts. Claiming at call time makes poll order irrelevant, which
   removes the hazard rather than merely detecting it. A memoized step SHOULD NOT construct
   its work at all.
2. An SDK that offers concurrency MUST provide a **structured parallel construct** that
   assigns every hash up front, sequentially, and only then runs the work:
   ```rust
   let (a, b) = ctx.parallel((
       ctx.step("fetch-invoice", || async { … }),   // occurrence assigned here, in order
       ctx.step("fetch-customer", || async { … }),
   )).await?;
   ```
3. Inside a parallel construct, step ids MUST be unique. Repeating an id within one group is
   a non-retryable `ambiguous_step_id` error. Loops that fan out MUST supply an explicit
   discriminator (`format!("charge-{invoice_id}")`), not rely on the counter.
4. An SDK MUST detect an occurrence claimed off the sequential path — for example by binding
   the counter to a replay-pass token that concurrent tasks do not carry — and fail the
   attempt non-retryably rather than emit a guessed hash.
5. SDKs SHOULD offer an opt-in strict mode that hashes the *sequence* of step ids seen in a
   pass and warns when it differs from the previous attempt, catching accidental
   non-determinism in user code.

Conformance suite `loops` and `parallel` both assert these properties (§12).

**Versioning rule.** Because identity is `(step_id, occurrence)`, changing code around steps is safe:

| Change | Effect on in-flight runs |
|---|---|
| Add a step *after* the current position | Executes normally |
| Add a step *before* completed steps | Executes on next attempt, then existing memoized steps replay |
| Remove a step | Its stored result is orphaned and ignored |
| Rename a step id | Treated as a new step; the old result is orphaned (re-executes side effects — **avoid**) |
| Reorder steps | Safe unless it changes occurrence counters for a repeated id |

SDKs SHOULD warn when a memoized hash present in `steps` is not encountered during a replay pass (`orphaned step`) and MUST include the count in `logs`.

## 7. Delivery semantics

### 7.1 Guarantees
* **Steps: at-least-once execution, exactly-once recorded effect.** A step closure MAY run more than once (crash after execution, before commit). Once a result is recorded it is never re-executed.
* **Events: at-least-once delivery, deduplicated by `id`+`source` within the ingest window.**
* **Ops commit: atomic.** Op results, emitted events (`emit`) and the next-attempt schedule are one transaction.

#### 7.1.1 Abandoned steps and cancel safety

A step closure may be executing when the attempt deadline expires, the connection drops, or
the process dies. The step's *effect* may or may not have happened; the server cannot know.

* The server MUST treat an attempt with no response as **unknown**, not failed. It retries
  the attempt; the step re-executes. This is the at-least-once guarantee in §7.1, and it is
  why every step effect must be idempotent (§7.2).
* An SDK MUST NOT cancel or drop a running step future in order to meet the attempt
  deadline. Dropping a future mid-await in Rust leaves the external effect in an
  indeterminate state *and* loses the result that would have made it durable. The SDK MUST
  instead let the closure run to completion and abandon the HTTP response, or refuse to
  start work it cannot finish within the deadline.
* Consequently `timeouts.attempt` MUST exceed the slowest expected single step, and SDKs
  SHOULD warn at start-up when a step's own timeout exceeds the attempt deadline.
* If a step's result arrives after the server has already re-dispatched (stale fence), the
  response is rejected with `409` and discarded (§7.3). The work was done twice; the record
  is written once.
* Steps whose effects cannot be made idempotent SHOULD use `ctx.idempotency_key()` with a
  provider-side idempotency mechanism, or be split into a reserve/confirm pair.

### 7.2 Idempotency
Steps whose effects are not naturally idempotent SHOULD derive an idempotency key from `run.id` + `hash` — both are stable across re-execution of the same step. SDKs MUST expose these to user code.

### 7.3 Fencing
Each attempt carries a monotonically increasing `fence`. If the server has already advanced the run past this attempt (e.g. the previous attempt was assumed lost and re-dispatched), it MUST reject the response with `409` and discard its ops. Apps MUST NOT treat `409` as an error to retry.

### 7.4 Cancellation

On cancel the server stops scheduling ordinary attempts. If the function config
sets `on_cancel: true`, the run enters a **compensation phase**: it is dispatched
again with `run.cancelling: true`, and the SDK runs only the compensation path.
Compensation steps memoize normally.

The phase gets as many attempts as the compensation path needs — one new step per
attempt means a three-step undo takes three. An earlier wording said "one final
attempt", which read as a single dispatch and is not implementable alongside
memoisation; the run is finished when the compensation path returns, not when the
first attempt does.

A run in the compensation phase **keeps its business key**. Releasing it when
cancel was requested would let the next run on that key start while the previous
one is still undoing things — two runs interleaved on one key, which is the
failure keyed ordering exists to prevent, and which would appear only when a
cancel raced an event.

However the compensation path ends, the run's final status is `cancelled`. A
compensation that itself failed is a fact about the compensation, recorded in the
run's `error`; it does not make the run `failed`, because an operator counting
cancelled runs should not have to know which of them had an undo that threw.

Servers MUST NOT dispatch a compensation attempt for a function that does not
declare `on_cancel`.

### 7.5 Cascade and the invoke tree

A run and its non-detached children form a tree. Terminal state propagates as follows:

| Event | Effect on tree |
|---|---|
| Parent cancelled | All non-detached descendants are cancelled, depth-first, before the parent reaches `cancelled`. Each runs its own `on_cancel` path (§7.4). |
| Parent run timeout | Same as cancellation, with reason `parent_timeout`. |
| Parent fails | Non-detached descendants are cancelled; the parent's terminal state is unaffected by their outcomes. |
| Child fails | The parent's `invoke` step resolves as failed and is retried per the **parent's** retry policy for that step. Retrying starts a *new* child run; the failed child stays visible for audit. |
| Child times out (`invoke.timeout`) | The child is cancelled; the parent's step resolves `timed_out`. |
| Parent continues as new | Not permitted while a non-detached child is live: the op is rejected (§5.1). Detached children are unaffected and survive the transition. |
| Detached child | Unaffected by any parent transition, in either direction. Its lifecycle is independent from the moment it is created. |

Limits, enforced by the server:
* Tree depth: default 10, error `invoke_depth_exceeded` (non-retryable).
* Live descendants per run: default 1 000, error `invoke_fanout_exceeded`.
* A run MUST NOT invoke an ancestor with the same key; the server rejects it with
  `invoke_cycle` rather than deadlocking on keyed ordering.
* Cancellation propagation is itself durable and resumable: a server crash mid-cascade
  resumes the cascade, it does not orphan descendants.

### 7.6 Early signals and the lost-signal race

A run that has not yet reached `wait_event` may be sent the very event it is about to wait
for. Naïve implementations lose it and the run waits forever. The server MUST NOT lose such
an event.

**Mechanism.** Every run has a durable **inbox**. An event enters the inbox when it is
directed at the run — via a `signal` op targeting the run id or key, or via a matching
correlation on the run's key — regardless of whether a wait is currently registered.

**Mutual exclusion.** Delivery into the inbox and registration of a wait MUST be mutually
exclusive, and a server MUST make that exclusion explicit rather than relying on an
incidental one. (Implementation note: on PostgreSQL a foreign key from the inbox to the run
already takes a conflicting row lock, which closes the race by accident; a schema change
that removes or defers it would silently reopen an R1 race. Take the run row lock
deliberately, as the first statement of both paths.)

When a `wait_event` op commits, the server MUST, in the same transaction:
1. Evaluate the inbox for entries matching `event` and `expr`, restricted by `since`.
2. If one or more match, resolve the wait immediately with the **earliest** matching entry
   and consume it. The next attempt is scheduled at once; the run never suspends.
3. Otherwise register the wait and suspend.

With `since: run_start` (the default) the window opens when the run begins, so an event that
arrives at any point during the run — before or after the wait is registered — is matched.

**Consumption rules.**
* An inbox entry is consumed by at most one wait. Two waits for the same event type resolve
  against two distinct entries, in arrival order.
* Entries not consumed by the time the run reaches a terminal state are discarded.
* Inbox depth is bounded (default 1 000 entries per run) and retained for the run's lifetime.
  Overflow drops the **oldest** entry and increments a per-namespace `inbox_overflow` metric;
  servers SHOULD alert on it, because it means a run is being signalled faster than it consumes.
* Inbox entries are subject to the namespace payload and retention policies.

**Ordering.** Entries are ordered by server receipt time, not producer timestamp. A wait
resolving against the inbox consumes the earliest match, so signals are processed FIFO.

**Idempotent producers.** A `signal` op carries the sender's step hash; redelivery of the same
`(sender_run, step_hash)` is deduplicated at the inbox, so a retried sender never
double-signals.

## 8. Payloads

### 8.1 Payload tiers

stepd is a control plane. Bulk data MUST NOT traverse the server. Three tiers, selected by size:

| Tier | Size | Treatment | Who owns the bytes |
|---|---|---|---|
| **Inline** | < 1 MiB | JSON value in the step result | stepd (in Postgres) |
| **Managed blob** | 1 MiB – 100 MiB | `$blob` reference; bytes in the configured blob store, uploaded directly by the app | stepd (refcounted, GC'd) |
| **External reference** | > 100 MiB | `$ref` pointer to an object the application already owns | the application |

Thresholds are server configuration; the values above are defaults. An app MAY use a
higher tier than the size requires, and SHOULD do so for anything it already stores
elsewhere. Video, raw scans and archives SHOULD use `$ref`.

### 8.2 Limits

| Item | Default limit | Configurable |
|---|---|---|
| Attempt request body | 4 MiB | yes |
| Single step result, inline | 1 MiB | yes |
| Managed blob | 100 MiB | yes |
| Total inline run state | 32 MiB | yes |
| Blob references per run | 1 000 | yes |
| Steps per run | 10 000 | yes |

Exceeding a limit is a non-retryable error (`payload_too_large`); the SDK MUST surface it
naming the offending step id, and SHOULD suggest the next tier.

### 8.3 Managed blobs

#### 8.3.1 Reference shape
```jsonc
{ "$blob": {
    "id": "01926f5a-2200-7aaa-8000-0123456789ab",
    "size": 4194304,
    "sha256": "e3b0c442…",
    "content_type": "image/png",
    "filename": "receipt-4711.png",     // optional, presentational
    "url": "https://…?sig=…"            // short-lived, added by the server on read
  } }
```

`url` is **never** supplied by the app and MUST NOT be persisted by the SDK. The server
mints it per attempt with a default TTL of 300 s.

#### 8.3.2 Direct upload (two-phase)

Bytes MUST NOT be POSTed to the stepd server. The app reserves, uploads directly to the
blob store, then returns the reference:

```
1. app  → server   POST /v1/blobs:reserve
                   { "run_id": "…", "size": 4194304, "sha256": "e3b0…",
                     "content_type": "image/png" }
2. server → app    201 { "blob_id": "01926f…", "upload_url": "https://…",
                        "method": "PUT", "headers": { … }, "expires_at": "…" }
3. app  → store    PUT {upload_url}  (bytes; never through stepd)
4. app  → server   the step op result contains { "$blob": { "id": "01926f…", … } }
```

Rules:
* The server MUST verify `size` and `sha256` before the blob becomes readable; a mismatch
  fails the commit with `blob_digest_mismatch` and the ops are discarded.
* Reserved-but-uncommitted blobs are garbage collected after a configurable window (default 24 h).
* Blobs are **content-addressed**: reserving a `sha256` that already exists in the namespace
  MAY return the existing `blob_id` with no `upload_url`, and the app MUST skip the upload.
* If the blob store cannot issue presigned URLs, the server MAY expose a relay endpoint
  (`PUT /v1/blobs/{id}/content`). This is a compatibility fallback, not the default path,
  and servers SHOULD warn when it is used.

#### 8.3.3 Reading
* SDKs MUST fetch lazily: constructing or replaying a `$blob` value costs nothing; bytes
  are fetched only when user code dereferences it. This is what keeps replay cheap — a run
  with forty blob-bearing steps re-downloads nothing on attempt forty-one.
* SDKs SHOULD expose a streaming reader and MUST support HTTP `Range`, so a step can read
  a file header without pulling the whole object.
* SDKs SHOULD cache fetched bytes for the duration of a single attempt only.

#### 8.3.4 Lifecycle
* The server maintains a reference from every step result, run input and emitted event that
  contains a `$blob`.
* Bytes are deleted when the reference count reaches zero **and** the namespace retention
  window for the referencing runs has elapsed — whichever is later.
* A blob referenced by a parent run and a child run survives until both are collected.
* Deleting a run does not delete bytes still referenced elsewhere.
* Replaying an event or retrying a run does not duplicate bytes (content addressing).

### 8.4 External references

For data the application already stores, or anything above the managed-blob ceiling, the
step result carries a pointer and stepd stores no bytes at all:

```jsonc
{ "$ref": {
    "uri": "s3://media-raw/orders/4711/walkthrough.mp4",
    "size": 2147483648,
    "sha256": "9f86d081…",              // optional but RECOMMENDED
    "content_type": "video/mp4",
    "meta": { "duration_s": 812, "codec": "h264" }   // opaque to the server
  } }
```

* The server treats `$ref` as an opaque value: it never fetches, validates, transcodes or
  inspects it, and never mints credentials for it.
* Access is the application's concern. The console displays the URI and metadata, and
  renders a preview only if the operator's browser can already reach it.
* `$ref` is the RECOMMENDED representation for video, medical imaging, backups and any
  payload whose lifecycle the application already manages.

### 8.5 Engine neutrality

The server MUST NOT transcode, resize, decompress, parse or otherwise interpret payload
bytes in any tier. Media processing is a step's job. The engine's only interactions with
blob content are: digest verification on commit, presigning on read, and deletion on GC.

### 8.6 State truncation
If `steps` exceeds the request limit the server sends the most recent window, sets
`state_truncated: true` and provides `GET /v1/runs/{id}/steps?after=…`. SDKs MUST fetch the
remainder before replaying, or fail the attempt with a non-retryable error.

## 9. Security

* **Signature.** `stepd-signature: t=<unix>,v1=<hex>` where `hex = HMAC-SHA256(key, "<t>.<raw_body>")`. Receivers MUST reject if `|now − t| > 300s` or the MAC does not match, using a constant-time comparison. Both directions sign; keys are per-app and rotatable (two active keys during rotation, `v1=` may appear twice).
* **Replay protection.** The timestamp window alone permits replay of a captured body for its
  duration. Receivers MUST additionally maintain a nonce cache: the `stepd-nonce` header
  carries a unique value per request, is covered by the MAC, and MUST be rejected if seen
  before within the window. Servers and SDKs both enforce this. A bounded in-memory cache
  sized to the window is sufficient; app replicas MAY each keep their own, since a replay to
  a different replica still cannot produce a duplicate *recorded* effect (fencing, §7.3).
* Apps MUST reject unsigned requests outside development mode.
* **SSRF.** The server dereferences app-supplied URLs (`app.url`, discovery, relay). Servers
  MUST apply an egress policy to every such fetch: deny link-local and cloud metadata ranges
  (169.254.0.0/16, fd00:ec2::/32), deny loopback and private ranges unless explicitly
  allowlisted, resolve DNS once and connect to the resolved address to prevent rebinding,
  cap redirects at zero, and cap response size. Multi-tenant deployments MUST require an
  operator-configured allowlist per namespace.
* Servers MUST NOT log raw payloads at default log levels; payload redaction is per-namespace policy.
* Blob URLs are pre-signed, short-lived (default 300 s), single-purpose (read or write, never both) and MUST NOT be logged, persisted by SDKs, or included in console links that outlive the page view.
* Upload URLs are scoped to the reserved `blob_id`, the declared `content_type` and the declared `size`; the blob store MUST reject a PUT that exceeds the reserved size.
* The server verifies `sha256` before a blob is readable, so a compromised upload URL cannot substitute different content for a committed reference.
* Content-addressed deduplication is scoped **per namespace**, so a digest cannot be used to probe for another tenant's data.
* `$ref` URIs are opaque to the server: it never resolves them, and never holds credentials for the stores they point at.

## 10. Expression language

All `*_expr` fields are **CEL** (Common Expression Language). Available bindings:

| Binding | Type | Available in |
|---|---|---|
| `event` | map (CloudEvent, with `data`) | triggers, key, cancel_on, wait_event |
| `events` | list | batched triggers |
| `run.id`, `run.key`, `run.key_suffix`, `run.function_id` | string | cancel_on, wait_event |
| `now` | timestamp | all |

Expressions MUST evaluate within 1 ms and MUST be side-effect free. Evaluation errors are treated as `false` (non-match) and reported as function health warnings.

## 11. Versioning and compatibility

* Protocol version is a single integer, sent in `stepd-protocol`. Servers MUST support version N and N−1 for at least 12 months.
* Additive fields do not bump the version. Receivers MUST ignore unknown fields and MUST NOT reject unknown op `meta` keys.
* **Retired fields are the exception, and MUST be rejected rather than ignored.**
  The ignore rule above exists so a receiver tolerates a field added by a newer
  minor — a field it has no opinion about. A retired field is one that *did* have
  a meaning and no longer does, and ignoring it lets a sender believe it
  requested behaviour that never took effect. That is the failure mode retirement
  is usually correcting, so it must not survive the correction.

  | Field | Where | Retired in | Why |
  |---|---|---|---|
  | `join` | `AttemptResponse` | rev 1.2, ADR-023 | The `all` and `any` policies were never implementable in this execution model; every batch always resolved as `all_settled` (§5.2.1). |

  A receiver rejecting a retired field MUST name it, so the sender learns what to
  remove rather than that its envelope was malformed.
* Unknown **op type** received by a server: reject the attempt with `400` and a Problem Details body naming the op; the run fails non-retryably. SDKs therefore MUST NOT emit ops above the negotiated version.

## 12. Conformance

`stepd conformance --app <url>` drives an SDK through a fixed battery and asserts observable behaviour:

| Suite | Asserts |
|---|---|
| `memoization` | Second attempt does not re-execute a recorded step |
| `loops` | Occurrence counters produce stable hashes across attempts |
| `determinism` | Hashes assigned in program order; a step created off the sequential path fails non-retryably; repeated id inside a parallel group raises `ambiguous_step_id` (§6.1) |
| `parallel` | Multiple ops in one envelope, atomic commit, and a failing member neither cancels its siblings nor hides their outcomes |
| `sleep` | Timer accuracy, replay after wake, `null` result |
| `wait` | Match, timeout, expression binding, `since` windows |
| `early_signal` | Event sent **before** the wait is registered still resolves it; FIFO consumption; one entry consumed by one wait; sender dedupe (§7.6) |
| `invoke` | Child run, result memoization, detach, depth and fan-out limits, cycle rejection |
| `cascade` | Parent cancellation cancels non-detached descendants and leaves detached ones running (§7.5) |
| `continue_as_new` | Successor starts with empty state, keeps key and lineage, no run interleaves on the key |
| `errors` | Retryable vs non-retryable, backoff honoured, `on_failure` |
| `cancel` | `on_cancel` path executes exactly once |
| `abandonment` | Attempt abandoned mid-step re-executes the step and records it once (§7.1.1); SDK does not drop the future |
| `blobs` | Large payload round-trip, lazy fetch, `Range` reads, digest mismatch rejected, dedupe skips upload |
| `refs` | `$ref` values pass through untouched and are never dereferenced by the server |
| `fencing` | Stale fence response rejected and ignored, and not retried by the SDK |
| `signature` | Rejects bad MAC, expired timestamp, **replayed nonce** |
| `truncation` | Paginated state fetch and correct replay |
| `cron` | Catch-up policies, misfire window, DST spring-forward skip and fall-back single fire |

An implementation is **conformant at level 1** if it passes `memoization`, `loops`,
`determinism`, `sleep`, `errors`, `abandonment` and `signature`; **level 2** adds the rest.
`determinism` and `early_signal` are level-1 and level-2 gates respectively because they
guard the two silent-corruption failure modes in the design.

### 12.1 What the app under test must expose

The table above says what each suite asserts. It does not, on its own, let anyone
build an implementation that the battery can run — which makes "a third party can
implement this specification" a claim with nothing behind it. This section is the
missing half: the fixed surface an app MUST expose to be testable.

An app under test serves the ordinary attempt endpoint and manifest (§3), and
additionally:

**`GET {app_url}/.well-known/stepd-conformance`** — a `ConformanceManifest`
(`schemas/conformance-manifest.schema.json`):

```jsonc
{
  "protocol": "1",
  "sdk": "rust/0.1.0",
  "suites": ["memoization", "loops", "determinism", "sleep", "errors",
             "abandonment", "signature"]
}
```

`suites` is what this app claims to implement. Declaring a suite means every
function that suite requires (§12.2) is registered and behaves as specified. A
runner MUST NOT report a level as passed unless every suite in that level was
declared *and* run *and* passed: an undeclared suite is an unknown, not a pass.
An implementation that supports only level 1 declares only the level-1 suites and
is reported as level 1, rather than failing eleven suites it never claimed.

**`GET {app_url}/_conformance/effects?run={run_id}`** — the effect log:

```jsonc
{ "effects": ["charge", "ship"] }        // execution order, append-only
```

This endpoint is why the battery can test anything at all. The headline guarantee
is "a recorded step is never re-executed", and that is **not observable in server
state**: the journal after one execution and after two executions of the same
step body is byte-identical. Something has to report what the handler actually
ran, and only the app can. An app is free to implement the log however it likes;
it MUST be per-run, ordered, and MUST record an entry each time a step *body*
executes rather than each time a step is recorded.

**`POST {app_url}/_conformance/reset`** — clears every effect log. A runner calls
this once before the battery so that a rerun does not inherit the previous one.

**`POST {app_url}/_conformance/configure`** — REQUIRED of an app declaring
`blobs` or `truncation`, OPTIONAL otherwise. The runner calls it once, after its
own API is serving and before any case runs:

```jsonc
{ "api_base": "http://127.0.0.1:54321", "token": "…" }        // operator role
```

Any 2xx acknowledges it. The app keeps both for the run and uses them to call
back into the server: `POST {api_base}/v1/blobs:reserve` for the two-phase upload
(§8.3.2), and `GET {api_base}/v1/runs/{id}/steps` to page a truncated journal
(§8.6). Both are `Authorization: Bearer {token}`.

This endpoint exists because **there is no earlier moment**. Whoever launches the
app cannot supply these: a runner binds its API to an ephemeral port and mints
the token itself, so neither value exists until the app under test is already
running and has had its manifest read. An app that could not be told them could
not implement two of the nineteen suites, whatever its SDK did — which for a
year meant those suites were reachable only by an app the runner started
in-process, and therefore only by one written in the runner's own language.

A runner MUST fail the whole run, naming configuration as the cause, if the call
does not succeed. Letting it fail quietly makes `blobs` report a protocol
divergence when the real fault is a wrong URL, and an SDK author would go looking
in their blob client.

**Hazards prevented by construction.** A manifest MAY also carry:

```jsonc
{ "statically_prevented": ["offpath_claim"] }
```

This exists because the battery as first written was unsatisfiable by the best
implementations. `determinism` requires that "a step created off the sequential
path fails non-retryably" — but an SDK whose context type cannot cross a task
boundary makes that program **fail to compile**, and a runtime assertion cannot
be made about a program that does not exist. Demanding a runtime failure would
mean the strongest defence scores worse than a weaker one, which is a test
rewarding the wrong thing.

So an implementation that makes a hazard unrepresentable declares it, omits the
corresponding function, and the runner records the case as satisfied **by
construction** — labelled distinctly, because it is a claim the runner is
relaying rather than one it checked. An implementation that cannot prevent the
hazard statically registers the function and must fail non-retryably at run time.

The only defined value is `offpath_claim`: creating a step outside the
sequential replay pass (§6.1 rule 4). Others require a specification change,
because a runner must never be able to be told to skip an assertion by a value
it does not recognise.

> These endpoints expose execution detail and take no authentication. An app MUST
> NOT serve them outside conformance mode. An SDK that offers a conformance app
> SHOULD make it a separate binary or an explicit opt-in flag, never a default
> route on the app that serves production traffic.

### 12.2 The function battery

Every function is triggered by an event of the same name unless stated. Handlers
branch on `attempt` from the `AttemptRequest` (§4), which is what lets a case
force a second attempt deterministically rather than by racing a timeout.

| Function | Suite | Required behaviour |
|---|---|---|
| `conf-memoize` | `memoization` | Step `work` (effect `work`); on attempt 1 return a **retryable** error; step `after` (effect `after`); done. |
| `conf-loops` | `loops` | Five steps with generated ids `item-0`…`item-4`, each recording its own effect; a retryable error after the third on attempt 1. |
| `conf-order` | `determinism` | Steps `a`, `b`, `c` in that program order, then done. |
| `conf-offpath` | `determinism` | Attempts to create a step outside the sequential pass. MUST fail **non-retryably**; MUST NOT record the step. Omitted by an app declaring `offpath_claim` in `statically_prevented`. |
| `conf-ambiguous` | `determinism` | Uses the same step id twice inside one parallel group. MUST raise `ambiguous_step_id` (§6.1) and fail non-retryably. |
| `conf-parallel` | `parallel` | Three steps in one envelope, each with an effect; then done with all three results. |
| `conf-parallel-partial` | `parallel` | Three steps in one envelope where the second fails non-retryably, each recording an effect before it resolves. |
| `conf-sleep` | `sleep` | Step `before`, `sleep` of `PT2S`, step `after`. The sleep op's recorded result MUST be `null`. |
| `conf-wait` | `wait` | `wait_event` on `conf.signal` with a `PT10S` timeout, binding `event.data.token`; done with the token. |
| `conf-wait-timeout` | `wait` | `wait_event` on an event that never arrives, timeout `PT2S`; done with `null`. |
| `conf-early-signal` | `early_signal` | Step `settle` (slow enough that the runner can deliver first), then `wait_event` on `conf.signal`. MUST resolve from the inbox. |
| `conf-invoke` | `invoke` | Invokes `conf-invoke-child` and returns its result. |
| `conf-invoke-child` | `invoke`, `cascade` | Trigger `invoke` only. Step `child-work` (effect `child-work`); done with its input echoed. |
| `conf-cascade` | `cascade` | Invokes `conf-cascade-child` (attached) and `conf-cascade-detached` (detached), then waits on `conf.signal`. |
| `conf-cascade-child` | `cascade` | Trigger `invoke`. Sleeps `PT30S`, then done. |
| `conf-cascade-detached` | `cascade` | Trigger `invoke`. Sleeps `PT30S`, then done. |
| `conf-continue` | `continue_as_new` | Keyed. Step `tick` (effect `tick-{n}`); `continue_as_new` while `n < 2`, else done. |
| `conf-cancel` | `cancel` | Step `work`, then `wait_event` on `conf.signal`. Its `on_cancel` path records effect `cancelled` exactly once. |
| `conf-errors-retryable` | `errors` | Returns a retryable error on attempts 1 and 2; succeeds on 3. |
| `conf-errors-terminal` | `errors` | Returns a non-retryable error on attempt 1. MUST NOT be retried. |
| `conf-abandon` | `abandonment` | On attempt 1, runs step `slow` (effect `slow`) and then **never responds**. On attempt 2, runs `slow` again and completes. |
| `conf-fencing` | `fencing` | Step `work`, then done. Driven twice by the runner, the second time with a stale fence. |
| `conf-refs` | `refs` | Returns a `$ref` value in its output, unchanged from its input. |
| `conf-truncation` | `truncation` | Creates more steps than the server ships inline, forcing a paginated state fetch. |
| `conf-cron` | `cron` | Trigger `cron`, not event. Step `tick` recording the occurrence from its input. |
| `conf-blobs` | `blobs` | Round-trips a payload larger than the inline limit: uploads it inside a step, uploads identical bytes a second time (which must deduplicate), then reads it back in full and by `Range`. |

### 12.3 What a run of the battery means

A conformance run reports, per suite: **passed**, **passed by construction**
(the app declared the hazard unrepresentable and the runner relayed that without
checking it), **failed** (with the assertion), **not declared** (the app did not
claim the suite), or **not implemented by this runner**.

The last of those exists because a runner that silently omits a suite and then
certifies a level is asserting something it did not check — the same failure this
specification's own structural tests exist to prevent. A runner MUST distinguish
"the app does not claim this" from "I did not test this", and MUST NOT certify a
level containing either.

### 12.4 What this battery does not cover

It tests an **app** against a server, and nothing else. A second implementation of
the *server* is equally within the protocol's claim and is not covered here: the
suites assert what an app must do when driven, not what a server must do when
driving. A server battery would need the mirror image — a fixed app that reports
what it was sent — and is not specified.

Nor does a passing run establish that the app's *own* engine is correct, only that
its observable behaviour under this server matches the specification.

## 13. Appendix A — worked exchange

Run of `order-fulfilment` keyed `order:4711`.

1. `order.created` ingested → run created → **attempt 1**, `steps: {}`.
   App replays: hits `charge` (no result) → executes → `{"ops":[{"op":"step","id":"charge","hash":"3f2a91c4","data":{"tx":"ch_1"}}]}`.
2. **Attempt 2**, `steps: {3f2a91c4: …}`. App replays `charge` from memo, reaches `ctx.sleep("cooldown","P1D")` → `{"ops":[{"op":"sleep","id":"cooldown","hash":"a71bd0e2","duration":"P1D"}]}`. Run suspends.
3. 24 h later, **attempt 3**. App replays two steps, reaches `wait_event("approval")` → suspends again.
4. Operator resolves the wait in the console → synthetic `order.approved` recorded → **attempt 4**. App replays three steps, executes `ship` → `step` op.
5. **Attempt 5**: handler returns → `{"ops":[{"op":"done","data":{"shipped":true}}]}`. Run `completed`.

## 14. Appendix B — schema index

| File | Describes |
|---|---|
| `app-manifest.schema.json` | Registration payload |
| `function-config.schema.json` | Function definition |
| `attempt-request.schema.json` | Server → app |
| `attempt-response.schema.json` | App → server (op envelope) |
| `op.schema.json` | All op variants |
| `event.schema.json` | CloudEvents 1.0 + stepd extensions |
| `problem.schema.json` | RFC 9457 error body |
