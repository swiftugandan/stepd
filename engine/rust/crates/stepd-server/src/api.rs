//! The management and read API the console consumes.
//!
//! Three rules run through every handler:
//!
//! * **Namespace is a `WHERE` clause, never a filter.** Every query is scoped in
//!   SQL. Fetching and then discarding would leak through counts and cursors.
//! * **Every mutating command goes through the same audited path the console
//!   uses** (F-UI-4). There is no privileged route for the UI, so anything an
//!   operator can do through the console, an operator can do with `curl` — and
//!   anything the console can do is in the audit log.
//! * **Cursor pagination, never `OFFSET`.** A deep page with `OFFSET` scans
//!   everything before it, so the hundredth page of an incident's failures is
//!   the slowest query in the system at the worst possible moment.

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::auth::{Principal, Role};
use crate::problem::{ApiResult, Problem};
use crate::ServerState;

/// Maximum page size, whatever the caller asks for.
const MAX_LIMIT: i64 = 200;

fn clamp(limit: Option<i64>) -> i64 {
    limit.unwrap_or(50).clamp(1, MAX_LIMIT)
}

#[derive(Debug, Serialize)]
/// A cursor-paginated response.
pub struct Page<T> {
    /// This page's rows.
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Cursor for the next page; absent when the list is exhausted.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
/// A run as it appears in a list.
pub struct RunSummary {
    /// Run id.
    pub id: String,
    /// Function this run executes.
    pub fn_id: String,
    /// Lifecycle state.
    pub status: String,
    /// Business key, when the function is keyed.
    pub key: Option<String>,
    /// When the run began.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When the run reached a terminal state.
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Attempts made so far.
    pub attempt_no: i32,
    /// Position within a `continue_as_new` chain.
    pub chain_position: i32,
    /// Rewound by a point-in-time restore (F-DL-5).
    ///
    /// Surfaced on the summary, not buried in the detail view, because after a
    /// restore an operator's first question is "which runs are suspect?" and
    /// the answer must be visible in a list.
    pub restored: bool,
}

#[derive(Debug, Serialize)]
/// One recorded step, as the console renders it.
pub struct StepView {
    /// Step identity.
    pub step_hash: String,
    /// Developer-supplied step id.
    pub step_id: String,
    /// Op kind that produced the step.
    pub op: String,
    /// Lifecycle state.
    pub status: String,
    /// Recorded result.
    pub result: Option<serde_json::Value>,
    /// Observability metadata, opaque to the engine.
    pub meta: Option<serde_json::Value>,
    /// Attempts made for this step.
    pub attempts: i32,
    /// When the run reached a terminal state.
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Error, for a failed step or run.
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
/// A run with its journal, pending waits and children.
pub struct RunDetail {
    #[serde(flatten)]
    /// The list-view fields.
    pub summary: RunSummary,
    /// Final output of a completed run.
    pub output: Option<serde_json::Value>,
    /// Error, for a failed step or run.
    pub error: Option<serde_json::Value>,
    /// Parent run, for a child.
    pub parent_run_id: Option<String>,
    /// Stable across a `continue_as_new` chain.
    pub lineage_id: String,
    /// The run's journal.
    pub steps: Vec<StepView>,
    /// Waits registered and not yet resolved.
    pub pending_waits: Vec<serde_json::Value>,
    /// Unconsumed inbox entries.
    pub inbox_pending: i64,
    /// Child run ids.
    pub children: Vec<String>,
}

#[derive(Debug, Deserialize)]
/// Query parameters for the run list and the DLQ.
pub struct RunFilter {
    /// Lifecycle state.
    pub status: Option<String>,
    /// Function this run executes.
    pub fn_id: Option<String>,
    /// Business key, when the function is keyed.
    pub key: Option<String>,
    /// Opaque cursor from a previous page.
    pub cursor: Option<String>,
    /// Page size; clamped by the server.
    pub limit: Option<i64>,
}

/// Every route the console and the CLI use.
pub fn router() -> Router<ServerState> {
    Router::new()
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{run_id}", get(get_run))
        .route("/v1/runs/{run_id}/steps", get(run_steps))
        .route("/v1/runs/{run_id}/cancel", post(cancel_run))
        .route("/v1/runs/{run_id}/retry", post(retry_run))
        .route("/v1/runs/{run_id}/resolve-wait", post(resolve_wait))
        .route("/v1/events", get(list_events).post(crate::ingest::ingest))
        .route("/v1/apps", axum::routing::put(crate::registry::register))
        .route("/v1/queue/stats", get(queue_stats))
        .route("/v1/dlq", get(dead_letter))
        .route("/v1/functions", get(list_functions))
        .route("/v1/schedules", get(list_schedules))
        .route("/v1/schedules/{id}/resume", post(resume_schedule))
        .route("/v1/metrics", get(metrics))
        .route("/v1/health", get(health))
}

fn row_to_summary(r: &sqlx::postgres::PgRow) -> RunSummary {
    RunSummary {
        id: r.get::<Uuid, _>("id").to_string(),
        fn_id: r.get("fn_id"),
        status: r.get("status"),
        key: r.get("key"),
        started_at: r.get("started_at"),
        ended_at: r.get("ended_at"),
        attempt_no: r.get("attempt_no"),
        chain_position: r.get("chain_position"),
        restored: r.get("restored"),
    }
}

async fn list_runs(
    State(state): State<ServerState>,
    principal: Principal,
    Query(f): Query<RunFilter>,
) -> ApiResult<Json<Page<RunSummary>>> {
    let limit = clamp(f.limit);
    let cursor = match &f.cursor {
        Some(c) => Some(
            Uuid::parse_str(c)
                .map_err(|_| Problem::bad_request("bad_cursor", "cursor must be a run id"))?,
        ),
        None => None,
    };

    // Keyset pagination on (started_at, id): the cursor row is looked up inside
    // the same namespace scope, so a cursor from another tenant selects nothing
    // rather than paging into their data.
    let rows = sqlx::query(
        r#"SELECT id, fn_id, status::text AS status, key, started_at, ended_at,
                  attempt_no, chain_position, (restored_at IS NOT NULL) AS restored
             FROM runs
            WHERE ns = $1
              AND ($2::text IS NULL OR status = $2::run_status)
              AND ($3::text IS NULL OR fn_id = $3)
              AND ($4::text IS NULL OR key = $4)
              AND ($5::uuid IS NULL OR (started_at, id) <
                   (SELECT started_at, id FROM runs WHERE id = $5 AND ns = $1))
            ORDER BY started_at DESC, id DESC
            LIMIT $6"#,
    )
    .bind(&principal.namespace)
    .bind(&f.status)
    .bind(&f.fn_id)
    .bind(&f.key)
    .bind(cursor)
    .bind(limit + 1)
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let next_cursor = if rows.len() as i64 > limit {
        Some(rows[limit as usize].get::<Uuid, _>("id").to_string())
    } else {
        None
    };
    let items = rows
        .iter()
        .take(limit as usize)
        .map(row_to_summary)
        .collect();
    Ok(Json(Page { items, next_cursor }))
}

async fn get_run(
    State(state): State<ServerState>,
    principal: Principal,
    Path(run_id): Path<Uuid>,
) -> ApiResult<Json<RunDetail>> {
    let row = sqlx::query(
        r#"SELECT id, fn_id, status::text AS status, key, started_at, ended_at,
                  attempt_no, chain_position, output, error,
                  parent_run_id, lineage_id, (restored_at IS NOT NULL) AS restored
             FROM runs WHERE id = $1 AND ns = $2"#,
    )
    .bind(run_id)
    .bind(&principal.namespace)
    .fetch_optional(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?
    // 404, never 403: see `Problem::not_found`.
    .ok_or_else(|| Problem::not_found("run_not_found", "no such run"))?;

    let steps = sqlx::query(
        r#"SELECT step_hash, step_id, op::text AS op, status::text AS status,
                  result, meta, attempts, ended_at, error
             FROM run_steps WHERE run_id = $1
            ORDER BY ended_at NULLS LAST, step_hash"#,
    )
    .bind(run_id)
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?
    .iter()
    .map(|r| StepView {
        step_hash: r.get("step_hash"),
        step_id: r.get("step_id"),
        op: r.get("op"),
        status: r.get("status"),
        result: r.get("result"),
        meta: r.get("meta"),
        attempts: r.get("attempts"),
        ended_at: r.get("ended_at"),
        error: r.get("error"),
    })
    .collect();

    let waits = sqlx::query(
        r#"SELECT step_hash, event_type, expires_at, prompt
             FROM waits WHERE run_id = $1 AND resolved_at IS NULL"#,
    )
    .bind(run_id)
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?
    .iter()
    .map(|r| {
        serde_json::json!({
            "step_hash": r.get::<String, _>("step_hash"),
            "event_type": r.get::<String, _>("event_type"),
            "expires_at": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("expires_at"),
            "prompt": r.get::<Option<serde_json::Value>, _>("prompt"),
        })
    })
    .collect();

    let inbox_pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM run_inbox WHERE run_id = $1 AND consumed_by_step_hash IS NULL",
    )
    .bind(run_id)
    .fetch_one(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let children: Vec<String> =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM runs WHERE parent_run_id = $1")
            .bind(run_id)
            .fetch_all(state.store.pool())
            .await
            .map_err(|e| Problem::internal(e.to_string()))?
            .into_iter()
            .map(|u| u.to_string())
            .collect();

    Ok(Json(RunDetail {
        summary: row_to_summary(&row),
        output: row.get("output"),
        error: row.get("error"),
        parent_run_id: row
            .get::<Option<Uuid>, _>("parent_run_id")
            .map(|u| u.to_string()),
        lineage_id: row.get::<Uuid, _>("lineage_id").to_string(),
        steps,
        pending_waits: waits,
        inbox_pending,
        children,
    }))
}

#[derive(Debug, Deserialize)]
/// Query parameters for a page of a run's journal.
pub struct StepsQuery {
    /// Return steps after this hash.
    pub after: Option<String>,
    /// Page size; clamped by the server.
    pub limit: Option<i64>,
}

/// A page of a run's journal (protocol §8.6).
///
/// The SDK calls this when an attempt arrives with `state_truncated`. Without it
/// a run at the step ceiling can only fail or be silently truncated, and silent
/// truncation makes the SDK re-execute steps whose results merely were not sent.
async fn run_steps(
    State(state): State<ServerState>,
    principal: Principal,
    Path(run_id): Path<Uuid>,
    Query(q): Query<StepsQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM runs WHERE id = $1 AND ns = $2")
        .bind(run_id)
        .bind(&principal.namespace)
        .fetch_optional(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;
    if exists.is_none() {
        return Err(Problem::not_found("run_not_found", "no such run"));
    }

    use stepd_core::traits::StateStore;
    let page = state
        .store
        .steps_page(run_id, q.after.as_deref(), clamp(q.limit))
        .await
        .map_err(Problem::from)?;

    Ok(Json(serde_json::json!({
        "steps": page.steps,
        "next": page.next,
    })))
}

// ---------------------------------------------------------------- commands

#[derive(Debug, Serialize)]
/// The outcome of an operator command.
pub struct CommandResult {
    /// Whether the command applied.
    pub ok: bool,
    /// What was done.
    pub command: String,
    /// What it was done to.
    pub target: String,
}

/// Record a command in the audit log.
///
/// Every mutation, from any caller, including the console. An audit log with
/// exceptions answers "who cancelled this run?" with "someone".
async fn audit(state: &ServerState, p: &Principal, command: &str, target: &str) {
    let _ = sqlx::query(
        "INSERT INTO commands_audit (ns, actor, command, target, request_id)
         VALUES ($1,$2,$3,$4,$5)",
    )
    .bind(&p.namespace)
    .bind(p.role.as_str())
    .bind(command)
    .bind(target)
    .bind(Uuid::new_v4().to_string())
    .execute(state.store.pool())
    .await;
}

async fn cancel_run(
    State(state): State<ServerState>,
    principal: Principal,
    Path(run_id): Path<Uuid>,
) -> ApiResult<Json<CommandResult>> {
    principal.require(Role::Operator)?;
    use stepd_core::traits::StateStore;

    let ok = state
        .store
        .cancel_run(&principal.namespace, run_id)
        .await
        .map_err(Problem::from)?;
    audit(&state, &principal, "cancel_run", &run_id.to_string()).await;

    if !ok {
        return Err(Problem::not_found(
            "run_not_found",
            "no such active run; it may already be complete, failed or cancelled",
        ));
    }
    Ok(Json(CommandResult {
        ok: true,
        command: "cancel_run".into(),
        target: run_id.to_string(),
    }))
}

async fn retry_run(
    State(state): State<ServerState>,
    principal: Principal,
    Path(run_id): Path<Uuid>,
) -> ApiResult<Json<CommandResult>> {
    principal.require(Role::Operator)?;
    use stepd_core::traits::StateStore;

    let ok = state
        .store
        .retry_run(&principal.namespace, run_id)
        .await
        .map_err(Problem::from)?;
    audit(&state, &principal, "retry_run", &run_id.to_string()).await;

    if !ok {
        return Err(Problem::not_found(
            "run_not_retryable",
            "no such run in a failed, cancelled or quarantined state",
        ));
    }
    Ok(Json(CommandResult {
        ok: true,
        command: "retry_run".into(),
        target: run_id.to_string(),
    }))
}

#[derive(Debug, Deserialize)]
/// Body of a `resolve-wait` command.
pub struct ResolveWaitIn {
    /// CloudEvents `type` to inject.
    pub event_type: String,
    #[serde(default)]
    /// Payload for the injected event.
    pub data: serde_json::Value,
}

/// Unstick a run parked on a wait by injecting a synthetic event.
///
/// Goes through `deliver_to_inbox`, the same path a real event takes — so the
/// early-signal rules, the FIFO ordering and the serialization lock all apply,
/// and an operator cannot produce a state a real event could not.
async fn resolve_wait(
    State(state): State<ServerState>,
    principal: Principal,
    Path(run_id): Path<Uuid>,
    Json(body): Json<ResolveWaitIn>,
) -> ApiResult<Json<CommandResult>> {
    principal.require(Role::Operator)?;

    let exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM runs WHERE id = $1 AND ns = $2")
        .bind(run_id)
        .bind(&principal.namespace)
        .fetch_optional(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;
    if exists.is_none() {
        return Err(Problem::not_found("run_not_found", "no such run"));
    }

    use stepd_core::traits::EventLog;
    let outcome = state
        .store
        .deliver(run_id, &body.event_type, &body.data, None)
        .await
        .map_err(Problem::from)?;
    audit(&state, &principal, "resolve_wait", &run_id.to_string()).await;

    use stepd_core::traits::Delivery;
    match outcome {
        Delivery::NoRun => Err(Problem::conflict(
            "run_inactive",
            "the run is no longer active, so nothing is waiting",
        )),
        other => Ok(Json(CommandResult {
            ok: true,
            command: format!("resolve_wait:{other:?}").to_lowercase(),
            target: run_id.to_string(),
        })),
    }
}

// ---------------------------------------------------------------- reads

#[derive(Debug, Deserialize)]
/// Query parameters for the event list.
pub struct EventFilter {
    #[serde(rename = "type")]
    /// CloudEvents `type` to inject.
    pub event_type: Option<String>,
    /// Page size; clamped by the server.
    pub limit: Option<i64>,
}

async fn list_events(
    State(state): State<ServerState>,
    principal: Principal,
    Query(f): Query<EventFilter>,
) -> ApiResult<Json<Page<serde_json::Value>>> {
    let rows = sqlx::query(
        r#"SELECT id, type, source, time, key, data, received_at
             FROM events
            WHERE ns = $1 AND ($2::text IS NULL OR type = $2)
            ORDER BY received_at DESC LIMIT $3"#,
    )
    .bind(&principal.namespace)
    .bind(&f.event_type)
    .bind(clamp(f.limit))
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let items = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<Uuid, _>("id").to_string(),
                "type": r.get::<String, _>("type"),
                "source": r.get::<String, _>("source"),
                "time": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("time"),
                "key": r.get::<Option<String>, _>("key"),
                "data": r.get::<serde_json::Value, _>("data"),
                "received_at": r.get::<chrono::DateTime<chrono::Utc>, _>("received_at"),
            })
        })
        .collect();
    Ok(Json(Page {
        items,
        next_cursor: None,
    }))
}

async fn queue_stats(
    State(state): State<ServerState>,
    principal: Principal,
) -> ApiResult<Json<serde_json::Value>> {
    use stepd_core::traits::Queue;
    let stats = state
        .store
        .stats(&principal.namespace)
        .await
        .map_err(Problem::from)?;
    Ok(Json(serde_json::json!({ "functions": stats })))
}

async fn dead_letter(
    State(state): State<ServerState>,
    principal: Principal,
    Query(f): Query<RunFilter>,
) -> ApiResult<Json<Page<serde_json::Value>>> {
    // Grouped by error signature so bulk action is possible: a poison pill
    // produces hundreds of identical failures, and retrying them one at a time
    // is how an incident becomes an afternoon (F-LP-7).
    let rows = sqlx::query(
        r#"SELECT id, fn_id, status::text AS status, key, error, error_signature,
                  quarantined_at, ended_at
             FROM runs
            WHERE ns = $1 AND status IN ('failed','quarantined')
            ORDER BY COALESCE(quarantined_at, ended_at) DESC NULLS LAST
            LIMIT $2"#,
    )
    .bind(&principal.namespace)
    .bind(clamp(f.limit))
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let items = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<Uuid, _>("id").to_string(),
                "fn_id": r.get::<String, _>("fn_id"),
                "status": r.get::<String, _>("status"),
                "key": r.get::<Option<String>, _>("key"),
                "error": r.get::<Option<serde_json::Value>, _>("error"),
                "error_signature": r.get::<Option<String>, _>("error_signature"),
                "quarantined_at": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("quarantined_at"),
                "ended_at": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("ended_at"),
            })
        })
        .collect();
    Ok(Json(Page {
        items,
        next_cursor: None,
    }))
}

/// `GET /v1/schedules`
///
/// Exists so that "the nightly job stopped running" is answerable without
/// psql. A cron schedule that has stopped produces no failed run, no error and
/// no log line after the one that paused it — the only evidence is the row, so
/// the row has to be reachable.
async fn list_schedules(
    State(state): State<ServerState>,
    principal: Principal,
) -> ApiResult<Json<serde_json::Value>> {
    let rows = sqlx::query(
        r#"SELECT s.id, s.fn_id, s.trigger_idx, s.expr, s.tz, s.catchup,
                  s.catchup_limit, s.singleton, s.run_key, s.next_fire_at,
                  s.last_fired_at, s.paused, s.last_error,
                  EXTRACT(EPOCH FROM s.misfire_window)::bigint AS misfire_window_secs,
                  (SELECT count(*) FROM cron_fires f
                    WHERE f.schedule_id = s.id AND f.outcome = 'fired'
                      AND f.decided_at > now() - interval '24 hours') AS fired_24h,
                  (SELECT count(*) FROM cron_fires f
                    WHERE f.schedule_id = s.id AND f.outcome <> 'fired'
                      AND f.decided_at > now() - interval '24 hours') AS skipped_24h
             FROM cron_schedules s
            WHERE s.ns = $1
            ORDER BY s.fn_id, s.trigger_idx"#,
    )
    .bind(&principal.namespace)
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let schedules: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            let next: chrono::DateTime<chrono::Utc> = r.get("next_fire_at");
            let paused: bool = r.get("paused");
            serde_json::json!({
                "id": r.get::<Uuid, _>("id").to_string(),
                "fn_id": r.get::<String, _>("fn_id"),
                "trigger_idx": r.get::<i32, _>("trigger_idx"),
                "cron": r.get::<String, _>("expr"),
                "tz": r.get::<String, _>("tz"),
                "catchup": r.get::<String, _>("catchup"),
                "catchup_limit": r.get::<i32, _>("catchup_limit"),
                "misfire_window_secs": r.get::<i64, _>("misfire_window_secs"),
                "singleton": r.get::<bool, _>("singleton"),
                "run_key": r.get::<Option<String>, _>("run_key"),
                "next_fire_at": next,
                "last_fired_at": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_fired_at"),
                "paused": paused,
                // The reason is carried next to the flag. A paused schedule
                // without one is an operator's deliberate pause; with one it is
                // the sweep giving up, and those need different responses.
                "last_error": r.get::<Option<String>, _>("last_error"),
                "fired_24h": r.get::<i64, _>("fired_24h"),
                "skipped_24h": r.get::<i64, _>("skipped_24h"),
                // Computed here rather than left to the reader: "is this
                // schedule behind?" is the question the endpoint is for.
                "overdue_secs": (!paused)
                    .then(|| (chrono::Utc::now() - next).num_seconds().max(0))
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "schedules": schedules })))
}

/// `POST /v1/schedules/{id}/resume`
///
/// Clears `paused` and the error that caused it. The next fire time is
/// recomputed from now rather than resumed from where it stopped: catching up
/// occurrences from before a fault was fixed is rarely what anyone wants, and
/// the misfire window would have discarded most of them anyway.
async fn resume_schedule(
    State(state): State<ServerState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    principal.require(Role::Operator)?;

    let row = sqlx::query("SELECT expr, tz FROM cron_schedules WHERE id = $1 AND ns = $2")
        .bind(id)
        .bind(&principal.namespace)
        .fetch_optional(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?
        .ok_or_else(|| {
            Problem::not_found("no_such_schedule", "no such schedule in this namespace")
        })?;

    let expr: String = row.get("expr");
    let tz: String = row.get("tz");

    // Re-parse before clearing the flag. Resuming a schedule that still cannot
    // be scheduled would pause it again on the next sweep, and the operator
    // would be told the resume succeeded.
    let schedule = stepd_core::cron::Schedule::parse(&expr, &tz).map_err(|e| {
        Problem::bad_request(
            "unschedulable",
            format!("'{expr}' in {tz} still does not parse: {e}"),
        )
    })?;

    let now: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT now()")
        .fetch_one(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

    let next = schedule.next_after(now).ok_or_else(|| {
        Problem::bad_request(
            "unschedulable",
            format!("'{expr}' in {tz} has no occurrence after {now}"),
        )
    })?;

    sqlx::query(
        "UPDATE cron_schedules SET paused = false, last_error = NULL,                 next_fire_at = $2, updated_at = now() WHERE id = $1",
    )
    .bind(id)
    .bind(next)
    .execute(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    Ok(Json(
        serde_json::json!({ "id": id.to_string(), "next_fire_at": next }),
    ))
}

async fn list_functions(
    State(state): State<ServerState>,
    principal: Principal,
) -> ApiResult<Json<serde_json::Value>> {
    // Circuit state is deliberately NOT reported here.
    //
    // It used to be, read from `app_health` — a table nothing writes. Breakers
    // live in each dispatcher process's memory and are neither shared nor
    // persisted, so the column reported `closed` for an app every replica had
    // stopped calling. An operator checking "is the breaker open?" during an
    // incident got a confident wrong answer, which is worse than no answer.
    //
    // What is reported instead is observable and true: recent failures, and
    // whether anything is queued and not moving. Until breaker state is
    // persisted, that is the honest substitute.
    let rows = sqlx::query(
        r#"SELECT f.fn_id, f.version, f.paused, f.archived_at,
                  a.url, a.last_seen,
                  (SELECT count(*) FROM runs r
                    WHERE r.ns = f.ns AND r.fn_id = f.fn_id
                      AND r.status = 'failed' AND r.ended_at > now() - interval '15 minutes')
                    AS recent_failures,
                  (SELECT count(*) FROM queue q
                    WHERE q.ns = f.ns AND q.fn_id = f.fn_id AND q.claimed_by IS NULL)
                    AS backlog,
                  (SELECT count(*) FROM runs r
                    WHERE r.ns = f.ns AND r.fn_id = f.fn_id AND r.status = 'quarantined')
                    AS quarantined
             FROM functions f
             JOIN app_bindings a ON a.id = f.app_binding_id
            WHERE f.ns = $1 ORDER BY f.fn_id"#,
    )
    .bind(&principal.namespace)
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let functions: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "fn_id": r.get::<String, _>("fn_id"),
                "version": r.get::<String, _>("version"),
                "paused": r.get::<bool, _>("paused"),
                "archived_at": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("archived_at"),
                "url": r.get::<String, _>("url"),
                "last_seen": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_seen"),
                "recent_failures": r.get::<i64, _>("recent_failures"),
                "backlog": r.get::<i64, _>("backlog"),
                "quarantined": r.get::<i64, _>("quarantined"),
                // Absent rather than wrong: breaker state is per-replica and
                // in-memory, so no single value here would be true.
                "circuit": serde_json::Value::Null,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "functions": functions })))
}

/// Counters an operator needs that no row count can reconstruct.
///
/// `inbox_overflow` is the one that matters: an overflow drops the evidence of
/// itself, so if this counter is not read, nothing anywhere records that a run
/// was signalled faster than it could consume.
async fn metrics(
    State(state): State<ServerState>,
    principal: Principal,
) -> ApiResult<Json<serde_json::Value>> {
    let rows = sqlx::query("SELECT name, value FROM engine_counters WHERE ns = $1")
        .bind(&principal.namespace)
        .fetch_all(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

    let mut counters = serde_json::Map::new();
    for r in &rows {
        counters.insert(
            r.get::<String, _>("name"),
            serde_json::json!(r.get::<i64, _>("value")),
        );
    }
    Ok(Json(serde_json::json!({ "counters": counters })))
}

/// Liveness. Deliberately unauthenticated: a health check that needs a token is
/// one more thing to get wrong in a deployment, and it reveals nothing.
async fn health(State(state): State<ServerState>) -> ApiResult<Json<serde_json::Value>> {
    sqlx::query("SELECT 1")
        .execute(state.store.pool())
        .await
        .map_err(|e| Problem::internal(format!("database unreachable: {e}")))?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "protocol": stepd_proto::PROTOCOL_VERSION,
        "time": chrono::Utc::now(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_is_clamped_whatever_the_caller_asks_for() {
        assert_eq!(clamp(None), 50);
        assert_eq!(clamp(Some(10)), 10);
        assert_eq!(
            clamp(Some(100_000)),
            MAX_LIMIT,
            "an unbounded page is a denial of service"
        );
        assert_eq!(clamp(Some(0)), 1);
        assert_eq!(
            clamp(Some(-5)),
            1,
            "a negative limit must not become an unbounded query"
        );
    }
}
