//! # Postgres store
//!
//! The v1 implementation of every engine interface.
//!
//! ## Why so little SQL lives here
//!
//! An earlier version of this crate reimplemented the op commit in Rust: roughly
//! four hundred lines of `sqlx::query` recording steps, creating child runs,
//! consuming inbox entries and cascading cancellation. It worked, and it had two
//! problems that no test would have caught.
//!
//! The first is that the project's structural invariant tests
//! (`rust/tests/sql/test_invariants.sql`) assert properties of the *SQL
//! functions* — that `deliver_to_inbox` takes the run row lock as its first
//! statement, that `commit_ops` checks the fence under that lock. A second
//! implementation in Rust is covered by none of them, so the countermeasure to
//! this project's own finding — "correctness can rest on undocumented
//! accidents" — silently did not apply to the path that actually ran.
//!
//! The second is that the two implementations had already diverged, in three
//! ways that were live defects:
//!
//! * `signal` ops were inserted straight into `run_inbox`, bypassing
//!   `deliver_to_inbox`, so a signal sent to a run already parked on a matching
//!   wait was buffered and never woke it.
//! * `wait_event` scheduled no timer, so `timeout` was accepted and ignored and
//!   a run could wait for an event that never came, forever.
//! * cascade cancellation was a recursive `async fn` committing per level, so a
//!   crash mid-cascade orphaned the descendants below the point it reached.
//!
//! So this crate is now a thin, honest adapter: marshal to JSON, call the
//! function, map the result. The correctness centre is one reviewable place, and
//! the invariant tests guard the code that runs.
//!
//! Two constraints still run through it:
//!
//! * **No session-scoped advisory locks.** All claiming is row-level
//!   `FOR UPDATE SKIP LOCKED`, so the engine works behind a transaction-mode
//!   connection pooler such as pgbouncer, which is how most production
//!   deployments run.
//! * **The op commit is one transaction.** Step results, inbox consumption,
//!   emitted events and the next schedule commit together or not at all.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use stepd_core::traits::*;
use stepd_core::{Error, Result};
use stepd_proto::*;
use uuid::Uuid;

mod blobs;
mod cron;
pub use blobs::{attach_read_urls, blob_ids, sha256_hex, Capability, PostgresBlobStore};

/// Translate a database failure into an engine error.
pub(crate) fn db(e: sqlx::Error) -> Error {
    Error::Store(e.to_string())
}

/// Convert a `chrono::Duration` to the interval type the SQL functions take.
pub(crate) fn interval(d: Duration) -> Result<sqlx::postgres::types::PgInterval> {
    sqlx::postgres::types::PgInterval::try_from(d.to_std().unwrap_or_default())
        .map_err(|e| Error::Store(e.to_string()))
}

/// Postgres-backed store, queue, event log, timer store and housekeeping.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    /// Mints the short-lived read URLs attached to `$blob` values on the way out.
    ///
    /// Holds no pool and no filesystem root, only the signing key and base URL,
    /// which is why it can live here without the store gaining a dependency on
    /// where bytes are stored. `None` on a server with managed blobs disabled.
    blob_caps: Option<Arc<blobs::Capability>>,
    /// The store used by the housekeeper's collection sweep.
    ///
    /// Separate from `blob_caps` because they need different things: minting a
    /// read URL needs only a key, while deleting bytes needs a filesystem root.
    /// A server that serves blobs has both; one that only reads references
    /// minted elsewhere would have only the first.
    blob_collector: Option<Arc<blobs::PostgresBlobStore>>,
    /// Round-robin position for the cron sweep's namespace rotation.
    ///
    /// Per-replica and not persisted, deliberately. It exists to stop one
    /// replica always starting at the same namespace; where several replicas
    /// start does not need coordinating, and coordinating it would mean shared
    /// state on the hot path of the component that must keep working when
    /// everything else is broken.
    cron_cursor: Arc<tokio::sync::Mutex<usize>>,
}

impl PostgresStore {
    /// Connect with a bounded pool.
    pub async fn connect(url: &str, max_conns: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_conns)
            .connect(url)
            .await
            .map_err(db)?;
        Ok(Self::from_pool(pool))
    }

    /// Wrap an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            blob_caps: None,
            blob_collector: None,
            cron_cursor: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    /// Attach the capability minter, so `$blob` values shipped to an attempt
    /// carry a usable read URL (protocol §8.3.1).
    ///
    /// Without it the app receives a reference it cannot dereference. The URL is
    /// minted per attempt and must never be persisted by an SDK, which is why it
    /// is added here on the way out rather than stored with the step.
    pub fn with_blob_capability(mut self, caps: blobs::Capability) -> Self {
        self.blob_caps = Some(Arc::new(caps));
        self
    }

    /// Attach the blob store the housekeeper collects through.
    pub fn with_blob_collector(mut self, store: Arc<blobs::PostgresBlobStore>) -> Self {
        self.blob_collector = Some(store);
        self
    }

    /// The underlying pool, for the API layer's read queries.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the bundled migrations.
    ///
    /// Refuses, with an explanation, if the schema was applied by hand instead.
    /// Running the `.sql` files through `psql` and then running `stepd migrate`
    /// is a natural thing to do — the files are right there — and sqlx has no
    /// record of them, so it starts from migration 1 and fails on
    /// `type "run_status" already exists`. That error names a symptom four steps
    /// removed from the cause, at the exact moment somebody is trying to bring a
    /// deployment up.
    pub async fn migrate(&self) -> Result<()> {
        let applied_by_hand: bool = sqlx::query_scalar(
            r#"SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'run_status')
                  AND NOT EXISTS (SELECT 1 FROM pg_tables
                                   WHERE tablename = '_sqlx_migrations')"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        if applied_by_hand {
            return Err(Error::Config(
                "this database already has the stepd schema, but no migration history —                  it looks like the .sql files were applied directly with psql. Migrating                  now would start from migration 1 and fail on objects that already exist.                  Either use a fresh database and `stepd migrate`, or apply the remaining                  files by hand and keep doing so. Do not mix the two."
                    .into(),
            ));
        }

        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))
    }

    /// Create a namespace if it does not exist. Idempotent.
    pub async fn ensure_namespace(&self, ns: &str) -> Result<()> {
        sqlx::query("INSERT INTO namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(ns)
            .execute(&self.pool)
            .await
            .map_err(db)?;
        Ok(())
    }

    /// Route an ingested event to every run waiting for it.
    ///
    /// Correlation is by `(namespace, type)` and, when the run is keyed, the key:
    /// an approval for order 4711 must not resolve the wait belonging to 4712.
    pub async fn correlate(
        &self,
        namespace: &str,
        event_type: &str,
        key: Option<&str>,
        data: &serde_json::Value,
    ) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT correlate_event($1,$2,$3,$4)")
            .bind(namespace)
            .bind(event_type)
            .bind(key)
            .bind(data)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(n as u64)
    }

    /// Value of an engine limit, for the console and for error messages.
    pub async fn limit(&self, name: &str) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT engine_limit($1)")
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(db)
    }
}

/// Decode the `run_steps` shape the read queries return.
fn recorded_step(r: &sqlx::postgres::PgRow) -> (String, RecordedStep) {
    let op: String = r.get("op");
    let status: String = r.get("status");
    (
        r.get::<String, _>("step_hash"),
        RecordedStep {
            id: r.get("step_id"),
            op: match op.as_str() {
                "sleep" => StepOp::Sleep,
                "wait_event" => StepOp::WaitEvent,
                "invoke" => StepOp::Invoke,
                "signal" => StepOp::Signal,
                _ => StepOp::Step,
            },
            status: match status.as_str() {
                "failed" => StepStatus::Failed,
                "timed_out" => StepStatus::TimedOut,
                "cancelled" => StepStatus::Cancelled,
                "unknown" => StepStatus::Unknown,
                "pending" => StepStatus::Pending,
                _ => StepStatus::Completed,
            },
            data: r.get("result"),
            error: r
                .get::<Option<serde_json::Value>, _>("error")
                .and_then(|v| serde_json::from_value(v).ok()),
        },
    )
}

/// Steps that may be sent to an app.
///
/// Pending rows exist in the store but are never shipped: a handler that saw an
/// unresolved sleep in its memo map would treat it as already done and step
/// straight past a timer that has not fired.
const SHIPPABLE: &str = "status IN ('completed','failed','timed_out','cancelled')";

/// Inline journal ceiling for one attempt, in steps.
///
/// Configurable because the right value depends on payload sizes the engine
/// cannot see; the default keeps a full journal comfortably inside the 4 MiB
/// attempt-body limit for typical step results.
fn attempt_state_limit() -> i64 {
    std::env::var("STEPD_ATTEMPT_STATE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(2000)
}

#[async_trait]
impl StateStore for PostgresStore {
    async fn create_run(&self, new: NewRun) -> Result<Option<RunId>> {
        let id = Uuid::now_v7();
        let lineage = new.lineage_id.unwrap_or(id);

        // Keyed exclusivity is a database invariant (a partial unique index over
        // the active statuses), not application logic — so a race between two
        // creators cannot produce two active runs on one key.
        let res = sqlx::query(
            r#"INSERT INTO runs
                 (id, ns, fn_id, key, input, parent_run_id, parent_step_hash,
                  detached, lineage_id, chain_position, trigger_event_id, status)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')
               RETURNING id"#,
        )
        .bind(id)
        .bind(&new.namespace)
        .bind(&new.function_id)
        .bind(&new.key)
        .bind(&new.input)
        .bind(new.parent)
        .bind(&new.parent_step_hash)
        .bind(new.detached)
        .bind(lineage)
        .bind(new.chain_position)
        .bind(new.trigger_event_id)
        .fetch_optional(&self.pool)
        .await;

        match res {
            Ok(None) => Ok(None),
            Ok(Some(_)) => {
                sqlx::query("INSERT INTO queue (ns, fn_id, key, run_id) VALUES ($1,$2,$3,$4)")
                    .bind(&new.namespace)
                    .bind(&new.function_id)
                    .bind(&new.key)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(db)?;
                Ok(Some(id))
            }
            // A unique violation here is the keyed-exclusivity index doing its
            // job, not an error worth propagating.
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => Ok(None),
            Err(e) => Err(db(e)),
        }
    }

    async fn load_attempt(&self, lease: &Lease) -> Result<Attempt> {
        let run = sqlx::query(
            r#"SELECT r.id, r.ns, r.fn_id, r.key, r.input, r.started_at,
                      r.lineage_id, r.chain_position, r.status::text AS status,
                      r.compensating,
                      e.type AS trigger_type, e.source AS trigger_source,
                      e.data AS trigger_data, e.time AS trigger_time
                 FROM runs r
            LEFT JOIN events e ON e.id = r.trigger_event_id
                WHERE r.id = $1"#,
        )
        .bind(lease.run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::Store(format!("run {} not found", lease.run_id)))?;

        // §7.4. Read from `compensating`, not from the status: a run in the
        // compensation phase cycles pending → running like any other, because
        // one new step per attempt means a three-step undo takes three attempts.
        // The previous version derived this from `status == 'cancelled'`, which
        // could never be true — a cancelled run had its queue row deleted and
        // was never dispatched again, so the whole compensation feature was
        // reachable only in tests that built the flag by hand.
        let cancelling: bool = run.get("compensating");

        // Bound the journal shipped inline. Over the limit the app fetches the
        // rest through `steps_page` (protocol §8.6); silently truncating instead
        // would make the SDK re-execute steps whose results merely were not sent.
        let cap = attempt_state_limit();
        let rows = sqlx::query(&format!(
            "SELECT step_hash, step_id, op::text AS op, status::text AS status, result, error
               FROM run_steps WHERE run_id = $1 AND {SHIPPABLE}
              ORDER BY step_hash LIMIT $2"
        ))
        .bind(lease.run_id)
        .bind(cap + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        let truncated = rows.len() as i64 > cap;
        let mut steps: HashMap<String, RecordedStep> =
            rows.iter().take(cap as usize).map(recorded_step).collect();

        // §8.3.1: mint the read URLs now, on the way out. They are deliberately
        // short-lived and deliberately not stored — a reference replayed on
        // attempt forty needs a URL minted for attempt forty, and one persisted
        // from attempt one expired long ago. Doing it here means every path that
        // ships a journal gets it, including the paging endpoint.
        if let Some(caps) = &self.blob_caps {
            let ttl = Duration::seconds(300);
            for step in steps.values_mut() {
                if let Some(data) = step.data.as_mut() {
                    blobs::attach_read_urls(caps, data, ttl);
                }
            }
        }

        let events = match run.get::<Option<String>, _>("trigger_type") {
            Some(t) => vec![Event {
                specversion: "1.0".into(),
                id: None,
                source: run
                    .get::<Option<String>, _>("trigger_source")
                    .unwrap_or_default(),
                event_type: t,
                time: run.get("trigger_time"),
                data: run
                    .get::<Option<serde_json::Value>, _>("trigger_data")
                    .unwrap_or(serde_json::Value::Null),
                key: run.get("key"),
                idempotency: None,
            }],
            None => vec![],
        };

        Ok(Attempt {
            protocol: PROTOCOL_VERSION.into(),
            attempt: lease.attempt,
            fence: lease.fence,
            deadline: Some(lease.until),
            run: RunContext {
                id: run.get("id"),
                function_id: run.get("fn_id"),
                namespace: run.get("ns"),
                key: run.get("key"),
                started_at: run.get("started_at"),
                input: run.get("input"),
                lineage_id: run.get("lineage_id"),
                chain_position: run.get("chain_position"),
                cancelling,
            },
            events,
            steps,
            state_truncated: truncated,
        })
    }

    async fn commit(&self, run_id: RunId, fence: i64, commit: OpCommit) -> Result<CommitOutcome> {
        // One call, one transaction, every op. See the module header for why this
        // is a marshalling function and not an implementation.
        let outcome: String = sqlx::query_scalar("SELECT commit_ops($1,$2,$3,$4)")
            .bind(run_id)
            .bind(fence)
            .bind(serde_json::to_value(&commit.ops)?)
            .bind(serde_json::to_value(&commit.emit)?)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(CommitOutcome::from_sql(&outcome))
    }

    async fn cancel_run(&self, namespace: &str, run_id: RunId) -> Result<bool> {
        sqlx::query_scalar("SELECT cancel_run($1,$2)")
            .bind(namespace)
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .map_err(db)
    }

    async fn retry_run(&self, namespace: &str, run_id: RunId) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let row = sqlx::query(
            r#"UPDATE runs
                  SET status='pending', ended_at=NULL, error=NULL,
                      quarantined_at=NULL, error_signature=NULL
                WHERE id=$1 AND ns=$2 AND status IN ('failed','cancelled','quarantined')
                RETURNING fn_id, key"#,
        )
        .bind(run_id)
        .bind(namespace)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;

        let Some(row) = row else { return Ok(false) };
        sqlx::query(
            r#"INSERT INTO queue (ns, fn_id, key, run_id) VALUES ($1,$2,$3,$4)
               ON CONFLICT (run_id) DO UPDATE
                 SET claimed_by=NULL, claimed_until=NULL, available_at=now()"#,
        )
        .bind(namespace)
        .bind(row.get::<String, _>("fn_id"))
        .bind(row.get::<Option<String>, _>("key"))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    async fn quarantine(&self, run_id: RunId, signature: &str) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query(
            "UPDATE runs SET status='quarantined', quarantined_at=now(), error_signature=$2
              WHERE id=$1",
        )
        .bind(run_id)
        .bind(signature)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query("DELETE FROM queue WHERE run_id=$1")
            .bind(run_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn record_failure(
        &self,
        run_id: RunId,
        err: &ErrorBody,
        retry_at: DateTime<Utc>,
    ) -> Result<FailureRecord> {
        // One SQL function, one statement: the attempt count, the consecutive
        // count and the signature are computed together, so they cannot disagree
        // about what just happened.
        let row = sqlx::query("SELECT * FROM record_failure($1,$2,$3,$4)")
            .bind(run_id)
            .bind(err.code.as_deref())
            .bind(&err.message)
            .bind(retry_at)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(FailureRecord {
            attempts: row.get("attempts"),
            consecutive: row.get("consecutive"),
        })
    }

    async fn release(&self, run_id: RunId, available_at: DateTime<Utc>) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query(
            "UPDATE runs SET status='pending', lease_owner=NULL, lease_until=NULL WHERE id=$1",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "UPDATE queue SET claimed_by=NULL, claimed_until=NULL, available_at=$2 WHERE run_id=$1",
        )
        .bind(run_id)
        .bind(available_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn run_status(&self, run_id: RunId) -> Result<Option<RunStatus>> {
        let s: Option<String> = sqlx::query_scalar("SELECT status::text FROM runs WHERE id=$1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        Ok(s.map(|s| match s.as_str() {
            "running" => RunStatus::Running,
            "sleeping" => RunStatus::Sleeping,
            "waiting" => RunStatus::Waiting,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            "quarantined" => RunStatus::Quarantined,
            _ => RunStatus::Pending,
        }))
    }

    async fn steps_page(&self, run_id: RunId, after: Option<&str>, limit: i64) -> Result<StepPage> {
        // Ordered by hash, and the cursor is a hash: an unordered page boundary
        // drops or repeats steps between requests, and either one corrupts a
        // replay silently.
        let rows = sqlx::query(&format!(
            "SELECT step_hash, step_id, op::text AS op, status::text AS status, result, error
               FROM run_steps
              WHERE run_id = $1 AND {SHIPPABLE} AND ($2::text IS NULL OR step_hash > $2)
              ORDER BY step_hash LIMIT $3"
        ))
        .bind(run_id)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        let next = if rows.len() as i64 == limit {
            rows.last().map(|r| r.get::<String, _>("step_hash"))
        } else {
            None
        };
        Ok(StepPage {
            steps: rows.iter().map(recorded_step).collect(),
            next,
        })
    }
}

#[async_trait]
impl Queue for PostgresStore {
    async fn claim(
        &self,
        namespace: &str,
        worker: &str,
        max: i64,
        lease: Duration,
    ) -> Result<Vec<Lease>> {
        let rows = sqlx::query("SELECT * FROM claim_runs_ns($1,$2,$3,$4)")
            .bind(namespace)
            .bind(worker)
            .bind(max as i32)
            .bind(interval(lease)?)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;

        Ok(rows
            .into_iter()
            .map(|r| Lease {
                run_id: r.get("run_id"),
                fence: r.get("fence"),
                attempt: r.get("attempt"),
                until: r.get("lease_until"),
            })
            .collect())
    }

    async fn heartbeat(&self, run_id: RunId, until: DateTime<Utc>) -> Result<()> {
        // Both rows, not just `runs`: the queue row is what the lease reclaimer
        // reads, so extending only the run would let a live attempt be stolen.
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("UPDATE runs SET lease_until=$2 WHERE id=$1")
            .bind(run_id)
            .bind(until)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE queue SET claimed_until=$2 WHERE run_id=$1")
            .bind(run_id)
            .bind(until)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn stats(&self, namespace: &str) -> Result<Vec<QueueStat>> {
        let rows = sqlx::query(
            r#"SELECT fn_id,
                      count(*) FILTER (WHERE claimed_by IS NULL) AS backlog,
                      count(*) FILTER (WHERE claimed_by IS NOT NULL) AS in_flight,
                      COALESCE(EXTRACT(epoch FROM now() -
                        min(available_at) FILTER (WHERE claimed_by IS NULL)), 0)::bigint AS oldest
                 FROM queue WHERE ns=$1 GROUP BY fn_id ORDER BY backlog DESC"#,
        )
        .bind(namespace)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        Ok(rows
            .into_iter()
            .map(|r| QueueStat {
                function_id: r.get("fn_id"),
                backlog: r.get("backlog"),
                in_flight: r.get("in_flight"),
                oldest_seconds: r.get("oldest"),
            })
            .collect())
    }

    async fn active_namespaces(&self) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT ns FROM active_namespaces()")
            .fetch_all(&self.pool)
            .await
            .map_err(db)
    }
}

#[async_trait]
impl TimerStore for PostgresStore {
    async fn fire_due(&self, now: DateTime<Utc>, max: i64) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT fire_due_timers($1,$2)")
            .bind(now)
            .bind(max as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(n as u64)
    }
}

#[async_trait]
impl Housekeeping for PostgresStore {
    async fn drain_signals(&self, max: i64) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT drain_signals($1)")
            .bind(max as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(n as u64)
    }

    async fn resolve_finished_children(&self, max: i64) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT resolve_finished_children($1)")
            .bind(max as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(n as u64)
    }

    async fn reclaim_expired_leases(&self, max: i64) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT reclaim_expired_leases($1)")
            .bind(max as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(n as u64)
    }

    async fn collect_blobs(&self, before: DateTime<Utc>) -> Result<u64> {
        // Only when a store is attached. A server with managed blobs disabled
        // has no filesystem root to delete from, and deleting index rows while
        // leaving bytes behind is worse than doing nothing.
        match &self.blob_collector {
            Some(store) => store.collect(before).await,
            None => Ok(0),
        }
    }
}

#[async_trait]
impl EventLog for PostgresStore {
    async fn append(&self, namespace: &str, event: &Event) -> Result<(Uuid, bool)> {
        // `ingest_event` claims the idempotency key and inserts the event in one
        // transaction. Doing it in two round trips would leave a claimed key with
        // no event behind it if the process died between them, and every retry of
        // that event would then be deduplicated against nothing.
        let row = sqlx::query("SELECT * FROM ingest_event($1,$2,$3,$4,$5,$6)")
            .bind(namespace)
            .bind(&event.event_type)
            .bind(&event.source)
            .bind(&event.data)
            .bind(&event.key)
            .bind(&event.idempotency)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok((row.get("event_id"), row.get("deduplicated")))
    }

    async fn deliver(
        &self,
        run_id: RunId,
        event_type: &str,
        payload: &serde_json::Value,
        sender: Option<(RunId, String)>,
    ) -> Result<Delivery> {
        let (sender_run, sender_hash) = match sender {
            Some((r, h)) => (Some(r), Some(h)),
            None => (None, None),
        };
        let res: String = sqlx::query_scalar("SELECT deliver_to_inbox($1,$2,$3,$4,$5)")
            .bind(run_id)
            .bind(event_type)
            .bind(payload)
            .bind(sender_run)
            .bind(&sender_hash)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;

        Ok(match res.as_str() {
            "resolved" => Delivery::Resolved,
            "buffered" => Delivery::Buffered,
            "duplicate" => Delivery::Duplicate,
            _ => Delivery::NoRun,
        })
    }

    async fn drain_outbox(&self, max: i64) -> Result<u64> {
        let n = sqlx::query(
            r#"UPDATE outbox SET published=true, published_at=now()
                WHERE id IN (SELECT id FROM outbox WHERE NOT published
                             ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)"#,
        )
        .bind(max)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(n.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_outcomes_map_from_every_sql_return() {
        assert_eq!(
            CommitOutcome::from_sql("committed"),
            CommitOutcome::Committed
        );
        assert_eq!(
            CommitOutcome::from_sql("stale_fence"),
            CommitOutcome::StaleFence
        );
        assert_eq!(CommitOutcome::from_sql("terminal"), CommitOutcome::Terminal);
        assert_eq!(
            CommitOutcome::from_sql("no_such_run"),
            CommitOutcome::NoSuchRun
        );
        assert_eq!(
            CommitOutcome::from_sql("failed:invoke_cycle"),
            CommitOutcome::Rejected("invoke_cycle".into())
        );
    }

    #[test]
    fn an_unrecognised_return_is_a_rejection_not_a_success() {
        // Mapping an unknown string to `Committed` would turn a future rule
        // violation into a silently successful commit — exactly the class of
        // quiet failure this project exists to avoid.
        assert!(matches!(
            CommitOutcome::from_sql("something_new"),
            CommitOutcome::Rejected(_)
        ));
    }

    #[test]
    fn pending_steps_are_never_shipped_to_an_app() {
        // A handler that saw a pending sleep in its memo map would treat it as
        // done and step straight past a timer that has not fired.
        assert!(!SHIPPABLE.contains("'pending'"));
        assert!(SHIPPABLE.contains("'completed'"));
    }

    #[test]
    fn attempt_state_limit_rejects_nonsense() {
        std::env::set_var("STEPD_ATTEMPT_STATE_LIMIT", "0");
        assert_eq!(
            attempt_state_limit(),
            2000,
            "a zero limit would ship no journal at all"
        );
        std::env::set_var("STEPD_ATTEMPT_STATE_LIMIT", "not-a-number");
        assert_eq!(attempt_state_limit(), 2000);
        std::env::set_var("STEPD_ATTEMPT_STATE_LIMIT", "50");
        assert_eq!(attempt_state_limit(), 50);
        std::env::remove_var("STEPD_ATTEMPT_STATE_LIMIT");
    }
}
