//! Cron scheduling against Postgres.
//!
//! This module executes plans. It does not make them.
//!
//! Every decision — which occurrences fire, which are skipped and for which of
//! the four reasons, where `next_fire_at` goes next, whether a schedule is
//! plannable at all — is made by [`stepd_core::cron::plan`], which is pure, has
//! no database, and is tested against a table of known DST transitions. What is
//! here is the transaction: claim, ask, apply, advance.
//!
//! That division is deliberate and it is the second time this project has drawn
//! it. Migration 006 exists because the Rust store had grown four hundred lines
//! of application SQL reimplementing the commit path, so there were two
//! correctness centres and the structural tests guarded the one that was not
//! running. Three live defects were sitting in the other. A scheduler is an
//! especially easy place to make the same mistake, because "just work out the
//! next time in SQL" always looks like one small query.
//!
//! ## The two properties that make this safe
//!
//! **At most once per occurrence** is a primary key on `(schedule_id,
//! occurrence_at)`, not a check this code performs. Two replicas sweeping the
//! same schedule — after a lease expiry, a failover, a restart between the run
//! insert and the commit — produce one run, and the loser is told it lost.
//!
//! **One slow schedule must not stop the others.** Each schedule is applied
//! inside its own savepoint, so a schedule whose SQL fails is rolled back to the
//! start of its own work and the sweep carries on. Without this, one hand-edited
//! row aborts the transaction and every schedule in the fleet stops advancing —
//! a single bad row taking down all scheduling is the failure this component
//! can least afford, since nothing about it looks like an outage.

use crate::{db, PostgresStore};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::{Acquire, Row};
use stepd_core::cron::{self, DueSchedule, SchedulePlan};
use stepd_core::traits::{CronRegistration, CronStore, CronSweep};
use stepd_core::Result;
use uuid::Uuid;

#[async_trait]
impl CronStore for PostgresStore {
    async fn sweep(&self, max: i64) -> Result<CronSweep> {
        let mut namespaces = self.cron_namespaces().await?;
        if namespaces.is_empty() {
            return Ok(CronSweep::default());
        }

        // Rotate the starting point, exactly as the dispatcher does. Sorting
        // alone would mean the alphabetically first namespace is served first on
        // every single sweep, so under sustained load the last one is served
        // never — a starvation that reads as "our schedules are unreliable" and
        // has no failing component to point at.
        namespaces.sort();
        let start = {
            let mut c = self.cron_cursor.lock().await;
            *c = (*c + 1) % namespaces.len();
            *c
        };
        namespaces.rotate_left(start);

        let mut total = CronSweep::default();
        for ns in &namespaces {
            match self.sweep_namespace(ns, max).await {
                Ok(s) => {
                    total.considered += s.considered;
                    total.fired += s.fired;
                    total.skipped += s.skipped;
                    total.duplicates += s.duplicates;
                    total.unschedulable += s.unschedulable;
                }
                // One namespace's failure must not stop the rest, for the same
                // reason one schedule's does not.
                Err(e) => tracing::warn!(namespace = %ns, error = %e, "cron sweep failed"),
            }
        }
        Ok(total)
    }

    async fn cron_namespaces(&self) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT ns FROM cron_active_namespaces()")
            .fetch_all(self.pool())
            .await
            .map_err(db)
    }

    async fn sweep_namespace(&self, namespace: &str, max: i64) -> Result<CronSweep> {
        let mut tx = self.pool().begin().await.map_err(db)?;

        // The claim and everything that follows it are one transaction: the row
        // locks taken here are what stop a second replica planning the same
        // schedule, and they last exactly as long as the transaction does.
        let rows = sqlx::query("SELECT * FROM claim_due_cron($1,$2)")
            .bind(namespace)
            .bind(max as i32)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;

        if rows.is_empty() {
            // Nothing due. Roll back rather than commit — there is nothing to
            // commit, and an empty transaction per tick on an idle system is
            // still a write-ahead log record per tick.
            tx.rollback().await.map_err(db)?;
            return Ok(CronSweep::default());
        }

        // Database time, from the claim itself. Never `Utc::now()`: a replica
        // running a minute fast that trusts its own clock fires every schedule
        // in the fleet a minute early, all the runs succeed, and nothing
        // anywhere reports a problem (F-DL-8).
        let now: DateTime<Utc> = rows[0].get("db_now");

        let due: Vec<DueSchedule> = rows
            .iter()
            .map(|r| DueSchedule {
                id: r.get("id"),
                expr: r.get("expr"),
                tz: r.get("tz"),
                catchup: r.get("catchup"),
                catchup_limit: r.get("catchup_limit"),
                misfire_window: Duration::milliseconds(
                    (r.get::<f64, _>("misfire_window_secs") * 1000.0) as i64,
                ),
                next_fire_at: r.get("next_fire_at"),
            })
            .collect();

        let plans = cron::plan(&due, now);

        let mut stats = CronSweep {
            considered: plans.len() as u64,
            ..Default::default()
        };

        for plan in &plans {
            // A savepoint per schedule. One schedule's failure rolls back that
            // schedule's work and nothing else; without it, a single failing
            // statement aborts the whole transaction and every other schedule
            // in this sweep silently fails to advance.
            let mut sp = tx.begin().await.map_err(db)?;
            match apply(&mut sp, plan).await {
                Ok(applied) => {
                    sp.commit().await.map_err(db)?;
                    stats.fired += applied.fired;
                    stats.skipped += applied.skipped;
                    stats.duplicates += applied.duplicates;
                    stats.unschedulable += applied.unschedulable;
                }
                Err(e) => {
                    sp.rollback().await.map_err(db)?;
                    tracing::warn!(
                        schedule = %plan.id, error = %e,
                        "cron schedule failed to apply; it will be retried next sweep"
                    );
                }
            }
        }

        tx.commit().await.map_err(db)?;
        Ok(stats)
    }

    async fn register_schedules(&self, regs: &[CronRegistration]) -> Result<u64> {
        let mut tx = self.pool().begin().await.map_err(db)?;
        let n = self.register_schedules_in(&mut tx, regs).await?;
        tx.commit().await.map_err(db)?;
        Ok(n)
    }

    async fn trim_fires(&self, max: i64) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT trim_cron_fires($1)")
            .bind(max as i32)
            .fetch_one(self.pool())
            .await
            .map_err(db)?;
        Ok(n as u64)
    }
}

/// What applying one plan did.
#[derive(Default)]
struct Applied {
    fired: u64,
    skipped: u64,
    duplicates: u64,
    unschedulable: u64,
}

/// Execute one schedule's plan. No decisions are taken here.
async fn apply(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan: &SchedulePlan,
) -> Result<Applied> {
    let mut out = Applied::default();

    // Unplannable: pause it. Not "log and retry" — a due row is re-claimed on
    // every sweep until its `next_fire_at` moves, and a schedule that cannot be
    // planned is exactly one whose `next_fire_at` cannot be computed. Retrying
    // is a hot loop for as long as the row exists.
    if let Some(reason) = &plan.unschedulable {
        sqlx::query("SELECT pause_cron_schedule($1,$2)")
            .bind(plan.id)
            .bind(reason)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        tracing::warn!(
            schedule = %plan.id, reason = %reason,
            "cron schedule paused; it will not fire until an operator fixes it"
        );
        out.unschedulable = 1;
        return Ok(out);
    }

    for (occurrence, reason) in &plan.skip {
        let outcome = record(
            tx,
            plan.id,
            *occurrence,
            &format!("skipped_{}", reason.as_str()),
        )
        .await?;
        match outcome.as_str() {
            "duplicate" => out.duplicates += 1,
            _ => out.skipped += 1,
        }
    }

    for occurrence in &plan.fire {
        let outcome = record(tx, plan.id, *occurrence, "fired").await?;
        match outcome.as_str() {
            "fired" => out.fired += 1,
            "duplicate" => out.duplicates += 1,
            // `skipped_singleton` is decided by the keyed-exclusivity index at
            // insert time, not by the planner — the planner cannot know whether
            // a run is still active without reading the database, and asking
            // before inserting would be a check-then-act race between replicas.
            _ => out.skipped += 1,
        }
    }

    // Advance last, once, whatever happened above. A sweep that fired nothing
    // still has to move the schedule on: if it did not, the row stays due and is
    // re-claimed every pass forever, which costs a claim per tick to accomplish
    // nothing.
    if let Some(next) = plan.advance_to {
        sqlx::query("SELECT advance_cron($1,$2,$3)")
            .bind(plan.id)
            .bind(next)
            .bind(plan.last_fired)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
    }

    Ok(out)
}

async fn record(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schedule: Uuid,
    occurrence: DateTime<Utc>,
    outcome: &str,
) -> Result<String> {
    let row = sqlx::query("SELECT * FROM fire_cron_occurrence($1,$2,$3)")
        .bind(schedule)
        .bind(occurrence)
        .bind(outcome)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    Ok(row.get("outcome"))
}

impl PostgresStore {
    /// Register a function's cron triggers inside a caller's transaction.
    ///
    /// The transactional entry point exists because app registration must be
    /// atomic across both halves. Committing the functions and then registering
    /// the schedules leaves two ways to be wrong, and both are silent: a crash
    /// between them leaves either a cron function that never fires, or schedules
    /// firing for a function no app is bound to. The first is the exact failure
    /// this whole component was built to eliminate.
    ///
    /// [`CronStore::register_schedules`] is this function with a transaction
    /// wrapped round it — one implementation, two entry points, so there is no
    /// second copy to drift.
    pub async fn register_schedules_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        regs: &[CronRegistration],
    ) -> Result<u64> {
        if regs.is_empty() {
            return Ok(0);
        }

        // Every registration in this call is for one function; the caller builds
        // them from one config. Asserting it rather than trusting it means a
        // future caller that batches two functions together gets an error
        // instead of a prune that deletes the other function's schedules.
        let ns = &regs[0].namespace;
        let fn_id = &regs[0].function_id;
        if regs
            .iter()
            .any(|r| &r.namespace != ns || &r.function_id != fn_id)
        {
            return Err(stepd_core::Error::Store(
                "register_schedules takes the triggers of exactly one function".into(),
            ));
        }

        // Database time, for the same reason the sweep uses it: the first fire
        // of a newly registered schedule is computed from it, and a replica that
        // trusts its own clock would set it wrong (F-DL-8).
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;

        for r in regs {
            // `next_after` can decline — `0 0 30 2 *` parses cleanly and names a
            // day February does not have. Registering it anyway would create a
            // schedule that is due immediately and forever, which the sweep
            // would pause on its first pass. Better to refuse at the door, where
            // the error reaches the person deploying the function.
            let first = r.schedule.next_after(now).ok_or_else(|| {
                stepd_core::Error::Store(format!(
                    "cron expression '{}' in {} has no occurrence after {now}",
                    r.schedule.source(),
                    r.schedule.timezone().name(),
                ))
            })?;

            sqlx::query("SELECT * FROM upsert_cron_schedule($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
                .bind(&r.namespace)
                .bind(&r.function_id)
                .bind(r.trigger_idx)
                .bind(r.schedule.source())
                .bind(r.schedule.timezone().name())
                .bind(r.catchup.as_str())
                .bind(r.catchup_limit)
                .bind(crate::interval(r.misfire_window)?)
                .bind(r.singleton)
                .bind(&r.run_key)
                .bind(first)
                .execute(&mut **tx)
                .await
                .map_err(db)?;
        }

        self.prune_schedules_in(
            tx,
            ns,
            fn_id,
            &regs.iter().map(|r| r.trigger_idx).collect::<Vec<_>>(),
        )
        .await?;
        Ok(regs.len() as u64)
    }

    /// Retire the cron schedules of `fn_id` whose trigger index is not in `keep`.
    ///
    /// Half of registration. A cron trigger deleted from a function and
    /// redeployed must stop firing, or the deploy is indistinguishable from not
    /// having happened. Passing an empty `keep` retires all of them, which is
    /// what a function that dropped cron entirely needs.
    pub async fn prune_schedules_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        namespace: &str,
        function_id: &str,
        keep: &[i32],
    ) -> Result<u64> {
        let n: i32 = sqlx::query_scalar("SELECT prune_cron_schedules($1,$2,$3)")
            .bind(namespace)
            .bind(function_id)
            .bind(keep)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        Ok(n as u64)
    }
}
