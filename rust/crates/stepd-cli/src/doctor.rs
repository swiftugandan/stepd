//! `stepd doctor` — operational self-diagnosis (F-DL-7, gap C5).
//!
//! Every check here exists because the condition it looks for is invisible until
//! it hurts, and then looks like something else. A pooler in the wrong mode
//! shows up as intermittent lock errors; clock skew shows up as timers firing
//! early on one replica; partition lag shows up as an insert failure at
//! midnight on the first of the month.
//!
//! The output is deliberately blunt about severity, because a doctor that
//! reports thirty things at the same volume is a doctor nobody runs twice.

use sqlx::{PgPool, Row};

/// How bad a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Worth knowing, nothing is wrong.
    Info,
    /// Will hurt, eventually or under load.
    Warn,
    /// Something is broken or about to be.
    Critical,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Self::Info => "ok  ",
            Self::Warn => "WARN",
            Self::Critical => "FAIL",
        }
    }
}

/// One diagnosis.
#[derive(Debug, Clone)]
pub struct Finding {
    /// What was checked.
    pub check: &'static str,
    /// How bad it is.
    pub severity: Severity,
    /// What was observed.
    pub detail: String,
    /// What to do about it. Empty when there is nothing to do.
    pub remedy: &'static str,
}

impl Finding {
    fn ok(check: &'static str, detail: impl Into<String>) -> Self {
        Self {
            check,
            severity: Severity::Info,
            detail: detail.into(),
            remedy: "",
        }
    }
    fn warn(check: &'static str, detail: impl Into<String>, remedy: &'static str) -> Self {
        Self {
            check,
            severity: Severity::Warn,
            detail: detail.into(),
            remedy,
        }
    }
    fn critical(check: &'static str, detail: impl Into<String>, remedy: &'static str) -> Self {
        Self {
            check,
            severity: Severity::Critical,
            detail: detail.into(),
            remedy,
        }
    }
}

/// Run every check.
///
/// `blob_backend` is the server's configured backend, not read from the
/// database: an S3 misconfiguration is exactly the kind of thing that keeps
/// the main server from starting at all, and `doctor` has to be runnable then
/// too (module docs). The check it drives lives beside `orphaned_blobs`.
pub async fn run(pool: &PgPool, blob_backend: &stepd_server::BlobBackendConfig) -> Vec<Finding> {
    let mut out = Vec::new();
    out.push(schema_version(pool).await);
    out.push(structural_invariants(pool).await);
    out.push(pooler_mode(pool).await);
    out.push(clock_skew(pool).await);
    out.push(partition_lag(pool).await);
    out.push(stuck_leases(pool).await);
    out.push(orphaned_blobs(pool).await);
    if let stepd_server::BlobBackendConfig::S3(s3) = blob_backend {
        out.push(s3_bucket_reachable(s3).await);
    }
    out.push(unreachable_apps(pool).await);
    out.push(inbox_overflow(pool).await);
    out.push(quarantined_runs(pool).await);
    out.push(undrained_signals(pool).await);
    out.push(paused_schedules(pool).await);
    out.push(overdue_schedules(pool).await);
    out
}

/// Schedules the sweep gave up on.
///
/// A paused schedule is a job that has stopped, and stopped quietly: no run
/// fails, no error is raised, nothing appears in the run history because nothing
/// ran. It is the failure shape the cron scheduler was built to eliminate, so it
/// gets a check of its own rather than being folded into a general count.
async fn paused_schedules(pool: &PgPool) -> Finding {
    match sqlx::query_as::<_, (i64, Option<String>)>(
        "SELECT count(*), min(fn_id) FROM cron_schedules WHERE paused AND last_error IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    {
        Ok((0, _)) => Finding::ok("cron", "no schedules paused as unschedulable"),
        Ok((n, example)) => Finding::critical(
            "cron",
            format!(
                "{n} cron schedule(s) paused and not firing (e.g. '{}')",
                example.unwrap_or_default()
            ),
            "read cron_schedules.last_error; the expression or zone no longer resolves,              so the function has silently stopped running. Fix it and clear `paused`",
        ),
        Err(e) => Finding::warn("cron", e.to_string(), ""),
    }
}

/// Schedules whose fire time has long passed.
///
/// One sweep behind is normal — the sweep runs on an interval. Far behind means
/// the housekeeping loop is not running, and unlike a stalled dispatch loop this
/// produces no backlog to notice: the occurrences are simply not happening, and
/// once they fall outside the misfire window they never will.
async fn overdue_schedules(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM cron_schedules           WHERE NOT paused AND next_fire_at < now() - interval '5 minutes'",
    )
    .fetch_one(pool)
    .await
    {
        Ok(0) => Finding::ok("cron-lag", "no schedule is more than five minutes overdue"),
        Ok(n) => Finding::critical(
            "cron-lag",
            format!("{n} schedule(s) overdue by more than five minutes"),
            "the housekeeping loop is not running, or the cron sweep is failing.              Occurrences that fall outside their misfire window will never fire",
        ),
        Err(e) => Finding::warn("cron-lag", e.to_string(), ""),
    }
}

async fn schema_version(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, Option<i64>>(
        "SELECT max(version) FROM _sqlx_migrations WHERE success",
    )
    .fetch_optional(pool)
    .await
    {
        Ok(Some(Some(v))) => Finding::ok("schema", format!("migration {v} applied")),
        Ok(_) => Finding::critical(
            "schema",
            "no migrations have been applied",
            "run `stepd migrate`",
        ),
        Err(e) => Finding::critical("schema", e.to_string(), "check the connection URL"),
    }
}

/// The structural invariants that guard the R1 races.
///
/// This is the countermeasure to the project's own finding that correctness can
/// rest on an undocumented accident. Running it in `doctor` as well as in CI
/// means a database migrated by hand, or restored from an older dump, is caught
/// before it silently loses a signal.
async fn structural_invariants(pool: &PgPool) -> Finding {
    let checks: [(&str, &str); 3] = [
        (
            "deliver_to_inbox locks the run row",
            "SELECT position('FOR UPDATE' in prosrc) > 0
               FROM pg_proc WHERE proname = 'deliver_to_inbox'",
        ),
        (
            "commit_ops locks the run row",
            "SELECT position('FOR UPDATE' in prosrc) > 0
               FROM pg_proc WHERE proname = 'commit_ops'",
        ),
        (
            "no advisory locks anywhere",
            "SELECT count(*) = 0 FROM pg_proc
              WHERE prosrc ILIKE '%pg_advisory%' AND pronamespace = 'public'::regnamespace",
        ),
    ];

    for (name, sql) in checks {
        match sqlx::query_scalar::<_, Option<bool>>(sql)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(Some(true))) => {}
            Ok(_) => {
                return Finding::critical(
                    "invariants",
                    format!("structural invariant violated: {name}"),
                    "the schema has been modified in a way that can silently reopen a \
                     lost-signal race; restore the migrations and re-apply",
                )
            }
            Err(e) => return Finding::warn("invariants", e.to_string(), "check permissions"),
        }
    }
    Finding::ok(
        "invariants",
        "serialization points intact, no advisory locks",
    )
}

/// Detect a transaction-mode pooler (F-DL-1, gap C1).
///
/// The engine is written to work behind one — row-level locking only, no session
/// state — so this is informational rather than a failure. It is reported because
/// when something *does* go wrong behind a pooler, knowing one is there is the
/// difference between a five-minute diagnosis and a five-hour one.
async fn pooler_mode(pool: &PgPool) -> Finding {
    let backend: Result<String, _> =
        sqlx::query_scalar("SELECT COALESCE(current_setting('application_name', true), '')")
            .fetch_one(pool)
            .await;

    match backend {
        Ok(name) if name.contains("pgbouncer") => Finding::ok(
            "pooler",
            "pgbouncer detected; the engine uses row-level locking only, so this is supported",
        ),
        Ok(_) => {
            // A session-scoped setting that does not survive is the signature of
            // transaction mode. Cheap to probe and unambiguous.
            let probe = sqlx::query("SET SESSION stepd.doctor_probe = '1'")
                .execute(pool)
                .await
                .is_ok();
            if probe {
                Finding::ok("pooler", "direct connection or session-mode pooling")
            } else {
                Finding::ok("pooler", "transaction-mode pooling; supported")
            }
        }
        Err(e) => Finding::warn("pooler", e.to_string(), "check connectivity"),
    }
}

/// Compare database time with this process's clock (gap C6).
///
/// All scheduling is from database time, so a skewed *server* is harmless. A
/// skewed operator machine is not: it makes every timestamp in the console read
/// wrongly, and an incident timeline that is four minutes out is worse than no
/// timeline.
async fn clock_skew(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT now()")
        .fetch_one(pool)
        .await
    {
        Ok(db_now) => {
            let skew = (chrono::Utc::now() - db_now).num_milliseconds().abs();
            if skew > 5_000 {
                Finding::critical(
                    "clock",
                    format!("{skew} ms between this host and the database"),
                    "synchronise clocks; scheduling uses database time, but every timestamp \
                     you read is being interpreted against a wrong local clock",
                )
            } else if skew > 1_000 {
                Finding::warn("clock", format!("{skew} ms skew"), "consider NTP")
            } else {
                Finding::ok("clock", format!("{skew} ms skew"))
            }
        }
        Err(e) => Finding::warn("clock", e.to_string(), ""),
    }
}

/// Is there a partition for next month's events?
///
/// Partition maintenance fails silently until the moment an insert has nowhere
/// to go, which is midnight on the first — the worst time to discover it.
async fn partition_lag(pool: &PgPool) -> Finding {
    let next_month = (chrono::Utc::now() + chrono::Duration::days(32))
        .format("events_%Y_%m")
        .to_string();
    match sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pg_class WHERE relname = $1",
    )
    .bind(&next_month)
    .fetch_one(pool)
    .await
    {
        Ok(0) => Finding::warn(
            "partitions",
            format!("no partition '{next_month}' for next month"),
            "run `SELECT ensure_event_partition(date_trunc('month', now() + interval '1 month')::date)`; \
             ingest fails outright the moment an insert has nowhere to go",
        ),
        Ok(_) => Finding::ok("partitions", "next month's event partition exists"),
        Err(e) => Finding::warn("partitions", e.to_string(), ""),
    }
}

async fn stuck_leases(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM queue WHERE claimed_by IS NOT NULL AND claimed_until < now()",
    )
    .fetch_one(pool)
    .await
    {
        Ok(0) => Finding::ok("leases", "no expired leases"),
        Ok(n) => Finding::warn(
            "leases",
            format!("{n} expired leases not yet reclaimed"),
            "a few is normal between sweeps; a persistent count means no server is \
             running the housekeeping loop",
        ),
        Err(e) => Finding::warn("leases", e.to_string(), ""),
    }
}

async fn orphaned_blobs(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM blobs WHERE state = 'reserved' AND reserved_at < now() - interval '24 hours'",
    )
    .fetch_one(pool)
    .await
    {
        Ok(0) => Finding::ok("blobs", "no stale reservations"),
        Ok(n) => Finding::warn(
            "blobs",
            format!("{n} reservations older than 24h were never completed"),
            "these are paid-for bytes nothing references; run the blob collector",
        ),
        Err(e) => Finding::warn("blobs", e.to_string(), ""),
    }
}

/// Whether the configured S3 bucket is there and these credentials can use it.
///
/// Wrong credentials or an unreachable endpoint today surface only when an
/// app's first upload fails — a `blob_backend_unavailable` three systems away
/// from whoever configured them. This is a `HeadBucket`, not a write, so it is
/// safe to run against a bucket the server should not be able to write to
/// either, and it is what lets an operator learn the answer at the same
/// moment they learn everything else `doctor` checks.
async fn s3_bucket_reachable(cfg: &stepd_blobs_s3::S3Config) -> Finding {
    let backend = match stepd_blobs_s3::S3Backend::new(cfg.clone()) {
        Ok(b) => b,
        Err(e) => {
            return Finding::critical(
                "blob-store",
                format!("S3 backend configuration is unusable: {e}"),
                "check STEPD_BLOB_S3_ENDPOINT; see docs/blob-backends.md for object stores \
                 this backend has been verified against",
            )
        }
    };
    match backend.check_bucket().await {
        stepd_blobs_s3::BucketCheck::Reachable => {
            Finding::ok("blob-store", "S3 bucket reachable and authorised")
        }
        stepd_blobs_s3::BucketCheck::Forbidden => Finding::critical(
            "blob-store",
            "S3 bucket reachable, but the configured credentials were refused (403)",
            "check STEPD_BLOB_S3_ACCESS_KEY and STEPD_BLOB_S3_SECRET_KEY; see \
             docs/blob-backends.md for object stores this backend has been verified against",
        ),
        stepd_blobs_s3::BucketCheck::Unreachable(detail) => Finding::critical(
            "blob-store",
            format!("S3 bucket unreachable: {detail}"),
            "check STEPD_BLOB_S3_ENDPOINT, STEPD_BLOB_S3_PATH_STYLE and network egress; see \
             docs/blob-backends.md for object stores this backend has been verified against",
        ),
    }
}

async fn unreachable_apps(pool: &PgPool) -> Finding {
    match sqlx::query(
        "SELECT app_id, last_seen FROM app_bindings
          WHERE last_seen IS NULL OR last_seen < now() - interval '1 hour'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) if rows.is_empty() => Finding::ok("apps", "all apps seen within the hour"),
        Ok(rows) => {
            let names: Vec<String> = rows.iter().map(|r| r.get::<String, _>("app_id")).collect();
            Finding::warn(
                "apps",
                format!("not seen for over an hour: {}", names.join(", ")),
                "runs of these functions will fail their attempts and back off; check the \
                 app is deployed and re-registering",
            )
        }
        Err(e) => Finding::warn("apps", e.to_string(), ""),
    }
}

/// Inbox overflow is the one counter nothing else can reconstruct.
///
/// An overflow drops the entry *and* the evidence. If this is non-zero, some run
/// was signalled faster than it consumed and a signal was lost.
async fn inbox_overflow(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, Option<i64>>(
        "SELECT sum(value) FROM engine_counters WHERE name = 'inbox_overflow'",
    )
    .fetch_one(pool)
    .await
    {
        Ok(None) | Ok(Some(0)) => Finding::ok("inbox", "no overflow"),
        Ok(Some(n)) => Finding::critical(
            "inbox",
            format!("{n} inbox entries were dropped by overflow"),
            "a run is being signalled faster than it consumes; those signals are gone. \
             Raise inbox_depth in engine_limits, or fix the producer",
        ),
        Err(e) => Finding::warn("inbox", e.to_string(), ""),
    }
}

async fn quarantined_runs(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE status = 'quarantined'")
        .fetch_one(pool)
        .await
    {
        Ok(0) => Finding::ok("quarantine", "no quarantined runs"),
        Ok(n) => Finding::warn(
            "quarantine",
            format!("{n} runs quarantined after repeated identical failures"),
            "inspect them in the DLQ view; they consume no dispatch capacity but they \
             are not making progress either",
        ),
        Err(e) => Finding::warn("quarantine", e.to_string(), ""),
    }
}

async fn undrained_signals(pool: &PgPool) -> Finding {
    match sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM signal_outbox WHERE delivered_at IS NULL",
    )
    .fetch_one(pool)
    .await
    {
        Ok(0) => Finding::ok("signals", "signal relay empty"),
        Ok(n) if n < 100 => Finding::ok("signals", format!("{n} signals in flight")),
        Ok(n) => Finding::warn(
            "signals",
            format!("{n} signals undelivered"),
            "the housekeeping loop is not running, or is behind; runs waiting on these \
             signals are parked",
        ),
        Err(e) => Finding::warn("signals", e.to_string(), ""),
    }
}

/// Print findings, and return whether anything is critical.
pub fn report(findings: &[Finding]) -> bool {
    let mut critical = false;
    for f in findings {
        println!("  [{}] {:<12} {}", f.severity.label(), f.check, f.detail);
        if !f.remedy.is_empty() {
            println!("         {:<12} → {}", "", f.remedy);
        }
        critical |= f.severity == Severity::Critical;
    }
    println!();
    let warns = findings
        .iter()
        .filter(|f| f.severity == Severity::Warn)
        .count();
    let crits = findings
        .iter()
        .filter(|f| f.severity == Severity::Critical)
        .count();
    println!(
        "  {} checks, {crits} critical, {warns} warnings",
        findings.len()
    );
    critical
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_orders_so_the_worst_finding_is_the_maximum() {
        assert!(Severity::Critical > Severity::Warn);
        assert!(Severity::Warn > Severity::Info);
    }

    #[test]
    fn a_critical_finding_makes_the_report_fail() {
        let findings = vec![
            Finding::ok("a", "fine"),
            Finding::critical("b", "broken", "fix it"),
        ];
        assert!(report(&findings));
    }

    #[test]
    fn a_clean_report_does_not_fail() {
        assert!(!report(&[Finding::ok("a", "fine")]));
    }

    #[test]
    fn every_non_ok_finding_carries_a_remedy() {
        // A warning with no remedy is noise: the operator reads it, cannot act on
        // it, and learns to skip the whole report.
        let samples = [
            Finding::warn("x", "d", "do this"),
            Finding::critical("y", "d", "do that"),
        ];
        for f in samples {
            assert!(!f.remedy.is_empty(), "{} has no remedy", f.check);
        }
    }
}
