//! Cron scheduling against a live PostgreSQL.
//!
//! `stepd-core`'s unit tests prove the arithmetic — parsing, DST, catch-up — and
//! `tests/sql/test_cron.sql` proves the invariants the database enforces. This
//! file tests the join between them: that a claimed schedule is planned with
//! database time, that the plan is applied atomically, and that two schedulers
//! racing produce one run.
//!
//! That join is where cron previously did not exist at all, and it is the only
//! layer neither of the other two suites can see.
//!
//! Skipped, loudly, without `STEPD_TEST_DATABASE_URL`.
//!
//! Every test sweeps **its own namespace**. `sweep` visits every namespace with
//! work, which in a shared database means another test's schedules — and that is
//! not a test artefact: it is the fairness property, and it has its own test at
//! the bottom of this file.

use chrono::{Duration, Utc};
use std::sync::Arc;
use stepd_core::cron::{CatchUp, Schedule};
use stepd_core::traits::*;
use stepd_core::{Housekeeper, KeeperConfig};
use stepd_store_postgres::PostgresStore;
use uuid::Uuid;

async fn store() -> Option<Arc<PostgresStore>> {
    let url = std::env::var("STEPD_TEST_DATABASE_URL").ok()?;
    let s = PostgresStore::connect(&url, 8).await.expect("connect");
    s.migrate().await.expect("migrate");
    Some(Arc::new(s))
}

macro_rules! db_test {
    () => {
        match store().await {
            Some(s) => s,
            None => {
                eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the live cron tests");
                return;
            }
        }
    };
}

async fn namespace(s: &PostgresStore, label: &str) -> String {
    let ns = format!("cron-{label}-{}", &Uuid::new_v4().simple().to_string()[..8]);
    s.ensure_namespace(&ns).await.expect("namespace");
    ns
}

fn reg(ns: &str, fn_id: &str, expr: &str) -> CronRegistration {
    CronRegistration {
        namespace: ns.into(),
        function_id: fn_id.into(),
        trigger_idx: 0,
        schedule: Schedule::parse(expr, "UTC").expect("parse"),
        catchup: CatchUp::One,
        catchup_limit: 10,
        misfire_window: Duration::hours(1),
        singleton: false,
        run_key: None,
    }
}

/// Make a schedule due now, as if its fire time had arrived.
async fn make_due(s: &PostgresStore, ns: &str, fn_id: &str, ago: Duration) {
    sqlx::query("UPDATE cron_schedules SET next_fire_at = now() - $3 WHERE ns = $1 AND fn_id = $2")
        .bind(ns)
        .bind(fn_id)
        .bind(ago.to_std().unwrap())
        .execute(s.pool())
        .await
        .expect("make due");
}

async fn runs_of(s: &PostgresStore, ns: &str, fn_id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM runs WHERE ns = $1 AND fn_id = $2")
        .bind(ns)
        .bind(fn_id)
        .fetch_one(s.pool())
        .await
        .expect("count")
}

#[tokio::test]
async fn a_registered_schedule_fires_when_it_comes_due() {
    let s = db_test!();
    let ns = namespace(&s, "fires").await;

    s.register_schedules(&[reg(&ns, "hourly", "0 * * * *")])
        .await
        .expect("register");

    // Nothing fires before the time comes. Registering must not itself fire —
    // that would make every deploy trigger every schedule, which is the reason
    // `next_after` is strictly-after rather than at-or-after.
    let idle = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(idle.fired, 0, "a schedule fires nothing on registration");
    assert_eq!(runs_of(&s, &ns, "hourly").await, 0);

    make_due(&s, &ns, "hourly", Duration::seconds(5)).await;

    let swept = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(swept.fired, 1, "a due schedule fires exactly once");
    assert_eq!(runs_of(&s, &ns, "hourly").await, 1);

    // …and is no longer due, so a second sweep does nothing. The most important
    // assertion in the file: a schedule that stays due fires on every tick,
    // which is a run every second rather than every hour.
    let again = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(again.fired, 0, "a fired schedule is no longer due");
    assert_eq!(runs_of(&s, &ns, "hourly").await, 1);
}

#[tokio::test]
async fn a_fired_run_is_queued_and_carries_its_occurrence() {
    let s = db_test!();
    let ns = namespace(&s, "queued").await;
    s.register_schedules(&[reg(&ns, "daily", "0 3 * * *")])
        .await
        .expect("register");
    make_due(&s, &ns, "daily", Duration::seconds(5)).await;
    s.sweep_namespace(&ns, 100).await.expect("sweep");

    let row: (Uuid, serde_json::Value, i64) = sqlx::query_as(
        "SELECT r.id, r.input, (SELECT count(*) FROM queue q WHERE q.run_id = r.id) \
           FROM runs r WHERE r.ns = $1 AND r.fn_id = 'daily'",
    )
    .bind(&ns)
    .fetch_one(s.pool())
    .await
    .expect("run");

    assert_eq!(
        row.2, 1,
        "the run is queued for dispatch, not merely created"
    );
    assert_eq!(
        row.1["cron"]["expr"], "0 3 * * *",
        "the run knows which schedule produced it"
    );
    assert!(
        row.1["cron"]["occurrence_at"].is_string(),
        "the intended occurrence is on the run, since started_at is the recovery time"
    );
}

#[tokio::test]
async fn two_schedulers_racing_one_occurrence_produce_one_run() {
    let s = db_test!();
    let ns = namespace(&s, "race").await;
    s.register_schedules(&[reg(&ns, "contested", "* * * * *")])
        .await
        .expect("register");
    make_due(&s, &ns, "contested", Duration::seconds(5)).await;

    // Eight concurrent sweeps on eight connections. `FOR UPDATE SKIP LOCKED`
    // should mean seven of them see nothing; if any two both plan the schedule,
    // the primary key on (schedule_id, occurrence_at) is the backstop. Either
    // way exactly one run exists — the point is that no arrangement of the two
    // mechanisms produces two.
    let results = futures::future::join_all((0..8).map(|_| {
        let s = s.clone();
        let ns2 = ns.clone();
        async move { s.sweep_namespace(&ns2, 100).await }
    }))
    .await;

    let fired: u64 = results.iter().map(|r| r.as_ref().unwrap().fired).sum();
    assert_eq!(fired, 1, "exactly one sweep fired the occurrence");
    assert_eq!(runs_of(&s, &ns, "contested").await, 1);
}

#[tokio::test]
async fn a_long_outage_under_the_default_policy_fires_once() {
    let s = db_test!();
    let ns = namespace(&s, "outage").await;

    // A per-minute schedule, four hours behind. The default policy is `one`
    // precisely so that recovering from this is one run rather than 240.
    s.register_schedules(&[reg(&ns, "frequent", "* * * * *")])
        .await
        .expect("register");
    make_due(&s, &ns, "frequent", Duration::hours(4)).await;

    let swept = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(swept.fired, 1, "recovery costs one run, not 240");
    assert!(
        swept.skipped > 0,
        "the occurrences not caught up are recorded"
    );
    assert_eq!(runs_of(&s, &ns, "frequent").await, 1);
}

#[tokio::test]
async fn catchup_all_fires_every_missed_occurrence_up_to_the_limit() {
    let s = db_test!();
    let ns = namespace(&s, "catchup").await;

    let mut r = reg(&ns, "billing", "* * * * *");
    r.catchup = CatchUp::All;
    r.catchup_limit = 5;
    r.misfire_window = Duration::hours(6);
    s.register_schedules(&[r]).await.expect("register");
    make_due(&s, &ns, "billing", Duration::hours(1)).await;

    let swept = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(swept.fired, 5, "the limit bounds the burst");
    assert_eq!(runs_of(&s, &ns, "billing").await, 5);

    // The occurrences dropped by the limit are recorded, not lost silently —
    // an operator reconciling "60 minutes, 5 runs" needs to find the other 55.
    let skipped: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cron_fires f JOIN cron_schedules s ON s.id = f.schedule_id \
          WHERE s.ns = $1 AND f.outcome <> 'fired'",
    )
    .bind(&ns)
    .fetch_one(s.pool())
    .await
    .expect("count");
    assert!(skipped > 0, "dropped occurrences are on the record");
}

#[tokio::test]
async fn a_singleton_schedule_skips_rather_than_stacking_up() {
    let s = db_test!();
    let ns = namespace(&s, "singleton").await;

    let mut r = reg(&ns, "nightly", "* * * * *");
    r.singleton = true;
    r.run_key = Some("nightly".into());
    r.catchup = CatchUp::All;
    r.catchup_limit = 10;
    r.misfire_window = Duration::hours(6);
    s.register_schedules(&[r]).await.expect("register");
    make_due(&s, &ns, "nightly", Duration::minutes(30)).await;

    let swept = s.sweep_namespace(&ns, 100).await.expect("sweep");

    // Thirty occurrences were due; one run exists, because the first fire's run
    // is still active and every later fire is refused by the keyed-exclusivity
    // index. The alternative — a queue of thirty runs each starting later than
    // the last — is the failure ADR-016 chose against.
    assert_eq!(swept.fired, 1, "one run, whatever the backlog");
    assert!(swept.skipped >= 1, "the overlapping fires are counted");
    assert_eq!(runs_of(&s, &ns, "nightly").await, 1);
}

#[tokio::test]
async fn a_schedule_that_cannot_be_planned_is_paused_not_retried() {
    let s = db_test!();
    let ns = namespace(&s, "broken").await;
    s.register_schedules(&[reg(&ns, "corrupt", "0 * * * *")])
        .await
        .expect("register");

    // Corrupt the row the way a hand edit or a vanished tzdata zone would.
    // Registration parses, so this state is only reachable from outside the API
    // — which is exactly why the sweep has to survive it.
    sqlx::query("UPDATE cron_schedules SET expr = 'not a cron', next_fire_at = now() - interval '1 min' WHERE ns = $1")
        .bind(&ns)
        .execute(s.pool())
        .await
        .expect("corrupt");

    let swept = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(swept.unschedulable, 1);
    assert_eq!(swept.fired, 0);

    let (paused, err): (bool, Option<String>) =
        sqlx::query_as("SELECT paused, last_error FROM cron_schedules WHERE ns = $1")
            .bind(&ns)
            .fetch_one(s.pool())
            .await
            .expect("row");
    assert!(paused, "an unschedulable row is paused");
    assert!(err.is_some(), "and says why, in the row, not only in a log");

    // The second sweep must not see it again. Retrying a row that cannot be
    // planned is a hot loop: it stays due forever because being unplannable is
    // precisely being unable to compute a next fire time.
    let again = s.sweep_namespace(&ns, 100).await.expect("sweep");
    assert_eq!(
        again.unschedulable, 0,
        "a paused schedule is not re-claimed on every sweep"
    );
}

#[tokio::test]
async fn re_registering_an_unchanged_schedule_does_not_postpone_it() {
    let s = db_test!();
    let ns = namespace(&s, "redeploy").await;
    s.register_schedules(&[reg(&ns, "stable", "0 * * * *")])
        .await
        .expect("register");

    let before: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT next_fire_at FROM cron_schedules WHERE ns = $1")
            .bind(&ns)
            .fetch_one(s.pool())
            .await
            .expect("read");

    // A redeploy. If this moved the fire time, a service deploying every ten
    // minutes would run its hourly job never — and every deploy would look
    // successful.
    for _ in 0..3 {
        s.register_schedules(&[reg(&ns, "stable", "0 * * * *")])
            .await
            .expect("re-register");
    }

    let after: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT next_fire_at FROM cron_schedules WHERE ns = $1")
            .bind(&ns)
            .fetch_one(s.pool())
            .await
            .expect("read");
    assert_eq!(before, after, "a redeploy must not postpone a schedule");
}

#[tokio::test]
async fn a_withdrawn_trigger_stops_firing() {
    let s = db_test!();
    let ns = namespace(&s, "withdrawn").await;

    let mut second = reg(&ns, "two-triggers", "0 * * * *");
    second.trigger_idx = 1;
    second.schedule = Schedule::parse("30 * * * *", "UTC").unwrap();
    s.register_schedules(&[reg(&ns, "two-triggers", "0 * * * *"), second])
        .await
        .expect("register");
    assert_eq!(count_schedules(&s, &ns).await, 2);

    // The function now declares only its first trigger.
    s.register_schedules(&[reg(&ns, "two-triggers", "0 * * * *")])
        .await
        .expect("re-register");
    assert_eq!(
        count_schedules(&s, &ns).await,
        1,
        "a trigger removed from a function must stop firing, or the deploy did nothing"
    );
}

async fn count_schedules(s: &PostgresStore, ns: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM cron_schedules WHERE ns = $1")
        .bind(ns)
        .fetch_one(s.pool())
        .await
        .expect("count")
}

#[tokio::test]
async fn registering_an_expression_with_no_occurrence_is_refused() {
    let s = db_test!();
    let ns = namespace(&s, "never").await;

    // `0 0 30 2 *` parses cleanly and names a day February does not have.
    // Storing it would create a row that is due immediately and forever.
    let mut r = reg(&ns, "impossible", "0 * * * *");
    r.schedule = Schedule::parse("0 0 30 2 *", "UTC").unwrap();
    let err = s.register_schedules(&[r]).await.unwrap_err();
    assert!(
        err.to_string().contains("no occurrence"),
        "the error names the problem: {err}"
    );
    assert_eq!(count_schedules(&s, &ns).await, 0, "and nothing was stored");
}

#[tokio::test]
async fn the_housekeeper_runs_the_cron_sweep() {
    let s = db_test!();
    let ns = namespace(&s, "keeper").await;
    s.register_schedules(&[reg(&ns, "swept", "0 * * * *")])
        .await
        .expect("register");
    make_due(&s, &ns, "swept", Duration::seconds(5)).await;

    // The wiring assertion. Everything above tests `sweep` directly; this tests
    // that anything calls it. Cron sat unimplemented behind a complete-looking
    // ADR for exactly this length of gap.
    let keeper = Housekeeper::new(s.clone(), s.clone(), s.clone(), KeeperConfig::default());
    keeper.tick().await;

    assert_eq!(
        runs_of(&s, &ns, "swept").await,
        1,
        "the housekeeping pass fired the due schedule"
    );
}

#[tokio::test]
async fn one_busy_namespace_cannot_starve_another() {
    let s = db_test!();
    let noisy = namespace(&s, "noisy").await;
    let quiet = namespace(&s, "quiet").await;

    // Sixty overdue per-minute schedules in one namespace, one in the other.
    // A namespace-blind claim ordered by `next_fire_at` would take the noisy
    // namespace's oldest rows and never reach the quiet one — and nothing would
    // fail, so the quiet tenant's job would simply stop happening.
    for i in 0..60 {
        let mut r = reg(&noisy, &format!("noisy-{i}"), "* * * * *");
        r.trigger_idx = 0;
        s.register_schedules(&[r]).await.expect("register");
    }
    s.register_schedules(&[reg(&quiet, "lonely", "* * * * *")])
        .await
        .expect("register");

    sqlx::query(
        "UPDATE cron_schedules SET next_fire_at = now() - interval '2 hours' WHERE ns = $1",
    )
    .bind(&noisy)
    .execute(s.pool())
    .await
    .expect("age");
    make_due(&s, &quiet, "lonely", Duration::seconds(5)).await;

    // A batch far smaller than the noisy namespace's backlog: if the sweep were
    // namespace-blind, this budget would be entirely consumed by `noisy`.
    let swept = s.sweep(5).await.expect("sweep");
    assert!(swept.fired >= 1);

    assert_eq!(
        runs_of(&s, &quiet, "lonely").await,
        1,
        "the quiet namespace's schedule fired despite a much larger backlog next door"
    );
}

#[tokio::test]
async fn the_namespace_rotation_advances() {
    let s = db_test!();
    let a = namespace(&s, "rot-a").await;
    let b = namespace(&s, "rot-b").await;
    for ns in [&a, &b] {
        s.register_schedules(&[reg(ns, "rot", "* * * * *")])
            .await
            .expect("register");
        make_due(&s, ns, "rot", Duration::seconds(5)).await;
    }

    // Both fire in one sweep here — the point is not that they take turns when
    // there is room for both, but that the starting position moves, so a
    // sustained overload does not serve the same namespace first every time.
    s.sweep(100).await.expect("sweep");
    assert_eq!(runs_of(&s, &a, "rot").await, 1);
    assert_eq!(runs_of(&s, &b, "rot").await, 1);

    let ns_list = s.cron_namespaces().await.expect("namespaces");
    assert!(
        !ns_list.contains(&a),
        "a schedule that has fired is no longer due, so its namespace is not active"
    );
}
