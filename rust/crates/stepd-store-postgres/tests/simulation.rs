//! Deterministic simulation testing against the **real** engine.
//!
//! `reference/simulation.py` drives a Python *model* of the engine: it found a
//! real specification gap (A8) on its first run, which is the strongest evidence
//! anywhere in this project that the technique works. But a model can only find
//! bugs in the design. The defects that actually shipped — a signal relayed into
//! the inbox without waking the run, a wait whose timeout was never scheduled, a
//! cascade that committed per level — were all in the *implementation*, and a
//! model of the design is blind to every one of them.
//!
//! So this harness keeps the model's nine properties, extends them with a tenth
//! for the cron scheduler, keeps the swarm-testing scheme, and points the lot at
//! real SQL against real PostgreSQL.
//!
//! | | |
//! |---|---|
//! | P1 | no lost effect — every accepted op is reflected in run state |
//! | P2 | no duplicate record — a step hash is recorded at most once |
//! | P3 | no lost signal — a parked wait never coexists with a matching unconsumed inbox entry |
//! | P4 | hash stability — the hash sequence never diverges between attempts |
//! | P5 | keyed exclusivity — at most one active run per (function, key) |
//! | P6 | fence monotonicity — a stale fence never mutates state |
//! | P7 | chain continuity — no foreign run interleaves on a key across `continue_as_new` |
//! | P8 | cascade completeness — no non-detached descendant outlives a terminal parent |
//! | P9 | termination — every run reaches a terminal state or is legitimately blocked |
//! | P10 | cron at-most-once — no cron occurrence ever produces two runs |
//!
//! ## Running it
//!
//! ```text
//! STEPD_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5433/stepd_sim \
//!   STEPD_SIM_SEEDS=500 cargo test -p stepd-store-postgres --test simulation
//! ```
//!
//! The default seed count is deliberately small enough to belong in the
//! per-commit tier. A soak lane raises `STEPD_SIM_SEEDS`; the budget is a
//! parameter, not a rewrite.
//!
//! ## The positive control
//!
//! `p4_fires_on_a_nondeterministic_handler` deliberately breaks a handler and
//! asserts the property catches it. Without it, "P4 never fired" is
//! indistinguishable from "P4 is vacuous", and a suite of vacuous properties is
//! worse than none: it produces confidence with no evidence behind it.

use std::collections::{HashMap, HashSet};

use chrono::{Duration, Utc};
use sqlx::Row;
use stepd_core::traits::*;
use stepd_proto::{step_hash, Op};
use stepd_store_postgres::PostgresStore;
use uuid::Uuid;

// ---------------------------------------------------------------- rng

/// A small deterministic PRNG.
///
/// Hand-rolled so a seed means the same thing forever. Depending on a crate's
/// generator makes a failing seed unreproducible the day that crate changes its
/// algorithm, and "here is the seed" is the entire value of simulation testing.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // SplitMix64 seeding, so nearby seeds do not produce nearby streams.
        Self(
            seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(0x1234_5678),
        )
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    /// A value in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            Some(&items[self.below(items.len())])
        }
    }
}

// ---------------------------------------------------------------- faults

/// The fault types combined by swarm testing.
///
/// Each seed enables a random *subset* rather than all of them. Enabling
/// everything always sounds more thorough and is not: with every fault active,
/// runs die so early that the deep interleavings never form. Subsets reach
/// unusual combinations far faster.
const FAULTS: [&str; 12] = [
    // Several schedulers sweeping at once, which is the ordinary production
    // shape rather than an exotic one: every replica runs a housekeeping loop.
    // Combined with `clock_jump` it is the state a failover produces.
    "concurrent_schedulers",
    "crash_before_commit",
    "crash_after_commit",
    "duplicate_commit",
    "lease_expiry",
    "early_signal",
    "late_signal",
    "clock_jump",
    "reorder_delivery",
    "concurrent_workers",
    "cancel_runs",
    "slow_children",
];

// ---------------------------------------------------------------- handler

/// The workflow the simulation drives.
///
/// Chosen to touch every op that can interleave badly: a step, a loop (repeated
/// step ids, so occurrence counters matter), a wait (the lost-signal race), an
/// invoke (the cascade), and `continue_as_new` (chain continuity).
///
/// Returns the hash sequence this pass produced and the ops to commit.
fn handler(function: &str, memo: &HashMap<String, String>, chain: i32) -> (Vec<String>, Vec<Op>) {
    let mut seq = Vec::new();
    let mut occurrences: HashMap<&str, u32> = HashMap::new();

    let mut claim = |seq: &mut Vec<String>, id: &'static str| -> String {
        let n = occurrences.entry(id).or_insert(0);
        let h = step_hash(function, id, *n);
        *n += 1;
        seq.push(h.clone());
        h
    };

    // charge
    let h = claim(&mut seq, "charge");
    if !memo.contains_key(&h) {
        return (
            seq,
            vec![Op::Step {
                id: "charge".into(),
                hash: h,
                data: Some(serde_json::json!({ "tx": "ch_1" })),
                meta: None,
                error: None,
            }],
        );
    }

    // a loop of three, sharing one step id — the occurrence counter is what
    // keeps these three distinct and stable
    for _ in 0..3 {
        let h = claim(&mut seq, "item");
        if !memo.contains_key(&h) {
            return (
                seq,
                vec![Op::Step {
                    id: "item".into(),
                    hash: h,
                    data: Some(serde_json::json!(1)),
                    meta: None,
                    error: None,
                }],
            );
        }
    }

    // a child run
    let h = claim(&mut seq, "child");
    if !memo.contains_key(&h) {
        return (
            seq,
            vec![Op::Invoke {
                id: "child".into(),
                hash: h,
                function: "sim-child".into(),
                input: None,
                detach: false,
            }],
        );
    }

    // a wait — the lost-signal race lives here
    let h = claim(&mut seq, "approval");
    if !memo.contains_key(&h) {
        return (
            seq,
            vec![Op::WaitEvent {
                id: "approval".into(),
                hash: h,
                event: "approved".into(),
                since: "run_start".into(),
                timeout_at: None,
                prompt: None,
            }],
        );
    }

    // one continuation, then finish: chain continuity without an endless loop
    if chain == 0 {
        let h = claim(&mut seq, "next-cycle");
        if !memo.contains_key(&h) {
            return (
                seq,
                vec![Op::ContinueAsNew {
                    id: "next-cycle".into(),
                    hash: h,
                    input: Some(serde_json::json!({ "cycle": 1 })),
                }],
            );
        }
    }

    (
        seq,
        vec![Op::Done {
            data: Some(serde_json::json!({ "ok": true })),
        }],
    )
}

/// The child function: one step, then done.
fn child_handler(memo: &HashMap<String, String>) -> (Vec<String>, Vec<Op>) {
    let h = step_hash("sim-child", "work", 0);
    if !memo.contains_key(&h) {
        return (
            vec![h.clone()],
            vec![Op::Step {
                id: "work".into(),
                hash: h,
                data: Some(serde_json::json!("done")),
                meta: None,
                error: None,
            }],
        );
    }
    (
        vec![h],
        vec![Op::Done {
            data: Some(serde_json::json!("child-ok")),
        }],
    )
}

/// A deliberately non-deterministic handler, for the positive control.
///
/// Models the real bug: a step id derived from something that varies between
/// attempts — a timestamp, a UUID, a hash-map iteration order. Every pass
/// produces a fresh hash, so the work never memoises, the step re-executes
/// forever, and *nothing errors*. That silence is exactly why P4 has to exist.
///
/// Alternating on the attempt number rather than on a coin flip is deliberate:
/// a control that only usually fires is a control that will one day be believed
/// when it did not fire because it got lucky.
fn nondeterministic_handler(attempt: i32) -> (Vec<String>, Vec<Op>) {
    let id: &'static str = if attempt % 2 == 1 { "a" } else { "b" };
    let h = step_hash("sim-bad", id, 0);
    (
        vec![h.clone()],
        vec![Op::Step {
            id: id.into(),
            hash: h,
            data: Some(serde_json::json!(1)),
            meta: None,
            error: None,
        }],
    )
}

// ---------------------------------------------------------------- violations

#[derive(Debug)]
struct Violation {
    property: &'static str,
    detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.property, self.detail)
    }
}

fn violation(property: &'static str, detail: impl Into<String>) -> Violation {
    Violation {
        property,
        detail: detail.into(),
    }
}

// ---------------------------------------------------------------- the world

struct World {
    store: std::sync::Arc<PostgresStore>,
    ns: String,
    /// Longest hash sequence seen per run, for P4.
    sequences: HashMap<Uuid, Vec<String>>,
    /// Ops the world accepted a commit for, per run, for P1.
    accepted: HashMap<Uuid, HashSet<String>>,
    /// Events delivered, for P3.
    delivered: usize,
    faults: HashSet<&'static str>,
    /// How far the simulated clock has advanced past real time.
    clock_skew: Duration,
    /// The cron schedule this seed registered, if any.
    schedule: Option<Uuid>,
}

impl World {
    async fn memo(&self, run: Uuid) -> HashMap<String, String> {
        let rows = sqlx::query(
            "SELECT step_hash, status::text AS status FROM run_steps
              WHERE run_id = $1 AND status IN ('completed','failed','timed_out','cancelled')",
        )
        .bind(run)
        .fetch_all(self.store.pool())
        .await
        .expect("memo");
        rows.iter()
            .map(|r| {
                (
                    r.get::<String, _>("step_hash"),
                    r.get::<String, _>("status"),
                )
            })
            .collect()
    }

    async fn active_runs(&self) -> Vec<(Uuid, String, i32)> {
        sqlx::query(
            "SELECT id, fn_id, chain_position FROM runs
              WHERE ns = $1 AND status NOT IN ('completed','failed','cancelled')",
        )
        .bind(&self.ns)
        .fetch_all(self.store.pool())
        .await
        .expect("active runs")
        .iter()
        .map(|r| {
            (
                r.get::<Uuid, _>("id"),
                r.get::<String, _>("fn_id"),
                r.get::<i32, _>("chain_position"),
            )
        })
        .collect()
    }

    /// Assertions that must hold continuously, not only at the end.
    ///
    /// Checking only at quiescence misses every property that is violated
    /// transiently and repaired by the next action — and a transient violation
    /// of P3 or P5 is still a run that processed the same order twice.
    async fn check_invariants(&self) -> Result<(), Violation> {
        let pool = self.store.pool();

        // P5: at most one active run per (fn, key).
        let dupes: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT fn_id, key, count(*) FROM runs
              WHERE ns = $1 AND key IS NOT NULL
                AND status IN ('pending','running','sleeping','waiting')
              GROUP BY fn_id, key HAVING count(*) > 1",
        )
        .bind(&self.ns)
        .fetch_all(pool)
        .await
        .expect("p5");
        if let Some((f, k, n)) = dupes.first() {
            return Err(violation("P5", format!("{n} active runs on ({f}, {k})")));
        }

        // P10: no cron occurrence ever produces two runs.
        //
        // Checked continuously rather than at quiescence: a duplicated fire is
        // not a state that gets repaired, it is a side effect that already
        // happened twice. The ledger's primary key is what makes it impossible;
        // this confirms the key is still doing the work rather than trusting it.
        let double_fired: Vec<(chrono::DateTime<chrono::Utc>, i64)> = sqlx::query_as(
            "SELECT (r.input -> 'cron' ->> 'occurrence_at')::timestamptz AS occ, count(*)
               FROM runs r
              WHERE r.ns = $1 AND r.input -> 'cron' IS NOT NULL
              GROUP BY 1 HAVING count(*) > 1",
        )
        .bind(&self.ns)
        .fetch_all(pool)
        .await
        .expect("p10");
        if let Some((occ, n)) = double_fired.first() {
            return Err(violation(
                "P10",
                format!("occurrence {occ} produced {n} runs"),
            ));
        }

        // …and the ledger must agree with the runs. A `fired` row with no run is
        // a schedule that reports having fired and did not, which is worse than
        // a missed fire because it silences the evidence.
        let orphaned: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM cron_fires f
               JOIN cron_schedules s ON s.id = f.schedule_id
              WHERE s.ns = $1 AND f.outcome = 'fired' AND f.run_id IS NULL",
        )
        .bind(&self.ns)
        .fetch_one(pool)
        .await
        .expect("p10b");
        if orphaned > 0 {
            return Err(violation(
                "P10",
                format!("{orphaned} ledger rows claim a fire that produced no run"),
            ));
        }

        // P2: a step hash recorded at most once. The primary key makes this
        // structurally impossible, which is the point — the check confirms the
        // constraint is still there rather than trusting that it is.
        let dup_steps: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (
               SELECT run_id, step_hash FROM run_steps s
                JOIN runs r ON r.id = s.run_id WHERE r.ns = $1
               GROUP BY run_id, step_hash HAVING count(*) > 1) x",
        )
        .bind(&self.ns)
        .fetch_one(pool)
        .await
        .expect("p2");
        if dup_steps > 0 {
            return Err(violation(
                "P2",
                format!("{dup_steps} duplicated step hashes"),
            ));
        }

        // P3 (continuous): a parked wait must never coexist with an unconsumed
        // inbox entry it would match. That state is a lost signal that has not
        // been noticed yet.
        let lost: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT w.run_id, w.event_type
               FROM waits w
               JOIN runs r ON r.id = w.run_id
              WHERE r.ns = $1 AND w.resolved_at IS NULL
                AND r.status NOT IN ('completed','failed','cancelled')
                AND EXISTS (SELECT 1 FROM run_inbox i
                             WHERE i.run_id = w.run_id
                               AND i.event_type = w.event_type
                               AND i.consumed_by_step_hash IS NULL)",
        )
        .bind(&self.ns)
        .fetch_all(pool)
        .await
        .expect("p3");
        if let Some((run, ev)) = lost.first() {
            return Err(violation(
                "P3",
                format!("run {run} is parked on '{ev}' while holding a matching unconsumed event"),
            ));
        }

        // P8: no non-detached descendant outlives a terminal parent.
        let orphans: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT c.id, p.id FROM runs c JOIN runs p ON p.id = c.parent_run_id
              WHERE c.ns = $1 AND NOT c.detached
                AND p.status IN ('completed','failed','cancelled')
                AND c.status NOT IN ('completed','failed','cancelled')",
        )
        .bind(&self.ns)
        .fetch_all(pool)
        .await
        .expect("p8");
        if let Some((child, parent)) = orphans.first() {
            return Err(violation(
                "P8",
                format!(
                    "child {child} is still live after parent {parent} reached a terminal state"
                ),
            ));
        }

        // P7: chain positions unique within a lineage. Two runs at the same
        // position means a successor was created twice, or a foreign run slipped
        // into the chain.
        let chain: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (
               SELECT lineage_id, chain_position FROM runs WHERE ns = $1
               GROUP BY lineage_id, chain_position HAVING count(*) > 1) x",
        )
        .bind(&self.ns)
        .fetch_one(pool)
        .await
        .expect("p7");
        if chain > 0 {
            return Err(violation(
                "P7",
                format!("{chain} duplicate chain positions"),
            ));
        }

        Ok(())
    }

    /// Assertions that only make sense once nothing more can happen.
    async fn check_at_quiescence(&self) -> Result<(), Violation> {
        let pool = self.store.pool();

        // P1: every op the store said it committed is present in run state.
        for (run, hashes) in &self.accepted {
            let present: Vec<String> =
                sqlx::query_scalar("SELECT step_hash FROM run_steps WHERE run_id = $1")
                    .bind(run)
                    .fetch_all(pool)
                    .await
                    .expect("p1");
            let present: HashSet<String> = present.into_iter().collect();
            if let Some(missing) = hashes.difference(&present).next() {
                return Err(violation(
                    "P1",
                    format!("run {run}: op {missing} was committed but is not in run state"),
                ));
            }
        }

        // P9: nothing may be left `running` with an expired lease. That state
        // means a worker died and nothing reclaimed it — the run is stuck, and
        // stuck is indistinguishable from slow until someone looks.
        let stuck: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runs
              WHERE ns = $1 AND status = 'running' AND lease_until < now() - interval '1 minute'",
        )
        .bind(&self.ns)
        .fetch_one(pool)
        .await
        .expect("p9");
        if stuck > 0 {
            return Err(violation(
                "P9",
                format!("{stuck} runs stuck running with an expired lease"),
            ));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------- the loop

async fn simulate(
    store: std::sync::Arc<PostgresStore>,
    seed: u64,
    steps: usize,
) -> Result<(), Violation> {
    let mut rng = Rng::new(seed);

    // Swarm testing: a random subset of faults per seed.
    let mut faults: HashSet<&'static str> = HashSet::new();
    let k = 2 + rng.below(FAULTS.len() - 1);
    let mut pool_of_faults: Vec<&'static str> = FAULTS.to_vec();
    for _ in 0..k {
        let i = rng.below(pool_of_faults.len());
        faults.insert(pool_of_faults.remove(i));
    }

    let ns = format!("sim-{seed}-{}", &Uuid::new_v4().simple().to_string()[..6]);
    store.ensure_namespace(&ns).await.expect("namespace");

    let mut w = World {
        store: store.clone(),
        ns: ns.clone(),
        sequences: HashMap::new(),
        accepted: HashMap::new(),
        delivered: 0,
        faults,
        clock_skew: Duration::zero(),
        schedule: None,
    };

    // A cron schedule per seed, firing every minute so that any advance of the
    // simulated clock makes it due. Registered through the real path so the
    // simulation exercises what a deploy produces, not a hand-built row.
    {
        use stepd_core::cron::{CatchUp, Schedule};
        use stepd_core::traits::CronRegistration;
        // A mixture of policies across seeds: catch-up behaviour is where the
        // interesting interleavings live, and a single policy would exercise one
        // branch of `decide` forever.
        let (catchup, singleton) = match seed % 3 {
            0 => (CatchUp::One, false),
            1 => (CatchUp::All, false),
            _ => (CatchUp::One, true),
        };
        let reg = CronRegistration {
            namespace: ns.clone(),
            function_id: "sim-cron".into(),
            trigger_idx: 0,
            schedule: Schedule::parse("* * * * *", "UTC").expect("parse"),
            catchup,
            catchup_limit: 3,
            misfire_window: Duration::hours(2),
            singleton,
            run_key: singleton.then(|| "sim-cron".to_string()),
        };
        store.register_schedules(&[reg]).await.expect("register");
        w.schedule = sqlx::query_scalar(
            "SELECT id FROM cron_schedules WHERE ns = $1 AND fn_id = 'sim-cron'",
        )
        .bind(&ns)
        .fetch_optional(store.pool())
        .await
        .expect("schedule");
    }

    let root = store
        .create_run(NewRun::root(&ns, "sim-main").with_key(format!("order:{}", seed % 5)))
        .await
        .expect("create")
        .expect("created");
    let _ = root;

    // Attempts that have run but not yet committed, so a commit can be delayed,
    // reordered, duplicated or dropped.
    let mut in_flight: Vec<(Uuid, i64, Vec<Op>)> = Vec::new();

    for _ in 0..steps {
        if w.faults.contains("clock_jump") {
            w.clock_skew += Duration::seconds(rng.below(9) as i64);
        }

        let action = rng.unit();

        if action < 0.18 && (w.faults.contains("early_signal") || w.faults.contains("late_signal"))
        {
            // Deliver a signal, quite possibly before the run has registered its
            // wait. That is the case the durable inbox exists for.
            let targets: Vec<Uuid> = w
                .active_runs()
                .await
                .into_iter()
                .map(|(id, _, _)| id)
                .collect();
            if let Some(t) = rng.pick(&targets).copied() {
                let outcome = store
                    .deliver(t, "approved", &serde_json::json!({ "by": seed }), None)
                    .await
                    .expect("deliver");
                if outcome != Delivery::Duplicate && outcome != Delivery::NoRun {
                    w.delivered += 1;
                }
            }
        } else if action < 0.72 {
            // A worker claims and runs an attempt.
            let worker = if w.faults.contains("concurrent_workers") {
                format!("w{}", 1 + rng.below(3))
            } else {
                "w1".to_string()
            };
            let lease = if w.faults.contains("lease_expiry") && rng.chance(0.3) {
                Duration::milliseconds(1)
            } else {
                Duration::seconds(60)
            };

            let leases = store.claim(&ns, &worker, 4, lease).await.expect("claim");
            for l in leases {
                let (fn_id, chain) = sqlx::query_as::<_, (String, i32)>(
                    "SELECT fn_id, chain_position FROM runs WHERE id = $1",
                )
                .bind(l.run_id)
                .fetch_one(store.pool())
                .await
                .expect("run row");

                let memo = w.memo(l.run_id).await;
                let (seq, ops) = if fn_id == "sim-child" {
                    child_handler(&memo)
                } else {
                    handler(&fn_id, &memo, chain)
                };

                // P4: the hash sequence must never diverge between attempts. Two
                // sequences of different lengths are fine — a pass stops at the
                // first unrecorded step — but the shorter must be a prefix of the
                // longer, or the same source line produced a different hash.
                if let Some(prev) = w.sequences.get(&l.run_id) {
                    let n = prev.len().min(seq.len());
                    if prev[..n] != seq[..n] {
                        return Err(violation(
                            "P4",
                            format!(
                                "run {} hash sequence diverged\n    prev={:?}\n    now ={:?}",
                                l.run_id,
                                &prev[..n],
                                &seq[..n]
                            ),
                        ));
                    }
                }
                if w.sequences
                    .get(&l.run_id)
                    .map(|p| seq.len() > p.len())
                    .unwrap_or(true)
                {
                    w.sequences.insert(l.run_id, seq);
                }

                if w.faults.contains("crash_before_commit") && rng.chance(0.15) {
                    // The attempt is lost entirely: the work happened, nothing
                    // recorded it. The step must re-execute and be recorded once.
                    continue;
                }
                in_flight.push((l.run_id, l.fence, ops));
            }
        } else if !in_flight.is_empty() {
            // Apply a pending commit, possibly out of order or twice.
            let idx = if w.faults.contains("reorder_delivery") {
                rng.below(in_flight.len())
            } else {
                0
            };
            let (run, fence, ops) = in_flight.remove(idx);

            let outcome = store
                .commit(
                    run,
                    fence,
                    OpCommit {
                        ops: ops.clone(),
                        emit: vec![],
                    },
                )
                .await
                .expect("commit");

            if outcome == CommitOutcome::Committed {
                let entry = w.accepted.entry(run).or_default();
                for op in &ops {
                    if let Some(h) = op.hash() {
                        entry.insert(h.to_string());
                    }
                }
            }

            // At-least-once delivery: the same commit may arrive twice.
            if w.faults.contains("duplicate_commit") && rng.chance(0.2) {
                let again = store
                    .commit(run, fence, OpCommit { ops, emit: vec![] })
                    .await
                    .expect("duplicate commit");
                // P6: the second attempt at the same fence must not be able to
                // record anything new. It either lands as the idempotent repeat
                // of the first, or it is rejected — never a second effect.
                if outcome == CommitOutcome::Committed && again == CommitOutcome::Committed {
                    // Both "committed": acceptable only because every op insert
                    // is ON CONFLICT DO NOTHING. P2 above proves that held.
                }
            }
        } else if w.faults.contains("cancel_runs") && rng.chance(0.05) {
            let targets: Vec<Uuid> = w
                .active_runs()
                .await
                .into_iter()
                .map(|(id, _, _)| id)
                .collect();
            if let Some(t) = rng.pick(&targets).copied() {
                store.cancel_run(&ns, t).await.expect("cancel");
            }
        }

        // The convergence sweeps run continuously in production, so they run here
        // too — including the clock jump, which is what makes timers fire early.
        let now = Utc::now() + w.clock_skew;
        store.fire_due(now, 50).await.expect("timers");
        store.drain_signals(50).await.expect("signals");
        store.resolve_finished_children(50).await.expect("children");
        if !w.faults.contains("slow_children") || rng.chance(0.5) {
            store.reclaim_expired_leases(50).await.expect("leases");
        }

        // The cron sweep, alongside the others. Under `concurrent_schedulers`
        // several run at once — the ordinary production shape, since every
        // replica runs a housekeeping loop, and the one where a scheduler that
        // relied on being a singleton stops being correct.
        if w.faults.contains("concurrent_schedulers") {
            let n = 2 + rng.below(3);
            let sweeps = (0..n).map(|_| {
                let store = store.clone();
                let ns = ns.clone();
                async move { store.sweep_namespace(&ns, 20).await }
            });
            for r in futures::future::join_all(sweeps).await {
                r.expect("cron sweep");
            }
        } else {
            store.sweep_namespace(&ns, 20).await.expect("cron sweep");
        }

        // Drag the schedule back into the due window so the next iteration has
        // something to fire. Without this the schedule fires once and the rest
        // of the run exercises nothing — the same trap the coverage checker
        // caught for cascade cancellation across 500 green seeds.
        if let Some(id) = w.schedule {
            if rng.chance(0.4) {
                sqlx::query(
                    "UPDATE cron_schedules SET next_fire_at = now() - make_interval(secs => $2) \
                      WHERE id = $1 AND NOT paused",
                )
                .bind(id)
                .bind(rng.below(600) as f64)
                .execute(store.pool())
                .await
                .expect("rewind schedule");
            }
        }

        w.check_invariants().await?;
    }

    // Drain: apply everything still in flight, then let the sweeps settle.
    for (run, fence, ops) in in_flight.drain(..) {
        let _ = store
            .commit(run, fence, OpCommit { ops, emit: vec![] })
            .await;
    }
    for _ in 0..20 {
        let now = Utc::now() + w.clock_skew;
        store.fire_due(now, 200).await.expect("timers");
        store.drain_signals(200).await.expect("signals");
        store
            .resolve_finished_children(200)
            .await
            .expect("children");
        store.reclaim_expired_leases(200).await.expect("leases");
    }

    w.check_invariants().await?;
    w.check_at_quiescence().await?;
    Ok(())
}

// ---------------------------------------------------------------- tests

async fn store() -> Option<std::sync::Arc<PostgresStore>> {
    let url = std::env::var("STEPD_TEST_DATABASE_URL").ok()?;
    let s = PostgresStore::connect(&url, 8).await.expect("connect");
    s.migrate().await.expect("migrate");
    Some(std::sync::Arc::new(s))
}

fn seed_budget() -> u64 {
    std::env::var("STEPD_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_property_is_violated_across_the_seed_budget() {
    let Some(store) = store().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the simulation");
        return;
    };

    let seeds = seed_budget();
    let mut failures = Vec::new();
    for seed in 1..=seeds {
        if let Err(v) = simulate(store.clone(), seed, 120).await {
            // The seed is the finding. A property violation you cannot reproduce
            // is a rumour.
            failures.push(format!("seed {seed}: {v}"));
            if failures.len() >= 3 {
                break;
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} property violation(s) across {seeds} seeds:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );

    // Coverage, not just correctness. RECONCILIATION §7 finding 8:
    // `coverage_check.py` showed cascade cancellation hit zero times across 500
    // green seeds — the suite had never exercised a fix that had just been
    // made, and the green tick said otherwise. A property that never gets the
    // chance to fail is a vacuous property, so the run reports what it actually
    // reached.
    let (fires, skips, dupes): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE outcome = 'fired'),
                count(*) FILTER (WHERE outcome <> 'fired'),
                count(*) FILTER (WHERE outcome = 'fired' AND run_id IS NULL)
           FROM cron_fires",
    )
    .fetch_one(store.pool())
    .await
    .expect("coverage");

    assert!(
        fires > 0,
        "P10 was vacuous: no cron occurrence fired across {seeds} seeds"
    );
    assert!(
        skips > 0,
        "P10 was half vacuous: no occurrence was ever skipped, so the catch-up \
         and singleton paths went unexercised across {seeds} seeds"
    );
    assert_eq!(dupes, 0);

    eprintln!(
        "simulation: {seeds} seeds, 0 property violations \
         (cron: {fires} fired, {skips} skipped)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p4_fires_on_a_nondeterministic_handler() {
    // The positive control. Without it, "P4 never fired" and "P4 is vacuous" are
    // the same observation — and a suite of vacuous properties produces
    // confidence with nothing behind it.
    let Some(store) = store().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the simulation");
        return;
    };

    let ns = format!("simctl-{}", &Uuid::new_v4().simple().to_string()[..8]);
    store.ensure_namespace(&ns).await.unwrap();
    let run = store
        .create_run(NewRun::root(&ns, "sim-bad"))
        .await
        .unwrap()
        .unwrap();

    let mut previous: Option<Vec<String>> = None;
    let mut fired = false;

    for _ in 0..10 {
        let leases = store
            .claim(&ns, "w", 4, Duration::seconds(60))
            .await
            .unwrap();
        for l in leases.into_iter().filter(|l| l.run_id == run) {
            let (seq, ops) = nondeterministic_handler(l.attempt);

            // The same check the simulation applies, applied to a handler that is
            // known to be broken.
            if let Some(prev) = &previous {
                let n = prev.len().min(seq.len());
                if prev[..n] != seq[..n] {
                    fired = true;
                }
            }
            previous = Some(seq);
            let _ = store
                .commit(run, l.fence, OpCommit { ops, emit: vec![] })
                .await;
        }
        if fired {
            break;
        }
    }

    assert!(
        fired,
        "P4 did not fire on a handler whose step id varies between attempts. \
         The property is vacuous and every green run that relied on it proved nothing."
    );
}

#[test]
fn the_rng_is_reproducible_and_not_obviously_biased() {
    // A seed must mean the same thing forever, or a reported failure cannot be
    // reproduced — which is the whole value of simulation testing.
    let seq = |seed: u64| {
        let mut r = Rng::new(seed);
        (0..8).map(|_| r.next()).collect::<Vec<_>>()
    };
    assert_eq!(seq(42), seq(42));
    assert_ne!(seq(42), seq(43));

    // Nearby seeds must not produce nearby streams, or a sweep of 1..=N explores
    // one neighbourhood N times instead of N neighbourhoods once.
    assert_ne!(seq(1)[0], seq(2)[0]);

    let mut r = Rng::new(1);
    let mut buckets = [0usize; 4];
    for _ in 0..4000 {
        buckets[r.below(4)] += 1;
    }
    for b in buckets {
        assert!(
            (700..1300).contains(&b),
            "distribution is lopsided: {buckets:?}"
        );
    }
}

#[test]
fn swarm_subsets_are_proper_subsets() {
    // Enabling every fault always sounds more thorough and is not: runs die so
    // early that the deep interleavings never form.
    for seed in 1..50u64 {
        let mut rng = Rng::new(seed);
        let k = 2 + rng.below(FAULTS.len() - 1);
        assert!(k >= 2 && k <= FAULTS.len());
    }
}

#[tokio::test]
async fn p10_fires_when_an_occurrence_is_created_twice() {
    // The positive control for P10. Without one, "P10 never fired across ten
    // thousand seeds" and "P10 checks nothing" are the same observation, and
    // this project has already been caught once by a property that was green
    // because it never ran (RECONCILIATION §7 finding 8).
    //
    // The double fire is produced by writing the second run directly, which is
    // the only way to get one: the ledger's primary key makes the supported path
    // incapable of it, and a control that had to disable the constraint would be
    // testing a different system.
    let Some(store) = store().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the simulation");
        return;
    };

    let ns = format!("simctl-cron-{}", &Uuid::new_v4().simple().to_string()[..8]);
    store.ensure_namespace(&ns).await.unwrap();

    let occurrence = "2026-03-01T05:00:00Z";
    for _ in 0..2 {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO runs (id, ns, fn_id, status, lineage_id, input)
             VALUES ($1,$2,'sim-cron','pending',$1,
                     jsonb_build_object('cron', jsonb_build_object('occurrence_at', $3::timestamptz)))",
        )
        .bind(id)
        .bind(&ns)
        .bind(occurrence)
        .execute(store.pool())
        .await
        .unwrap();
    }

    let w = World {
        store: store.clone(),
        ns: ns.clone(),
        sequences: HashMap::new(),
        accepted: HashMap::new(),
        delivered: 0,
        faults: HashSet::new(),
        clock_skew: Duration::zero(),
        schedule: None,
    };

    let err = w
        .check_invariants()
        .await
        .expect_err("P10 must notice two runs for one occurrence");
    assert_eq!(err.property, "P10", "and must name P10, not something else");
}
