//! Integration tests against a live PostgreSQL.
//!
//! These are the tests the archived Rust tree never had: everything else in the
//! workspace runs against in-memory fakes, which cannot tell you whether the SQL
//! the store actually sends does what the store thinks it does.
//!
//! Skipped, loudly, when `STEPD_TEST_DATABASE_URL` is unset — a database test
//! that silently passes when it did not run is worse than no test, because the
//! green tick is then a lie about the thing most likely to break.
//!
//!     STEPD_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5433/stepd_it \
//!       cargo test -p stepd-store-postgres --test live
//!
//! Each test works in its own namespace and creates its own runs, so the file is
//! safe to run repeatedly against the same database without a reset step.

use chrono::{Duration, Utc};
use std::sync::Arc;
use stepd_core::traits::*;
use stepd_core::{DispatchConfig, Dispatcher, Housekeeper, KeeperConfig, TargetResolver};
use stepd_proto::*;
use stepd_store_postgres::PostgresStore;
use uuid::Uuid;

/// Connect and migrate, or skip.
async fn store() -> Option<Arc<PostgresStore>> {
    let url = std::env::var("STEPD_TEST_DATABASE_URL").ok()?;
    let s = PostgresStore::connect(&url, 8).await.expect("connect");
    s.migrate().await.expect("migrate");
    Some(Arc::new(s))
}

macro_rules! db_test {
    ($store:ident) => {
        match store().await {
            Some(s) => s,
            None => {
                eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the live-database tests");
                return;
            }
        }
    };
}

/// A fresh namespace per test, so concurrent runs of the suite cannot collide.
async fn namespace(s: &PostgresStore, label: &str) -> String {
    let ns = format!("it-{label}-{}", &Uuid::new_v4().simple().to_string()[..8]);
    s.ensure_namespace(&ns).await.expect("namespace");
    ns
}

/// An app that replays a fixed script: the Nth attempt returns the Nth response.
///
/// Scripted rather than generated because these tests are about the *store*.
/// Using a real SDK here would mean a store failure and an SDK failure look the
/// same, and the whole point is to separate them.
struct ScriptedApp {
    responses: std::sync::Mutex<Vec<AttemptResponse>>,
    seen: std::sync::Mutex<Vec<Attempt>>,
}

impl ScriptedApp {
    fn new(responses: Vec<AttemptResponse>) -> Arc<Self> {
        Arc::new(Self {
            responses: std::sync::Mutex::new(responses.into_iter().rev().collect()),
            seen: std::sync::Mutex::new(vec![]),
        })
    }
    fn attempts(&self) -> Vec<Attempt> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Transport for ScriptedApp {
    async fn deliver(
        &self,
        _t: &AppTarget,
        attempt: &Attempt,
    ) -> std::result::Result<AttemptResponse, stepd_core::Error> {
        self.seen.lock().unwrap().push(attempt.clone());
        self.responses
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| stepd_core::Error::Transport("script exhausted".into()))
    }
}

struct FixedTarget;

#[async_trait::async_trait]
impl TargetResolver for FixedTarget {
    async fn resolve(&self, _ns: &str, _f: &str) -> stepd_core::Result<AppTarget> {
        Ok(AppTarget {
            url: "http://app.invalid".into(),
            keys: vec![b"k".to_vec()],
        })
    }
}

/// Lease a *specific* run.
///
/// `claim` deliberately takes whatever is available, so a test with two queued
/// runs will have the first call swallow both and the second call find nothing.
/// Releasing the leases we did not want keeps each test's intent explicit
/// instead of depending on queue ordering.
async fn lease_for(s: &PostgresStore, ns: &str, run: Uuid) -> Lease {
    for _ in 0..8 {
        let leases = s
            .claim(ns, "it", 16, Duration::seconds(60))
            .await
            .expect("claim");
        let mut found = None;
        for l in leases {
            if l.run_id == run {
                found = Some(l);
            } else {
                s.release(l.run_id, Utc::now()).await.expect("release");
            }
        }
        if let Some(l) = found {
            return l;
        }
    }
    panic!("run {run} never became claimable");
}

fn step(id: &str, hash: &str, data: serde_json::Value) -> AttemptResponse {
    AttemptResponse::single(Op::Step {
        id: id.into(),
        hash: hash.into(),
        data: Some(data),
        meta: None,
        error: None,
    })
}

// ---------------------------------------------------------------- end to end

/// The headline claim: a five-step workflow runs to completion against a real
/// database, through the real dispatcher, and every step is recorded once.
#[tokio::test]
async fn a_five_step_workflow_runs_to_completion() {
    let s = db_test!(s);
    let ns = namespace(&s, "e2e").await;

    let app = ScriptedApp::new(vec![
        step(
            "charge",
            "e2e0000000000001",
            serde_json::json!({ "tx": "ch_1" }),
        ),
        AttemptResponse::single(Op::Sleep {
            id: "cooldown".into(),
            hash: "e2e0000000000002".into(),
            until: Utc::now() + Duration::milliseconds(1),
        }),
        AttemptResponse::single(Op::WaitEvent {
            id: "approval".into(),
            hash: "e2e0000000000003".into(),
            event: "order.approved".into(),
            since: "run_start".into(),
            timeout_at: None,
            prompt: None,
        }),
        step(
            "ship",
            "e2e0000000000004",
            serde_json::json!({ "carrier": "dhl" }),
        ),
        AttemptResponse::single(Op::Done {
            data: Some(serde_json::json!({ "shipped": true })),
        }),
    ]);

    let run = s
        .create_run(NewRun::root(&ns, "order-fulfilment").with_key("order:4711"))
        .await
        .unwrap()
        .expect("run created");

    let d = Dispatcher::new(
        s.clone(),
        s.clone(),
        app.clone(),
        Arc::new(FixedTarget),
        DispatchConfig {
            worker: "it".into(),
            // Timer jitter is deliberately off: a test that sometimes waits an
            // extra minute is a flaky test, and this suite has a zero budget.
            timer_jitter: Duration::zero(),
            ..Default::default()
        },
    );
    let keeper = Housekeeper::new(s.clone(), s.clone(), s.clone(), KeeperConfig::default());

    // Attempts 1 and 2: the step, then the sleep.
    d.tick_namespace(&ns).await.unwrap();
    d.tick_namespace(&ns).await.unwrap();
    assert_eq!(
        s.run_status(run).await.unwrap(),
        Some(RunStatus::Sleeping),
        "a run on a timer reports `sleeping`"
    );

    // The timer fires and the run becomes claimable again.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    // The keeper is deliberately global — timers are not a per-namespace concern —
    // so count the effect on *this* run rather than the sweep's total, which in a
    // shared database also includes whatever the other tests scheduled.
    assert!(keeper.tick().await.timers_fired >= 1);
    assert_eq!(
        s.run_status(run).await.unwrap(),
        Some(RunStatus::Pending),
        "the sleep timer woke the run"
    );

    // Attempt 3 registers the wait.
    d.tick_namespace(&ns).await.unwrap();
    assert_eq!(
        s.run_status(run).await.unwrap(),
        Some(RunStatus::Waiting),
        "a run on an event reports `waiting`, distinguishably from `sleeping`"
    );

    // The event arrives, the wait resolves, and attempts 4 and 5 finish the run.
    assert_eq!(
        s.deliver(
            run,
            "order.approved",
            &serde_json::json!({ "by": "priya" }),
            None
        )
        .await
        .unwrap(),
        Delivery::Resolved
    );
    d.tick_namespace(&ns).await.unwrap();
    d.tick_namespace(&ns).await.unwrap();

    assert_eq!(s.run_status(run).await.unwrap(), Some(RunStatus::Completed));

    // Five attempts, five distinct memo maps, each strictly larger than the last.
    let attempts = app.attempts();
    assert_eq!(attempts.len(), 5, "one attempt per op, no re-dispatch");
    let sizes: Vec<usize> = attempts.iter().map(|a| a.steps.len()).collect();
    assert_eq!(
        sizes,
        vec![0, 1, 2, 3, 4],
        "the journal grows by exactly one step per attempt"
    );

    // The memoised charge result is byte-identical on every later attempt, which
    // is the property the whole design exists to provide.
    for a in &attempts[1..] {
        assert_eq!(
            a.steps["e2e0000000000001"].data,
            Some(serde_json::json!({ "tx": "ch_1" })),
            "a recorded step result never changes"
        );
    }

    let stats = d.stats().await;
    assert_eq!(stats.dispatched, 5);
    assert_eq!(stats.committed, 5);
    assert_eq!(stats.stale, 0);
    assert_eq!(stats.rejected, 0);
}

// ---------------------------------------------------------------- fencing

#[tokio::test]
async fn a_superseded_attempt_cannot_commit() {
    let s = db_test!(s);
    let ns = namespace(&s, "fence").await;
    let run = s.create_run(NewRun::root(&ns, "f")).await.unwrap().unwrap();

    let first = s.claim(&ns, "w1", 10, Duration::seconds(60)).await.unwrap();
    let first = first
        .into_iter()
        .find(|l| l.run_id == run)
        .expect("claimed");

    // The lease is assumed lost and the run re-dispatched.
    s.release(run, Utc::now()).await.unwrap();
    let second = s.claim(&ns, "w2", 10, Duration::seconds(60)).await.unwrap();
    let second = second
        .into_iter()
        .find(|l| l.run_id == run)
        .expect("reclaimed");
    assert!(second.fence > first.fence, "claiming must bump the fence");

    let ops = OpCommit {
        ops: vec![Op::Step {
            id: "x".into(),
            hash: "fence00000000001".into(),
            data: Some(serde_json::json!(1)),
            meta: None,
            error: None,
        }],
        emit: vec![],
    };
    assert_eq!(
        s.commit(run, first.fence, ops.clone()).await.unwrap(),
        CommitOutcome::StaleFence
    );

    let page = s.steps_page(run, None, 100).await.unwrap();
    assert!(
        page.steps.is_empty(),
        "a stale attempt must write nothing at all"
    );

    assert_eq!(
        s.commit(run, second.fence, ops).await.unwrap(),
        CommitOutcome::Committed
    );
}

// ---------------------------------------------------------------- early signal

#[tokio::test]
async fn an_event_that_arrives_before_the_wait_still_resolves_it() {
    let s = db_test!(s);
    let ns = namespace(&s, "early").await;
    let run = s.create_run(NewRun::root(&ns, "w")).await.unwrap().unwrap();

    // The signal arrives first. A naive implementation loses it here.
    assert_eq!(
        s.deliver(run, "go", &serde_json::json!({ "n": 1 }), None)
            .await
            .unwrap(),
        Delivery::Buffered
    );

    let lease = lease_for(&s, &ns, run).await;

    assert_eq!(
        s.commit(
            run,
            lease.fence,
            OpCommit {
                ops: vec![Op::WaitEvent {
                    id: "w".into(),
                    hash: "early00000000001".into(),
                    event: "go".into(),
                    since: "run_start".into(),
                    timeout_at: None,
                    prompt: None,
                }],
                emit: vec![],
            }
        )
        .await
        .unwrap(),
        CommitOutcome::Committed
    );

    assert_eq!(
        s.run_status(run).await.unwrap(),
        Some(RunStatus::Pending),
        "the run must not suspend: the event was already in the inbox"
    );
    let page = s.steps_page(run, None, 100).await.unwrap();
    assert_eq!(
        page.steps["early00000000001"].data,
        Some(serde_json::json!({ "n": 1 })),
        "the wait resolved with the buffered event"
    );
}

// ---------------------------------------------------------------- keyed exclusivity

#[tokio::test]
async fn only_one_run_per_key_is_active_at_a_time() {
    let s = db_test!(s);
    let ns = namespace(&s, "keyed").await;

    let first = s
        .create_run(NewRun::root(&ns, "k").with_key("order:1"))
        .await
        .unwrap();
    assert!(first.is_some());
    let second = s
        .create_run(NewRun::root(&ns, "k").with_key("order:1"))
        .await
        .unwrap();
    assert!(
        second.is_none(),
        "a second active run on the same key must be refused"
    );

    // Once the first is terminal the key is reusable.
    let lease = s
        .claim(&ns, "w", 10, Duration::seconds(60))
        .await
        .unwrap()
        .into_iter()
        .find(|l| l.run_id == first.unwrap())
        .unwrap();
    s.commit(
        first.unwrap(),
        lease.fence,
        OpCommit {
            ops: vec![Op::Done { data: None }],
            emit: vec![],
        },
    )
    .await
    .unwrap();

    assert!(
        s.create_run(NewRun::root(&ns, "k").with_key("order:1"))
            .await
            .unwrap()
            .is_some(),
        "the key is free once the holder reaches a terminal state"
    );
}

// ---------------------------------------------------------------- fair dispatch

#[tokio::test]
async fn claiming_is_scoped_to_one_namespace() {
    let s = db_test!(s);
    let a = namespace(&s, "fair-a").await;
    let b = namespace(&s, "fair-b").await;

    for _ in 0..5 {
        s.create_run(NewRun::root(&a, "noisy")).await.unwrap();
    }
    s.create_run(NewRun::root(&b, "quiet")).await.unwrap();

    // A namespace-blind claim would let the five-run backlog crowd out the one.
    let claimed = s.claim(&b, "w", 10, Duration::seconds(60)).await.unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "a claim against one namespace must not reach another's work"
    );

    let active = s.active_namespaces().await.unwrap();
    assert!(
        active.contains(&a),
        "namespaces with claimable work are reported"
    );
}

// ---------------------------------------------------------------- invoke tree

#[tokio::test]
async fn a_child_run_resolves_its_parents_step() {
    let s = db_test!(s);
    let ns = namespace(&s, "invoke").await;
    let parent = s
        .create_run(NewRun::root(&ns, "parent"))
        .await
        .unwrap()
        .unwrap();

    let lease = lease_for(&s, &ns, parent).await;
    s.commit(
        parent,
        lease.fence,
        OpCommit {
            ops: vec![Op::Invoke {
                id: "refund".into(),
                hash: "inv00000000000001".into(),
                function: "refunds".into(),
                input: Some(serde_json::json!({ "order": 4711 })),
                detach: false,
            }],
            emit: vec![],
        },
    )
    .await
    .unwrap();

    assert_eq!(
        s.run_status(parent).await.unwrap(),
        Some(RunStatus::Sleeping),
        "the parent suspends until the child finishes"
    );

    // Find and complete the child.
    let child_lease = s
        .claim(&ns, "w", 10, Duration::seconds(60))
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("the child is dispatchable");
    let child_attempt = s.load_attempt(&child_lease).await.unwrap();
    assert_eq!(child_attempt.run.function_id, "refunds");
    assert_eq!(
        child_attempt.run.input,
        Some(serde_json::json!({ "order": 4711 }))
    );

    s.commit(
        child_lease.run_id,
        child_lease.fence,
        OpCommit {
            ops: vec![Op::Done {
                data: Some(serde_json::json!({ "refunded": 42 })),
            }],
            emit: vec![],
        },
    )
    .await
    .unwrap();

    let page = s.steps_page(parent, None, 100).await.unwrap();
    assert_eq!(
        page.steps["inv00000000000001"].data,
        Some(serde_json::json!({ "refunded": 42 })),
        "the parent's invoke step carries the child's output"
    );
    assert_eq!(
        s.run_status(parent).await.unwrap(),
        Some(RunStatus::Pending)
    );
}

// ---------------------------------------------------------------- rejections

#[tokio::test]
async fn a_rule_violation_fails_the_run_inside_the_commit() {
    let s = db_test!(s);
    let ns = namespace(&s, "reject").await;
    let run = s
        .create_run(NewRun::root(&ns, "cyc").with_key("k:1"))
        .await
        .unwrap()
        .unwrap();

    let lease = lease_for(&s, &ns, run).await;

    // Invoking an ancestor holding the same key would deadlock keyed ordering.
    let outcome = s
        .commit(
            run,
            lease.fence,
            OpCommit {
                ops: vec![Op::Invoke {
                    id: "self".into(),
                    hash: "rej00000000000001".into(),
                    function: "cyc".into(),
                    input: None,
                    detach: false,
                }],
                emit: vec![],
            },
        )
        .await
        .unwrap();

    assert_eq!(outcome, CommitOutcome::Rejected("invoke_cycle".into()));
    assert_eq!(
        s.run_status(run).await.unwrap(),
        Some(RunStatus::Failed),
        "the store fails the run in the same transaction; the caller writes nothing further"
    );
}

// ---------------------------------------------------------------- signals

#[tokio::test]
async fn a_signal_wakes_a_run_already_parked_on_the_wait() {
    let s = db_test!(s);
    let ns = namespace(&s, "signal").await;
    let target = s
        .create_run(NewRun::root(&ns, "target"))
        .await
        .unwrap()
        .unwrap();
    let sender = s
        .create_run(NewRun::root(&ns, "sender"))
        .await
        .unwrap()
        .unwrap();

    let lease = lease_for(&s, &ns, target).await;
    s.commit(
        target,
        lease.fence,
        OpCommit {
            ops: vec![Op::WaitEvent {
                id: "a".into(),
                hash: "sig00000000000001".into(),
                event: "approved".into(),
                since: "run_start".into(),
                timeout_at: None,
                prompt: None,
            }],
            emit: vec![],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        s.run_status(target).await.unwrap(),
        Some(RunStatus::Waiting)
    );

    let lease = lease_for(&s, &ns, sender).await;
    s.commit(
        sender,
        lease.fence,
        OpCommit {
            ops: vec![Op::Signal {
                id: "notify".into(),
                hash: "sig00000000000002".into(),
                target_run: target,
                event: Event::new("approved", "/sender", serde_json::json!({ "by": "omar" })),
            }],
            emit: vec![],
        },
    )
    .await
    .unwrap();

    // The relay is what removes the lock-ordering deadlock; delivery is a
    // separate, idempotent sweep.
    assert_eq!(s.drain_signals(10).await.unwrap(), 1);
    assert_eq!(
        s.run_status(target).await.unwrap(),
        Some(RunStatus::Pending),
        "the signalled run woke; before the relay it slept until its timeout"
    );

    // Redelivery must not double-signal (protocol §7.6).
    s.drain_signals(10).await.unwrap();
    let page = s.steps_page(target, None, 100).await.unwrap();
    assert_eq!(
        page.steps["sig00000000000001"].data,
        Some(serde_json::json!({ "by": "omar" }))
    );
}

// ---------------------------------------------------------------- lease recovery

#[tokio::test]
async fn an_expired_lease_returns_its_run_to_the_queue() {
    let s = db_test!(s);
    let ns = namespace(&s, "lease").await;
    let run = s.create_run(NewRun::root(&ns, "l")).await.unwrap().unwrap();

    // A worker claims the run and then dies.
    let lease = s
        .claim(&ns, "dead-worker", 10, Duration::milliseconds(1))
        .await
        .unwrap()
        .into_iter()
        .find(|l| l.run_id == run)
        .unwrap();
    assert!(s
        .claim(&ns, "w2", 10, Duration::seconds(60))
        .await
        .unwrap()
        .is_empty());

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(s.reclaim_expired_leases(10).await.unwrap(), 1);

    let retaken = s
        .claim(&ns, "w2", 10, Duration::seconds(60))
        .await
        .unwrap()
        .into_iter()
        .find(|l| l.run_id == run)
        .expect("another worker can take over");
    assert!(
        retaken.fence > lease.fence,
        "the fence advanced, so the dead worker's response can no longer commit"
    );
}

// ---------------------------------------------------------------- pagination

#[tokio::test]
async fn the_journal_pages_without_dropping_or_repeating_steps() {
    let s = db_test!(s);
    let ns = namespace(&s, "page").await;
    let run = s.create_run(NewRun::root(&ns, "p")).await.unwrap().unwrap();

    for i in 0..25u32 {
        let lease = lease_for(&s, &ns, run).await;
        s.commit(
            run,
            lease.fence,
            OpCommit {
                ops: vec![Op::Step {
                    id: format!("s{i}"),
                    hash: format!("page{i:012}"),
                    data: Some(serde_json::json!(i)),
                    meta: None,
                    error: None,
                }],
                emit: vec![],
            },
        )
        .await
        .unwrap();
    }

    let mut seen: Vec<String> = vec![];
    let mut cursor: Option<String> = None;
    loop {
        let page = s.steps_page(run, cursor.as_deref(), 10).await.unwrap();
        seen.extend(page.steps.keys().cloned());
        match page.next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    seen.sort();
    let mut unique = seen.clone();
    unique.dedup();
    assert_eq!(seen.len(), 25, "every step appears");
    assert_eq!(unique.len(), 25, "and none appears twice");
}
