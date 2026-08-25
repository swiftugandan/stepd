//! Engine behaviour, driven through the real dispatch loop against in-memory
//! components. No database, no network — every assertion here is about the
//! engine's logic rather than any backend's.

use chrono::{Duration, Utc};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use stepd_core::testing::*;
use stepd_core::*;
use stepd_proto::*;

fn dispatcher(
    store: Arc<MemStore>,
    transport: Arc<MemTransport>,
    cfg: DispatchConfig,
) -> Dispatcher<MemStore, MemStore, MemTransport> {
    Dispatcher::new(
        store.clone(),
        store,
        transport,
        Arc::new(StaticTargets),
        cfg,
    )
}

/// A handler shaped like real SDK output: replay from the top, return memoized
/// values, execute the first unmemoized step, yield.
fn order_workflow(
    charges: Arc<AtomicU32>,
    ships: Arc<AtomicU32>,
) -> impl Fn(&Attempt) -> std::result::Result<AttemptResponse, String> + Send + Sync + 'static {
    move |a: &Attempt| {
        let mut c = OccurrenceCounter::new(&a.run.function_id);
        let seen = &a.steps;

        let h = c.claim("charge");
        if !seen.contains_key(&h) {
            charges.fetch_add(1, Ordering::SeqCst);
            return Ok(AttemptResponse::single(Op::Step {
                id: "charge".into(),
                hash: h,
                data: Some(serde_json::json!({"tx": "ch_1"})),
                meta: None,
                error: None,
            }));
        }

        // Parallel pair: hashes claimed in declaration order, before any work.
        let a1 = c.claim("fetch-invoice");
        let a2 = c.claim("fetch-customer");
        let missing: Vec<_> = [&a1, &a2]
            .into_iter()
            .filter(|h| !seen.contains_key(*h))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Ok(AttemptResponse::batch(
                missing
                    .into_iter()
                    .map(|h| Op::Step {
                        id: "fetch".into(),
                        hash: h,
                        data: Some(serde_json::json!(1)),
                        meta: None,
                        error: None,
                    })
                    .collect(),
            ));
        }

        let h = c.claim("approval");
        if !seen.contains_key(&h) {
            return Ok(AttemptResponse::single(Op::WaitEvent {
                id: "approval".into(),
                hash: h,
                event: "order.approved".into(),
                since: "run_start".into(),
                timeout_at: None,
                prompt: None,
            }));
        }
        let approved_by = seen[&h].data.as_ref().and_then(|d| d.get("by")).cloned();

        let h = c.claim("ship");
        if !seen.contains_key(&h) {
            ships.fetch_add(1, Ordering::SeqCst);
            let mut r = AttemptResponse::single(Op::Step {
                id: "ship".into(),
                hash: h,
                data: Some(serde_json::json!({"carrier": "dhl"})),
                meta: None,
                error: None,
            });
            r.emit
                .push(Event::new("order.shipped", "/fn", serde_json::json!({})));
            return Ok(r);
        }

        Ok(AttemptResponse::single(Op::Done {
            data: Some(serde_json::json!({"shipped": true, "approved_by": approved_by})),
        }))
    }
}

#[tokio::test]
async fn workflow_runs_end_to_end_and_each_step_executes_once() {
    let store = MemStore::new();
    let charges = Arc::new(AtomicU32::new(0));
    let ships = Arc::new(AtomicU32::new(0));
    let transport = MemTransport::new(order_workflow(charges.clone(), ships.clone()));
    let d = dispatcher(store.clone(), transport, DispatchConfig::default());

    let run = store
        .create_run(NewRun::root("prod", "order-fulfilment"))
        .await
        .unwrap()
        .unwrap();

    d.run_until_idle(20).await.unwrap();
    assert_eq!(
        store.get(run).unwrap().status,
        RunStatus::Sleeping,
        "parks on the wait"
    );

    store
        .deliver(
            run,
            "order.approved",
            &serde_json::json!({"by": "priya"}),
            None,
        )
        .await
        .unwrap();
    d.run_until_idle(20).await.unwrap();

    let r = store.get(run).unwrap();
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.unwrap()["approved_by"], "priya");
    assert_eq!(
        charges.load(Ordering::SeqCst),
        1,
        "charge must execute exactly once"
    );
    assert_eq!(
        ships.load(Ordering::SeqCst),
        1,
        "ship must execute exactly once"
    );
    assert_eq!(
        store.published().len(),
        1,
        "emitted event committed with the step"
    );
}

#[tokio::test]
async fn signal_arriving_before_the_wait_is_not_lost() {
    let store = MemStore::new();
    let transport = MemTransport::new(order_workflow(
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    ));
    let d = dispatcher(store.clone(), transport, DispatchConfig::default());
    let run = store
        .create_run(NewRun::root("prod", "order-fulfilment"))
        .await
        .unwrap()
        .unwrap();

    // Delivered before the run has executed a single step.
    let outcome = store
        .deliver(
            run,
            "order.approved",
            &serde_json::json!({"by": "early"}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(outcome, Delivery::Buffered);

    d.run_until_idle(20).await.unwrap();
    let r = store.get(run).unwrap();
    assert_eq!(r.status, RunStatus::Completed, "must never suspend");
    assert_eq!(r.output.unwrap()["approved_by"], "early");
}

#[tokio::test]
async fn keyed_runs_are_mutually_exclusive() {
    let store = MemStore::new();
    let first = store
        .create_run(NewRun::root("prod", "order-fulfilment").with_key("order:1"))
        .await
        .unwrap();
    assert!(first.is_some());

    let second = store
        .create_run(NewRun::root("prod", "order-fulfilment").with_key("order:1"))
        .await
        .unwrap();
    assert!(
        second.is_none(),
        "a second active run on the same key must be refused"
    );

    store.cancel_run("prod", first.unwrap()).await.unwrap();
    let third = store
        .create_run(NewRun::root("prod", "order-fulfilment").with_key("order:1"))
        .await
        .unwrap();
    assert!(third.is_some(), "key frees up once the run is terminal");
}

#[tokio::test]
async fn stale_fence_response_is_discarded() {
    let store = MemStore::new();
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();
    let leases = store
        .claim("prod", "w1", 1, Duration::seconds(60))
        .await
        .unwrap();
    let lease = leases[0].clone();

    // Another worker takes the run over, bumping the fence.
    store.expire_lease(run);
    let _second = store
        .claim("prod", "w2", 1, Duration::seconds(60))
        .await
        .unwrap();

    let outcome = store
        .commit(
            run,
            lease.fence,
            OpCommit {
                ops: vec![Op::Step {
                    id: "ghost".into(),
                    hash: step_hash("f", "ghost", 0),
                    data: Some(serde_json::json!("should not land")),
                    meta: None,
                    error: None,
                }],
                emit: vec![],
            },
        )
        .await
        .unwrap();

    assert_eq!(outcome, CommitOutcome::StaleFence);
    assert!(
        store.get(run).unwrap().steps.is_empty(),
        "superseded attempt wrote nothing"
    );
}

#[tokio::test]
async fn duplicate_commit_records_a_step_once() {
    let store = MemStore::new();
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();
    let lease = store
        .claim("prod", "w1", 1, Duration::seconds(60))
        .await
        .unwrap()[0]
        .clone();
    let hash = step_hash("f", "charge", 0);

    let op = |v: i32| OpCommit {
        ops: vec![Op::Step {
            id: "charge".into(),
            hash: hash.clone(),
            data: Some(serde_json::json!(v)),
            meta: None,
            error: None,
        }],
        emit: vec![],
    };

    store.commit(run, lease.fence, op(1)).await.unwrap();
    let lease2 = store
        .claim("prod", "w1", 1, Duration::seconds(60))
        .await
        .unwrap()[0]
        .clone();
    store.commit(run, lease2.fence, op(2)).await.unwrap();

    let r = store.get(run).unwrap();
    assert_eq!(r.steps.len(), 1);
    assert_eq!(
        r.steps[&hash].data,
        Some(serde_json::json!(1)),
        "first write wins"
    );
}

#[tokio::test]
async fn continue_as_new_rejects_live_children_but_allows_detached() {
    let store = MemStore::new();

    // Non-detached child in flight: the op must fail rather than orphan it.
    let parent = store
        .create_run(NewRun::root("prod", "f").with_key("k1"))
        .await
        .unwrap()
        .unwrap();
    let mut child = NewRun::root("prod", "child");
    child.parent = Some(parent);
    store.create_run(child).await.unwrap();

    let lease = store
        .claim("prod", "w", 10, Duration::seconds(60))
        .await
        .unwrap();
    let l = lease.iter().find(|l| l.run_id == parent).unwrap();
    store
        .commit(
            parent,
            l.fence,
            OpCommit {
                ops: vec![Op::ContinueAsNew {
                    id: "c".into(),
                    hash: step_hash("f", "c", 0),
                    input: None,
                }],
                emit: vec![],
            },
        )
        .await
        .unwrap();

    let r = store.get(parent).unwrap();
    assert_eq!(r.status, RunStatus::Failed);
    assert_eq!(
        r.error.unwrap().code.unwrap(),
        "continue_as_new_with_live_children"
    );

    // Detached child: the op is allowed and the successor is created.
    let p2 = store
        .create_run(NewRun::root("prod", "f").with_key("k2"))
        .await
        .unwrap()
        .unwrap();
    let mut bg = NewRun::root("prod", "child");
    bg.parent = Some(p2);
    bg.detached = true;
    store.create_run(bg).await.unwrap();

    let leases = store
        .claim("prod", "w", 10, Duration::seconds(60))
        .await
        .unwrap();
    let l = leases.iter().find(|l| l.run_id == p2).unwrap();
    store
        .commit(
            p2,
            l.fence,
            OpCommit {
                ops: vec![Op::ContinueAsNew {
                    id: "c".into(),
                    hash: step_hash("f", "c", 0),
                    input: None,
                }],
                emit: vec![],
            },
        )
        .await
        .unwrap();

    assert_eq!(store.get(p2).unwrap().status, RunStatus::Completed);
    let successor = store
        .all()
        .into_iter()
        .find(|r| r.chain_position == 1 && r.key.as_deref() == Some("k2"));
    assert!(successor.is_some(), "successor created, inheriting the key");
}

#[tokio::test]
async fn terminal_parent_cancels_non_detached_children_only() {
    let store = MemStore::new();
    let parent = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();

    let mut kid = NewRun::root("prod", "child");
    kid.parent = Some(parent);
    let kid_id = store.create_run(kid).await.unwrap().unwrap();

    let mut bg = NewRun::root("prod", "child");
    bg.parent = Some(parent);
    bg.detached = true;
    let bg_id = store.create_run(bg).await.unwrap().unwrap();

    store.cancel_run("prod", parent).await.unwrap();

    assert_eq!(store.get(kid_id).unwrap().status, RunStatus::Cancelled);
    assert_eq!(
        store.get(bg_id).unwrap().status,
        RunStatus::Pending,
        "detached survives"
    );
}

#[tokio::test]
async fn malformed_envelope_is_rejected_before_it_reaches_the_store() {
    let store = MemStore::new();
    // Two ops sharing a hash: forbidden by the protocol.
    let transport = MemTransport::new(|_a: &Attempt| {
        Ok(AttemptResponse::batch(vec![
            Op::Step {
                id: "a".into(),
                hash: "1111111111111111".into(),
                data: None,
                meta: None,
                error: None,
            },
            Op::Step {
                id: "b".into(),
                hash: "1111111111111111".into(),
                data: None,
                meta: None,
                error: None,
            },
        ]))
    });
    let d = dispatcher(store.clone(), transport, DispatchConfig::default());
    store.create_run(NewRun::root("prod", "f")).await.unwrap();

    d.tick().await.unwrap();
    assert_eq!(
        store.commits.load(Ordering::Relaxed),
        0,
        "nothing may be committed"
    );
    assert_eq!(d.stats().await.failed, 1);
}

#[tokio::test]
async fn open_circuit_stops_hammering_a_failing_app() {
    let store = MemStore::new();
    let transport =
        MemTransport::new(|_a: &Attempt| Ok(AttemptResponse::single(Op::Done { data: None })));
    transport.fail(100);

    let cfg = DispatchConfig {
        batch: 4,
        quarantine_after: 1000,
        ..Default::default()
    };
    let d = dispatcher(store.clone(), transport.clone(), cfg);
    for _ in 0..6 {
        store.create_run(NewRun::root("prod", "f")).await.unwrap();
    }

    for _ in 0..6 {
        let _ = d.tick().await;
    }
    assert_eq!(d.circuit("prod/f").await, Some(Circuit::Open));

    let before = d.stats().await.dispatched;
    let _ = d.tick().await;
    assert_eq!(d.stats().await.dispatched, before, "no dispatch while open");
    assert!(d.stats().await.skipped_circuit > 0);
}

#[tokio::test]
async fn repeatedly_failing_run_is_quarantined_and_leaves_the_queue() {
    let store = MemStore::new();
    let transport =
        MemTransport::new(|_a: &Attempt| Ok(AttemptResponse::single(Op::Done { data: None })));
    transport.fail(10_000);

    let cfg = DispatchConfig {
        quarantine_after: 3,
        retry: RetryPolicy {
            initial: Duration::milliseconds(1),
            jitter: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let d = dispatcher(store.clone(), transport, cfg);
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();

    for _ in 0..10 {
        let _ = d.tick().await;
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    }

    assert_eq!(store.get(run).unwrap().status, RunStatus::Quarantined);
    assert!(d.stats().await.quarantined >= 1);
}

#[tokio::test]
async fn non_retryable_app_error_fails_the_run_immediately() {
    let store = MemStore::new();
    let transport = MemTransport::new(|_a: &Attempt| {
        Ok(AttemptResponse::single(Op::Error {
            retryable: false,
            step: Some("charge".into()),
            error: ErrorBody::coded("card_declined", "declined"),
        }))
    });
    let d = dispatcher(store.clone(), transport, DispatchConfig::default());
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();

    d.tick().await.unwrap();
    let r = store.get(run).unwrap();
    assert_eq!(r.status, RunStatus::Failed);
    assert_eq!(r.error.unwrap().code.unwrap(), "card_declined");
}

#[tokio::test]
async fn idempotent_ingest_returns_the_same_event_id() {
    let store = MemStore::new();
    let mut e = Event::new("order.created", "/shop", serde_json::json!({"id": 1}));
    e.idempotency = Some("idem-1".into());

    let (id1, dup1) = store.append("prod", &e).await.unwrap();
    let (id2, dup2) = store.append("prod", &e).await.unwrap();

    assert!(!dup1);
    assert!(dup2, "second ingest must be recognised as a duplicate");
    assert_eq!(id1, id2, "and resolve to the same event");
}

#[tokio::test]
async fn dispatch_rotates_across_namespaces() {
    // A noisy tenant must not starve a quiet one.
    let store = MemStore::new();
    let transport =
        MemTransport::new(|_a: &Attempt| Ok(AttemptResponse::single(Op::Done { data: None })));
    let d = dispatcher(
        store.clone(),
        transport,
        DispatchConfig {
            batch: 2,
            ..Default::default()
        },
    );

    for _ in 0..20 {
        store.create_run(NewRun::root("noisy", "f")).await.unwrap();
    }
    let quiet = store
        .create_run(NewRun::root("quiet", "f"))
        .await
        .unwrap()
        .unwrap();

    for _ in 0..4 {
        let _ = d.tick().await;
    }
    assert_eq!(
        store.get(quiet).unwrap().status,
        RunStatus::Completed,
        "the quiet namespace's single run must get dispatched"
    );
}

#[tokio::test]
async fn timers_fire_and_resume_sleeping_runs() {
    let store = MemStore::new();
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();
    let lease = store
        .claim("prod", "w", 1, Duration::seconds(60))
        .await
        .unwrap()[0]
        .clone();

    store
        .commit(
            run,
            lease.fence,
            OpCommit {
                ops: vec![Op::Sleep {
                    id: "nap".into(),
                    hash: step_hash("f", "nap", 0),
                    until: Utc::now() - Duration::seconds(1),
                }],
                emit: vec![],
            },
        )
        .await
        .unwrap();

    assert_eq!(store.get(run).unwrap().status, RunStatus::Sleeping);
    let fired = store.fire_due(Utc::now(), 100).await.unwrap();
    assert_eq!(fired, 1);
    assert_eq!(store.get(run).unwrap().status, RunStatus::Pending);

    let step = &store.get(run).unwrap().steps[&step_hash("f", "nap", 0)];
    assert_eq!(
        step.status,
        StepStatus::Completed,
        "the sleep resolves when it fires"
    );
}

#[tokio::test]
async fn quarantine_needs_the_same_failure_repeatedly_not_merely_many_failures() {
    // F-LP-6 specifies quarantine on repeated *identical* failure. An earlier
    // version counted attempts, so a run failing a different way each time — a
    // run that deserves a human — was removed from dispatch exactly like a
    // poison pill, and the variety that would have explained it was hidden.
    use stepd_core::traits::*;
    use stepd_proto::ErrorBody;

    let store = stepd_core::testing::MemStore::new();
    let run = store
        .create_run(NewRun::root("test", "f"))
        .await
        .unwrap()
        .unwrap();

    // Five different failures.
    for i in 0..5 {
        let r = store
            .record_failure(
                run,
                &ErrorBody::coded(format!("code_{i}"), "boom"),
                chrono::Utc::now(),
            )
            .await
            .unwrap();
        assert_eq!(
            r.consecutive, 1,
            "a different failure resets the run of identical ones"
        );
    }

    // Five of the same.
    for i in 1..=5 {
        let r = store
            .record_failure(
                run,
                &ErrorBody::coded("gateway_down", "boom"),
                chrono::Utc::now(),
            )
            .await
            .unwrap();
        assert_eq!(r.consecutive, i, "identical failures accumulate");
    }

    // And a different one resets it again.
    let r = store
        .record_failure(run, &ErrorBody::coded("other", "boom"), chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(r.consecutive, 1);
}

#[tokio::test]
async fn the_signature_ignores_a_message_that_embeds_an_identifier() {
    // The grouping property the DLQ depends on: a hundred runs failing the same
    // way must produce one signature, even though each message names its own run.
    use stepd_proto::ErrorBody;
    let a = ErrorBody::coded("gateway_down", "upstream 503 while charging order 4711");
    let b = ErrorBody::coded("gateway_down", "upstream 503 while charging order 9999");
    assert_eq!(a.signature(), b.signature());
}

// -------------------------------------------------------- managed blobs (§8.3)

/// A step result carrying one `$blob`, so a test can drive the verification path.
fn blob_bearing_result(
    blob: uuid::Uuid,
) -> impl Fn(&Attempt) -> std::result::Result<AttemptResponse, String> + Send + Sync + 'static {
    move |_a: &Attempt| {
        Ok(AttemptResponse::single(Op::Step {
            id: "capture".into(),
            hash: "aaaaaaaaaaaaaaaa".into(),
            // Nested in an array inside an object: the ordinary payload shape,
            // and the one a top-level-only walk would miss.
            data: Some(serde_json::json!({
                "receipts": [ { "file": { "$blob": { "id": blob, "size": 1, "sha256": "ab" } } } ]
            })),
            meta: None,
            error: None,
        }))
    }
}

/// Protocol §8.3.2: "a mismatch fails the commit with `blob_digest_mismatch` and
/// the ops are discarded."
///
/// The defect this guards is the one a presigning backend exposes: nothing else
/// in the tree commits a blob when the bytes went straight from the app to the
/// object store, so without this the reference would be recorded unverified —
/// and `docs/adr/010-payload-tiering.md` rejected checking on first read because
/// by then the run has already proceeded on a value nobody checked.
#[tokio::test]
async fn a_blob_reference_that_fails_verification_fails_the_run_and_records_no_ops() {
    let blob = uuid::Uuid::now_v7();
    let store = MemStore::new();
    let transport = MemTransport::new(blob_bearing_result(blob));
    let blobs = MemBlobs::mismatching(blob);
    let d = dispatcher(store.clone(), transport, DispatchConfig::default())
        .with_blob_store(blobs.clone());
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();

    d.tick().await.unwrap();

    let r = store.get(run).unwrap();
    assert!(
        r.steps.is_empty(),
        "the ops must be discarded, not recorded: {:?}",
        r.steps
    );
    assert_eq!(
        r.status,
        RunStatus::Failed,
        "the failure is non-retryable, so the run is terminal rather than queued for another go"
    );
    assert_eq!(
        r.error.as_ref().and_then(|e| e.code.as_deref()),
        Some("blob_digest_mismatch"),
        "§8.3.2 names the code"
    );
    assert_eq!(d.stats().await.rejected, 1);
    assert_eq!(d.stats().await.committed, 0);
    assert!(
        !blobs.is_committed(blob),
        "a blob that failed verification must not become readable"
    );
}

/// The two shapes that must behave exactly as they did before this path existed:
/// an envelope with no `$blob` in it, and a deployment with no managed blobs at
/// all. `None` has to be a clean skip, not a warning and not a cost.
#[tokio::test]
async fn a_run_with_no_blob_references_commits_the_same_with_or_without_a_blob_store() {
    for configured in [false, true] {
        let store = MemStore::new();
        let transport = MemTransport::new(|_a: &Attempt| {
            Ok(AttemptResponse::single(Op::Done {
                data: Some(serde_json::json!({ "ok": true })),
            }))
        });
        let blobs = MemBlobs::new();
        let d = dispatcher(store.clone(), transport, DispatchConfig::default());
        let d = if configured {
            d.with_blob_store(blobs.clone())
        } else {
            d
        };
        let run = store
            .create_run(NewRun::root("prod", "f"))
            .await
            .unwrap()
            .unwrap();

        d.tick().await.unwrap();

        assert_eq!(
            store.get(run).unwrap().status,
            RunStatus::Completed,
            "blob store configured: {configured}"
        );
        assert_eq!(d.stats().await.committed, 1);
        assert_eq!(store.commits.load(Ordering::Relaxed), 1);
        assert_eq!(
            blobs.verifications.load(Ordering::Relaxed),
            0,
            "an envelope with no `$blob` gives the blob store nothing to do"
        );
    }
}

/// The relay path's regression guard, at the dispatcher.
///
/// On a backend that cannot presign, the transfer endpoint verifies and commits
/// the blob while it still holds the bytes — before the ops carrying the
/// reference are ever returned. So every reference the dispatcher sees there is
/// already committed, and committing it again has to be a no-op rather than an
/// error or a second read of the object.
///
/// The no-op itself lives in `commit_blob`, which is where the state that
/// decides it lives; this asserts the dispatcher goes through that call
/// unconditionally rather than keeping its own idea of what is committed.
#[tokio::test]
async fn an_already_committed_blob_reference_commits_normally() {
    let blob = uuid::Uuid::now_v7();
    let store = MemStore::new();
    let transport = MemTransport::new(blob_bearing_result(blob));
    let blobs = MemBlobs::already_committed(blob);
    let d = dispatcher(store.clone(), transport, DispatchConfig::default())
        .with_blob_store(blobs.clone());
    let run = store
        .create_run(NewRun::root("prod", "f"))
        .await
        .unwrap()
        .unwrap();

    d.tick().await.unwrap();

    assert!(
        store
            .get(run)
            .unwrap()
            .steps
            .contains_key("aaaaaaaaaaaaaaaa"),
        "the step must be recorded exactly as it would be with no blob in it"
    );
    assert_eq!(d.stats().await.committed, 1);
    assert_eq!(d.stats().await.rejected, 0);
    assert_eq!(
        blobs.skipped.load(Ordering::Relaxed),
        1,
        "the dispatcher must still call commit_blob; the store decides it is a no-op"
    );
    assert_eq!(
        blobs.verifications.load(Ordering::Relaxed),
        0,
        "an already-committed blob must not be verified again"
    );
}

// ---------------------------------------------------------------- cron wiring

/// A housekeeper with fakes for everything but the cron store under test.
fn keeper_with(cron: Arc<MemCron>) -> Housekeeper<NoopKeeping, MemStore, MemCron> {
    Housekeeper::new(
        NoopKeeping::new(),
        MemStore::new(),
        cron,
        KeeperConfig::default(),
    )
}

#[tokio::test]
async fn the_housekeeper_sweeps_cron_and_trims_the_ledger() {
    // The wiring assertion, with no database in the room. Cron sat behind a
    // complete-looking ADR for the whole life of this project because nothing
    // anywhere called it, and nothing failed as a result.
    let cron = MemCron::returning(CronSweep {
        considered: 3,
        fired: 2,
        skipped: 1,
        ..Default::default()
    });
    let stats = keeper_with(cron.clone()).tick().await;

    assert_eq!(
        cron.sweeps.load(Ordering::Relaxed),
        1,
        "the sweep is called"
    );
    assert_eq!(cron.trims.load(Ordering::Relaxed), 1, "so is the trim");
    assert_eq!(stats.cron.fired, 2);
    assert_eq!(stats.cron.skipped, 1);
    assert!(!stats.is_idle(), "a pass that fired runs is not idle");
}

#[tokio::test]
async fn a_failing_cron_sweep_does_not_stop_the_other_sweeps() {
    // Reclaiming leases matters most precisely when something else is broken,
    // so a cron store that is failing must not take the convergence loop with
    // it. This is the same isolation the other three sweeps already have.
    let cron = MemCron::failing();
    let stats = keeper_with(cron.clone()).tick().await;

    assert_eq!(cron.sweeps.load(Ordering::Relaxed), 1);
    assert_eq!(
        cron.trims.load(Ordering::Relaxed),
        1,
        "the trim still ran after the sweep failed"
    );
    assert_eq!(stats.cron, CronSweep::default(), "and nothing was counted");
}

#[tokio::test]
async fn an_idle_cron_sweep_leaves_the_pass_idle() {
    // `run_until_idle` stops when a pass does nothing. A cron sweep that
    // reported activity for merely having looked would make it spin forever.
    let cron = MemCron::returning(CronSweep {
        considered: 5,
        ..Default::default()
    });
    let stats = keeper_with(cron).tick().await;
    assert!(
        stats.is_idle(),
        "considering schedules without firing any is not work"
    );
}
