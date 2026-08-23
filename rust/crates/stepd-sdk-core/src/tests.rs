//! Properties of the R1 machinery.
//!
//! Every test here asserts a behaviour whose absence corrupts silently. Two of
//! them — `naive_counter_under_reordering_is_demonstrably_broken` and
//! `eager_claiming_makes_poll_order_irrelevant` — deliberately demonstrate that
//! the hazard is real and that this design removes it, rather than asserting
//! safety on authority.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use stepd_proto::StepOp;

// ---------------------------------------------------------------- driver

/// A miniature server: invokes the handler, commits whatever it yields into the
/// memo, and invokes again, until the handler returns.
///
/// Deliberately not the real dispatcher. A handler bug and a dispatcher bug must
/// not be able to look the same from in here.
struct Driver {
    memo: HashMap<String, RecordedStep>,
    /// Attempts recorded, for assertions about replay.
    passes: Vec<Vec<String>>,
    /// Event payloads to resolve waits with, in order.
    events: Vec<serde_json::Value>,
}

impl Driver {
    fn new() -> Self {
        Self {
            memo: HashMap::new(),
            passes: vec![],
            events: vec![],
        }
    }

    fn with_event(mut self, v: serde_json::Value) -> Self {
        self.events.push(v);
        self
    }

    fn run_context() -> RunContext {
        RunContext {
            id: uuid::Uuid::nil(),
            function_id: "test-fn".into(),
            namespace: "test".into(),
            key: None,
            started_at: chrono::Utc::now(),
            input: None,
            lineage_id: uuid::Uuid::nil(),
            chain_position: 0,
            cancelling: false,
        }
    }

    /// Drive to completion, or to `max` attempts.
    fn drive<H, T>(&mut self, max: usize, handler: H) -> Result<serde_json::Value, String>
    where
        H: for<'a> Handler<'a, T> + Copy,
        T: Serialize,
    {
        for attempt in 1..=max {
            let ctx = Ctx::new(Self::run_context(), self.memo.clone(), attempt as u64);
            let outcome = futures::executor::block_on(run_pass(&ctx, handler));
            self.passes.push(ctx.id_sequence());

            match outcome {
                PassOutcome::Done(v) => return Ok(v),
                PassOutcome::Error { error, .. } => {
                    return Err(error.code.unwrap_or_else(|| error.message.clone()))
                }
                PassOutcome::Yield(ops) => {
                    for op in ops {
                        self.commit(op);
                    }
                }
            }
        }
        Err(format!("did not complete within {max} attempts"))
    }

    /// Apply one op, exactly as `commit_ops` would.
    fn commit(&mut self, op: Op) {
        let (hash, id, kind, data, status) = match op {
            Op::Step { hash, id, data, .. } => {
                (hash, id, StepOp::Step, data, StepStatus::Completed)
            }
            Op::Sleep { hash, id, .. } => (hash, id, StepOp::Sleep, None, StepStatus::Completed),
            Op::WaitEvent { hash, id, .. } => {
                let v = if self.events.is_empty() {
                    Some(serde_json::json!({ "approved": true }))
                } else {
                    Some(self.events.remove(0))
                };
                (hash, id, StepOp::WaitEvent, v, StepStatus::Completed)
            }
            Op::Invoke { hash, id, .. } => (
                hash,
                id,
                StepOp::Invoke,
                Some(serde_json::json!({ "child": "done" })),
                StepStatus::Completed,
            ),
            Op::Signal { hash, id, .. } => (hash, id, StepOp::Signal, None, StepStatus::Completed),
            Op::ContinueAsNew { hash, id, .. } => {
                (hash, id, StepOp::Step, None, StepStatus::Completed)
            }
            Op::Done { .. } | Op::Error { .. } => return,
        };
        // First write wins, mirroring ON CONFLICT DO NOTHING. A step already
        // recorded is never overwritten, whatever a later attempt produces.
        self.memo.entry(hash).or_insert(RecordedStep {
            id,
            op: kind,
            status,
            data,
            error: None,
        });
    }
}

// ---------------------------------------------------------------- memoisation

#[test]
fn each_step_executes_exactly_once_across_all_attempts() {
    static CHARGE: AtomicUsize = AtomicUsize::new(0);
    static SHIP: AtomicUsize = AtomicUsize::new(0);
    CHARGE.store(0, Ordering::SeqCst);
    SHIP.store(0, Ordering::SeqCst);

    let out = Driver::new()
        .drive(
            10,
            wf!(|ctx| {
                let tx: String = ctx
                    .step("charge", || async {
                        CHARGE.fetch_add(1, Ordering::SeqCst);
                        Ok("ch_1".to_string())
                    })
                    .await?;
                let carrier: String = ctx
                    .step("ship", || async {
                        SHIP.fetch_add(1, Ordering::SeqCst);
                        Ok("dhl".to_string())
                    })
                    .await?;
                Ok(serde_json::json!({ "tx": tx, "carrier": carrier }))
            }),
        )
        .expect("completes");

    assert_eq!(out["tx"], "ch_1");
    assert_eq!(
        CHARGE.load(Ordering::SeqCst),
        1,
        "a recorded step must never re-execute"
    );
    assert_eq!(SHIP.load(Ordering::SeqCst), 1);
}

#[test]
fn a_memoized_step_never_constructs_its_future() {
    // The claim in the design doc: replay is cheap because a recorded step does
    // not merely skip its result, it never builds the work at all.
    static CONSTRUCTED: AtomicUsize = AtomicUsize::new(0);
    CONSTRUCTED.store(0, Ordering::SeqCst);

    let mut d = Driver::new();
    d.drive(
        5,
        wf!(|ctx| {
            let _: i32 = ctx
                .step("s", || {
                    CONSTRUCTED.fetch_add(1, Ordering::SeqCst);
                    async { Ok(1) }
                })
                .await?;
            Ok(serde_json::json!("done"))
        }),
    )
    .unwrap();

    assert_eq!(
        CONSTRUCTED.load(Ordering::SeqCst),
        1,
        "the closure must not even be called on a replay of a recorded step"
    );
}

#[test]
fn inserting_a_step_before_completed_ones_does_not_re_execute_them() {
    static OLD: AtomicUsize = AtomicUsize::new(0);
    OLD.store(0, Ordering::SeqCst);

    // First deploy: one step.
    let mut d = Driver::new();
    d.drive(
        5,
        wf!(|ctx| {
            let v: i32 = ctx
                .step("existing", || async {
                    OLD.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                })
                .await?;
            Ok(serde_json::json!(v))
        }),
    )
    .unwrap();
    assert_eq!(OLD.load(Ordering::SeqCst), 1);

    // Second deploy adds a step *before* it. Identity is (id, occurrence), not
    // position, so the existing result still matches and must not re-run.
    d.passes.clear();
    d.drive(
        5,
        wf!(|ctx| {
            let _: i32 = ctx.step("inserted", || async { Ok(0) }).await?;
            let v: i32 = ctx
                .step("existing", || async {
                    OLD.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                })
                .await?;
            Ok(serde_json::json!(v))
        }),
    )
    .unwrap();

    assert_eq!(
        OLD.load(Ordering::SeqCst),
        1,
        "inserting a step before a recorded one must not re-execute it"
    );
}

#[test]
fn renaming_a_step_orphans_its_result() {
    let mut d = Driver::new();
    d.drive(
        5,
        wf!(|ctx| {
            let v: i32 = ctx.step("old-name", || async { Ok(7) }).await?;
            Ok(serde_json::json!(v))
        }),
    )
    .unwrap();

    // Rename. The old result is orphaned and the new id re-executes — which is
    // why renaming is documented as a hazard rather than a refactor.
    let ctx = Ctx::new(Driver::run_context(), d.memo.clone(), 99);
    let _ = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let v: i32 = ctx.step("new-name", || async { Ok(7) }).await?;
            Ok(serde_json::json!(v))
        }),
    ));

    assert_eq!(
        ctx.orphaned().len(),
        1,
        "the old hash must be reported as orphaned (F-DX-4)"
    );
}

// ---------------------------------------------------------------- loops

#[test]
fn loop_occurrences_are_stable_across_attempts() {
    let mut d = Driver::new();
    let out = d
        .drive(
            20,
            wf!(|ctx| {
                let mut total = 0;
                for _ in 0..3 {
                    let v: i32 = ctx.step("item", || async { Ok(1) }).await?;
                    total += v;
                }
                Ok(serde_json::json!(total))
            }),
        )
        .expect("completes");

    assert_eq!(out, 3);
    // A pass stops at the first unrecorded step, so each pass sees one more id
    // than the last. What must hold is that every pass is a *prefix* of the
    // final one: the same ids in the same order, never a different id at the
    // same position. A divergence there is the silent-corruption case.
    let final_pass = d.passes.last().unwrap().clone();
    assert_eq!(final_pass, vec!["item", "item", "item"]);
    for (n, p) in d.passes.iter().enumerate() {
        assert_eq!(
            p[..],
            final_pass[..p.len()],
            "pass {} diverged from the final id sequence",
            n + 1
        );
    }
    assert_eq!(
        d.memo.len(),
        3,
        "three distinct occurrences, three distinct hashes"
    );
}

#[test]
fn hash_sequence_is_identical_across_many_replays() {
    let sequence = |pass: u64| {
        let ctx = Ctx::new(Driver::run_context(), HashMap::new(), pass);
        let _ = futures::executor::block_on(run_pass(
            &ctx,
            wf!(|ctx| {
                for _ in 0..5 {
                    let _: i32 = ctx.step("a", || async { Ok(1) }).await?;
                    let _: i32 = ctx.step("b", || async { Ok(2) }).await?;
                }
                Ok(serde_json::json!(0))
            }),
        ));
        ctx.id_sequence()
    };
    let first = sequence(1);
    for pass in 2..=50 {
        assert_eq!(sequence(pass), first, "pass {pass} diverged from pass 1");
    }
}

#[test]
fn hash_is_scoped_by_function_id() {
    // A child run of a different function must not collide with its parent, or a
    // parent's recorded result would satisfy a child's step.
    let mut parent_ctx = Driver::run_context();
    parent_ctx.function_id = "parent".into();
    let mut child_ctx = Driver::run_context();
    child_ctx.function_id = "child".into();

    let hash_for = |run: RunContext| {
        let ctx = Ctx::new(run, HashMap::new(), 1);
        // Read the op out of the outcome: `run_pass` drains the context's pending
        // buffer, which is the whole point of it owning that decision.
        match futures::executor::block_on(run_pass(
            &ctx,
            wf!(|ctx| {
                let _: i32 = ctx.step("same-id", || async { Ok(1) }).await?;
                Ok(serde_json::json!(0))
            }),
        )) {
            PassOutcome::Yield(ops) => ops[0].hash().unwrap().to_string(),
            other => panic!("expected a yield, got {other:?}"),
        }
    };
    assert_ne!(hash_for(parent_ctx), hash_for(child_ctx));
}

// ---------------------------------------------------------------- concurrency

#[test]
fn eager_claiming_makes_poll_order_irrelevant() {
    // The finding the whole design rests on. Futures are created in program order
    // `a, b, a` and polled in REVERSE; the hashes must be unchanged.
    let hashes_for = |reverse: bool| {
        let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
        let outcome = futures::executor::block_on(run_pass(
            &ctx,
            wf!(|ctx| {
                let f1 = ctx.step::<i32, _, _>("a", || async { Ok(1) });
                let f2 = ctx.step::<i32, _, _>("b", || async { Ok(2) });
                let f3 = ctx.step::<i32, _, _>("a", || async { Ok(3) });
                if reverse {
                    let _ = futures::future::join3(f3, f2, f1).await;
                } else {
                    let _ = futures::future::join3(f1, f2, f3).await;
                }
                Ok(serde_json::json!(0))
            }),
        ));
        // Read the ops off the outcome, not off the context: `run_pass` drains
        // the buffer so that no arm can leave recorded work behind (ADR-023).
        // This handler swallows the halts, so the outcome is the swallowed-halt
        // error — which now carries the ops rather than dropping them, and that
        // is the point being asserted.
        let ops = match outcome {
            PassOutcome::Yield(ops) => ops,
            PassOutcome::Error { ops, .. } => ops,
            PassOutcome::Done(_) => vec![],
        };
        let mut h: Vec<String> = ops
            .iter()
            .filter_map(|o| o.hash().map(str::to_string))
            .collect();
        h.sort();
        h
    };
    assert_eq!(
        hashes_for(false),
        hashes_for(true),
        "polling order changed the hashes — occurrence is being claimed lazily"
    );
    assert_eq!(
        hashes_for(false).len(),
        3,
        "all three members are emitted in one envelope"
    );
}

#[test]
fn naive_counter_under_reordering_is_demonstrably_broken() {
    // The control. If claiming happened at poll time, this is what would occur —
    // included so the property above is a measurement rather than an assertion of
    // faith.
    use std::cell::RefCell;
    let counters: RefCell<HashMap<&str, u32>> = RefCell::new(HashMap::new());
    let lazy = |id: &'static str| {
        let counters = &counters;
        async move {
            let mut c = counters.borrow_mut();
            let n = c.entry(id).or_insert(0);
            let occ = *n;
            *n += 1;
            format!("{id}#{occ}")
        }
    };
    let forward = {
        counters.borrow_mut().clear();
        let (a, b) = (lazy("x"), lazy("x"));
        futures::executor::block_on(async { futures::future::join(a, b).await })
    };
    let reverse = {
        counters.borrow_mut().clear();
        let (a, b) = (lazy("x"), lazy("x"));
        futures::executor::block_on(async {
            let (b, a) = futures::future::join(b, a).await;
            (a, b)
        })
    };
    assert_ne!(
        forward, reverse,
        "the lazy scheme is supposed to be order-dependent; if this passes, the control is broken"
    );
}

#[test]
fn a_duplicate_id_inside_a_parallel_group_is_fatal() {
    let mut d = Driver::new();
    let err = d
        .drive(
            5,
            wf!(|ctx| {
                let a = ctx.step::<i32, _, _>("dup", || async { Ok(1) });
                let b = ctx.step::<i32, _, _>("dup", || async { Ok(2) });
                let _ = ctx.join((a, b)).await?;
                Ok(serde_json::json!(0))
            }),
        )
        .unwrap_err();
    assert_eq!(err, "protocol_violation");
}

#[test]
fn a_duplicate_id_is_rejected_before_any_member_runs() {
    // Rejecting after a member executed would leave a side effect that nothing
    // recorded — the one outcome a durable engine must never produce.
    static RAN: AtomicUsize = AtomicUsize::new(0);
    RAN.store(0, Ordering::SeqCst);

    let mut d = Driver::new();
    let _ = d.drive(
        5,
        wf!(|ctx| {
            let a = ctx.step::<i32, _, _>("dup", || async {
                RAN.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            });
            let b = ctx.step::<i32, _, _>("dup", || async {
                RAN.fetch_add(1, Ordering::SeqCst);
                Ok(2)
            });
            let _ = ctx.join((a, b)).await?;
            Ok(serde_json::json!(0))
        }),
    );
    assert_eq!(
        RAN.load(Ordering::SeqCst),
        0,
        "no member may run before the group is validated"
    );
}

#[test]
fn a_parallel_group_emits_one_envelope_and_each_member_runs_once() {
    static A: AtomicUsize = AtomicUsize::new(0);
    static B: AtomicUsize = AtomicUsize::new(0);
    A.store(0, Ordering::SeqCst);
    B.store(0, Ordering::SeqCst);

    let mut d = Driver::new();
    let out = d
        .drive(
            10,
            wf!(|ctx| {
                let inv = ctx.step::<String, _, _>("fetch-invoice", || async {
                    A.fetch_add(1, Ordering::SeqCst);
                    Ok("inv-1".into())
                });
                let risk = ctx.step::<i32, _, _>("score-risk", || async {
                    B.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                });
                let (invoice, score) = ctx.join((inv, risk)).await?;
                Ok(serde_json::json!({ "invoice": invoice, "score": score }))
            }),
        )
        .expect("completes");

    assert_eq!(out["invoice"], "inv-1");
    assert_eq!(out["score"], 42);
    assert_eq!(A.load(Ordering::SeqCst), 1);
    assert_eq!(B.load(Ordering::SeqCst), 1);
    // Two members, two ops, one attempt to discover them, one to replay past.
    assert_eq!(
        d.passes.len(),
        2,
        "a parallel group must not cost one attempt per member"
    );
}

#[test]
fn join_all_handles_a_fan_out_with_discriminated_ids() {
    let mut d = Driver::new();
    let out = d
        .drive(
            10,
            wf!(|ctx| {
                let ids: Vec<String> = (0..5).map(|i| format!("charge-{i}")).collect();
                let members: Vec<_> = ids
                    .iter()
                    .enumerate()
                    .map(|(i, id)| ctx.step::<i32, _, _>(id, move || async move { Ok(i as i32) }))
                    .collect();
                let results = ctx.join_all(members).await?;
                Ok(serde_json::json!(results.iter().sum::<i32>()))
            }),
        )
        .expect("completes");
    assert_eq!(out, 10, "0 + 1 + 2 + 3 + 4, one per fanned-out member");
    assert_eq!(d.memo.len(), 5);
    assert_eq!(d.passes.len(), 2, "all five discovered in one envelope");
}

// ---------------------------------------------------------------- halts

#[test]
fn a_swallowed_halt_is_a_loud_error_not_a_silent_success() {
    // The hazard the `?`-based short circuit creates: user code that absorbs the
    // halt continues in a state that does not exist, and would otherwise commit a
    // `done` for a run whose middle never happened.
    let mut d = Driver::new();
    let err = d
        .drive(
            5,
            wf!(|ctx| {
                let v = ctx
                    .step::<i32, _, _>("s", || async { Ok(1) })
                    .await
                    .unwrap_or_default(); // <-- swallows the halt
                Ok(serde_json::json!(v))
            }),
        )
        .unwrap_err();
    assert_eq!(err, "swallowed_halt");
}

#[test]
fn a_swallowed_failure_is_also_caught() {
    // `.ok()` on a genuinely failing step is the same mistake wearing a different
    // hat: the handler reports success for work that did not happen.
    let mut d = Driver::new();
    let err = d
        .drive(
            5,
            wf!(|ctx| {
                let v = ctx
                    .step::<i32, _, _>("s", || async { Err(StepError::retryable("gateway down")) })
                    .await
                    .ok();
                Ok(serde_json::json!(v))
            }),
        )
        .unwrap_err();
    assert_eq!(err, "swallowed_halt");
}

#[test]
fn a_step_failure_propagates_with_its_retryability() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let _: i32 = ctx
                .step("charge", || async {
                    Err(StepError::coded("gateway_down", "upstream 503"))
                })
                .await?;
            Ok(serde_json::json!(0))
        }),
    ));
    match outcome {
        PassOutcome::Error {
            ops,
            retryable,
            error,
        } => {
            assert!(retryable, "a coded failure defaults to retryable");
            assert_eq!(error.code.as_deref(), Some("gateway_down"));
            // A retryable failure records nothing: the retry has to re-execute
            // the body, and a recorded step is memoised (§5.2.2).
            assert!(ops.is_empty(), "got {ops:?}");
        }
        other => panic!("expected an error, got {other:?}"),
    }
}

/// The defect ADR-023 was written for.
///
/// A group whose members mostly succeeded used to commit nothing at all: the
/// successful members' ops sat on the context, `run_pass` took the error arm,
/// and the buffer was dropped. Two step bodies had run and the journal knew
/// about neither.
#[test]
fn a_group_with_one_fatal_member_still_records_the_others() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let a = ctx.step::<i32, _, _>("ok-1", || async { Ok(1) });
            let b = ctx.step::<i32, _, _>("bad", || async {
                Err(StepError::fatal_coded("member_failed", "on purpose"))
            });
            let c = ctx.step::<i32, _, _>("ok-2", || async { Ok(3) });
            let (x, y, z) = ctx.join3((a, b, c)).await?;
            Ok(serde_json::json!([x, y, z]))
        }),
    ));
    let PassOutcome::Error {
        ops,
        retryable,
        error,
    } = outcome
    else {
        panic!("expected the group's failure to surface");
    };
    assert!(!retryable);
    assert_eq!(error.code.as_deref(), Some("member_failed"));

    let recorded: Vec<(&str, bool)> = ops
        .iter()
        .filter_map(|o| match o {
            Op::Step { id, error, .. } => Some((id.as_str(), error.is_some())),
            _ => None,
        })
        .collect();
    assert_eq!(
        recorded,
        vec![("ok-1", false), ("bad", true), ("ok-2", false)],
        "every member's outcome must ride with the error"
    );
}

/// The other half of the rule: a *retryable* member failure records nothing
/// about itself, because the retry has to run the body again.
#[test]
fn a_retryable_member_defers_the_error_and_commits_its_siblings() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let a = ctx.step::<i32, _, _>("ok-1", || async { Ok(1) });
            let b = ctx.step::<i32, _, _>("flaky", || async {
                Err(StepError::coded("gateway_down", "upstream 503"))
            });
            let (x, y) = ctx.join((a, b)).await?;
            Ok(serde_json::json!([x, y]))
        }),
    ));
    // A yield, not an error: the sibling's result goes now, and the failure is
    // raised again next attempt with nothing new to record, where it can be
    // sent alone and the dispatcher can apply its backoff.
    let PassOutcome::Yield(ops) = outcome else {
        panic!("a retryable failure alongside recorded work must defer");
    };
    let ids: Vec<&str> = ops.iter().filter_map(|o| o.step_id()).collect();
    assert_eq!(ids, vec!["ok-1"]);
}

#[test]
fn a_non_retryable_failure_stays_non_retryable() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let _: i32 = ctx
                .step("v", || async { Err(StepError::fatal("bad input")) })
                .await?;
            Ok(serde_json::json!(0))
        }),
    ));
    assert!(matches!(
        outcome,
        PassOutcome::Error {
            retryable: false,
            ..
        }
    ));
}

// ---------------------------------------------------------------- other ops

#[test]
fn sleep_replays_as_a_null_result_without_re_suspending() {
    let mut d = Driver::new();
    let out = d
        .drive(
            10,
            wf!(|ctx| {
                let a: i32 = ctx.step("before", || async { Ok(1) }).await?;
                ctx.sleep("cooldown", chrono::Duration::days(1)).await?;
                let b: i32 = ctx.step("after", || async { Ok(2) }).await?;
                Ok(serde_json::json!(a + b))
            }),
        )
        .expect("completes");
    assert_eq!(out, 3);
    assert_eq!(d.passes.len(), 4, "step, sleep, step, done");
}

#[test]
fn wait_event_defaults_to_the_run_start_window() {
    // The default that closes the lost-signal race. An SDK that emitted
    // `registration` here would reopen it for every wait, silently.
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let _: Option<serde_json::Value> = ctx.wait_event("a", "order.approved").await?;
            Ok(serde_json::json!(0))
        }),
    ));
    match outcome {
        PassOutcome::Yield(ops) => match &ops[0] {
            Op::WaitEvent { since, .. } => assert_eq!(since, "run_start"),
            other => panic!("expected wait_event, got {other:?}"),
        },
        other => panic!("expected a yield, got {other:?}"),
    }
}

#[test]
fn wait_event_resolves_with_the_delivered_payload() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Approval {
        by: String,
    }
    let mut d = Driver::new().with_event(serde_json::json!({ "by": "priya" }));
    let out = d
        .drive(
            5,
            wf!(|ctx| {
                let a: Option<Approval> = ctx.wait_event("approval", "order.approved").await?;
                Ok(serde_json::json!(a.map(|a| a.by)))
            }),
        )
        .expect("completes");
    assert_eq!(out, "priya");
}

#[test]
fn a_timed_out_wait_resolves_to_none_rather_than_failing() {
    // The handler must get a chance to compensate. Failing the run on timeout
    // would take the decision away from the code that owns it.
    let mut d = Driver::new();
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let _: Option<i32> = ctx
                .wait_event("a", "never")
                .timeout(chrono::Duration::days(7))
                .await?;
            Ok(serde_json::json!(0))
        }),
    ));
    let hash = match outcome {
        PassOutcome::Yield(ops) => ops[0].hash().unwrap().to_string(),
        other => panic!("expected a yield, got {other:?}"),
    };
    d.memo.insert(
        hash,
        RecordedStep {
            id: "a".into(),
            op: StepOp::WaitEvent,
            status: StepStatus::TimedOut,
            data: None,
            error: None,
        },
    );

    let out = d
        .drive(
            5,
            wf!(|ctx| {
                let v: Option<i32> = ctx
                    .wait_event("a", "never")
                    .timeout(chrono::Duration::days(7))
                    .await?;
                Ok(serde_json::json!(v.is_none()))
            }),
        )
        .expect("completes");
    assert_eq!(out, true);
}

#[test]
fn invoke_memoizes_the_child_output() {
    let mut d = Driver::new();
    let out = d
        .drive(
            5,
            wf!(|ctx| {
                let r: serde_json::Value = ctx
                    .invoke(
                        "refund",
                        "refunds/issue",
                        serde_json::json!({ "order": 4711 }),
                    )
                    .await?;
                Ok(r)
            }),
        )
        .expect("completes");
    assert_eq!(out["child"], "done");
}

#[test]
fn continue_as_new_always_halts() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let outcome = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            Err::<serde_json::Value, _>(
                ctx.continue_as_new("next", serde_json::json!({ "cursor": 1 })),
            )
        }),
    ));
    match outcome {
        PassOutcome::Yield(ops) => {
            assert!(matches!(ops[0], Op::ContinueAsNew { .. }));
            assert_eq!(ops.len(), 1, "continue_as_new must appear alone");
        }
        other => panic!("expected a yield, got {other:?}"),
    }
}

// ---------------------------------------------------------------- decode safety

#[test]
fn a_memoized_value_that_no_longer_decodes_fails_rather_than_re_executing() {
    // Re-running the closure would repeat a side effect that already happened.
    // Failing names the step and leaves the operator a choice.
    static RAN: AtomicUsize = AtomicUsize::new(0);
    RAN.store(0, Ordering::SeqCst);

    let mut d = Driver::new();
    d.drive(
        5,
        wf!(|ctx| {
            let _: String = ctx.step("s", || async { Ok("text".to_string()) }).await?;
            Ok(serde_json::json!(0))
        }),
    )
    .unwrap();

    // The handler's type changed between deploys.
    let err = d
        .drive(
            5,
            wf!(|ctx| {
                let v: i32 = ctx
                    .step("s", || async {
                        RAN.fetch_add(1, Ordering::SeqCst);
                        Ok(1)
                    })
                    .await?;
                Ok(serde_json::json!(v))
            }),
        )
        .unwrap_err();

    assert_eq!(err, "memo_decode_failed");
    assert_eq!(
        RAN.load(Ordering::SeqCst),
        0,
        "the step must NOT be re-executed"
    );
}

// ---------------------------------------------------------------- crash safety

#[test]
fn a_crash_at_any_point_yields_the_same_outcome() {
    // Seeded crash interleavings: for every prefix of attempts, drop the pass's
    // ops and re-run. The result must be identical and no step may be recorded
    // twice, because "the work happened twice, the record was written once" is
    // the entire contract.
    for crash_after in 0..6 {
        static RUNS: AtomicUsize = AtomicUsize::new(0);
        RUNS.store(0, Ordering::SeqCst);

        let mut memo: HashMap<String, RecordedStep> = HashMap::new();
        let mut attempt = 0usize;
        let mut result = None;

        while attempt < 30 {
            attempt += 1;
            let ctx = Ctx::new(Driver::run_context(), memo.clone(), attempt as u64);
            let outcome = futures::executor::block_on(run_pass(
                &ctx,
                wf!(|ctx| {
                    let a: i32 = ctx
                        .step("a", || async {
                            RUNS.fetch_add(1, Ordering::SeqCst);
                            Ok(1)
                        })
                        .await?;
                    let b: i32 = ctx
                        .step("b", || async {
                            RUNS.fetch_add(1, Ordering::SeqCst);
                            Ok(2)
                        })
                        .await?;
                    let c: i32 = ctx
                        .step("c", || async {
                            RUNS.fetch_add(1, Ordering::SeqCst);
                            Ok(3)
                        })
                        .await?;
                    Ok(serde_json::json!(a + b + c))
                }),
            ));

            match outcome {
                PassOutcome::Done(v) => {
                    result = Some(v);
                    break;
                }
                PassOutcome::Yield(ops) => {
                    // The crash: this attempt's work happened, but the commit
                    // never landed.
                    if attempt == crash_after {
                        continue;
                    }
                    for op in ops {
                        if let (Some(h), Some(id)) = (op.hash(), op.step_id()) {
                            let data = match &op {
                                Op::Step { data, .. } => data.clone(),
                                _ => None,
                            };
                            memo.entry(h.to_string()).or_insert(RecordedStep {
                                id: id.to_string(),
                                op: StepOp::Step,
                                status: StepStatus::Completed,
                                data,
                                error: None,
                            });
                        }
                    }
                }
                PassOutcome::Error { error, .. } => panic!("unexpected error: {error:?}"),
            }
        }

        assert_eq!(
            result,
            Some(serde_json::json!(6)),
            "crash_after={crash_after}"
        );
        assert_eq!(
            memo.len(),
            3,
            "exactly three records, whatever the crash point"
        );
        // Three steps take three yielding attempts, so only crash points 1..=3
        // land on an attempt that had work to lose. A crash on the fourth
        // attempt — the one that returns `done` — costs nothing, because that
        // attempt executed nothing.
        let expected_executions = if (1..=3).contains(&crash_after) { 4 } else { 3 };
        assert_eq!(
            RUNS.load(Ordering::SeqCst),
            expected_executions,
            "crash_after={crash_after}: the crashed step re-executes exactly once, and only it"
        );
    }
}

// ---------------------------------------------------------------- diagnostics

#[test]
fn idempotency_keys_are_stable_across_re_execution() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let a = ctx.idempotency_key("deadbeef");
    let ctx2 = Ctx::new(Driver::run_context(), HashMap::new(), 2);
    assert_eq!(
        a,
        ctx2.idempotency_key("deadbeef"),
        "must not vary by attempt"
    );
    assert_ne!(a, ctx.idempotency_key("cafebabe"), "must vary by step");
}

#[test]
fn emitted_events_travel_with_the_envelope() {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let _ = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            ctx.emit(stepd_proto::Event::new(
                "shipped",
                "/orders",
                serde_json::json!({ "id": 1 }),
            ));
            let _: i32 = ctx.step("s", || async { Ok(1) }).await?;
            Ok(serde_json::json!(0))
        }),
    ));
    assert_eq!(ctx.take_emit().len(), 1);
}

// ---------------------------------------------------------------- claim timing

/// The op emitted by one pass of `handler`, with its hash.
fn emitted_op(
    reverse: bool,
    build: fn(&Ctx, bool) -> BoxFut<'_, serde_json::Value>,
) -> (String, String) {
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    // `workflow` supplies the `for<'a>` bound the closure cannot infer on its own.
    let handler = workflow(move |c: &Ctx| build(c, reverse));
    let outcome = futures::executor::block_on(run_pass(&ctx, handler));
    match outcome {
        PassOutcome::Yield(ops) => (
            ops[0].step_id().unwrap().to_string(),
            ops[0].hash().unwrap().to_string(),
        ),
        other => panic!("expected a yield, got {other:?}"),
    }
}

#[test]
fn a_wait_claims_at_the_call_site_not_at_the_await() {
    // Found while writing ADR-012: `ctx.step` claimed eagerly, but `wait_event`
    // and `invoke` returned builders that claimed inside `into_future` — that is,
    // at `.await`. Two waits sharing an id, awaited out of declaration order,
    // therefore swapped their occurrences: precisely the lazy-claim hazard the
    // whole design exists to remove, reintroduced through a different door.
    //
    // Two builders share the id "w". Whichever is awaited first, the *second
    // declared* one must carry occurrence 1.
    fn build(ctx: &Ctx, reverse: bool) -> BoxFut<'_, serde_json::Value> {
        Box::pin(async move {
            let first = ctx.wait_event::<i32>("w", "ev");
            let second = ctx.wait_event::<i32>("w", "ev");
            if reverse {
                let _: Option<i32> = second.await?;
                let _: Option<i32> = first.await?;
            } else {
                let _: Option<i32> = first.await?;
                let _: Option<i32> = second.await?;
            }
            Ok(serde_json::json!(0))
        })
    }

    let occ0 = step_hash("test-fn", "w", 0);
    let occ1 = step_hash("test-fn", "w", 1);

    let (_, forward) = emitted_op(false, build);
    assert_eq!(forward, occ0, "the first-declared wait holds occurrence 0");

    let (_, reversed) = emitted_op(true, build);
    assert_eq!(
        reversed, occ1,
        "awaiting the second-declared wait first must NOT give it occurrence 0 — \
         the occurrence follows declaration order, never await order"
    );
}

#[test]
fn an_invoke_claims_at_the_call_site_too() {
    fn build(ctx: &Ctx, reverse: bool) -> BoxFut<'_, serde_json::Value> {
        Box::pin(async move {
            let a = ctx.invoke::<i32>("kid", "f", serde_json::json!({}));
            let b = ctx.invoke::<i32>("kid", "f", serde_json::json!({}));
            if reverse {
                let _: i32 = b.await?;
                let _: i32 = a.await?;
            } else {
                let _: i32 = a.await?;
                let _: i32 = b.await?;
            }
            Ok(serde_json::json!(0))
        })
    }
    assert_eq!(emitted_op(false, build).1, step_hash("test-fn", "kid", 0));
    assert_eq!(emitted_op(true, build).1, step_hash("test-fn", "kid", 1));
}

#[test]
fn a_builder_that_is_never_awaited_still_consumes_its_occurrence() {
    // The cost of claiming at construction, stated so it is a decision rather
    // than a surprise: an abandoned builder has already taken an occurrence, so
    // a handler that conditionally constructs one shifts every later occurrence
    // of that id. Constructing ops inside a branch is a determinism hazard for
    // exactly this reason, and `stepd lint` should flag it.
    let ctx = Ctx::new(Driver::run_context(), HashMap::new(), 1);
    let _ = futures::executor::block_on(run_pass(
        &ctx,
        wf!(|ctx| {
            let _abandoned = ctx.wait_event::<i32>("w", "ev");
            let _: Option<i32> = ctx.wait_event("w", "ev").await?;
            Ok(serde_json::json!(0))
        }),
    ));
    assert_eq!(
        ctx.id_sequence(),
        vec!["w", "w"],
        "both constructions claim, which is what makes await order irrelevant"
    );
}
