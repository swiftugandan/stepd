//! Asserts the properties the protocol claims. These map 1:1 onto conformance
//! suites `memoization`, `loops`, `determinism`, `parallel`.

use serde_json::{json, Value};
use std::cell::Cell;
use stepd_sdk_proto::*;

// --------------------------------------------------- memoization

#[test]
fn each_step_executes_exactly_once_across_all_attempts() {
    let charge = Cell::new(0);
    let ship = Cell::new(0);

    let handler = |ctx: &Ctx| -> StepResult<Value> {
        let c = ctx.run("charge", || {
            charge.set(charge.get() + 1);
            json!({"tx": "ch_1"})
        })?;
        ctx.sleep("cooldown", "P1D")?;
        let s = ctx.run("ship", || {
            ship.set(ship.get() + 1);
            json!({"carrier": "dhl"})
        })?;
        Ok(json!({"charge": c, "ship": s}))
    };

    let (out, attempts, _) = drive("order-fulfilment", handler, 20).unwrap();
    assert_eq!(charge.get(), 1, "charge must execute exactly once");
    assert_eq!(ship.get(), 1, "ship must execute exactly once");
    assert_eq!(attempts, 4, "3 ops + 1 final pass");
    assert_eq!(out["charge"]["tx"], "ch_1");
}

// --------------------------------------------------- loops

#[test]
fn loop_occurrences_are_stable_across_attempts() {
    let runs = Cell::new(0);
    let handler = |ctx: &Ctx| -> StepResult<Value> {
        let mut total = 0i64;
        for i in 0..5 {
            let v = ctx.run("charge-invoice", || {
                runs.set(runs.get() + 1);
                json!(i * 10)
            })?;
            total += v.as_i64().unwrap();
        }
        Ok(json!(total))
    };
    let (out, attempts, seqs) = drive("billing", handler, 20).unwrap();
    assert_eq!(out, json!(100), "0+10+20+30+40");
    assert_eq!(runs.get(), 5, "each iteration executes once, not once per attempt");
    assert_eq!(attempts, 6);
    // every pass sees the same id sequence prefix
    for s in &seqs {
        assert!(s.iter().all(|id| id == "charge-invoice"));
    }
}

#[test]
fn same_id_at_different_occurrences_yields_different_hashes() {
    let a = step_hash("f", "s", 0);
    let b = step_hash("f", "s", 1);
    assert_ne!(a, b);
    assert_eq!(a.len(), 16, "16 hex chars = 64 bits");
}

#[test]
fn hash_is_scoped_by_function_id() {
    assert_ne!(step_hash("fn-a", "s", 0), step_hash("fn-b", "s", 0),
        "child runs of different functions must not collide");
}

// --------------------------------------------------- determinism

#[test]
fn claiming_an_occurrence_off_the_sequential_path_is_fatal() {
    // Simulates a concurrent task holding a stale/foreign pass token.
    let handler = |ctx: &Ctx| -> StepResult<Value> {
        let foreign = Ctx::new("f", Default::default(), 999).token();
        ctx.run_with("sneaky", foreign, || json!(1))?;
        Ok(json!("unreachable"))
    };
    let err = drive("f", handler, 5).unwrap_err();
    assert!(err.contains("outside the sequential replay pass"), "got: {err}");
}

#[test]
fn duplicate_id_inside_a_parallel_group_is_fatal() {
    let handler = |ctx: &Ctx| -> StepResult<Value> {
        ctx.parallel::<()>(vec![
            ("fetch", Box::new(|| json!(1))),
            ("fetch", Box::new(|| json!(2))), // same id — ambiguous
        ])?;
        Ok(json!("unreachable"))
    };
    let err = drive("f", handler, 5).unwrap_err();
    assert!(err.contains("ambiguous_step_id"), "got: {err}");
}

// --------------------------------------------------- parallel

#[test]
fn parallel_group_hashes_are_stable_and_each_member_runs_once() {
    let hits = Cell::new(0);
    let handler = |ctx: &Ctx| -> StepResult<Value> {
        let vs = ctx.parallel::<()>(vec![
            ("fetch-invoice", Box::new(|| { json!("inv") })),
            ("fetch-customer", Box::new(|| { json!("cust") })),
            ("fetch-risk", Box::new(|| { json!("risk") })),
        ])?;
        hits.set(hits.get() + 1);
        Ok(json!(vs))
    };
    let (out, attempts, _) = drive("f", handler, 10).unwrap();
    assert_eq!(out, json!(["inv", "cust", "risk"]), "order follows declaration, not completion");
    assert_eq!(attempts, 2, "one attempt yields all three, the next replays them");
    assert_eq!(hits.get(), 1, "code after the group runs once");
}

#[test]
fn parallel_members_yield_in_a_single_envelope() {
    // Directly inspect the ops from the first pass.
    let ctx = Ctx::new("f", Default::default(), 1);
    let r = ctx.parallel::<()>(vec![
        ("a", Box::new(|| json!(1))),
        ("b", Box::new(|| json!(2))),
    ]);
    match r {
        Err(Halt::Yield(ops)) => assert_eq!(ops.len(), 2, "one envelope, both ops"),
        other => panic!("expected Yield, got {other:?}"),
    }
}

// --------------------------------------------------- versioning

#[test]
fn inserting_a_step_before_completed_ones_does_not_re_execute_them() {
    // Attempt sequence with v1 code, then swap to v2 which adds a step first.
    let v1_charge = Cell::new(0);
    let v1 = |ctx: &Ctx| -> StepResult<Value> {
        let c = ctx.run("charge", || { v1_charge.set(v1_charge.get()+1); json!("tx") })?;
        ctx.run("ship", || json!("shipped"))?;
        Ok(c)
    };
    // Build memo by driving v1 to completion, capturing state.
    let mut memo = std::collections::HashMap::new();
    for attempt in 1..=5u64 {
        let ctx = Ctx::new("f", memo.clone(), attempt);
        match v1(&ctx) {
            Ok(_) => break,
            Err(Halt::Yield(ops)) => for op in ops {
                if let (Some(h), Op::Step{data,..}) = (op.hash(), &op) {
                    memo.insert(h.to_string(), data.clone());
                }
            },
            Err(Halt::Fatal(m)) => panic!("{m}"),
        }
    }
    assert_eq!(v1_charge.get(), 1);

    // v2 inserts "audit" BEFORE charge. charge/ship keep their hashes because
    // identity is (id, occurrence), not position.
    let v2_charge = Cell::new(0);
    let ctx = Ctx::new("f", memo.clone(), 99);
    let v2 = |ctx: &Ctx| -> StepResult<Value> {
        ctx.run("audit", || json!("logged"))?;
        ctx.run("charge", || { v2_charge.set(v2_charge.get()+1); json!("tx") })?;
        ctx.run("ship", || json!("shipped"))?;
        Ok(json!("done"))
    };
    match v2(&ctx) {
        Err(Halt::Yield(ops)) => {
            assert_eq!(ops.len(), 1);
            assert!(matches!(&ops[0], Op::Step{id,..} if id == "audit"));
        }
        other => panic!("expected audit to yield, got {other:?}"),
    }
    assert_eq!(v2_charge.get(), 0, "charge must NOT re-execute when a step is inserted before it");
}

#[test]
fn renaming_a_step_orphans_its_result() {
    let mut memo = std::collections::HashMap::new();
    memo.insert(step_hash("f", "old-name", 0), json!("recorded"));
    let ctx = Ctx::new("f", memo, 1);
    let _ = ctx.run("new-name", || json!("re-executed"));
    let orphans = ctx.orphaned();
    assert_eq!(orphans.len(), 1, "the old hash is orphaned — this is the F-DX-4 diagnostic");
}

// --------------------------------------------------- wait defaults

#[test]
fn wait_event_defaults_to_run_start_window() {
    let ctx = Ctx::new("f", Default::default(), 1);
    match ctx.wait_event("approval", "order.approved") {
        Err(Halt::Yield(ops)) => match &ops[0] {
            Op::WaitEvent { since, .. } => assert_eq!(since, "run_start",
                "default must close the lost-signal race"),
            o => panic!("expected WaitEvent, got {o:?}"),
        },
        other => panic!("expected Yield, got {other:?}"),
    }
}
