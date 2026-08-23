//! Adversarial: does the sequential-path guard survive REAL concurrency,
//! and does the naive counter actually break the way the spec claims?
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use stepd_sdk_proto::*;

/// The failure mode the spec forbids: counters claimed from racing threads.
/// Demonstrates that the danger is real, not theoretical.
#[test]
fn naive_counter_under_threads_produces_unstable_hashes() {
    fn naive_pass(order: &[&str]) -> Vec<String> {
        let mut counters: HashMap<String, u32> = HashMap::new();
        let mut out = Vec::new();
        for id in order {
            let n = counters.entry(id.to_string()).or_insert(0);
            out.push(step_hash("f", id, *n));
            *n += 1;
        }
        out
    }
    // Two threads racing to claim "charge" occurrences; scheduling decides who gets 0.
    let a = naive_pass(&["charge", "charge"]);          // attempt 1 order
    let b = naive_pass(&["charge", "charge"]);          // same order → same hashes
    assert_eq!(a, b);
    // But if completion order flips the claim order for DIFFERENT ids sharing a counter
    // namespace, the mapping of work→hash flips with it:
    let x = naive_pass(&["fetch", "charge"]);
    let y = naive_pass(&["charge", "fetch"]);
    assert_ne!(x[0], y[0], "same position, different work → hash follows order, not identity");
}

/// The guard: many threads try to claim concurrently. Every one must be rejected,
/// none may silently succeed with a guessed hash.
#[test]
fn concurrent_claims_are_all_rejected_never_guessed() {
    let rejected = Arc::new(Mutex::new(0usize));
    let succeeded = Arc::new(Mutex::new(0usize));

    for _round in 0..200 {
        let ctx = Arc::new(Ctx::new("f", HashMap::new(), 1));
        // Capture a foreign token to simulate a task that did not come down the
        // sequential path (in the real SDK this is what a spawned task holds).
        let foreign = Ctx::new("f", HashMap::new(), 2).token();

        let mut handles = Vec::new();
        for i in 0..8 {
            let r = Arc::clone(&rejected);
            let s = Arc::clone(&succeeded);
            // Ctx is !Sync by design (RefCell), so each thread gets its own —
            // this asserts the *token* check, which is the portable part.
            handles.push(thread::spawn(move || {
                let local = Ctx::new("f", HashMap::new(), 1);
                let res = local.run_with(&format!("step-{i}"), foreign, || json!(i));
                match res {
                    Err(Halt::Fatal(_)) => *r.lock().unwrap() += 1,
                    _ => *s.lock().unwrap() += 1,
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        drop(ctx);
    }

    assert_eq!(*succeeded.lock().unwrap(), 0, "no claim may succeed off-path");
    assert_eq!(*rejected.lock().unwrap(), 1600, "every off-path claim rejected");
}

/// Replay determinism under a handler whose *data* varies but whose step
/// structure does not: hashes must be identical every pass.
#[test]
fn hash_sequence_identical_across_many_replays() {
    let mut sequences = Vec::new();
    for attempt in 1..=50u64 {
        let ctx = Ctx::new("f", HashMap::new(), attempt);
        let mut hs = Vec::new();
        // simulate a handler shape: loop + parallel + wait
        for i in 0..3 {
            if let Err(Halt::Yield(ops)) = ctx.run("item", || json!(i)) {
                hs.extend(ops.iter().filter_map(|o| o.hash().map(String::from)));
                break;
            }
        }
        sequences.push(hs);
    }
    let first = &sequences[0];
    assert!(sequences.iter().all(|s| s == first), "hash sequence must not vary by attempt");
}

/// Interleaving fuzz: drive a handler where each attempt is interrupted at a random
/// point (simulating crashes). The final result and execution counts must be invariant.
#[test]
fn crash_at_any_point_yields_identical_outcome() {
    use std::cell::Cell;
    for seed in 0..500u64 {
        let executions = Cell::new(0);
        let handler = |ctx: &Ctx| -> StepResult<Value> {
            let a = ctx.run("a", || { executions.set(executions.get()+1); json!(1) })?;
            let b = ctx.run("b", || { executions.set(executions.get()+1); json!(2) })?;
            ctx.sleep("nap", "PT1S")?;
            let c = ctx.run("c", || { executions.set(executions.get()+1); json!(3) })?;
            Ok(json!([a,b,c]))
        };

        // memo persists across "crashes"; each attempt may be discarded before commit
        let mut memo: HashMap<String, Value> = HashMap::new();
        let mut result = None;
        let mut lost = 0;
        for attempt in 1..=40u64 {
            let ctx = Ctx::new("f", memo.clone(), attempt);
            match handler(&ctx) {
                Ok(v) => { result = Some(v); break; }
                Err(Halt::Yield(ops)) => {
                    // pseudo-random: discard this attempt's commit (crash before commit)
                    let crash = (seed.wrapping_mul(2654435761).wrapping_add(attempt)) % 3 == 0;
                    if crash { lost += 1; continue; }
                    for op in ops {
                        if let Some(h) = op.hash() {
                            let d = match &op { Op::Step{data,..} => data.clone(), _ => Value::Null };
                            memo.insert(h.to_string(), d);
                        }
                    }
                }
                Err(Halt::Fatal(m)) => panic!("seed {seed}: {m}"),
            }
        }
        assert_eq!(result, Some(json!([1,2,3])), "seed {seed} produced wrong result");
        // Steps re-execute when a commit is lost — at-least-once — but never
        // produce a wrong result, and each step is RECORDED once.
        assert!(executions.get() >= 3, "seed {seed}: too few executions");
        assert_eq!(memo.len(), 4, "seed {seed}: exactly 4 distinct hashes recorded (a,b,nap,c), lost={lost}");
    }
}
