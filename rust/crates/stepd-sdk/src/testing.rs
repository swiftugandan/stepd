//! Unit-testing a workflow (gap E1, F-DX-1).
//!
//! The gap register rates "no way to unit-test a workflow" S4 and calls it
//! adoption-critical rather than a nicety, and it is right to: a developer who
//! cannot test a workflow without a database, a server and an HTTP round trip
//! will not write tests for it, and durable workflows are exactly the code where
//! an untested branch surfaces a month later in production.
//!
//! The harness drives the *same* [`run_pass`](stepd_sdk_core::run_pass) the real
//! server drives, against an in-memory model of the engine. Tests therefore
//! exercise production paths; a harness with its own replay logic would let a
//! workflow pass here and fail in production for reasons the test could not see.
//!
//! ```
//! use stepd_sdk::prelude::*;
//! use stepd_sdk::testing::harness;
//!
//! async fn charge_and_ship(ctx: &Ctx) -> StepResult<bool> {
//!     let _tx: String = ctx.step("charge", || async { Ok("ch_1".to_string()) }).await?;
//!     ctx.sleep("cooldown", chrono::Duration::days(1)).await?;
//!     let ok: Option<bool> = ctx.wait_event("approval", "order.approved").await?;
//!     Ok(ok.unwrap_or(false))
//! }
//!
//! # fn main() {
//! let mut t = harness(charge_and_ship);
//! t.send_event("order.approved", serde_json::json!(true));  // BEFORE the wait
//! let out = t.run_to_completion().expect("completes");
//!
//! assert_eq!(out, serde_json::json!(true));
//! t.assert_step_executed_once("charge");
//! # }
//! ```
//!
//! Note what that test proves: the event was delivered *before* the handler
//! reached its `wait_event`, and the run still resolved. That is the
//! early-signal semantics of protocol §7.6, and being able to write it as three
//! lines is the difference between a developer trusting the guarantee and hoping
//! for it.

use std::collections::HashMap;

use stepd_proto::{Op, RecordedStep, RunContext, StepOp, StepStatus};
use stepd_sdk_core::{run_pass, Handler, PassOutcome};
use uuid::Uuid;

/// An in-memory engine for one run.
pub struct Harness<H, T> {
    handler: H,
    /// Ties the harness to the handler's return type without storing one.
    _t: std::marker::PhantomData<fn() -> T>,
    run: RunContext,
    memo: HashMap<String, RecordedStep>,
    /// Events waiting to resolve a `wait_event`, oldest first.
    inbox: Vec<(String, serde_json::Value)>,
    /// Ops the handler emitted, per attempt.
    pub emitted: Vec<Vec<Op>>,
    /// Step ids executed, in order, across all attempts.
    pub executed: Vec<String>,
    /// Virtual clock offset, so a month-long sleep costs no wall-clock time.
    clock_offset: chrono::Duration,
    /// Steps whose result is forced, by step id.
    stubs: HashMap<String, serde_json::Value>,
    /// Steps forced to fail, by step id.
    failures: HashMap<String, String>,
    /// Waits forced to time out, by step id.
    timeouts: Vec<String>,
    /// Attempts made.
    pub attempts: usize,
}

/// Build a harness for a handler.
pub fn harness<H, T>(handler: H) -> Harness<H, T>
where
    H: for<'a> Handler<'a, T> + Copy,
    T: serde::Serialize,
{
    Harness {
        handler,
        _t: std::marker::PhantomData,
        run: RunContext {
            id: Uuid::now_v7(),
            function_id: "test-function".into(),
            namespace: "test".into(),
            key: None,
            started_at: chrono::Utc::now(),
            input: None,
            lineage_id: Uuid::now_v7(),
            chain_position: 0,
            cancelling: false,
        },
        memo: HashMap::new(),
        inbox: Vec::new(),
        emitted: Vec::new(),
        executed: Vec::new(),
        clock_offset: chrono::Duration::zero(),
        stubs: HashMap::new(),
        failures: HashMap::new(),
        timeouts: Vec::new(),
        attempts: 0,
    }
}

impl<H, T> Harness<H, T> {
    /// Set the function id, which participates in the step hash.
    pub fn function_id(mut self, id: &str) -> Self {
        self.run.function_id = id.into();
        self
    }

    /// Set the run's business key, visible to the handler.
    pub fn key(mut self, key: &str) -> Self {
        self.run.key = Some(key.into());
        self
    }

    /// Set the run input, for an invoke-triggered function.
    pub fn given_input(mut self, input: serde_json::Value) -> Self {
        self.run.input = Some(input);
        self
    }

    /// Run the handler in its cancellation path.
    pub fn cancelling(mut self) -> Self {
        self.run.cancelling = true;
        self
    }

    /// Force a step's result instead of running its closure.
    ///
    /// For steps that call something the test has no business calling. The step
    /// still counts as executed, because from the workflow's point of view it
    /// was: substituting a result must not quietly change what the test proves
    /// about execution counts.
    pub fn expect_step(&mut self, id: &str, returning: serde_json::Value) -> &mut Self {
        self.stubs.insert(id.into(), returning);
        self
    }

    /// Force a step to fail, so the handler's error path can be tested.
    pub fn fail_step(&mut self, id: &str, message: &str) -> &mut Self {
        self.failures.insert(id.into(), message.into());
        self
    }

    /// Force a wait to time out rather than resolve.
    pub fn time_out_wait(&mut self, id: &str) -> &mut Self {
        self.timeouts.push(id.into());
        self
    }

    /// Deliver an event.
    ///
    /// May be called *before* the handler reaches the corresponding
    /// `wait_event`; the harness buffers it exactly as the engine's durable
    /// inbox does, so the early-signal case is testable rather than theoretical.
    pub fn send_event(&mut self, event_type: &str, data: serde_json::Value) -> &mut Self {
        self.inbox.push((event_type.into(), data));
        self
    }

    /// Advance the virtual clock.
    ///
    /// Sleeps resolve instantly in the harness regardless; this exists so a
    /// handler that reads the clock sees time move, and so a test can express
    /// "a day passes" as intent rather than as a comment.
    pub fn advance_clock(&mut self, by: chrono::Duration) -> &mut Self {
        self.clock_offset += by;
        self
    }

    /// How many times a step id executed across every attempt.
    pub fn execution_count(&self, id: &str) -> usize {
        self.executed.iter().filter(|s| *s == id).count()
    }

    /// Assert a step ran exactly once across all attempts.
    ///
    /// The single most valuable assertion a workflow test can make: at-least-once
    /// execution is the contract, so "ran once" is a property of the memoisation
    /// working, not something the handler can arrange for itself.
    #[track_caller]
    pub fn assert_step_executed_once(&self, id: &str) {
        let n = self.execution_count(id);
        assert_eq!(
            n, 1,
            "step '{id}' executed {n} times across {} attempts; expected exactly once. \
             Executed steps in order: {:?}",
            self.attempts, self.executed
        );
    }

    /// Assert a step never ran.
    #[track_caller]
    pub fn assert_step_not_executed(&self, id: &str) {
        assert_eq!(
            self.execution_count(id),
            0,
            "step '{id}' ran, but the test expected the handler not to reach it"
        );
    }

    /// Assert the ops the handler yielded on one attempt (1-based).
    #[track_caller]
    pub fn assert_ops(&self, attempt: usize, expected: &[&str]) {
        let ops = self.emitted.get(attempt - 1).unwrap_or_else(|| {
            panic!(
                "no attempt {attempt}; only {} were made",
                self.emitted.len()
            )
        });
        let actual: Vec<&str> = ops
            .iter()
            .map(|o| match o {
                Op::Step { .. } => "step",
                Op::Sleep { .. } => "sleep",
                Op::WaitEvent { .. } => "wait_event",
                Op::Invoke { .. } => "invoke",
                Op::Signal { .. } => "signal",
                Op::ContinueAsNew { .. } => "continue_as_new",
                Op::Done { .. } => "done",
                Op::Error { .. } => "error",
            })
            .collect();
        assert_eq!(actual, expected, "attempt {attempt} emitted the wrong ops");
    }

    /// Every step id recorded in the journal, sorted.
    pub fn recorded_steps(&self) -> Vec<String> {
        let mut v: Vec<String> = self.memo.values().map(|s| s.id.clone()).collect();
        v.sort();
        v
    }
}

impl<H, T> Harness<H, T> {
    /// Apply one op to the in-memory journal, exactly as `commit_ops` would.
    fn commit(&mut self, op: &Op) {
        let (hash, id, kind, mut data, mut status) = match op {
            Op::Step { hash, id, data, .. } => (
                hash.clone(),
                id.clone(),
                StepOp::Step,
                data.clone(),
                StepStatus::Completed,
            ),
            Op::Sleep { hash, id, .. } => (
                hash.clone(),
                id.clone(),
                StepOp::Sleep,
                None,
                StepStatus::Completed,
            ),
            Op::WaitEvent {
                hash, id, event, ..
            } => {
                // FIFO over the buffered inbox, matching by type — the same rule
                // the engine applies, so a test cannot pass here by relying on an
                // ordering the engine does not guarantee.
                let hit = self.inbox.iter().position(|(t, _)| t == event);
                let (d, s) = if self.timeouts.contains(id) {
                    (None, StepStatus::TimedOut)
                } else if let Some(i) = hit {
                    (Some(self.inbox.remove(i).1), StepStatus::Completed)
                } else {
                    // No event and no forced timeout: the run would park forever.
                    // Recording it as pending makes `run_to_completion` stop and
                    // say so, rather than looping to the attempt cap.
                    (None, StepStatus::Pending)
                };
                (hash.clone(), id.clone(), StepOp::WaitEvent, d, s)
            }
            Op::Invoke { hash, id, .. } => (
                hash.clone(),
                id.clone(),
                StepOp::Invoke,
                Some(serde_json::Value::Null),
                StepStatus::Completed,
            ),
            Op::Signal { hash, id, .. } => (
                hash.clone(),
                id.clone(),
                StepOp::Signal,
                None,
                StepStatus::Completed,
            ),
            Op::ContinueAsNew { hash, id, .. } => (
                hash.clone(),
                id.clone(),
                StepOp::Step,
                None,
                StepStatus::Completed,
            ),
            Op::Done { .. } | Op::Error { .. } => return,
        };

        if let Some(v) = self.stubs.get(&id) {
            data = Some(v.clone());
        }
        let mut error = None;
        if let Some(msg) = self.failures.get(&id) {
            status = StepStatus::Failed;
            error = Some(stepd_proto::ErrorBody::msg(msg.clone()));
            data = None;
        }

        // First write wins, mirroring ON CONFLICT DO NOTHING: a step already
        // recorded is never overwritten.
        self.memo.entry(hash).or_insert(RecordedStep {
            id,
            op: kind,
            status,
            data,
            error,
        });
    }
}

/// Why a harness run stopped without completing.
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    /// The handler failed.
    #[error("the workflow failed: {code}: {message}")]
    Failed {
        /// Machine-readable code, if any.
        code: String,
        /// Human-readable message.
        message: String,
    },
    /// The run parked on a wait no event resolved.
    #[error(
        "the workflow is waiting for '{event}' and no matching event was sent. \
         Call send_event(\"{event}\", ..) before run_to_completion, or time_out_wait(\"{id}\")."
    )]
    Waiting {
        /// Step id of the wait.
        id: String,
        /// Event type it is waiting for.
        event: String,
    },
    /// The handler did not finish within the attempt budget.
    #[error(
        "the workflow did not complete within {0} attempts. Each attempt advances by one step, \
         so either the workflow has more steps than the budget or it is looping."
    )]
    Exhausted(usize),
}

impl<H, T> Harness<H, T>
where
    H: for<'a> Handler<'a, T> + Copy,
    T: serde::Serialize,
{
    /// Drive the workflow until it returns.
    pub fn run_to_completion(&mut self) -> Result<serde_json::Value, HarnessError> {
        self.run_bounded(1000)
    }

    /// Drive with an explicit attempt cap.
    pub fn run_bounded(&mut self, max_attempts: usize) -> Result<serde_json::Value, HarnessError> {
        for _ in 0..max_attempts {
            match self.step_once()? {
                Some(v) => return Ok(v),
                None => continue,
            }
        }
        Err(HarnessError::Exhausted(max_attempts))
    }

    /// Run exactly one attempt. `Ok(Some(v))` when the workflow returned.
    ///
    /// Exposed so a test can interleave engine events between attempts — deliver
    /// a signal after the second step, cancel after the third — which is how the
    /// interesting bugs in a workflow are actually reproduced.
    pub fn step_once(&mut self) -> Result<Option<serde_json::Value>, HarnessError> {
        self.attempts += 1;
        let mut run = self.run.clone();
        run.started_at += self.clock_offset;

        let settled = |m: &HashMap<String, RecordedStep>| -> usize {
            m.values()
                .filter(|r| r.status != StepStatus::Pending)
                .count()
        };
        let before = settled(&self.memo);

        // Ship only settled steps, exactly as the engine does. A handler that saw
        // a pending sleep or wait in its memo map would treat it as done and step
        // straight past a timer that has not fired — so a harness that shipped
        // them would be more permissive than production, and a workflow could
        // pass its tests and hang the first time it was deployed.
        let shipped: HashMap<String, RecordedStep> = self
            .memo
            .iter()
            .filter(|(_, r)| r.status != StepStatus::Pending)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // The harness counts attempts from 0 internally; the protocol counts
        // from 1, and a handler that reads `ctx.attempt()` must see the same
        // number in a test as in production or the test is testing a different
        // handler.
        let ctx = stepd_sdk_core::Ctx::new(run, shipped, self.attempts as u64)
            .with_attempt(self.attempts as i32 + 1);
        let outcome = block_on(run_pass(&ctx, self.handler));

        match outcome {
            PassOutcome::Done(v) => {
                self.emitted.push(vec![Op::Done {
                    data: Some(v.clone()),
                }]);
                Ok(Some(v))
            }
            PassOutcome::Error { error, .. } => Err(HarnessError::Failed {
                code: error.code.unwrap_or_default(),
                message: error.message,
            }),
            PassOutcome::Yield(ops) => {
                for op in &ops {
                    // A `step` op in the envelope means its closure ran during
                    // this attempt, which is what makes execution counting exact
                    // rather than inferred.
                    if let Op::Step { id, .. } = op {
                        self.executed.push(id.clone());
                    }
                    self.commit(op);
                }
                self.emitted.push(ops);

                // A wait that recorded nothing new means the run is parked. Say
                // so with the event name; "did not complete in 1000 attempts" is
                // a true statement that helps nobody.
                if settled(&self.memo) == before {
                    if let Some((_, rec)) = self
                        .memo
                        .iter()
                        .find(|(_, r)| r.status == StepStatus::Pending)
                    {
                        return Err(HarnessError::Waiting {
                            id: rec.id.clone(),
                            event: self
                                .emitted
                                .last()
                                .and_then(|ops| {
                                    ops.iter().find_map(|o| match o {
                                        Op::WaitEvent { event, .. } => Some(event.clone()),
                                        _ => None,
                                    })
                                })
                                .unwrap_or_else(|| "?".into()),
                        });
                    }
                }
                Ok(None)
            }
        }
    }
}

/// Block on a `!Send` future without pulling in a runtime.
///
/// The harness must not require `#[tokio::test]`: a workflow test should be an
/// ordinary `#[test]`, because the moment testing a workflow needs ceremony,
/// fewer workflows get tested.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let mut fut = Box::pin(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            // A handler that parks on real I/O would spin here. That is a
            // deliberate limit: the harness models the engine, not the network,
            // and a step that waits on an external service is exactly what
            // `expect_step` is for.
            Poll::Pending => std::hint::spin_loop(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stepd_sdk_core::{Ctx, StepResult};

    async fn simple(ctx: &Ctx) -> StepResult<i32> {
        let a: i32 = ctx.step("a", || async { Ok(1) }).await?;
        let b: i32 = ctx.step("b", || async { Ok(2) }).await?;
        Ok(a + b)
    }

    #[test]
    fn a_workflow_runs_to_completion_with_no_database_and_no_network() {
        let mut t = harness(simple);
        assert_eq!(t.run_to_completion().unwrap(), 3);
        t.assert_step_executed_once("a");
        t.assert_step_executed_once("b");
        assert_eq!(t.attempts, 3, "two steps plus the returning attempt");
    }

    async fn with_wait(ctx: &Ctx) -> StepResult<bool> {
        let _: i32 = ctx.step("charge", || async { Ok(1) }).await?;
        let ok: Option<bool> = ctx.wait_event("approval", "order.approved").await?;
        Ok(ok.unwrap_or(false))
    }

    #[test]
    fn an_event_sent_before_the_wait_still_resolves_it() {
        // The early-signal case (protocol §7.6), which the harness exists to make
        // testable by a developer rather than only by the engine's own suite.
        let mut t = harness(with_wait);
        t.send_event("order.approved", serde_json::json!(true));
        assert_eq!(t.run_to_completion().unwrap(), true);
    }

    #[test]
    fn an_event_sent_between_attempts_resolves_the_wait() {
        let mut t = harness(with_wait);
        t.step_once().unwrap(); // charge
        t.send_event("order.approved", serde_json::json!(true));
        assert_eq!(t.run_bounded(5).unwrap(), true);
    }

    #[test]
    fn a_wait_with_no_event_says_what_it_is_waiting_for() {
        let mut t = harness(with_wait);
        match t.run_bounded(5) {
            Err(HarnessError::Waiting { event, .. }) => assert_eq!(event, "order.approved"),
            other => panic!("expected a Waiting error naming the event, got {other:?}"),
        }
    }

    #[test]
    fn a_wait_can_be_forced_to_time_out() {
        let mut t = harness(with_wait);
        t.time_out_wait("approval");
        assert_eq!(t.run_to_completion().unwrap(), false);
    }

    #[test]
    fn a_step_result_can_be_stubbed() {
        let mut t = harness(simple);
        t.expect_step("a", serde_json::json!(100));
        assert_eq!(t.run_to_completion().unwrap(), 102);
    }

    async fn handles_failure(ctx: &Ctx) -> StepResult<&'static str> {
        match ctx.step::<i32, _, _>("risky", || async { Ok(1) }).await {
            Ok(_) => Ok("ok"),
            // Only a genuine failure is absorbed; a halt is re-raised, which is
            // what a correct handler must do.
            Err(e) if e.is_halt() => Err(e),
            Err(_) => Ok("compensated"),
        }
    }

    #[test]
    fn an_injected_failure_exercises_the_handlers_error_path() {
        let mut t = harness(handles_failure);
        t.fail_step("risky", "gateway down");
        assert_eq!(t.run_to_completion().unwrap(), "compensated");
    }

    async fn loops(ctx: &Ctx) -> StepResult<i32> {
        let mut total = 0;
        for _ in 0..3 {
            total += ctx.step::<i32, _, _>("item", || async { Ok(1) }).await?;
        }
        Ok(total)
    }

    #[test]
    fn a_loop_records_one_step_per_iteration_and_none_re_executes() {
        let mut t = harness(loops);
        assert_eq!(t.run_to_completion().unwrap(), 3);
        assert_eq!(
            t.execution_count("item"),
            3,
            "three iterations, three executions"
        );
        assert_eq!(t.recorded_steps().len(), 3, "three distinct occurrences");
    }

    #[test]
    fn ops_can_be_asserted_per_attempt() {
        let mut t = harness(with_wait);
        t.send_event("order.approved", serde_json::json!(true));
        t.run_to_completion().unwrap();
        t.assert_ops(1, &["step"]);
        t.assert_ops(2, &["wait_event"]);
        t.assert_ops(3, &["done"]);
    }

    async fn swallows(ctx: &Ctx) -> StepResult<i32> {
        let v = ctx
            .step::<i32, _, _>("s", || async { Ok(1) })
            .await
            .unwrap_or_default();
        Ok(v)
    }

    #[test]
    fn the_harness_catches_a_swallowed_halt_the_same_way_production_does() {
        // The harness must not be more forgiving than the server, or a workflow
        // passes its tests and corrupts a run the first time it is deployed.
        let mut t = harness(swallows);
        match t.run_to_completion() {
            Err(HarnessError::Failed { code, .. }) => assert_eq!(code, "swallowed_halt"),
            other => panic!("expected swallowed_halt, got {other:?}"),
        }
    }

    async fn parallel(ctx: &Ctx) -> StepResult<i32> {
        let a = ctx.step::<i32, _, _>("a", || async { Ok(1) });
        let b = ctx.step::<i32, _, _>("b", || async { Ok(2) });
        let (x, y) = ctx.join((a, b)).await?;
        Ok(x + y)
    }

    #[test]
    fn a_parallel_group_costs_one_attempt_not_one_per_member() {
        let mut t = harness(parallel);
        assert_eq!(t.run_to_completion().unwrap(), 3);
        assert_eq!(t.attempts, 2, "both members discovered in one envelope");
        t.assert_ops(1, &["step", "step"]);
    }

    #[test]
    fn a_workflow_that_never_finishes_is_reported_as_such() {
        async fn forever(ctx: &Ctx) -> StepResult<i32> {
            let mut n = 0;
            loop {
                n += ctx
                    .step::<i32, _, _>(&format!("s{n}"), || async { Ok(1) })
                    .await?;
            }
        }
        let mut t = harness(forever);
        assert!(matches!(t.run_bounded(5), Err(HarnessError::Exhausted(5))));
    }
}
