//! # stepd SDK core
//!
//! The two mechanisms that carry all the risk in the SDK, and nothing else:
//!
//! 1. **Occurrence assignment** — stable step hashes across replays, safe under
//!    concurrency (protocol §6.1).
//! 2. **Short-circuit control flow** — yielding an op from the middle of a
//!    handler (SDK design §3).
//!
//! Both are R1: a defect corrupts silently rather than erroring. That is why
//! they live in a crate with no async runtime, no I/O and no transport — the
//! logic can then be tested exhaustively without scheduling noise, and a failure
//! here is unambiguously a failure *here*.
//!
//! ## The finding this crate is built around
//!
//! Prototyping established a sharper rule than the protocol's prose. It is not
//! enough to assign occurrences "in program order"; **the claim must happen when
//! the step function is called, not when the future it returns is polled.**
//!
//! | | `join!` behaviour |
//! |---|---|
//! | Claim at poll time | Occurrence follows *scheduler* order. Two `ctx.step` calls joined together get different hashes on different attempts. Silent corruption. |
//! | Claim at call time | Occurrence follows *declaration* order. Poll order is irrelevant. Safe by construction. |
//!
//! So [`Ctx::step`] is **not** an `async fn`. It is a synchronous function that
//! claims the hash, consults the memo, and returns a future. This makes the
//! dangerous case impossible rather than merely detectable: `join!`, `select!`,
//! `FuturesUnordered` and hand-rolled polling all become safe, because every
//! hash was fixed before any of them ran.
//!
//! It also means a memoised step never constructs its future, so replaying forty
//! recorded steps does no work and allocates nothing for them.
//!
//! ## Defence in depth
//!
//! Eager claiming handles same-task concurrency. Two further guards cover the
//! rest, in decreasing order of how early they catch a mistake:
//!
//! 1. [`Ctx`] is `!Send + !Sync`, so `tokio::spawn(async move { ctx.step(..) })`
//!    fails to *compile*. The best possible outcome: caught before it runs.
//! 2. A pass-token check catches what the types cannot see — a `Ctx` smuggled
//!    through an `Rc`, a scoped thread, FFI. A claim carrying a foreign token is
//!    fatal, never a guessed hash.
//! 3. [`Ctx::join`] rejects a repeated step id inside one parallel group before
//!    any member runs.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

use serde::de::DeserializeOwned;
use serde::Serialize;
use stepd_proto::{step_hash, ErrorBody, Op, RecordedStep, RunContext, StepStatus};

pub mod join;
pub use join::JoinError;

// ---------------------------------------------------------------- errors

/// Why a handler stopped.
///
/// The `Halt` variant is control flow, not failure. It travels up through `?`
/// like an error because that is the only mechanism in Rust that is idiomatic,
/// zero-cost, identical in sync and async code, and composes with user error
/// handling. Panic-and-catch was rejected (unwinding across an async boundary is
/// fragile and `panic = "abort"` breaks it); generators were rejected as
/// ergonomically poor without stable syntax; a never-completing future was
/// rejected because it leaks the task and gives the runtime no way to return
/// the op.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// Real failure from user code.
    #[error("{message}")]
    Failed {
        /// Whether a retry could plausibly succeed.
        retryable: bool,
        /// Stable machine-readable code, if the caller supplied one.
        code: Option<String>,
        /// Human-readable description.
        message: String,
    },

    /// Not a failure: the handler reached work the server has not recorded, or
    /// broke a protocol rule.
    #[error("halt: {0}")]
    Halt(Halt),
}

impl StepError {
    /// A retryable failure.
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Failed {
            retryable: true,
            code: None,
            message: message.into(),
        }
    }

    /// A failure no retry can fix.
    pub fn fatal(message: impl Into<String>) -> Self {
        Self::Failed {
            retryable: false,
            code: None,
            message: message.into(),
        }
    }

    /// A **retryable** failure with a stable code, for signature-based poison
    /// detection.
    ///
    /// Retryable is the right default for a coded error — a code exists so that
    /// repeated identical failures can be recognised, and a failure that is
    /// never retried cannot repeat. Use [`StepError::fatal_coded`] when the
    /// failure is permanent.
    pub fn coded(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            retryable: true,
            code: Some(code.into()),
            message: message.into(),
        }
    }

    /// A failure with a stable code that no retry can fix.
    ///
    /// The combination the API was missing. `fatal` gave no code and `coded` was
    /// always retryable, so the one case where a machine-readable code is most
    /// useful — a permanent, classifiable failure like "this customer does not
    /// exist" — could not have one. Callers reached for `coded` because they
    /// wanted the code, and silently got a retry loop against a condition that
    /// would never change.
    pub fn fatal_coded(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            retryable: false,
            code: Some(code.into()),
            message: message.into(),
        }
    }

    /// Whether this is the short-circuit signal rather than a failure.
    pub fn is_halt(&self) -> bool {
        matches!(self, Self::Halt(_))
    }
}

/// The short-circuit signal.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Halt {
    /// The handler reached unmemoised work. The ops are held on the [`Ctx`]; the
    /// driver collects them with [`Ctx::take_pending`].
    ///
    /// They are not carried in this value because a parallel group must emit one
    /// envelope containing every member, and members complete at different times.
    /// Accumulating on the context and draining once is what makes that possible
    /// without the halt value having to be merged.
    #[error("yielded {0} op(s) to the server")]
    Yield(usize),

    /// A protocol rule was violated. Non-retryable: the SDK must never emit a
    /// guessed hash, because a wrong hash re-executes work that was already done.
    #[error("{0}")]
    Fatal(String),
}

/// Handler result.
pub type StepResult<T> = Result<T, StepError>;

// ---------------------------------------------------------------- pass token

/// Identifies one replay pass.
///
/// A concurrently scheduled task does not carry the current pass's token, which
/// is how a claim off the sequential path is detected (protocol §6.1 rule 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PassToken(u64);

// ---------------------------------------------------------------- context

struct Inner {
    function_id: String,
    run: RunContext,
    memo: HashMap<String, RecordedStep>,
    counters: HashMap<String, u32>,
    pending: Vec<Op>,
    emit: Vec<stepd_proto::Event>,
    id_sequence: Vec<String>,
    encountered: HashSet<String>,
    group: Option<Vec<String>>,
    pass: PassToken,
    /// Which attempt this is, from the server (protocol §4).
    ///
    /// A separate field from `pass` even though the executor currently seeds
    /// both from the same number. They are different things: the token detects a
    /// claim made from a stale pass, the attempt is information the protocol
    /// gives the handler. Sharing one field would mean a future change to how
    /// passes are numbered silently changed what a handler is told about its
    /// retries, which is the kind of coincidence this project keeps finding
    /// defects underneath.
    attempt: i32,
    halted: bool,
    logs: Vec<serde_json::Value>,
}

/// The handler's view of a run.
///
/// Deliberately `!Send` and `!Sync`. `tokio::spawn`-ing a task that claims a step
/// would tie occurrence assignment to scheduler order, so the type system
/// rejects it outright — the earliest and cheapest place to catch the mistake.
pub struct Ctx {
    inner: RefCell<Inner>,
    /// Makes `Ctx` `!Send`. `RefCell` alone only removes `Sync`.
    _not_send: PhantomData<Rc<()>>,
}

impl Ctx {
    /// Start a fresh replay pass.
    ///
    /// Counters reset on every attempt (protocol §6), which is what makes the
    /// hash a function of the handler's shape rather than of its history.
    pub fn new(run: RunContext, memo: HashMap<String, RecordedStep>, pass: u64) -> Self {
        Ctx {
            inner: RefCell::new(Inner {
                function_id: run.function_id.clone(),
                run,
                memo,
                counters: HashMap::new(),
                pending: Vec::new(),
                emit: Vec::new(),
                id_sequence: Vec::new(),
                encountered: HashSet::new(),
                group: None,
                pass: PassToken(pass),
                attempt: pass as i32,
                halted: false,
                logs: Vec::new(),
            }),
            _not_send: PhantomData,
        }
    }

    /// Set the attempt number the server reported (protocol §4).
    ///
    /// The executor calls this; `new` defaults it to the pass number so a
    /// hand-built `Ctx` in a test is not obliged to.
    pub fn with_attempt(self, attempt: i32) -> Self {
        self.inner.borrow_mut().attempt = attempt;
        self
    }

    /// Which attempt this is, counting from 1.
    ///
    /// Protocol §4 puts this on every `AttemptRequest` and it was reaching the
    /// SDK and going nowhere — a handler could not read it, so "log which retry
    /// this is", "escalate on the last attempt" and "behave differently the
    /// first time through" were all unavailable for no reason.
    ///
    /// Branching on it is not a way to make a handler deterministic-safe: the
    /// step journal is what makes replay safe, and a handler that takes a
    /// different *shape* on a later attempt changes its hash sequence and
    /// re-executes completed work (§6.1). Read it, log it, use it to decide
    /// whether to give up — do not use it to decide which steps exist.
    pub fn attempt(&self) -> i32 {
        self.inner.borrow().attempt
    }

    /// This pass's token, for combinators that need to prove they are on the
    /// sequential path.
    pub fn token(&self) -> PassToken {
        self.inner.borrow().pass
    }

    /// Run identity and input.
    pub fn run(&self) -> RunContext {
        self.inner.borrow().run.clone()
    }

    /// A key stable across re-execution of the same step, for provider-side
    /// idempotency.
    ///
    /// At-least-once execution is the contract (protocol §7.1), so a step whose
    /// effect is not naturally idempotent needs one of these or a reserve/confirm
    /// pair. Derived from the run id and the step hash, both of which survive a
    /// crash and a retry.
    pub fn idempotency_key(&self, step_hash: &str) -> String {
        format!("{}:{}", self.inner.borrow().run.id, step_hash)
    }

    /// Publish an event transactionally with the ops from this attempt.
    pub fn emit(&self, event: stepd_proto::Event) {
        self.inner.borrow_mut().emit.push(event);
    }

    /// Record a diagnostic line, returned to the server with the envelope.
    pub fn log(&self, level: &str, message: impl Into<String>) {
        self.inner.borrow_mut().logs.push(serde_json::json!({
            "level": level, "message": message.into(),
        }));
    }

    /// Claim the next occurrence for `step_id`, in program order.
    fn claim(&self, step_id: &str, token: PassToken) -> Result<String, StepError> {
        let mut inner = self.inner.borrow_mut();

        // Rule 4: a foreign token means a concurrent task tried to claim. Fail
        // rather than guess — a guessed hash silently re-executes recorded work.
        if token != inner.pass {
            return Err(StepError::Halt(Halt::Fatal(format!(
                "step '{step_id}' claimed an occurrence outside the sequential replay pass. \
                 Steps must be created on the handler's own task; use ctx.join(..) for \
                 parallelism, which assigns every hash up front (protocol §6.1)"
            ))));
        }

        // Rule 3: ids must be unique inside a parallel group. A loop that fans
        // out needs an explicit discriminator, not the occurrence counter.
        if let Some(group) = inner.group.as_mut() {
            if group.iter().any(|g| g == step_id) {
                return Err(StepError::Halt(Halt::Fatal(format!(
                    "ambiguous_step_id: '{step_id}' appears twice in one parallel group. \
                     Add a discriminator, e.g. format!(\"{step_id}-{{id}}\") (protocol §6.1)"
                ))));
            }
            group.push(step_id.to_string());
        }

        let n = inner.counters.entry(step_id.to_string()).or_insert(0);
        let occurrence = *n;
        *n += 1;
        let fid = inner.function_id.clone();
        inner.id_sequence.push(step_id.to_string());
        Ok(step_hash(&fid, step_id, occurrence))
    }

    /// Look up a recorded step, marking its hash as encountered this pass.
    fn lookup(&self, hash: &str) -> Option<RecordedStep> {
        let mut inner = self.inner.borrow_mut();
        let hit = inner.memo.get(hash).cloned();
        if hit.is_some() {
            inner.encountered.insert(hash.to_string());
        }
        hit
    }

    fn push_op(&self, op: Op) {
        let mut inner = self.inner.borrow_mut();
        inner.pending.push(op);
        inner.halted = true;
    }

    fn mark_halted(&self) {
        self.inner.borrow_mut().halted = true;
    }

    /// Ops discovered this pass, draining the buffer.
    pub fn take_pending(&self) -> Vec<Op> {
        std::mem::take(&mut self.inner.borrow_mut().pending)
    }

    /// Events to publish with this attempt's ops.
    pub fn take_emit(&self) -> Vec<stepd_proto::Event> {
        std::mem::take(&mut self.inner.borrow_mut().emit)
    }

    /// Diagnostic lines recorded this pass.
    pub fn take_logs(&self) -> Vec<serde_json::Value> {
        std::mem::take(&mut self.inner.borrow_mut().logs)
    }

    /// Whether a halt was raised during this pass.
    ///
    /// The driver checks this when the handler returns `Ok`: user code that
    /// swallows the halt (`let _ = ctx.step(..)`, `.unwrap_or_default()`, a bare
    /// `match` arm) would otherwise continue in an undefined state and report
    /// success for work that never ran. Turning that into a loud, specific error
    /// is the difference between a corrupted run and a failed one.
    pub fn halted(&self) -> bool {
        self.inner.borrow().halted
    }

    /// Hashes the server holds that this pass never encountered.
    ///
    /// A renamed or removed step. Renaming orphans the old result and
    /// re-executes the side effect, so this count is surfaced per attempt and
    /// answers "why did my step re-run?" (F-DX-2, F-DX-4).
    pub fn orphaned(&self) -> Vec<String> {
        let inner = self.inner.borrow();
        inner
            .memo
            .keys()
            .filter(|k| !inner.encountered.contains(*k))
            .cloned()
            .collect()
    }

    /// Step ids encountered this pass, in order.
    ///
    /// Strict mode compares this against the previous attempt to catch
    /// accidental non-determinism in user code (protocol §6.1 rule 5).
    pub fn id_sequence(&self) -> Vec<String> {
        self.inner.borrow().id_sequence.clone()
    }

    // ------------------------------------------------------------ step

    /// Record a unit of work.
    ///
    /// **Not** an `async fn`, and that is load-bearing: the hash is claimed when
    /// this function is *called*, so declaration order fixes it and poll order
    /// cannot change it. See the crate documentation.
    ///
    /// A memoised step never calls `f`, so its future is never constructed and
    /// replay costs nothing.
    pub fn step<'a, T, F, Fut>(&'a self, id: &'a str, f: F) -> StepFuture<'a, T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = StepResult<T>> + 'a,
    {
        // --- happens NOW, synchronously, in program order ---
        let hash = match self.claim(id, self.token()) {
            Ok(h) => h,
            Err(e) => return StepFuture::fatal(self, id, e),
        };

        match self.lookup(&hash) {
            Some(rec) => StepFuture::memoized(self, id, hash, rec),
            None => StepFuture::execute(self, id, hash, Box::pin(f())),
        }
    }

    // ------------------------------------------------------------ sleep

    /// Suspend for a duration. Consumes no app compute while parked.
    pub fn sleep<'a>(
        &'a self,
        id: &'a str,
        duration: chrono::Duration,
    ) -> impl Future<Output = StepResult<()>> + 'a {
        self.sleep_until(id, chrono::Utc::now() + duration)
    }

    /// Suspend until an absolute instant.
    pub fn sleep_until<'a>(
        &'a self,
        id: &'a str,
        until: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = StepResult<()>> + 'a {
        // Claimed eagerly, exactly like `step`, so a sleep inside a `join` is as
        // order-independent as anything else.
        let claimed = self.claim(id, self.token());
        let resolved = claimed.as_ref().ok().map(|h| (h.clone(), self.lookup(h)));

        async move {
            let hash = claimed?;
            match resolved.and_then(|(_, r)| r) {
                Some(_) => Ok(()),
                None => {
                    self.push_op(Op::Sleep {
                        id: id.to_string(),
                        hash,
                        until,
                    });
                    Err(StepError::Halt(Halt::Yield(1)))
                }
            }
        }
    }

    // ------------------------------------------------------------ wait_event

    /// Suspend until a matching event arrives.
    ///
    /// The matching window defaults to `run_start`, which is what closes the
    /// lost-signal race: an event sent before the handler reaches this line is
    /// still matched, because the server checks the run's durable inbox in the
    /// same transaction that registers the wait (protocol §7.6).
    pub fn wait_event<'a, T: DeserializeOwned>(
        &'a self,
        id: &'a str,
        event: &'a str,
    ) -> WaitBuilder<'a, T> {
        // Claimed HERE, when `wait_event` is called — not in `into_future`.
        //
        // Claiming at `.await` would make the occurrence follow await order
        // rather than declaration order, which is the exact hazard `ctx.step`
        // exists to remove. It is not hypothetical: `let a = ctx.wait_event(..);
        // let b = ctx.wait_event(..); b.await?; a.await?;` would swap their
        // hashes, and nothing would error.
        WaitBuilder {
            ctx: self,
            id,
            event,
            claimed: self.claim(id, self.token()),
            timeout: None,
            prompt: None,
            since: "run_start",
            _t: PhantomData,
        }
    }

    // ------------------------------------------------------------ invoke

    /// Call another function as a child run.
    pub fn invoke<'a, T: DeserializeOwned>(
        &'a self,
        id: &'a str,
        function: &'a str,
        input: serde_json::Value,
    ) -> InvokeBuilder<'a, T> {
        // Claimed here, for the same reason as `wait_event`: every op's
        // occurrence is fixed by where it appears in the source, never by when
        // it is awaited.
        InvokeBuilder {
            ctx: self,
            id,
            function,
            input,
            claimed: self.claim(id, self.token()),
            detach: false,
            _t: PhantomData,
        }
    }

    // ------------------------------------------------------------ signal

    /// Send an event to another run.
    pub fn signal<'a>(
        &'a self,
        id: &'a str,
        target: uuid::Uuid,
        event: stepd_proto::Event,
    ) -> impl Future<Output = StepResult<()>> + 'a {
        let claimed = self.claim(id, self.token());
        let resolved = claimed.as_ref().ok().map(|h| (h.clone(), self.lookup(h)));
        async move {
            let hash = claimed?;
            match resolved.and_then(|(_, r)| r) {
                Some(_) => Ok(()),
                None => {
                    self.push_op(Op::Signal {
                        id: id.to_string(),
                        hash,
                        target_run: target,
                        event,
                    });
                    Err(StepError::Halt(Halt::Yield(1)))
                }
            }
        }
    }

    // ------------------------------------------------------------ continue_as_new

    /// Close this run and start a successor with an empty journal.
    ///
    /// How an unbounded loop keeps run state finite. Always halts: there is no
    /// "already done" case, because the successor is a different run.
    pub fn continue_as_new(&self, id: &str, input: serde_json::Value) -> StepError {
        match self.claim(id, self.token()) {
            Ok(hash) => {
                self.push_op(Op::ContinueAsNew {
                    id: id.to_string(),
                    hash,
                    input: Some(input),
                });
                StepError::Halt(Halt::Yield(1))
            }
            Err(e) => e,
        }
    }

    /// Warn when a loop is growing run state without bound.
    ///
    /// Advisory, not enforced: the SDK cannot know whether the handler is about
    /// to finish. The server's hard limits are the actual containment; this is
    /// what gives a developer a chance to fix it before they hit one.
    pub fn should_continue_as_new(&self) -> bool {
        self.inner.borrow().memo.len() >= 500
    }
}

// ---------------------------------------------------------------- StepFuture

enum State<'a, T> {
    /// The server already holds this result. The closure was never called.
    Memoized(Option<StepResult<T>>),
    /// First execution.
    Execute {
        hash: String,
        fut: Pin<Box<dyn Future<Output = StepResult<T>> + 'a>>,
    },
    /// The claim itself failed; surface it on first poll.
    Fatal(Option<StepError>),
    /// Already resolved.
    Done,
}

/// The value of a step, once the server holds it.
///
/// Constructed eagerly by [`Ctx::step`] with its hash already fixed, which is
/// what makes polling order irrelevant.
pub struct StepFuture<'a, T> {
    ctx: &'a Ctx,
    id: &'a str,
    state: State<'a, T>,
}

impl<'a, T> StepFuture<'a, T> {
    /// The developer-supplied id, so [`Ctx::join`] can check group uniqueness
    /// before any member is polled.
    pub fn step_id(&self) -> &str {
        self.id
    }

    fn fatal(ctx: &'a Ctx, id: &'a str, e: StepError) -> Self {
        Self {
            ctx,
            id,
            state: State::Fatal(Some(e)),
        }
    }
}

impl<'a, T: DeserializeOwned> StepFuture<'a, T> {
    fn memoized(ctx: &'a Ctx, id: &'a str, _hash: String, rec: RecordedStep) -> Self {
        let value = match rec.status {
            StepStatus::Completed => {
                let raw = rec.data.unwrap_or(serde_json::Value::Null);
                // A decode failure on a memoised value is a non-retryable error
                // naming the step, never a silent re-execution: re-running the
                // closure would repeat a side effect that already happened
                // (SDK design §10 open question 2).
                serde_json::from_value::<T>(raw).map_err(|e| StepError::Failed {
                    retryable: false,
                    code: Some("memo_decode_failed".into()),
                    message: format!(
                        "step '{id}' has a recorded result that no longer decodes into the \
                         handler's type ({e}). The step will NOT be re-executed: its side \
                         effect already happened. Change the type back, or give the step a \
                         new id and accept the re-execution deliberately."
                    ),
                })
            }
            StepStatus::Failed => Err(StepError::Failed {
                retryable: false,
                code: rec.error.as_ref().and_then(|e| e.code.clone()),
                message: rec
                    .error
                    .as_ref()
                    .map(|e| e.message.clone())
                    .unwrap_or_else(|| format!("step '{id}' failed")),
            }),
            StepStatus::TimedOut => Err(StepError::Failed {
                retryable: false,
                code: Some("timed_out".into()),
                message: format!("step '{id}' timed out"),
            }),
            StepStatus::Cancelled => Err(StepError::Failed {
                retryable: false,
                code: Some("cancelled".into()),
                message: format!("step '{id}' was cancelled"),
            }),
            // Pending rows are never shipped, and `unknown` means an attempt was
            // abandoned mid-step; either way the right move is to re-execute,
            // which the server arranges by retrying the attempt.
            StepStatus::Pending | StepStatus::Unknown => Err(StepError::Halt(Halt::Yield(0))),
        };
        Self {
            ctx,
            id,
            state: State::Memoized(Some(value)),
        }
    }

    fn execute(
        ctx: &'a Ctx,
        id: &'a str,
        hash: String,
        fut: Pin<Box<dyn Future<Output = StepResult<T>> + 'a>>,
    ) -> Self {
        Self {
            ctx,
            id,
            state: State::Execute { hash, fut },
        }
    }
}

// `T: Unpin` is not a real restriction — only a type containing `PhantomPinned`
// is `!Unpin`, and a step result is by definition owned, serialisable data. The
// bound buys a `get_mut` in `poll`, which keeps the state machine readable
// instead of hiding it behind projection machinery.
impl<'a, T: Serialize + DeserializeOwned + Unpin> Future for StepFuture<'a, T> {
    type Output = StepResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // No field is structurally pinned: the inner future is already
        // `Pin<Box<..>>` and therefore address-stable on its own.
        let this = self.get_mut();
        match &mut this.state {
            State::Memoized(v) => {
                let out = v.take().expect("StepFuture polled after completion");
                this.state = State::Done;
                Poll::Ready(out)
            }
            State::Fatal(e) => {
                let out = e.take().expect("StepFuture polled after completion");
                this.state = State::Done;
                Poll::Ready(Err(out))
            }
            State::Execute { hash, fut } => match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(value)) => {
                    let data = match serde_json::to_value(&value) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            this.state = State::Done;
                            return Poll::Ready(Err(StepError::fatal(format!(
                                "step '{}' produced a result that cannot be serialised: {e}",
                                this.id
                            ))));
                        }
                    };
                    this.ctx.push_op(Op::Step {
                        id: this.id.to_string(),
                        hash: hash.clone(),
                        data,
                        meta: None,
                        error: None,
                    });
                    this.state = State::Done;
                    // The work is done and recorded. The handler stops here so
                    // the server can commit it; the next attempt replays past
                    // this point from the memo.
                    Poll::Ready(Err(StepError::Halt(Halt::Yield(1))))
                }
                Poll::Ready(Err(e)) => {
                    // A failure is not a halt, but it still ends the pass, and
                    // the driver needs to know a step was reached so a swallowed
                    // failure cannot masquerade as a clean return.
                    this.ctx.mark_halted();

                    // A *terminal* failure is an outcome, and outcomes are
                    // recorded. Without this the journal shows nothing at all
                    // for a step whose body raised — and in a parallel group the
                    // whole envelope went with it, taking the siblings' results
                    // that had already been produced (ADR-023).
                    //
                    // A retryable failure is deliberately not recorded. Retrying
                    // means re-executing, and a recorded failure is memoised:
                    // the next attempt would replay the error instead of the
                    // body and the retry would never happen.
                    if let StepError::Failed {
                        retryable: false,
                        code,
                        message,
                    } = &e
                    {
                        this.ctx.push_op(Op::Step {
                            id: this.id.to_string(),
                            hash: hash.clone(),
                            data: None,
                            meta: None,
                            error: Some(ErrorBody {
                                code: code.clone(),
                                message: message.clone(),
                                stack: None,
                                attempts: None,
                            }),
                        });
                    }
                    this.state = State::Done;
                    Poll::Ready(Err(e))
                }
            },
            State::Done => panic!("StepFuture polled after completion"),
        }
    }
}

// ---------------------------------------------------------------- wait builder

/// Configures a `wait_event` before it is awaited.
pub struct WaitBuilder<'a, T> {
    ctx: &'a Ctx,
    id: &'a str,
    event: &'a str,
    /// The hash, claimed when the builder was constructed.
    claimed: Result<String, StepError>,
    timeout: Option<chrono::Duration>,
    prompt: Option<serde_json::Value>,
    since: &'a str,
    _t: PhantomData<T>,
}

impl<'a, T: DeserializeOwned> WaitBuilder<'a, T> {
    /// Give up after this long. The result is then `None`.
    pub fn timeout(mut self, d: chrono::Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// Surface the pending decision in the console. Presentational only.
    pub fn prompt(mut self, title: &str, detail: serde_json::Value) -> Self {
        self.prompt = Some(serde_json::json!({ "title": title, "detail": detail }));
        self
    }

    /// Narrow the matching window to events received after this wait commits.
    ///
    /// Opting out of the safe default. It reopens the lost-signal race for this
    /// wait by design, and is only correct when an earlier event of the same type
    /// genuinely must not match.
    pub fn since_registration(mut self) -> Self {
        self.since = "registration";
        self
    }
}

impl<'a, T: DeserializeOwned> std::future::IntoFuture for WaitBuilder<'a, T> {
    type Output = StepResult<Option<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        // The hash was fixed at construction; only the memo lookup happens here.
        let claimed = self.claimed;
        let resolved = claimed
            .as_ref()
            .ok()
            .map(|h| (h.clone(), self.ctx.lookup(h)));

        Box::pin(async move {
            let hash = claimed?;
            match resolved.and_then(|(_, r)| r) {
                Some(rec) => match rec.status {
                    StepStatus::TimedOut => Ok(None),
                    // A pending wait is one the server has recorded but not
                    // resolved. The engine never ships pending rows, so seeing
                    // one means a store or harness did; treating it as "resolved
                    // with no data" would silently skip the wait entirely, which
                    // is the failure mode the whole inbox mechanism exists to
                    // prevent. Halt instead.
                    StepStatus::Pending => Err(StepError::Halt(Halt::Yield(0))),
                    _ => match rec.data {
                        Some(serde_json::Value::Null) | None => Ok(None),
                        Some(v) => serde_json::from_value(v).map(Some).map_err(|e| {
                            StepError::fatal(format!(
                                "wait '{}' received an event that does not decode into the \
                                 handler's type: {e}",
                                self.id
                            ))
                        }),
                    },
                },
                None => {
                    self.ctx.push_op(Op::WaitEvent {
                        id: self.id.to_string(),
                        hash,
                        event: self.event.to_string(),
                        since: self.since.to_string(),
                        timeout_at: self.timeout.map(|d| chrono::Utc::now() + d),
                        prompt: self.prompt,
                    });
                    Err(StepError::Halt(Halt::Yield(1)))
                }
            }
        })
    }
}

// ---------------------------------------------------------------- invoke builder

/// Configures an `invoke` before it is awaited.
pub struct InvokeBuilder<'a, T> {
    ctx: &'a Ctx,
    id: &'a str,
    function: &'a str,
    input: serde_json::Value,
    /// The hash, claimed when the builder was constructed.
    claimed: Result<String, StepError>,
    detach: bool,
    _t: PhantomData<T>,
}

impl<'a, T: DeserializeOwned> InvokeBuilder<'a, T> {
    /// Fire and forget: the child's lifecycle becomes independent of this run's,
    /// in both directions, from the moment it is created.
    pub fn detach(mut self) -> Self {
        self.detach = true;
        self
    }
}

impl<'a, T: DeserializeOwned> std::future::IntoFuture for InvokeBuilder<'a, T> {
    type Output = StepResult<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        // The hash was fixed at construction; only the memo lookup happens here.
        let claimed = self.claimed;
        let resolved = claimed
            .as_ref()
            .ok()
            .map(|h| (h.clone(), self.ctx.lookup(h)));

        Box::pin(async move {
            let hash = claimed?;
            match resolved.and_then(|(_, r)| r) {
                Some(rec) => match rec.status {
                    StepStatus::Completed => serde_json::from_value(
                        rec.data.unwrap_or(serde_json::Value::Null),
                    )
                    .map_err(|e| {
                        StepError::fatal(format!(
                            "invoke '{}' returned a value that does not decode into the \
                                 handler's type: {e}",
                            self.id
                        ))
                    }),
                    _ => Err(StepError::Failed {
                        retryable: false,
                        code: rec.error.as_ref().and_then(|e| e.code.clone()),
                        message: rec
                            .error
                            .as_ref()
                            .map(|e| e.message.clone())
                            .unwrap_or_else(|| {
                                format!("child run for '{}' did not complete", self.id)
                            }),
                    }),
                },
                None => {
                    self.ctx.push_op(Op::Invoke {
                        id: self.id.to_string(),
                        hash,
                        function: self.function.to_string(),
                        input: Some(self.input),
                        detach: self.detach,
                    });
                    Err(StepError::Halt(Halt::Yield(1)))
                }
            }
        })
    }
}

// ---------------------------------------------------------------- outcome

/// What one replay pass produced.
#[derive(Debug)]
pub enum PassOutcome {
    /// The handler returned. Emit `done`.
    Done(serde_json::Value),
    /// The handler reached unmemoised work. Emit these ops.
    Yield(Vec<Op>),
    /// The handler raised.
    Error {
        /// Work recorded before the failure, to be committed with it.
        ///
        /// A parallel group whose members mostly succeeded is the ordinary
        /// source: those results exist and must reach the journal in the same
        /// envelope as the error, or the run fails having forgotten work it
        /// did. Only ever non-empty for a non-retryable failure — see
        /// [`run_pass`] (§5.2.2, ADR-023).
        ops: Vec<Op>,
        /// Whether a retry could succeed.
        retryable: bool,
        /// Error body for the server.
        error: ErrorBody,
    },
}

/// A workflow handler.
///
/// Exists because `F: Fn(&Ctx) -> Fut` cannot express "the returned future
/// borrows the context it was given" — the async block captures `&'a Ctx`, so
/// its lifetime is tied to the argument, and a plain `Fn` bound has no way to
/// say so. The lifetime-parameterised trait plus a `for<'a>` bound at the use
/// site does say it, and keeps handlers as ordinary async closures rather than
/// forcing every user to `Box::pin`.
pub trait Handler<'a, T> {
    /// The future the handler returns, borrowing the context.
    type Future: Future<Output = StepResult<T>> + 'a;

    /// Invoke the handler for one replay pass.
    fn call(&self, ctx: &'a Ctx) -> Self::Future;
}

impl<'a, F, Fut, T> Handler<'a, T> for F
where
    F: Fn(&'a Ctx) -> Fut,
    Fut: Future<Output = StepResult<T>> + 'a,
{
    type Future = Fut;
    fn call(&self, ctx: &'a Ctx) -> Fut {
        self(ctx)
    }
}

/// A boxed handler future borrowing the context.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = StepResult<T>> + 'a>>;

/// Constrain a closure to the higher-ranked handler signature.
///
/// An `async fn` satisfies `for<'a> Handler<'a, T>` on its own, because its
/// desugaring is already generic over the lifetime of its argument. A *closure*
/// does not: the compiler infers one concrete return type for it, and then
/// cannot prove the borrow of `Ctx` outlives it. Passing the closure through a
/// function whose bound is written with `for<'a>` is what tells the inference
/// engine to look for the higher-ranked signature instead.
///
/// Prefer a plain `async fn` for a real workflow. This exists for closures that
/// need to capture something from their environment, and for tests.
pub fn workflow<F, T>(f: F) -> F
where
    F: for<'a> Fn(&'a Ctx) -> BoxFut<'a, T>,
{
    f
}

/// Write a handler as a closure without the boxing ceremony.
///
/// ```ignore
/// let handler = wf!(|ctx| {
///     let tx: String = ctx.step("charge", || async { gateway.charge().await }).await?;
///     Ok(Receipt { tx })
/// });
/// ```
#[macro_export]
macro_rules! wf {
    (|$ctx:ident| $body:block) => {
        $crate::workflow(|$ctx: &$crate::Ctx| {
            ::std::boxed::Box::pin(async move $body) as $crate::BoxFut<'_, _>
        })
    };
}

/// Run one replay pass and convert its outcome into an op envelope.
///
/// The single place that decides what a handler's return value means, so the
/// axum adapter, the Lambda adapter and the test harness cannot disagree about
/// it — and so the swallowed-halt check exists exactly once.
pub async fn run_pass<H, T>(ctx: &Ctx, handler: H) -> PassOutcome
where
    H: for<'a> Handler<'a, T>,
    T: Serialize,
{
    let returned = handler.call(ctx).await;

    // Drained once, after the pass, so every arm below has to decide what to do
    // with the work the pass recorded rather than silently leaving it on the
    // context. The defect this replaces was exactly that omission: only the
    // yield arm ever looked, so a pass that recorded three steps and then failed
    // committed none of them (ADR-023).
    let recorded = ctx.take_pending();

    match returned {
        Ok(v) => {
            if ctx.halted() {
                // User code absorbed the halt — `let _ = ctx.step(..)`,
                // `.unwrap_or_default()`, a catch-all `match` arm — and then
                // carried on and returned success. Everything after the absorbed
                // step ran against a state that does not exist. Fail loudly and
                // name the mechanism, rather than committing a `done` for a run
                // whose middle never happened.
                return PassOutcome::Error {
                    ops: recorded,
                    retryable: false,
                    error: ErrorBody::coded(
                        "swallowed_halt",
                        "the handler returned Ok after a step yielded. A step result was \
                         discarded instead of propagated — look for `let _ = ctx.step(..)`, \
                         `.ok()`, `.unwrap_or_default()`, or a match arm that absorbs the \
                         error. Propagate step results with `?` (SDK design §3).",
                    ),
                };
            }
            match serde_json::to_value(v) {
                Ok(v) => PassOutcome::Done(v),
                Err(e) => PassOutcome::Error {
                    ops: recorded,
                    retryable: false,
                    error: ErrorBody::coded(
                        "output_not_serialisable",
                        format!("the handler's return value could not be serialised: {e}"),
                    ),
                },
            }
        }
        Err(StepError::Halt(Halt::Yield(_))) => PassOutcome::Yield(recorded),
        Err(StepError::Halt(Halt::Fatal(msg))) => PassOutcome::Error {
            ops: recorded,
            retryable: false,
            error: ErrorBody::coded("protocol_violation", msg),
        },
        Err(StepError::Failed {
            retryable,
            code,
            message,
        }) => {
            // A retryable failure cannot travel with recorded ops: retrying is
            // the dispatcher's decision and committing is the store's, and one
            // envelope cannot ask for both (§5.2.2). So the recorded work goes
            // now, alone, and the failure is raised again on the next attempt —
            // where the siblings come from the memo, nothing new is recorded,
            // and the error is emitted on its own.
            //
            // This terminates: each pass records strictly less than the last,
            // because what it recorded is memoised.
            if retryable && !recorded.is_empty() {
                return PassOutcome::Yield(recorded);
            }
            PassOutcome::Error {
                ops: recorded,
                retryable,
                error: ErrorBody {
                    code,
                    message,
                    stack: None,
                    attempts: None,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests;
