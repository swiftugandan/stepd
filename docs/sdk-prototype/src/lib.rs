//! Prototype of the two subtlest mechanisms in the stepd Rust SDK:
//!   1. short-circuit control flow — yielding an op from the middle of a handler
//!   2. occurrence assignment — stable step hashes across replays, safe under concurrency
//!
//! No async runtime here: the mechanisms are independent of it, and keeping this
//! synchronous makes the properties testable without scheduling noise.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::HashMap;

// ---------------------------------------------------------------- hashing

/// hash = sha256(function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ occurrence)[0..8], hex
pub fn step_hash(function_id: &str, step_id: &str, occurrence: u32) -> String {
    let mut h = Sha256::new();
    h.update(function_id.as_bytes());
    h.update([0x1F]);
    h.update(step_id.as_bytes());
    h.update([0x1F]);
    h.update(occurrence.to_string().as_bytes());
    hex::encode(&h.finalize()[..8])
}

// ---------------------------------------------------------------- ops

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Step { id: String, hash: String, data: Value },
    Sleep { id: String, hash: String, duration: String },
    WaitEvent { id: String, hash: String, event: String, since: String },
    Done { data: Value },
    Error { message: String, retryable: bool },
}

impl Op {
    pub fn hash(&self) -> Option<&str> {
        match self {
            Op::Step { hash, .. } | Op::Sleep { hash, .. } | Op::WaitEvent { hash, .. } => Some(hash),
            _ => None,
        }
    }
}

/// Why a handler stopped early. This is the short-circuit signal: it travels up
/// through `?` like an error, but it is control flow, not failure.
#[derive(Debug, Clone)]
pub enum Halt {
    /// The handler reached work the server has not recorded. Yield these ops.
    Yield(Vec<Op>),
    /// A rule was violated (see §6.1). Non-retryable — never guess a hash.
    Fatal(String),
}

pub type StepResult<T> = Result<T, Halt>;

// ---------------------------------------------------------------- context

/// A replay-pass token. Concurrent tasks do not carry one, which is how the SDK
/// detects an occurrence being claimed off the sequential path (protocol §6.1 rule 4).
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct PassToken(u64);

struct Inner {
    function_id: String,
    /// Step results the server has already recorded, keyed by hash.
    memo: HashMap<String, Value>,
    /// Per-step-id occurrence counters for this pass.
    counters: HashMap<String, u32>,
    /// Ops discovered during this pass, to be returned to the server.
    pending: Vec<Op>,
    /// Ids seen in this pass, in order — used by strict mode to detect drift.
    id_sequence: Vec<String>,
    /// Hashes present in the memo but never encountered this pass.
    seen: Vec<String>,
    /// Set while inside a parallel group: ids must be unique within it.
    group: Option<Vec<String>>,
    pass: PassToken,
}

pub struct Ctx {
    inner: RefCell<Inner>,
}

impl Ctx {
    pub fn new(function_id: &str, memo: HashMap<String, Value>, pass: u64) -> Self {
        Ctx {
            inner: RefCell::new(Inner {
                function_id: function_id.to_string(),
                memo,
                counters: HashMap::new(),
                pending: Vec::new(),
                id_sequence: Vec::new(),
                seen: Vec::new(),
                group: None,
                pass: PassToken(pass),
            }),
        }
    }

    /// Assign the next occurrence for `step_id`, in program order.
    /// Returns Fatal if the id repeats inside a parallel group (§6.1 rule 3).
    fn claim(&self, step_id: &str, token: PassToken) -> Result<String, Halt> {
        let mut inner = self.inner.borrow_mut();

        // Rule 4: an occurrence claimed with a token that is not this pass's token
        // means a concurrent task tried to claim it. Fail rather than guess.
        if token != inner.pass {
            return Err(Halt::Fatal(format!(
                "step '{step_id}' claimed an occurrence outside the sequential replay pass; \
                 use ctx.parallel(...) to assign hashes up front (protocol §6.1)"
            )));
        }

        // Rule 3: unique ids inside a parallel group.
        if let Some(group) = inner.group.as_mut() {
            if group.iter().any(|g| g == step_id) {
                return Err(Halt::Fatal(format!(
                    "ambiguous_step_id: '{step_id}' used twice in one parallel group; \
                     add a discriminator (protocol §6.1)"
                )));
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

    fn lookup(&self, hash: &str) -> Option<Value> {
        let mut inner = self.inner.borrow_mut();
        if let Some(v) = inner.memo.get(hash).cloned() {
            inner.seen.push(hash.to_string());
            Some(v)
        } else {
            None
        }
    }

    pub fn token(&self) -> PassToken {
        self.inner.borrow().pass
    }

    /// Run a step. Returns the memoized value, or halts the handler with a Step op.
    pub fn run<F>(&self, step_id: &str, f: F) -> StepResult<Value>
    where
        F: FnOnce() -> Value,
    {
        self.run_with(step_id, self.token(), f)
    }

    /// As `run`, but with an explicit pass token — used by `parallel` to prove the
    /// claim happened on the sequential path.
    pub fn run_with<F>(&self, step_id: &str, token: PassToken, f: F) -> StepResult<Value>
    where
        F: FnOnce() -> Value,
    {
        let hash = self.claim(step_id, token)?;
        if let Some(v) = self.lookup(&hash) {
            return Ok(v); // memoized: the closure never runs
        }
        let data = f(); // first execution
        let op = Op::Step { id: step_id.into(), hash, data };
        self.inner.borrow_mut().pending.push(op);
        Err(Halt::Yield(self.take_pending()))
    }

    pub fn sleep(&self, step_id: &str, duration: &str) -> StepResult<()> {
        let hash = self.claim(step_id, self.token())?;
        if self.lookup(&hash).is_some() {
            return Ok(());
        }
        let op = Op::Sleep { id: step_id.into(), hash, duration: duration.into() };
        self.inner.borrow_mut().pending.push(op);
        Err(Halt::Yield(self.take_pending()))
    }

    pub fn wait_event(&self, step_id: &str, event: &str) -> StepResult<Value> {
        let hash = self.claim(step_id, self.token())?;
        if let Some(v) = self.lookup(&hash) {
            return Ok(v);
        }
        let op = Op::WaitEvent {
            id: step_id.into(),
            hash,
            event: event.into(),
            since: "run_start".into(), // default that closes the lost-signal race
        };
        self.inner.borrow_mut().pending.push(op);
        Err(Halt::Yield(self.take_pending()))
    }

    /// Structured parallelism (protocol §6.1 rule 2).
    ///
    /// Hashes for every member are claimed here, sequentially, in declaration order,
    /// BEFORE any work runs. Only unmemoized members execute, and they all yield in
    /// one envelope. This is what makes hashes stable regardless of completion order.
    pub fn parallel<T>(&self, members: Vec<(&str, Box<dyn FnOnce() -> Value>)>) -> StepResult<Vec<Value>>
    where
        T: Sized,
    {
        let token = self.token();
        self.inner.borrow_mut().group = Some(Vec::new());

        // Phase 1: claim every hash, in order, on the sequential path.
        let mut claimed = Vec::new();
        for (id, f) in members {
            match self.claim(id, token) {
                Ok(h) => claimed.push((id.to_string(), h, f)),
                Err(e) => {
                    self.inner.borrow_mut().group = None;
                    return Err(e);
                }
            }
        }
        self.inner.borrow_mut().group = None;

        // Phase 2: resolve. Memoized members return their value; the rest execute.
        let mut values = Vec::new();
        let mut new_ops = Vec::new();
        for (id, hash, f) in claimed {
            if let Some(v) = self.lookup(&hash) {
                values.push(v);
            } else {
                let data = f();
                new_ops.push(Op::Step { id, hash, data });
            }
        }

        if new_ops.is_empty() {
            Ok(values) // whole group memoized: continue past it
        } else {
            self.inner.borrow_mut().pending.extend(new_ops);
            Err(Halt::Yield(self.take_pending()))
        }
    }

    fn take_pending(&self) -> Vec<Op> {
        std::mem::take(&mut self.inner.borrow_mut().pending)
    }

    /// Hashes recorded by the server that this pass never encountered — a renamed or
    /// removed step (protocol §6, F-DX-4).
    pub fn orphaned(&self) -> Vec<String> {
        let inner = self.inner.borrow();
        inner
            .memo
            .keys()
            .filter(|k| !inner.seen.contains(k))
            .cloned()
            .collect()
    }

    pub fn id_sequence(&self) -> Vec<String> {
        self.inner.borrow().id_sequence.clone()
    }
}

// ---------------------------------------------------------------- driver

/// Simulates the server: repeatedly invokes the handler, commits the ops it yields
/// into the memo, and re-invokes, until the handler returns.
pub fn drive<H>(function_id: &str, handler: H, max_attempts: usize) -> Result<(Value, usize, Vec<Vec<String>>), String>
where
    H: Fn(&Ctx) -> StepResult<Value>,
{
    let mut memo: HashMap<String, Value> = HashMap::new();
    let mut sequences = Vec::new();

    for attempt in 1..=max_attempts {
        let ctx = Ctx::new(function_id, memo.clone(), attempt as u64);
        match handler(&ctx) {
            Ok(v) => {
                sequences.push(ctx.id_sequence());
                return Ok((v, attempt, sequences));
            }
            Err(Halt::Yield(ops)) => {
                sequences.push(ctx.id_sequence());
                for op in ops {
                    if let Some(h) = op.hash() {
                        let data = match &op {
                            Op::Step { data, .. } => data.clone(),
                            Op::Sleep { .. } => Value::Null,
                            Op::WaitEvent { .. } => serde_json::json!({"approved": true}),
                            _ => Value::Null,
                        };
                        memo.insert(h.to_string(), data);
                    }
                }
            }
            Err(Halt::Fatal(msg)) => return Err(msg),
        }
    }
    Err(format!("did not complete within {max_attempts} attempts"))
}
