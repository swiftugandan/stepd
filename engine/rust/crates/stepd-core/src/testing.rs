//! In-memory implementations of every engine interface.
//!
//! These exist so the dispatch loop can be tested exhaustively with no database
//! and no network — which is the whole payoff for defining the traits in the
//! first place. They are behind the `testing` feature and are also useful to
//! anyone building an alternative store, as an executable specification of what
//! the engine expects.

use crate::traits::*;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use stepd_proto::*;
use uuid::Uuid;

/// A run held in memory.
#[derive(Debug, Clone)]
pub struct MemRun {
    /// Run identity.
    pub id: RunId,
    /// Namespace.
    pub namespace: String,
    /// Function.
    pub function_id: String,
    /// Business key.
    pub key: Option<String>,
    /// Lifecycle state.
    pub status: RunStatus,
    /// Fencing token.
    pub fence: i64,
    /// Attempts made.
    pub attempt: i32,
    /// Recorded steps by hash.
    pub steps: HashMap<String, RecordedStep>,
    /// Undelivered inbox entries: (event type, payload, consumed-by hash).
    pub inbox: Vec<(String, serde_json::Value, Option<String>)>,
    /// Parked waits: (step hash, event type).
    pub waits: Vec<(String, String)>,
    /// When the run becomes claimable.
    pub available_at: DateTime<Utc>,
    /// Whether it is queued at all.
    pub queued: bool,
    /// Parent run, for children.
    pub parent: Option<RunId>,
    /// Whether the child outlives its parent.
    pub detached: bool,
    /// Lineage across `continue_as_new`.
    pub lineage: Uuid,
    /// Position in the lineage.
    pub chain_position: i32,
    /// Final output.
    pub output: Option<serde_json::Value>,
    /// Last error.
    pub error: Option<ErrorBody>,
    /// Failures in a row carrying the same signature.
    pub consecutive_failures: i32,
    /// Signature of the last failure.
    pub error_signature: Option<String>,
    /// When the run started.
    pub started_at: DateTime<Utc>,
}

/// A scripted app response, named so the transport's field stays readable.
pub type ScriptedHandler =
    Box<dyn Fn(&Attempt) -> std::result::Result<AttemptResponse, String> + Send + Sync>;

/// In-memory [`StateStore`], [`Queue`] and [`EventLog`].
#[derive(Default)]
pub struct MemStore {
    runs: Mutex<HashMap<RunId, MemRun>>,
    outbox: Mutex<Vec<Event>>,
    events: Mutex<Vec<(String, Event)>>,
    idem: Mutex<HashMap<(String, String), Uuid>>,
    /// Counts commits applied, for assertions.
    pub commits: AtomicU64,
}

impl MemStore {
    /// Empty store.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Snapshot a run.
    pub fn get(&self, id: RunId) -> Option<MemRun> {
        self.runs.lock().unwrap().get(&id).cloned()
    }

    /// All runs, for assertions.
    pub fn all(&self) -> Vec<MemRun> {
        self.runs.lock().unwrap().values().cloned().collect()
    }

    /// Events published from the outbox.
    pub fn published(&self) -> Vec<Event> {
        self.outbox.lock().unwrap().clone()
    }

    /// Force a run's lease to expire, simulating a worker dying mid-attempt.
    pub fn expire_lease(&self, id: RunId) {
        if let Some(r) = self.runs.lock().unwrap().get_mut(&id) {
            r.available_at = Utc::now() - Duration::seconds(1);
            r.status = RunStatus::Pending;
        }
    }
}

#[async_trait]
impl StateStore for MemStore {
    async fn create_run(&self, new: NewRun) -> Result<Option<RunId>> {
        let mut runs = self.runs.lock().unwrap();
        // Keyed exclusivity, mirroring the partial unique index in Postgres.
        if let Some(k) = &new.key {
            if runs.values().any(|r| {
                r.key.as_ref() == Some(k)
                    && r.function_id == new.function_id
                    && !r.status.is_terminal()
            }) {
                return Ok(None);
            }
        }
        let id = Uuid::now_v7();
        runs.insert(
            id,
            MemRun {
                id,
                namespace: new.namespace,
                function_id: new.function_id,
                key: new.key,
                status: RunStatus::Pending,
                fence: 0,
                attempt: 0,
                steps: HashMap::new(),
                inbox: Vec::new(),
                waits: Vec::new(),
                available_at: Utc::now(),
                queued: true,
                parent: new.parent,
                detached: new.detached,
                lineage: new.lineage_id.unwrap_or(id),
                chain_position: new.chain_position,
                output: None,
                error: None,
                consecutive_failures: 0,
                error_signature: None,
                started_at: Utc::now(),
            },
        );
        Ok(Some(id))
    }

    async fn load_attempt(&self, lease: &Lease) -> Result<Attempt> {
        let runs = self.runs.lock().unwrap();
        let r = runs
            .get(&lease.run_id)
            .ok_or_else(|| Error::Store("no such run".into()))?;
        Ok(Attempt {
            protocol: PROTOCOL_VERSION.into(),
            attempt: lease.attempt,
            fence: lease.fence,
            deadline: None,
            run: RunContext {
                id: r.id,
                function_id: r.function_id.clone(),
                namespace: r.namespace.clone(),
                key: r.key.clone(),
                started_at: r.started_at,
                input: None,
                lineage_id: r.lineage,
                chain_position: r.chain_position,
                cancelling: false,
            },
            events: vec![],
            // Only completed or terminally-failed steps are sent. Shipping
            // pending rows would make the handler treat an unresolved sleep as done.
            steps: r
                .steps
                .iter()
                .filter(|(_, s)| {
                    matches!(
                        s.status,
                        StepStatus::Completed | StepStatus::Failed | StepStatus::TimedOut
                    )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            state_truncated: false,
        })
    }

    async fn commit(&self, run_id: RunId, fence: i64, commit: OpCommit) -> Result<CommitOutcome> {
        let mut runs = self.runs.lock().unwrap();
        let (status, run_fence) = {
            let r = runs
                .get(&run_id)
                .ok_or_else(|| Error::Store("no such run".into()))?;
            (r.status, r.fence)
        };
        if fence != run_fence {
            return Ok(CommitOutcome::StaleFence);
        }
        if status.is_terminal() {
            return Ok(CommitOutcome::Terminal);
        }

        let mut suspend = false;
        let mut successor: Option<NewRun> = None;
        let mut cascade: Option<RunId> = None;

        {
            let r = runs.get_mut(&run_id).unwrap();
            for op in &commit.ops {
                match op {
                    Op::Step { id, hash, data, .. } => {
                        r.steps.entry(hash.clone()).or_insert(RecordedStep {
                            id: id.clone(),
                            op: StepOp::Step,
                            status: StepStatus::Completed,
                            data: data.clone(),
                            error: None,
                        });
                    }
                    Op::Sleep { id, hash, until } => {
                        r.steps.entry(hash.clone()).or_insert(RecordedStep {
                            id: id.clone(),
                            op: StepOp::Sleep,
                            status: StepStatus::Pending,
                            data: None,
                            error: None,
                        });
                        r.available_at = *until;
                        suspend = true;
                    }
                    Op::WaitEvent {
                        id, hash, event, ..
                    } => {
                        // Check the inbox in the same transaction. This is what
                        // closes the lost-signal race: an event that arrived
                        // before the wait was registered still matches.
                        let hit = r
                            .inbox
                            .iter_mut()
                            .find(|(t, _, c)| t == event && c.is_none());
                        match hit {
                            Some((_, payload, consumed)) => {
                                let p = payload.clone();
                                *consumed = Some(hash.clone());
                                r.steps.insert(
                                    hash.clone(),
                                    RecordedStep {
                                        id: id.clone(),
                                        op: StepOp::WaitEvent,
                                        status: StepStatus::Completed,
                                        data: Some(p),
                                        error: None,
                                    },
                                );
                            }
                            None => {
                                r.steps.entry(hash.clone()).or_insert(RecordedStep {
                                    id: id.clone(),
                                    op: StepOp::WaitEvent,
                                    status: StepStatus::Pending,
                                    data: None,
                                    error: None,
                                });
                                r.waits.push((hash.clone(), event.clone()));
                                suspend = true;
                            }
                        }
                    }
                    Op::Invoke { id, hash, .. } => {
                        r.steps.entry(hash.clone()).or_insert(RecordedStep {
                            id: id.clone(),
                            op: StepOp::Invoke,
                            status: StepStatus::Pending,
                            data: None,
                            error: None,
                        });
                        suspend = true;
                    }
                    Op::Signal { .. } => {}
                    Op::ContinueAsNew { input, .. } => {
                        r.status = RunStatus::Completed;
                        r.queued = false;
                        successor = Some(NewRun {
                            namespace: r.namespace.clone(),
                            function_id: r.function_id.clone(),
                            key: r.key.clone(),
                            input: input.clone(),
                            parent: None,
                            parent_step_hash: None,
                            detached: false,
                            lineage_id: Some(r.lineage),
                            chain_position: r.chain_position + 1,
                            trigger_event_id: None,
                        });
                    }
                    Op::Done { data } => {
                        r.status = RunStatus::Completed;
                        r.output = data.clone();
                        r.queued = false;
                        cascade = Some(run_id);
                    }
                    Op::Error { error, .. } => {
                        r.status = RunStatus::Failed;
                        r.error = Some(error.clone());
                        r.queued = false;
                        cascade = Some(run_id);
                    }
                }
            }

            if !r.status.is_terminal() {
                r.status = if suspend {
                    RunStatus::Sleeping
                } else {
                    RunStatus::Pending
                };
                if !suspend {
                    r.available_at = Utc::now();
                }
            }
        }

        // continue_as_new must not orphan live children: their results would be
        // delivered into a journal the successor discards.
        if let Some(succ) = successor {
            let live: Vec<RunId> = runs
                .values()
                .filter(|c| c.parent == Some(run_id) && !c.detached && !c.status.is_terminal())
                .map(|c| c.id)
                .collect();
            if !live.is_empty() {
                let r = runs.get_mut(&run_id).unwrap();
                r.status = RunStatus::Failed;
                r.error = Some(ErrorBody::coded(
                    "continue_as_new_with_live_children",
                    format!("{} non-detached child run(s) still in flight", live.len()),
                ));
            } else {
                let id = Uuid::now_v7();
                runs.insert(
                    id,
                    MemRun {
                        id,
                        namespace: succ.namespace,
                        function_id: succ.function_id,
                        key: succ.key,
                        status: RunStatus::Pending,
                        fence: 0,
                        attempt: 0,
                        steps: HashMap::new(),
                        inbox: Vec::new(),
                        waits: Vec::new(),
                        available_at: Utc::now(),
                        queued: true,
                        parent: None,
                        detached: false,
                        lineage: succ.lineage_id.unwrap(),
                        chain_position: succ.chain_position,
                        output: None,
                        error: None,
                        consecutive_failures: 0,
                        error_signature: None,
                        started_at: Utc::now(),
                    },
                );
            }
        }

        if let Some(parent) = cascade {
            let kids: Vec<RunId> = runs
                .values()
                .filter(|c| c.parent == Some(parent) && !c.detached && !c.status.is_terminal())
                .map(|c| c.id)
                .collect();
            for k in kids {
                let c = runs.get_mut(&k).unwrap();
                c.status = RunStatus::Cancelled;
                c.queued = false;
            }
        }

        self.outbox.lock().unwrap().extend(commit.emit);
        self.commits.fetch_add(1, Ordering::Relaxed);
        Ok(CommitOutcome::Committed)
    }

    async fn cancel_run(&self, _ns: &str, run_id: RunId) -> Result<bool> {
        let mut runs = self.runs.lock().unwrap();
        let Some(r) = runs.get_mut(&run_id) else {
            return Ok(false);
        };
        if r.status.is_terminal() {
            return Ok(false);
        }
        r.status = RunStatus::Cancelled;
        r.queued = false;
        let kids: Vec<RunId> = runs
            .values()
            .filter(|c| c.parent == Some(run_id) && !c.detached && !c.status.is_terminal())
            .map(|c| c.id)
            .collect();
        for k in kids {
            let c = runs.get_mut(&k).unwrap();
            c.status = RunStatus::Cancelled;
            c.queued = false;
        }
        Ok(true)
    }

    async fn retry_run(&self, _ns: &str, run_id: RunId) -> Result<bool> {
        let mut runs = self.runs.lock().unwrap();
        let Some(r) = runs.get_mut(&run_id) else {
            return Ok(false);
        };
        if !matches!(
            r.status,
            RunStatus::Failed | RunStatus::Cancelled | RunStatus::Quarantined
        ) {
            return Ok(false);
        }
        r.status = RunStatus::Pending;
        r.queued = true;
        r.error = None;
        r.available_at = Utc::now();
        Ok(true)
    }

    async fn quarantine(&self, run_id: RunId, _sig: &str) -> Result<()> {
        if let Some(r) = self.runs.lock().unwrap().get_mut(&run_id) {
            r.status = RunStatus::Quarantined;
            r.queued = false;
        }
        Ok(())
    }

    async fn record_failure(
        &self,
        run_id: RunId,
        err: &ErrorBody,
        retry_at: DateTime<Utc>,
    ) -> Result<FailureRecord> {
        let mut runs = self.runs.lock().unwrap();
        let r = runs
            .get_mut(&run_id)
            .ok_or_else(|| Error::Store("no such run".into()))?;

        // The same rule the SQL applies: consecutive *identical* failures, reset
        // when the signature changes. A fake that counted attempts instead would
        // let the dispatcher's quarantine tests pass against behaviour the real
        // store does not have.
        let signature = err.signature();
        r.consecutive_failures = if r.error_signature.as_deref() == Some(signature.as_str()) {
            r.consecutive_failures + 1
        } else {
            1
        };
        r.error_signature = Some(signature);
        r.error = Some(err.clone());
        r.status = RunStatus::Pending;
        r.available_at = retry_at;
        Ok(FailureRecord {
            attempts: r.attempt,
            consecutive: r.consecutive_failures,
        })
    }

    async fn release(&self, run_id: RunId, available_at: DateTime<Utc>) -> Result<()> {
        if let Some(r) = self.runs.lock().unwrap().get_mut(&run_id) {
            r.status = RunStatus::Pending;
            r.available_at = available_at;
        }
        Ok(())
    }

    async fn steps_page(&self, run_id: RunId, after: Option<&str>, limit: i64) -> Result<StepPage> {
        let runs = self.runs.lock().unwrap();
        let r = runs
            .get(&run_id)
            .ok_or_else(|| Error::Store("no such run".into()))?;
        // Ordered by hash so that paging is stable: an unordered page boundary
        // silently drops or repeats steps between requests.
        let mut keys: Vec<&String> = r.steps.keys().collect();
        keys.sort();
        let start = match after {
            Some(a) => keys
                .iter()
                .position(|k| k.as_str() == a)
                .map(|i| i + 1)
                .unwrap_or(0),
            None => 0,
        };
        let page: Vec<&String> = keys.into_iter().skip(start).take(limit as usize).collect();
        let next = if page.len() as i64 == limit {
            page.last().map(|s| s.to_string())
        } else {
            None
        };
        Ok(StepPage {
            steps: page
                .into_iter()
                .map(|k| (k.clone(), r.steps[k].clone()))
                .collect(),
            next,
        })
    }

    async fn run_status(&self, run_id: RunId) -> Result<Option<RunStatus>> {
        Ok(self.runs.lock().unwrap().get(&run_id).map(|r| r.status))
    }
}

#[async_trait]
impl Queue for MemStore {
    async fn claim(
        &self,
        namespace: &str,
        _worker: &str,
        max: i64,
        lease: Duration,
    ) -> Result<Vec<Lease>> {
        let now = Utc::now();
        let mut runs = self.runs.lock().unwrap();
        let mut ids: Vec<RunId> = runs
            .values()
            .filter(|r| {
                r.namespace == namespace
                    && r.queued
                    && matches!(r.status, RunStatus::Pending | RunStatus::Sleeping)
                    && r.available_at <= now
                    && r.waits.is_empty()
            })
            .map(|r| r.id)
            .collect();
        ids.sort();
        ids.truncate(max as usize);

        Ok(ids
            .into_iter()
            .map(|id| {
                let r = runs.get_mut(&id).unwrap();
                r.fence += 1;
                r.attempt += 1;
                r.status = RunStatus::Running;
                r.available_at = now + lease;
                Lease {
                    run_id: id,
                    fence: r.fence,
                    attempt: r.attempt,
                    until: now + lease,
                }
            })
            .collect())
    }

    async fn heartbeat(&self, run_id: RunId, until: DateTime<Utc>) -> Result<()> {
        if let Some(r) = self.runs.lock().unwrap().get_mut(&run_id) {
            r.available_at = until;
        }
        Ok(())
    }

    async fn stats(&self, namespace: &str) -> Result<Vec<QueueStat>> {
        let runs = self.runs.lock().unwrap();
        let mut by_fn: HashMap<String, (i64, i64)> = HashMap::new();
        for r in runs
            .values()
            .filter(|r| r.namespace == namespace && r.queued)
        {
            let e = by_fn.entry(r.function_id.clone()).or_default();
            if r.status == RunStatus::Running {
                e.1 += 1;
            } else {
                e.0 += 1;
            }
        }
        Ok(by_fn
            .into_iter()
            .map(|(function_id, (backlog, in_flight))| QueueStat {
                function_id,
                backlog,
                in_flight,
                oldest_seconds: 0,
            })
            .collect())
    }

    async fn active_namespaces(&self) -> Result<Vec<String>> {
        let runs = self.runs.lock().unwrap();
        let mut ns: Vec<String> = runs
            .values()
            .filter(|r| r.queued && !r.status.is_terminal())
            .map(|r| r.namespace.clone())
            .collect();
        ns.sort();
        ns.dedup();
        Ok(ns)
    }
}

#[async_trait]
impl EventLog for MemStore {
    async fn append(&self, namespace: &str, event: &Event) -> Result<(Uuid, bool)> {
        if let Some(idem) = &event.idempotency {
            let key = (namespace.to_string(), idem.clone());
            let mut map = self.idem.lock().unwrap();
            if let Some(existing) = map.get(&key) {
                return Ok((*existing, true));
            }
            let id = Uuid::now_v7();
            map.insert(key, id);
            self.events
                .lock()
                .unwrap()
                .push((namespace.into(), event.clone()));
            return Ok((id, false));
        }
        let id = Uuid::now_v7();
        self.events
            .lock()
            .unwrap()
            .push((namespace.into(), event.clone()));
        Ok((id, false))
    }

    async fn deliver(
        &self,
        run_id: RunId,
        event_type: &str,
        payload: &serde_json::Value,
        _sender: Option<(RunId, String)>,
    ) -> Result<Delivery> {
        let mut runs = self.runs.lock().unwrap();
        let Some(r) = runs.get_mut(&run_id) else {
            return Ok(Delivery::NoRun);
        };
        if r.status.is_terminal() {
            return Ok(Delivery::NoRun);
        }
        r.inbox.push((event_type.into(), payload.clone(), None));

        let Some(pos) = r.waits.iter().position(|(_, t)| t == event_type) else {
            return Ok(Delivery::Buffered);
        };
        let (hash, _) = r.waits.remove(pos);
        if let Some(entry) = r
            .inbox
            .iter_mut()
            .rev()
            .find(|(t, _, c)| t == event_type && c.is_none())
        {
            entry.2 = Some(hash.clone());
        }
        r.steps.insert(
            hash.clone(),
            RecordedStep {
                id: "wait".into(),
                op: StepOp::WaitEvent,
                status: StepStatus::Completed,
                data: Some(payload.clone()),
                error: None,
            },
        );
        r.status = RunStatus::Pending;
        r.available_at = Utc::now();
        r.queued = true;
        Ok(Delivery::Resolved)
    }

    async fn drain_outbox(&self, _max: i64) -> Result<u64> {
        Ok(self.outbox.lock().unwrap().len() as u64)
    }
}

#[async_trait]
impl TimerStore for MemStore {
    async fn fire_due(&self, now: DateTime<Utc>, _max: i64) -> Result<u64> {
        let mut runs = self.runs.lock().unwrap();
        let mut n = 0;
        for r in runs.values_mut() {
            if r.status == RunStatus::Sleeping && r.available_at <= now && r.waits.is_empty() {
                for s in r.steps.values_mut() {
                    if s.op == StepOp::Sleep && s.status == StepStatus::Pending {
                        s.status = StepStatus::Completed;
                    }
                }
                r.status = RunStatus::Pending;
                n += 1;
            }
        }
        Ok(n)
    }
}

/// A transport that runs a handler closure in-process.
pub struct MemTransport {
    handler: ScriptedHandler,
    /// Consecutive failures to inject before succeeding.
    pub fail_next: Mutex<u32>,
    /// Calls actually delivered to the handler.
    pub calls: AtomicU64,
}

impl MemTransport {
    /// Wrap a handler.
    pub fn new(
        handler: impl Fn(&Attempt) -> std::result::Result<AttemptResponse, String>
            + Send
            + Sync
            + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            handler: Box::new(handler),
            fail_next: Mutex::new(0),
            calls: AtomicU64::new(0),
        })
    }

    /// Make the next `n` deliveries fail at the transport layer.
    pub fn fail(&self, n: u32) {
        *self.fail_next.lock().unwrap() = n;
    }
}

#[async_trait]
impl Transport for MemTransport {
    async fn deliver(
        &self,
        _t: &AppTarget,
        attempt: &Attempt,
    ) -> std::result::Result<AttemptResponse, Error> {
        {
            let mut f = self.fail_next.lock().unwrap();
            if *f > 0 {
                *f -= 1;
                return Err(Error::Transport("app unreachable".into()));
            }
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        (self.handler)(attempt).map_err(Error::Transport)
    }
}

/// A resolver that points every function at one endpoint.
pub struct StaticTargets;

#[async_trait]
impl crate::TargetResolver for StaticTargets {
    async fn resolve(&self, _ns: &str, _fn_id: &str) -> Result<AppTarget> {
        Ok(AppTarget {
            url: "memory://app".into(),
            keys: vec![b"k".to_vec()],
        })
    }
}

/// A [`CronStore`] double that records calls and returns canned results.
///
/// Deliberately *not* an in-memory reimplementation of firing. The at-most-once
/// guarantee is a primary key and the claim is `FOR UPDATE SKIP LOCKED`; a fake
/// that reimplemented either would be a second correctness centre agreeing with
/// itself, which is how this project's three worst defects survived (README
/// finding 2). What this double is for is the wiring around the sweep — that
/// the housekeeper calls it, counts what it returns, and survives it failing.
#[derive(Default)]
pub struct MemCron {
    /// Sweeps performed.
    pub sweeps: AtomicU64,
    /// Trims performed.
    pub trims: AtomicU64,
    /// Registrations performed.
    pub registrations: AtomicU64,
    /// What the next sweep returns; `None` makes it fail.
    pub next: Mutex<Option<CronSweep>>,
}

impl MemCron {
    /// A double whose sweeps return `result`.
    pub fn returning(result: CronSweep) -> Arc<Self> {
        Arc::new(Self {
            next: Mutex::new(Some(result)),
            ..Default::default()
        })
    }

    /// A double whose sweeps fail.
    pub fn failing() -> Arc<Self> {
        Arc::new(Self {
            next: Mutex::new(None),
            ..Default::default()
        })
    }
}

#[async_trait]
impl CronStore for MemCron {
    async fn sweep(&self, _max: i64) -> Result<CronSweep> {
        self.sweeps.fetch_add(1, Ordering::Relaxed);
        self.next
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::Store("cron sweep failed".into()))
    }

    async fn sweep_namespace(&self, _ns: &str, max: i64) -> Result<CronSweep> {
        self.sweep(max).await
    }

    async fn cron_namespaces(&self) -> Result<Vec<String>> {
        Ok(vec!["test".into()])
    }

    async fn register_schedules(&self, regs: &[CronRegistration]) -> Result<u64> {
        self.registrations.fetch_add(1, Ordering::Relaxed);
        Ok(regs.len() as u64)
    }

    async fn trim_fires(&self, _max: i64) -> Result<u64> {
        self.trims.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
}

/// A [`BlobStore`] double for the dispatcher's verification path.
///
/// Only `commit_blob` is modelled, because it is the only method the dispatcher
/// calls. The rest are `unimplemented!` rather than faked, so a test that starts
/// to depend on one fails loudly instead of passing against a fiction.
///
/// It does model one thing the real store's contract requires: committing an
/// already-committed blob returns its reference without re-verifying. That makes
/// this double an executable statement of the contract, not evidence that
/// `PostgresBlobStore` honours it — the live test
/// `committing_an_already_committed_blob_does_not_look_at_the_object_again` in
/// `stepd-store-postgres/tests/live.rs` is what checks the implementation.
///
/// Every field is per-instance state, never a `static`: two tests sharing one
/// mutable set of committed ids is the interference this codebase has been
/// bitten by more than once.
#[derive(Default)]
pub struct MemBlobs {
    /// Ids whose verification fails as a digest mismatch.
    mismatched: Mutex<HashSet<Uuid>>,
    /// Ids that are already readable.
    committed: Mutex<HashSet<Uuid>>,
    /// Calls that got as far as verifying.
    pub verifications: AtomicU64,
    /// Calls that returned early because the blob was already committed.
    pub skipped: AtomicU64,
}

impl MemBlobs {
    /// A store holding nothing, in which every blob verifies.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A store in which `id` is reserved and fails verification.
    pub fn mismatching(id: Uuid) -> Arc<Self> {
        Arc::new(Self {
            mismatched: Mutex::new([id].into_iter().collect()),
            ..Default::default()
        })
    }

    /// A store in which `id` has already been committed — the state the relay
    /// path leaves behind before the ops carrying the reference are returned.
    pub fn already_committed(id: Uuid) -> Arc<Self> {
        Arc::new(Self {
            committed: Mutex::new([id].into_iter().collect()),
            ..Default::default()
        })
    }

    /// Whether `id` is readable.
    pub fn is_committed(&self, id: Uuid) -> bool {
        self.committed.lock().unwrap().contains(&id)
    }
}

#[async_trait]
impl BlobStore for MemBlobs {
    async fn reserve(&self, _ns: &str, _spec: BlobSpec) -> Result<Reservation> {
        unimplemented!("the dispatcher never reserves")
    }

    async fn commit_blob(&self, id: Uuid) -> Result<BlobRef> {
        let reference = BlobRef {
            id,
            size: 1,
            sha256: "ab".into(),
            content_type: None,
            filename: None,
            url: None,
        };
        if self.is_committed(id) {
            self.skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(reference);
        }
        self.verifications.fetch_add(1, Ordering::Relaxed);
        if self.mismatched.lock().unwrap().contains(&id) {
            return Err(Error::Config(format!(
                "blob_digest_mismatch: {id} content does not match the declared sha256"
            )));
        }
        self.committed.lock().unwrap().insert(id);
        Ok(reference)
    }

    async fn presign_read(&self, _id: Uuid, _ttl: Duration) -> Result<String> {
        unimplemented!("the dispatcher never mints a read url")
    }

    async fn add_ref(&self, _id: Uuid, _run: RunId, _step_hash: &str) -> Result<()> {
        unimplemented!("references are recorded by a trigger, in the commit transaction")
    }

    async fn collect(&self, _before: DateTime<Utc>) -> Result<u64> {
        unimplemented!("the dispatcher never collects")
    }
}

/// A [`Housekeeping`] double that does nothing and counts being asked.
///
/// The real sweeps are engine logic and are tested against a live database;
/// reimplementing them here would produce a second correctness centre agreeing
/// with itself. What this is for is the [`crate::Housekeeper`]'s own contract:
/// that it calls every sweep, that one sweep's failure does not stop the others,
/// and that a pass which found nothing reports itself idle.
#[derive(Default)]
pub struct NoopKeeping {
    /// Calls to each sweep, in order: signals, children, leases.
    pub calls: AtomicU64,
}

impl NoopKeeping {
    /// A double that answers every sweep with zero.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Housekeeping for NoopKeeping {
    async fn drain_signals(&self, _max: i64) -> Result<u64> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
    async fn resolve_finished_children(&self, _max: i64) -> Result<u64> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
    async fn reclaim_expired_leases(&self, _max: i64) -> Result<u64> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
}
