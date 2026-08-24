//! Component interfaces.
//!
//! The engine is generic over these. Postgres implements all of them in v1; the
//! seams exist so that swapping a backing service is a new crate rather than an
//! engine change. A CI lint fails the build if `stepd-core` gains a dependency
//! on any concrete backend.

use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use stepd_proto::{Attempt, Event, Op, RunId, RunStatus};
use uuid::Uuid;

/// A run leased to a worker for one attempt.
#[derive(Debug, Clone)]
pub struct Lease {
    /// Run being dispatched.
    pub run_id: RunId,
    /// Fencing token for this attempt.
    pub fence: i64,
    /// Attempt number.
    pub attempt: i32,
    /// When the lease expires; another worker may take over after this.
    pub until: DateTime<Utc>,
}

/// Everything to commit from one attempt, applied atomically.
#[derive(Debug, Clone)]
pub struct OpCommit {
    /// Ops returned by the app.
    pub ops: Vec<Op>,
    /// Events published in the same transaction.
    pub emit: Vec<Event>,
}

/// Result of attempting a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Applied.
    Committed,
    /// The attempt was superseded; its ops were discarded.
    StaleFence,
    /// The run had already reached a terminal state.
    Terminal,
    /// The run no longer exists.
    NoSuchRun,
    /// The envelope broke a protocol rule the store enforces — a limit, or
    /// `continue_as_new` with a live child. The run has already been failed
    /// non-retryably *inside the same transaction*, so the caller must not write
    /// again to make the state consistent; it only has to report.
    ///
    /// Carrying the code rather than a bare "rejected" is what lets the console
    /// tell an operator which rule was broken without reading the run row.
    Rejected(String),
}

impl CommitOutcome {
    /// Parse the string form returned by the `commit_ops` SQL function.
    ///
    /// Kept next to the enum rather than in the store so that an alternative
    /// backend implementing the same contract cannot drift on the spelling.
    pub fn from_sql(s: &str) -> Self {
        match s {
            "committed" => Self::Committed,
            "stale_fence" => Self::StaleFence,
            "terminal" => Self::Terminal,
            "no_such_run" => Self::NoSuchRun,
            other => Self::Rejected(other.strip_prefix("failed:").unwrap_or(other).to_string()),
        }
    }
}

/// A new run to create.
#[derive(Debug, Clone)]
pub struct NewRun {
    /// Namespace.
    pub namespace: String,
    /// Function to execute.
    pub function_id: String,
    /// Business key, enforcing at most one active run per key.
    pub key: Option<String>,
    /// Input for invoke-triggered runs.
    pub input: Option<serde_json::Value>,
    /// Parent, for child runs.
    pub parent: Option<RunId>,
    /// Parent's invoke step hash.
    pub parent_step_hash: Option<String>,
    /// Whether the child's lifecycle is independent of its parent.
    pub detached: bool,
    /// Lineage, preserved across `continue_as_new`.
    pub lineage_id: Option<Uuid>,
    /// Position within the lineage.
    pub chain_position: i32,
    /// Triggering event, if any.
    pub trigger_event_id: Option<Uuid>,
}

impl NewRun {
    /// A root run of `function_id` in `namespace`.
    pub fn root(namespace: impl Into<String>, function_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            function_id: function_id.into(),
            key: None,
            input: None,
            parent: None,
            parent_step_hash: None,
            detached: false,
            lineage_id: None,
            chain_position: 0,
            trigger_event_id: None,
        }
    }

    /// Set the business key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the run input.
    pub fn with_input(mut self, input: serde_json::Value) -> Self {
        self.input = Some(input);
        self
    }
}

/// Outcome of delivering an event to a run's inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// A parked wait was satisfied and the run requeued.
    Resolved,
    /// Held in the inbox for a wait that has not been registered yet.
    Buffered,
    /// Already delivered by this sender; ignored.
    Duplicate,
    /// No such run, or the run is terminal.
    NoRun,
}

/// Durable run and step state. `commit` is the correctness centre of the system.
#[async_trait]
pub trait StateStore: Send + Sync + 'static {
    /// Create a run. Returns `None` if a keyed run is already active on that key.
    async fn create_run(&self, run: NewRun) -> Result<Option<RunId>>;

    /// Build the attempt request for a leased run.
    async fn load_attempt(&self, lease: &Lease) -> Result<Attempt>;

    /// Apply ops, emitted events and the next schedule in one transaction,
    /// guarded by the fencing token.
    async fn commit(&self, run_id: RunId, fence: i64, commit: OpCommit) -> Result<CommitOutcome>;

    /// Cancel a run and cascade to its non-detached descendants.
    async fn cancel_run(&self, namespace: &str, run_id: RunId) -> Result<bool>;

    /// Requeue a failed, cancelled or quarantined run.
    async fn retry_run(&self, namespace: &str, run_id: RunId) -> Result<bool>;

    /// Move a run out of dispatch after repeated identical failures.
    async fn quarantine(&self, run_id: RunId, signature: &str) -> Result<()>;

    /// Record a failed attempt and schedule a retry.
    ///
    /// Returns how many times this run has now failed *with the same signature
    /// in a row*, not how many attempts it has made. Quarantine is defined on
    /// repeated **identical** failure (F-LP-6): a run failing a different way
    /// each time is a different problem, and quarantining it hides the very
    /// variety that would have explained it.
    async fn record_failure(
        &self,
        run_id: RunId,
        err: &stepd_proto::ErrorBody,
        retry_at: DateTime<Utc>,
    ) -> Result<FailureRecord>;

    /// Release a lease without recording progress, so another worker can pick it up.
    async fn release(&self, run_id: RunId, available_at: DateTime<Utc>) -> Result<()>;

    /// Current status of a run.
    async fn run_status(&self, run_id: RunId) -> Result<Option<RunStatus>>;

    /// A page of recorded steps, for the state-truncation path (protocol §8.6).
    ///
    /// A run at the 10 000-step ceiling cannot have its journal shipped inline in
    /// a 4 MiB attempt body. Without pagination the only options are to fail the
    /// attempt or to truncate silently, and silent truncation makes the SDK
    /// re-execute steps whose results were simply not sent — the exact
    /// silent-corruption failure mode the design exists to prevent.
    async fn steps_page(&self, run_id: RunId, after: Option<&str>, limit: i64) -> Result<StepPage>;
}

/// What a recorded failure tells the dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureRecord {
    /// Attempts this run has made in total.
    pub attempts: i32,
    /// Failures in a row carrying the same signature.
    pub consecutive: i32,
}

/// One page of a run's journal.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StepPage {
    /// Steps in this page, keyed by hash.
    pub steps: std::collections::HashMap<String, stepd_proto::RecordedStep>,
    /// Cursor for the next page; `None` when the journal is exhausted.
    pub next: Option<String>,
}

/// Work admission: leasing, concurrency, rate limiting and fairness.
#[async_trait]
pub trait Queue: Send + Sync + 'static {
    /// Claim up to `max` runs **from one namespace** for `worker`, bumping each
    /// run's fence.
    ///
    /// The namespace parameter is what makes fair dispatch possible: the caller
    /// rotates across namespaces and claims from each in turn. A namespace-blind
    /// claim would let one tenant's backlog starve every other, however the
    /// caller sequenced its calls.
    ///
    /// Implementations must use row-level locking only. Session-scoped advisory
    /// locks break under a transaction-mode connection pooler.
    async fn claim(
        &self,
        namespace: &str,
        worker: &str,
        max: i64,
        lease: chrono::Duration,
    ) -> Result<Vec<Lease>>;

    /// Extend a lease held by a still-running attempt.
    async fn heartbeat(&self, run_id: RunId, until: DateTime<Utc>) -> Result<()>;

    /// Backlog and in-flight counts per function.
    async fn stats(&self, namespace: &str) -> Result<Vec<QueueStat>>;

    /// Namespaces with claimable work, for fair round-robin dispatch.
    async fn active_namespaces(&self) -> Result<Vec<String>>;
}

/// Queue depth for one function.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueueStat {
    /// Function id.
    pub function_id: String,
    /// Runs waiting to be dispatched.
    pub backlog: i64,
    /// Runs currently leased.
    pub in_flight: i64,
    /// Age of the oldest queued item, in seconds.
    pub oldest_seconds: i64,
}

/// Durable timers for sleeps, wait timeouts and run deadlines.
#[async_trait]
pub trait TimerStore: Send + Sync + 'static {
    /// Wake runs whose timers are due. Returns how many were requeued.
    async fn fire_due(&self, now: DateTime<Utc>, max: i64) -> Result<u64>;
}

/// Append-only event history, correlation, and the per-run inbox.
#[async_trait]
pub trait EventLog: Send + Sync + 'static {
    /// Append an event, deduplicating on the idempotency key.
    /// Returns the event id and whether it was a duplicate.
    async fn append(&self, namespace: &str, event: &Event) -> Result<(Uuid, bool)>;

    /// Deliver an event to a run's inbox, resolving a parked wait if one matches.
    async fn deliver(
        &self,
        run_id: RunId,
        event_type: &str,
        payload: &serde_json::Value,
        sender: Option<(RunId, String)>,
    ) -> Result<Delivery>;

    /// Publish outbox entries that were committed with an attempt.
    async fn drain_outbox(&self, max: i64) -> Result<u64>;
}

/// Delivery of an attempt to an app. Push over HTTP in v1.
///
/// This is a trait rather than a hard-coded transport so that a pull-worker mode
/// is an alternative implementation rather than a protocol change.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Deliver an attempt and return the app's response.
    async fn deliver(
        &self,
        target: &AppTarget,
        attempt: &Attempt,
    ) -> std::result::Result<stepd_proto::AttemptResponse, Error>;
}

/// Where and how to reach an app.
#[derive(Debug, Clone)]
pub struct AppTarget {
    /// Endpoint URL.
    pub url: String,
    /// Signing keys: current first, previous second during rotation.
    pub keys: Vec<Vec<u8>>,
}

/// Periodic work that keeps the engine converging: timers, relayed signals,
/// finished children and expired leases.
///
/// Separate from the dispatch loop because every one of these must still make
/// progress when no app is reachable at all. Folding them into `tick` would mean
/// a total app outage also stops timers from firing and leases from being
/// reclaimed — the system would stop healing exactly when it needs to.
#[async_trait]
pub trait Housekeeping: Send + Sync + 'static {
    /// Deliver signals recorded by committed `signal` ops.
    async fn drain_signals(&self, max: i64) -> Result<u64>;

    /// Resolve parent steps for children that reached a terminal state.
    ///
    /// The commit path already does this inline; this is the recovery sweep for
    /// a server that died between committing a child's `done` and resolving its
    /// parent, which would otherwise leave the parent parked forever.
    async fn resolve_finished_children(&self, max: i64) -> Result<u64>;

    /// Return runs whose lease expired to the queue.
    async fn reclaim_expired_leases(&self, max: i64) -> Result<u64>;

    /// Delete reservations that were never uploaded and blobs nothing references.
    ///
    /// Part of housekeeping rather than a separate cron job because the failure
    /// it prevents is unbounded storage growth, and a reclamation task that runs
    /// only when somebody remembers to schedule it is one that does not run.
    /// Returns the number of blobs collected.
    ///
    /// Implementations that do not manage blobs return `Ok(0)`.
    async fn collect_blobs(&self, _before: DateTime<Utc>) -> Result<u64> {
        Ok(0)
    }
}

/// Cron schedule storage and firing (protocol §3.1, ADR-016).
///
/// The trait carries no policy. Every decision — which occurrences to fire,
/// which to skip and why, where `next_fire_at` goes next — is made by
/// [`crate::cron::plan`], which is pure and has no database. An implementation
/// of this trait claims rows, hands them to the planner, and applies what comes
/// back.
///
/// That split is not stylistic. The last time engine logic grew inside a storage
/// crate, this project ended up with two correctness centres and structural
/// tests guarding the one that was not running (README finding 2). Three live
/// defects were sitting in the other one.
#[async_trait]
pub trait CronStore: Send + Sync + 'static {
    /// Claim due schedules, plan them, fire what should fire, advance them.
    ///
    /// Must be atomic per schedule: an occurrence that is recorded as fired but
    /// whose run was rolled back is a lost job with a ledger entry saying
    /// otherwise, which is worse than either failure alone.
    ///
    /// Must use the database's own clock (F-DL-8). A replica running a minute
    /// fast that trusts itself fires the whole fleet a minute early, and every
    /// run succeeds, so nothing reports a problem.
    ///
    /// Sweeps every namespace with work, in rotation.
    async fn sweep(&self, max: i64) -> Result<CronSweep>;

    /// One sweep of a single namespace.
    ///
    /// Namespace scoping is a fairness requirement, not a convenience. A
    /// namespace-blind claim ordered by `next_fire_at` is won by whoever is
    /// furthest behind, so one tenant with a thousand overdue per-minute
    /// schedules fills every sweep and every other tenant's schedules stop
    /// firing — with nothing failing and no backlog anywhere an operator would
    /// think to look. It is the same argument that made dispatch claiming
    /// namespace-scoped, and structural invariant 15 guards that one.
    ///
    /// Exposed separately for the same two reasons `tick_namespace` is: a
    /// deployment sharding namespaces across pools needs to drive one without
    /// the others, and a test in a shared database needs to sweep its own
    /// schedules rather than whatever else happens to be due.
    async fn sweep_namespace(&self, namespace: &str, max: i64) -> Result<CronSweep>;

    /// Namespaces with at least one schedule due.
    async fn cron_namespaces(&self) -> Result<Vec<String>>;

    /// Register a function's cron triggers, retiring any it no longer declares.
    ///
    /// Returns how many schedules the function now has. Retiring is half the
    /// job: a cron trigger removed from a function and redeployed must stop
    /// firing, or the deploy is indistinguishable from not having happened.
    async fn register_schedules(&self, regs: &[CronRegistration]) -> Result<u64>;

    /// Drop ledger entries too old to affect any future decision.
    async fn trim_fires(&self, max: i64) -> Result<u64>;
}

/// One cron trigger, validated, ready to be stored.
///
/// Holding a parsed [`crate::cron::Schedule`] rather than a string is the point:
/// an expression that does not parse cannot reach the database, so it fails
/// registration loudly instead of registering cleanly and never firing. The
/// second is the shape ADR-016 was written about and the reason this whole
/// component existed as a stub for so long without anyone noticing.
#[derive(Debug, Clone)]
pub struct CronRegistration {
    /// Owning namespace.
    pub namespace: String,
    /// Owning function.
    pub function_id: String,
    /// Index of this trigger in the function's trigger array, its stable identity.
    pub trigger_idx: i32,
    /// The parsed schedule.
    pub schedule: crate::cron::Schedule,
    /// Catch-up policy.
    pub catchup: crate::cron::CatchUp,
    /// Cap on occurrences fired in one recovery.
    pub catchup_limit: i32,
    /// Occurrences older than this are never caught up.
    pub misfire_window: chrono::Duration,
    /// Whether a fire is skipped while a run on `run_key` is still active.
    pub singleton: bool,
    /// The run key for fires from this schedule.
    pub run_key: Option<String>,
}

/// What one cron sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CronSweep {
    /// Schedules examined.
    pub considered: u64,
    /// Runs created.
    pub fired: u64,
    /// Occurrences deliberately not fired.
    pub skipped: u64,
    /// Occurrences another replica had already claimed.
    pub duplicates: u64,
    /// Schedules paused because they could not be planned at all.
    ///
    /// Never expected to be non-zero. It is surfaced rather than logged because
    /// a paused schedule is a job that has silently stopped, and the only thing
    /// worse than that is nobody being able to count them.
    pub unschedulable: u64,
}

impl CronSweep {
    /// Whether the sweep found nothing to do.
    pub fn is_idle(&self) -> bool {
        *self == Self::default()
    }
}

/// Out-of-line payload storage (protocol §8.3).
///
/// Two-phase by construction: bytes never traverse the stepd server, so the
/// reservation and the upload are separate calls and the server only ever sees
/// the digest. `reserve` returning [`Reservation::Deduplicated`] is not an
/// optimisation — it is what makes replaying an event or retrying a run cheap
/// instead of re-uploading the same object.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Phase one: reserve an id and issue a write-scoped upload URL.
    async fn reserve(&self, namespace: &str, spec: BlobSpec) -> Result<Reservation>;

    /// Phase two: verify the declared size and digest, then mark the blob readable.
    ///
    /// Verification is what stops a compromised upload URL substituting different
    /// content for a committed reference, so it is not skippable.
    async fn commit_blob(&self, id: Uuid) -> Result<stepd_proto::BlobRef>;

    /// A read-scoped, short-lived URL. Minted per attempt; never persisted.
    async fn presign_read(&self, id: Uuid, ttl: chrono::Duration) -> Result<String>;

    /// Record that a run's step references a blob.
    async fn add_ref(&self, id: Uuid, run: RunId, step_hash: &str) -> Result<()>;

    /// Delete unreferenced blobs and reservations that were never completed.
    async fn collect(&self, before: DateTime<Utc>) -> Result<u64>;
}

/// Where a blob's bytes live, and who mints the URLs that reach them.
///
/// Split out of [`BlobStore`] because the index is the same everywhere — the
/// row, the per-namespace dedupe, the references recorded by trigger — and only
/// the bytes and the URLs vary. Writing a second `BlobStore` to change where
/// bytes live would duplicate the correctness-bearing half to swap the
/// mechanical one.
#[async_trait]
pub trait BlobBackend: Send + Sync + 'static {
    /// Where to PUT the bytes for a reserved blob, and what to send with them.
    ///
    /// A presigning backend MUST bind the declared size and digest into what it
    /// returns, so the store itself refuses bytes that do not match. That is
    /// what lets `stored` answer without reading the object.
    async fn upload_target(
        &self,
        id: Uuid,
        spec: &BlobSpec,
        ttl: chrono::Duration,
    ) -> Result<UploadTarget>;

    /// A read-scoped, short-lived URL.
    ///
    /// Synchronous because the journal walk that mints these
    /// (`attach_read_urls`) is synchronous, and neither an HMAC capability nor
    /// SigV4 needs the network to sign.
    fn read_url(&self, id: Uuid, size: i64, ttl: chrono::Duration) -> Result<String>;

    /// What the backend holds for `id`, without transferring the object.
    ///
    /// `sha256` is `None` when the backend cannot answer from metadata; the
    /// caller then falls back to reading the bytes, which is correct for a local
    /// filesystem and defeats the purpose on object storage.
    async fn stored(&self, id: Uuid) -> Result<Option<StoredObject>>;

    /// Remove an object. Absent is success: collection must be idempotent.
    async fn delete(&self, id: Uuid) -> Result<()>;

    /// Whether this backend issues URLs that reach the bytes directly.
    ///
    /// `false` mounts protocol §8.3.2's relay and its warning. A backend that
    /// answers `true` without truly presigning does not slow anything down — it
    /// hands apps URLs that go nowhere.
    fn can_presign(&self) -> bool;
}

/// Raw byte transfer through the control plane, for backends that cannot presign.
///
/// Separate from [`BlobBackend`] because it is the fallback path, not the
/// normal one: protocol §8.3.2 exists so a store with no signer of its own can
/// still work, and a backend that presigns must not be able to answer these at
/// all. If an S3 backend could implement this, the relay would be reachable for
/// a store that has no reason to relay.
#[async_trait]
pub trait RelayBytes: Send + Sync + 'static {
    /// Store bytes for a reserved blob, after the caller has verified the capability.
    async fn put_bytes(&self, id: Uuid, bytes: &[u8]) -> Result<()>;

    /// Read a committed blob, optionally a byte range (protocol §8.3.3).
    async fn get_bytes(&self, id: Uuid, range: Option<(u64, u64)>) -> Result<Vec<u8>>;
}

/// Where to send bytes for a reserved blob.
#[derive(Debug, Clone)]
pub struct UploadTarget {
    /// URL to send them to.
    pub url: String,
    /// HTTP method.
    pub method: String,
    /// Headers the caller must send verbatim. On a presigning backend these are
    /// signed, so altering or dropping one makes the upload fail rather than
    /// succeed unverified.
    pub headers: Vec<(String, String)>,
    /// When the URL stops working.
    pub expires_at: DateTime<Utc>,
}

/// What a backend holds, as metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    /// Size in bytes.
    pub size: i64,
    /// Lowercase hex SHA-256, when the backend knows it without reading bytes.
    pub sha256: Option<String>,
}

/// What an app wants to store.
#[derive(Debug, Clone)]
pub struct BlobSpec {
    /// Declared size in bytes. The store must reject a larger upload.
    pub size: i64,
    /// Lowercase hex SHA-256 of the content.
    pub sha256: String,
    /// Media type.
    pub content_type: Option<String>,
    /// Original filename; presentational only.
    pub filename: Option<String>,
}

/// Outcome of a reservation.
#[derive(Debug, Clone)]
pub enum Reservation {
    /// Upload required.
    Upload {
        /// Assigned blob id.
        id: Uuid,
        /// Write-scoped URL, valid until `expires_at`.
        url: String,
        /// HTTP method to use.
        method: String,
        /// Headers the store requires on the PUT.
        headers: Vec<(String, String)>,
        /// When the URL stops working.
        expires_at: DateTime<Utc>,
    },
    /// The digest already exists in this namespace; the app must skip the upload.
    Deduplicated {
        /// Existing blob id.
        id: Uuid,
    },
}

/// Expression evaluation for triggers, keys, waits and cancellation.
///
/// A trait rather than a direct dependency because the expression language is a
/// published part of the protocol (§10) and the interpreter is the part most
/// likely to be swapped — for a faster one, or for a stricter one.
pub trait ExprEngine: Send + Sync + 'static {
    /// Parse and validate an expression, returning an opaque compiled form.
    fn compile(&self, source: &str) -> Result<CompiledExpr>;

    /// Evaluate against the supplied bindings.
    ///
    /// Protocol §10 requires an evaluation error to be treated as a non-match
    /// rather than propagated, so this returns a value and reports failure out
    /// of band; a trigger whose expression throws must not take down ingest.
    fn eval(&self, expr: &CompiledExpr, bindings: &Bindings) -> Result<serde_json::Value>;

    /// Convenience: evaluate for truthiness, treating any error as `false`.
    fn matches(&self, expr: &CompiledExpr, bindings: &Bindings) -> bool {
        matches!(self.eval(expr, bindings), Ok(serde_json::Value::Bool(true)))
    }
}

/// A compiled expression. Opaque: the shape is the engine's business.
#[derive(Debug, Clone)]
pub struct CompiledExpr {
    /// Original source, retained for diagnostics and console display.
    pub source: String,
    /// Engine-private representation.
    pub program: std::sync::Arc<dyn std::any::Any + Send + Sync>,
}

/// Values available to an expression (protocol §10).
#[derive(Debug, Clone, Default)]
pub struct Bindings {
    /// The triggering event, as a CloudEvent map.
    pub event: Option<serde_json::Value>,
    /// Batched events, for batch triggers.
    pub events: Option<serde_json::Value>,
    /// Run identity fields: `id`, `key`, `key_suffix`, `function_id`.
    pub run: Option<serde_json::Value>,
    /// Evaluation time.
    pub now: Option<DateTime<Utc>>,
}
