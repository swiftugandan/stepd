//! The dispatch loop: claim → deliver → commit.
//!
//! Generic over [`StateStore`], [`Queue`] and [`Transport`], so the whole loop is
//! testable against in-memory fakes with no database and no network.

use crate::policy::{Circuit, CircuitBreaker, RetryPolicy};
use crate::traits::*;
use crate::{Error, Result};
use chrono::{Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use stepd_proto::{ErrorBody, Event, Op, RunId};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Tunables for the dispatch loop.
#[derive(Debug, Clone)]
pub struct DispatchConfig {
    /// Worker identity, recorded on leases.
    pub worker: String,
    /// Runs claimed per namespace per tick.
    pub batch: i64,
    /// Lease duration.
    pub lease: Duration,
    /// Retry policy for failed attempts.
    pub retry: RetryPolicy,
    /// Identical consecutive failures before a run is quarantined.
    ///
    /// Consecutive and identical: a run failing a different way each time is not
    /// a poison pill, and removing it from dispatch hides the variety that would
    /// have explained it.
    pub quarantine_after: i32,
    /// Spread applied to sleep wake-ups so a million midnight timers do not collide.
    pub timer_jitter: Duration,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            worker: "worker-1".into(),
            batch: 16,
            // Comfortably longer than the default attempt timeout (60 s). A lease
            // equal to the attempt timeout means an app that uses its whole
            // budget races its own lease: the run is reclaimed and re-dispatched
            // while the first attempt is still running, and the step executes
            // twice for no reason but arithmetic.
            lease: Duration::seconds(150),
            retry: RetryPolicy::default(),
            quarantine_after: 20,
            timer_jitter: Duration::seconds(60),
        }
    }
}

/// Counters for observability and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DispatchStats {
    /// Attempts delivered to apps.
    pub dispatched: u64,
    /// Attempts whose ops were committed.
    pub committed: u64,
    /// Responses discarded because the attempt had been superseded.
    pub stale: u64,
    /// Attempts that failed at the transport or app.
    pub failed: u64,
    /// Runs moved out of dispatch after repeated identical failures.
    pub quarantined: u64,
    /// Dispatches skipped because an app's circuit was open.
    pub skipped_circuit: u64,
    /// Envelopes the store refused because they broke a protocol rule.
    pub rejected: u64,
}

/// Drives runs from queued to complete.
pub struct Dispatcher<S, Q, T> {
    store: Arc<S>,
    queue: Arc<Q>,
    transport: Arc<T>,
    targets: Arc<dyn TargetResolver>,
    config: DispatchConfig,
    breakers: Mutex<HashMap<String, CircuitBreaker>>,
    stats: Mutex<DispatchStats>,
    /// Round-robin cursor across namespaces, so a noisy tenant cannot starve a quiet one.
    cursor: Mutex<usize>,
    /// Managed blobs, when the deployment has them (protocol §8.3).
    ///
    /// `None` on a deployment with no blob signing key, and then the whole
    /// verification path is skipped — no walk, no calls, no warning. A
    /// [`BlobStore`] is one of this crate's own interfaces, so holding one names
    /// no concrete backend.
    blobs: Option<Arc<dyn BlobStore>>,
}

/// What verifying an envelope's blob references found.
enum BlobCheck {
    /// Nothing to verify, or everything verified.
    Verified,
    /// The blob store could not answer. Retrying could still succeed, so this
    /// must not fail the run: a blip at the object store permanently failing
    /// runs is the same quiet outage as treating a retryable app error as
    /// terminal.
    Unavailable(ErrorBody),
    /// The blob store answered, and the reference must not be committed. §8.3.2
    /// requires the ops be discarded and the failure be non-retryable.
    Refused(ErrorBody),
}

/// Resolves a function to the app endpoint that hosts it.
#[async_trait::async_trait]
pub trait TargetResolver: Send + Sync + 'static {
    /// Endpoint and signing keys for a function.
    async fn resolve(&self, namespace: &str, function_id: &str) -> Result<AppTarget>;
}

impl<S, Q, T> Dispatcher<S, Q, T>
where
    S: StateStore,
    Q: Queue,
    T: Transport,
{
    /// Assemble a dispatcher from its components.
    pub fn new(
        store: Arc<S>,
        queue: Arc<Q>,
        transport: Arc<T>,
        targets: Arc<dyn TargetResolver>,
        config: DispatchConfig,
    ) -> Self {
        Self {
            store,
            queue,
            transport,
            targets,
            config,
            breakers: Mutex::new(HashMap::new()),
            stats: Mutex::new(DispatchStats::default()),
            cursor: Mutex::new(0),
            blobs: None,
        }
    }

    /// Verify managed-blob references against `blobs` before committing them.
    ///
    /// Optional because managed blobs are: a deployment with no blob signing key
    /// leaves this unset and every other path behaves exactly as it did.
    pub fn with_blob_store(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Snapshot of the counters.
    pub async fn stats(&self) -> DispatchStats {
        *self.stats.lock().await
    }

    /// Circuit state for an app, for the console and metrics.
    pub async fn circuit(&self, key: &str) -> Option<Circuit> {
        self.breakers.lock().await.get(key).map(|b| b.state())
    }

    /// One pass: claim work from each namespace in turn and drive it.
    ///
    /// Namespaces are visited round-robin from a rotating cursor rather than in a
    /// fixed order, which is what stops one tenant's backlog starving another.
    pub async fn tick(&self) -> Result<u64> {
        let mut namespaces = self.queue.active_namespaces().await?;
        if namespaces.is_empty() {
            return Ok(0);
        }
        namespaces.sort();
        let start = {
            let mut c = self.cursor.lock().await;
            *c = (*c + 1) % namespaces.len();
            *c
        };
        namespaces.rotate_left(start);

        let mut done = 0;
        for ns in &namespaces {
            done += self.tick_namespace(ns).await?;
        }
        Ok(done)
    }

    /// One pass over a single namespace.
    ///
    /// Exposed separately because a deployment that shards namespaces across
    /// worker pools — the usual answer to one tenant needing isolation from the
    /// rest — needs to drive one namespace without touching the others. It is
    /// also what makes a dispatcher test deterministic: `tick` visits whatever
    /// namespaces happen to have work, which in a shared database means another
    /// test's runs.
    pub async fn tick_namespace(&self, namespace: &str) -> Result<u64> {
        let leases = self
            .queue
            .claim(
                namespace,
                &self.config.worker,
                self.config.batch,
                self.config.lease,
            )
            .await?;
        let mut done = 0;
        for lease in leases {
            if self.drive(&lease).await? {
                done += 1;
            }
        }
        Ok(done)
    }

    /// Deliver one attempt and commit its result.
    async fn drive(&self, lease: &Lease) -> Result<bool> {
        let attempt = self.store.load_attempt(lease).await?;
        let key = format!("{}/{}", attempt.run.namespace, attempt.run.function_id);
        let target = self
            .targets
            .resolve(&attempt.run.namespace, &attempt.run.function_id)
            .await?;

        // Circuit check before we touch the app at all.
        {
            let mut breakers = self.breakers.lock().await;
            let cb = breakers.entry(key.clone()).or_default();
            if !cb.allow(Utc::now()) {
                self.stats.lock().await.skipped_circuit += 1;
                drop(breakers);
                self.store
                    .release(lease.run_id, Utc::now() + Duration::seconds(5))
                    .await?;
                debug!(run = %lease.run_id, "circuit open, deferring");
                return Ok(false);
            }
        }

        self.stats.lock().await.dispatched += 1;

        let response = match self.transport.deliver(&target, &attempt).await {
            Ok(r) => {
                self.breakers
                    .lock()
                    .await
                    .entry(key)
                    .or_default()
                    .record_success();
                r
            }
            Err(e) => {
                self.breakers
                    .lock()
                    .await
                    .entry(key)
                    .or_default()
                    .record_failure(Utc::now());
                self.handle_failure(lease, ErrorBody::coded("transport", e.to_string()))
                    .await?;
                return Ok(false);
            }
        };

        // Reject a malformed envelope before it reaches the store, so the store
        // never has to defend against a shape the protocol forbids.
        if let Err(e) = response.validate() {
            warn!(run = %lease.run_id, error = %e, "app returned an invalid envelope");
            self.handle_failure(lease, ErrorBody::coded("invalid_envelope", e.to_string()))
                .await?;
            return Ok(false);
        }

        // A *retryable* error is not a commit. Committing it would record the run
        // as failed, because the store treats an `error` op as terminal — the
        // distinction between "try again" and "give up" is the dispatcher's to
        // make, since only it holds the retry policy and the attempt count.
        //
        // Getting this the other way round is a quiet outage: every transient
        // gateway blip permanently fails a run that one retry would have saved.
        if let Some(Op::Error {
            retryable: true,
            error,
            ..
        }) = response.ops.first()
        {
            debug!(run = %lease.run_id, "app reported a retryable error; backing off");
            self.handle_failure(lease, error.clone()).await?;
            return Ok(false);
        }

        // Protocol §8.3.2: the server verifies a managed blob's size and digest
        // before it becomes readable, and "the commit" a mismatch fails is this
        // one — the op commit that puts the reference into the journal. On a
        // backend that presigns there is no other moment: the bytes went
        // straight from the app to the object store, so nothing else in the tree
        // ever calls `commit_blob` for them.
        //
        // What that cost before this call existed was not an unreadable
        // reference. Read URLs are minted by `attach_read_urls` on the
        // attempt-loading path, which looks at no state at all, so an unverified
        // blob read back perfectly well and the run worked. It was the 24-hour
        // sweep: the row was still `state='reserved'`, the collector takes
        // reserved rows past their window regardless of who references them, and
        // the run failed on a later attempt with nothing connecting it to a
        // collection that had run hours earlier. Unverified *and* readable is
        // also why `docs/adr/010-payload-tiering.md` rejected checking on first
        // read — by then the run has proceeded on a value nobody checked.
        let refused = match self.verify_blobs(&response.ops, &response.emit).await {
            BlobCheck::Verified => None,
            BlobCheck::Unavailable(error) => {
                debug!(run = %lease.run_id, "could not verify a blob reference; backing off");
                self.handle_failure(lease, error).await?;
                return Ok(false);
            }
            BlobCheck::Refused(error) => Some(error),
        };

        let commit = match &refused {
            // "the ops are discarded": the app's envelope is replaced by a
            // single non-retryable error op. The terminal write then still
            // happens inside the store's own commit transaction rather than in
            // a second place that also knows how to fail a run — and still
            // under the fence, so a superseded attempt cannot fail a run
            // another worker has already taken over.
            Some(error) => OpCommit {
                ops: vec![Op::Error {
                    retryable: false,
                    step: None,
                    error: error.clone(),
                }],
                emit: vec![],
            },
            None => OpCommit {
                ops: self.apply_timer_jitter(response.ops),
                emit: response.emit,
            },
        };

        match self.store.commit(lease.run_id, lease.fence, commit).await? {
            CommitOutcome::Committed => {
                match refused {
                    // Counted the way the store's own rejections are: the run is
                    // failed and its ops are gone, which is not a commit of the
                    // app's work in any sense the counter is used for.
                    Some(error) => {
                        warn!(
                            run = %lease.run_id,
                            code = error.code.as_deref().unwrap_or("unknown"),
                            "blob reference refused; ops discarded and the run failed non-retryably"
                        );
                        self.stats.lock().await.rejected += 1;
                    }
                    None => self.stats.lock().await.committed += 1,
                }
                Ok(true)
            }
            CommitOutcome::StaleFence => {
                // Expected under lease expiry: another worker took the run over.
                // The work happened twice; the record is written once.
                self.stats.lock().await.stale += 1;
                debug!(run = %lease.run_id, "discarded response from a superseded attempt");
                Ok(false)
            }
            CommitOutcome::Terminal => {
                debug!(run = %lease.run_id, "run already terminal; response discarded");
                Ok(false)
            }
            CommitOutcome::NoSuchRun => {
                warn!(run = %lease.run_id, "commit for a run that no longer exists");
                Ok(false)
            }
            // The store already failed the run inside the commit transaction, so
            // there is nothing left to write — only to count and to say which
            // rule was broken. Writing again here would race the store's own
            // terminal update.
            CommitOutcome::Rejected(code) => {
                warn!(run = %lease.run_id, code = %code, "envelope rejected; run failed non-retryably");
                self.stats.lock().await.rejected += 1;
                Ok(true)
            }
        }
    }

    /// Make every managed blob this envelope references readable, or say why not.
    ///
    /// `BlobStore::commit_blob` is the verification: it compares the stored
    /// object against the size and digest the app declared at reservation and,
    /// only if they agree, moves the blob to `committed`. Calling it here is what
    /// gives a presigning backend the commit step it otherwise never gets, and
    /// what stops a reference reaching the journal before anything checked it —
    /// `docs/adr/010-payload-tiering.md` rejected checking on first read for
    /// exactly that reason.
    ///
    /// A blob that is already committed — which on the relay path is every blob
    /// the dispatcher ever sees, because the transfer endpoint commits before the
    /// ops carrying the reference are returned — is a no-op inside `commit_blob`,
    /// not an error and not a second read of the bytes.
    ///
    /// References are verified one at a time. Each is a round trip on an object
    /// storage backend, so a fan-out would be faster for a single step result
    /// carrying many; it would also need a bound (a result with forty references
    /// otherwise opens forty concurrent requests and forty pool connections) and
    /// a rule for which of several simultaneous failures decides the run's fate.
    /// The dispatcher's concurrency is across runs, where it is already bounded
    /// by `batch`.
    async fn verify_blobs(&self, ops: &[Op], emit: &[Event]) -> BlobCheck {
        // No managed blobs configured: no walk, no calls, nothing to say.
        let Some(blobs) = &self.blobs else {
            return BlobCheck::Verified;
        };

        // §8.3.4: "every step result, run input and emitted event that contains
        // a `$blob`". The run inputs an envelope can carry are `invoke` and
        // `continue_as_new`, both ops.
        let mut ids = Vec::new();
        for op in ops {
            crate::blobs::op_blob_ids(op, &mut ids);
        }
        for event in emit {
            crate::blobs::event_blob_ids(event, &mut ids);
        }
        if ids.is_empty() {
            return BlobCheck::Verified;
        }
        ids.sort_unstable();
        ids.dedup();

        for id in ids {
            let Err(e) = blobs.commit_blob(id).await else {
                continue;
            };
            // A store that could not answer is a gateway failure and must not
            // fail a run: retry it. A store that answered is the app's problem.
            //
            // "There is no such blob" is on the non-retryable side by
            // consequence, not because retrying is mechanically impossible — it
            // is. `reserve` dedupes only on a *committed* digest, so a
            // re-executed step would get a fresh id and a fresh upload URL and
            // could well succeed. What it would cost is the point: the retry
            // here is not "retry the upload", it is up to `quarantine_after`
            // re-executions of a side effect the engine knows nothing about, to
            // rescue a claim the app was better placed to check than we are. A
            // conforming SDK already errors on a failed PUT rather than
            // returning the reference (`stepd-sdk/src/blobs.rs`), so most
            // references to an object that is not there mean a client that did
            // not, and twenty more attempts will not make it one. It keeps its
            // own code so an operator can tell it from a digest mismatch.
            //
            // Not the only cause, though, and the second one is a conforming
            // client losing a race. `PostgresBlobStore::reserve` dedupes on
            // `state='committed'` with no age or reference condition, and
            // `collect` deletes committed rows that no `blob_refs` row points
            // at once `committed_at` is past the window. An upload made outside
            // a step — which the SDK documents as supported, and which leaves
            // no journal reference to keep the bytes alive — can therefore be
            // deduplicated against a row the collector is about to take:
            // `reserve` answers `Deduplicated`, the app skips the upload
            // exactly as §8.3.2 requires it to, the collector removes the row
            // and the object, and this `commit_blob` finds nothing. A retry
            // would in fact rescue that one, because the row is gone and
            // `reserve` no longer dedupes. It is still on the non-retryable
            // side on balance: the cost of being wrong the other way is paid by
            // every app, on the far commoner cause, in side effects nobody
            // asked to repeat.
            if e.is_retryable() {
                return BlobCheck::Unavailable(ErrorBody::coded(
                    "blob_backend_unavailable",
                    format!("could not verify blob {id}: {e}"),
                ));
            }
            let (code, message) = match &e {
                Error::Config(msg) => ("blob_digest_mismatch", msg.clone()),
                Error::NotFound(msg) => ("no_such_blob", msg.clone()),
                // The backend answered and cannot do what the engine needs of
                // it — an object store that does not report the checksum an
                // S3 backend verifies from, say. Its own code because the
                // remedy is an operator's and has nothing to do with this run:
                // change the store, or relay bytes through the server instead.
                Error::Unsupported(msg) => ("blob_backend_incompatible", msg.clone()),
                other => ("blob_not_verified", other.to_string()),
            };
            return BlobCheck::Refused(ErrorBody::coded(code, message));
        }
        BlobCheck::Verified
    }

    /// Spread sleep wake-ups across a window.
    fn apply_timer_jitter(&self, ops: Vec<Op>) -> Vec<Op> {
        let jitter_ms = self.config.timer_jitter.num_milliseconds();
        if jitter_ms <= 0 {
            return ops;
        }
        ops.into_iter()
            .map(|op| match op {
                Op::Sleep { id, hash, until } => {
                    let offset = (rand::random::<f64>() * jitter_ms as f64) as i64;
                    Op::Sleep {
                        id,
                        hash,
                        until: until + Duration::milliseconds(offset),
                    }
                }
                other => other,
            })
            .collect()
    }

    /// Record a failed attempt, then either back off or quarantine.
    async fn handle_failure(&self, lease: &Lease, err: ErrorBody) -> Result<()> {
        self.stats.lock().await.failed += 1;
        let signature = err.signature();
        let delay = self
            .config
            .retry
            .backoff(lease.attempt, rand::random::<f64>());
        let record = self
            .store
            .record_failure(lease.run_id, &err, Utc::now() + delay)
            .await?;

        // Consecutive *identical* failures, not attempts. A run that fails five
        // different ways is a different problem from a poison pill, and
        // quarantining it hides the variety that would have explained it.
        if record.consecutive >= self.config.quarantine_after {
            warn!(
                run = %lease.run_id,
                consecutive = record.consecutive,
                attempts = record.attempts,
                %signature,
                "quarantining a run that has failed identically and repeatedly"
            );
            self.store.quarantine(lease.run_id, &signature).await?;
            self.stats.lock().await.quarantined += 1;
        }
        Ok(())
    }

    /// Drive until nothing is claimable, bounded so a test cannot spin forever.
    pub async fn run_until_idle(&self, max_ticks: u32) -> Result<u32> {
        for i in 0..max_ticks {
            if self.tick().await? == 0 {
                return Ok(i + 1);
            }
        }
        Ok(max_ticks)
    }
}

/// Convenience alias for a boxed dispatcher over trait objects.
pub type AnyDispatcher = Dispatcher<dyn StateStore, dyn Queue, dyn Transport>;

/// Errors surfaced when a run id is not found.
pub fn not_found(run: RunId) -> Error {
    Error::Store(format!("run {run} not found"))
}
