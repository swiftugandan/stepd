//! Running handlers where a dropped connection cannot drop a running step.
//!
//! ## The rule this exists to honour
//!
//! Protocol §7.1.1: *an SDK MUST NOT cancel or drop a running step future in
//! order to meet the attempt deadline.* Dropping a future mid-`await` in Rust
//! leaves the external effect in an indeterminate state **and** loses the result
//! that would have made it durable. The server treats a missing response as
//! `unknown` and retries; the step re-executes and is recorded once. That is
//! recoverable. A half-executed charge whose result was thrown away is not.
//!
//! ## Why an ordinary axum handler is not enough
//!
//! Hyper drops the request future when the client disconnects. If the handler
//! ran inline in that future, a server that timed out and closed the connection
//! would drop the user's `charge()` mid-flight — the exact thing the protocol
//! forbids, arriving through the transport rather than through our own code.
//!
//! So the pass runs on a detached task, and the request future only *waits* for
//! its result. Dropping the request drops the wait, never the work.
//!
//! ## Why it needs its own threads
//!
//! [`Ctx`](stepd_sdk_core::Ctx) is deliberately `!Send`: that is what makes
//! `tokio::spawn(async move { ctx.step(..) })` a compile error rather than a
//! silent hash corruption. The same property means the pass cannot be spawned
//! onto the shared multi-threaded runtime. Each worker here owns a
//! current-thread runtime and a `LocalSet`; what crosses the channel is the
//! *attempt* and the handler — both `Send` — and the `Ctx` is constructed on the
//! far side, never moved.

use std::sync::Arc;
use stepd_proto::Attempt;
use stepd_sdk_core::{run_pass, BoxFut, Ctx, PassOutcome};
use tokio::sync::{mpsc, oneshot};

/// A handler with its result type erased to JSON.
///
/// Erasure happens once, at registration, so the dispatch path is not generic
/// over every workflow's return type — an app with forty functions would
/// otherwise monomorphise forty copies of the whole serve stack.
pub type ErasedHandler =
    Arc<dyn for<'a> Fn(&'a Ctx) -> BoxFut<'a, serde_json::Value> + Send + Sync>;

/// One unit of work for a local worker.
struct Job {
    handler: ErasedHandler,
    attempt: Attempt,
    reply: oneshot::Sender<PassOutcome>,
    /// Extra channel carrying the emitted events and logs the pass produced,
    /// which live on the `Ctx` rather than in the outcome.
    extras: oneshot::Sender<Extras>,
}

/// What a pass produced besides its ops.
#[derive(Debug, Default)]
pub struct Extras {
    /// Events to publish transactionally with the ops.
    pub emit: Vec<stepd_proto::Event>,
    /// Diagnostic lines.
    pub logs: Vec<serde_json::Value>,
    /// Recorded hashes this pass never encountered — a renamed or removed step.
    pub orphaned: usize,
}

/// A pool of single-threaded workers that run passes to completion.
#[derive(Clone)]
pub struct LocalExecutor {
    tx: mpsc::UnboundedSender<Job>,
}

impl LocalExecutor {
    /// Start `threads` workers.
    pub fn new(threads: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Job>();
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        for i in 0..threads.max(1) {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("stepd-handler-{i}"))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("worker runtime");
                    let local = tokio::task::LocalSet::new();
                    local.block_on(&rt, async move {
                        loop {
                            let job = { rx.lock().await.recv().await };
                            let Some(job) = job else { break };

                            // Detached: if the caller has gone away, this task
                            // still finishes. That is the whole point — the
                            // result of a step that already ran must not be
                            // discarded because a socket closed.
                            tokio::task::spawn_local(async move {
                                let ctx = Ctx::new(
                                    job.attempt.run.clone(),
                                    job.attempt.steps.clone(),
                                    job.attempt.attempt as u64,
                                )
                                .with_attempt(job.attempt.attempt);
                                // `&dyn Fn` satisfies the higher-ranked handler
                                // bound; a closure wrapping it would not, for
                                // the reason `stepd_sdk_core::workflow` exists.
                                let outcome = run_pass(&ctx, &*job.handler).await;
                                let extras = Extras {
                                    emit: ctx.take_emit(),
                                    logs: ctx.take_logs(),
                                    orphaned: ctx.orphaned().len(),
                                };
                                // Both sends may fail if the request was
                                // abandoned. That is expected, not an error.
                                let _ = job.reply.send(outcome);
                                let _ = job.extras.send(extras);
                            });
                        }
                    });
                })
                .expect("spawn handler worker");
        }

        Self { tx }
    }

    /// Run one pass. The returned future may be dropped; the pass will not be.
    pub async fn run(
        &self,
        handler: ErasedHandler,
        attempt: Attempt,
    ) -> Option<(PassOutcome, Extras)> {
        let (reply, reply_rx) = oneshot::channel();
        let (extras, extras_rx) = oneshot::channel();
        self.tx
            .send(Job {
                handler,
                attempt,
                reply,
                extras,
            })
            .ok()?;
        let outcome = reply_rx.await.ok()?;
        let extras = extras_rx.await.unwrap_or_default();
        Some((outcome, extras))
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        // One worker per core: a pass is CPU-light but may block on user I/O for
        // as long as a step takes, so the pool exists to keep concurrent attempts
        // from queueing behind each other rather than to use the CPU.
        Self::new(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        )
    }
}
