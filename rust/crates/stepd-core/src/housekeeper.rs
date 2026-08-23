//! The convergence loop.
//!
//! Everything here must make progress when *no app is reachable at all*. That is
//! why it is a separate loop from dispatch rather than a step inside `tick`: if
//! timers, signal delivery, child resolution and lease reclamation ran only when
//! an attempt could be delivered, a total app outage would also stop the system
//! healing itself — and the backlog an operator has to clear after the outage
//! would be far larger than the outage caused.

use crate::traits::{CronStore, CronSweep, Housekeeping, TimerStore};
use crate::Result;
use chrono::Utc;
use std::sync::Arc;
use tracing::{debug, warn};

/// How much work each sweep will do in one pass.
#[derive(Debug, Clone)]
pub struct KeeperConfig {
    /// Timers fired per pass.
    pub timers: i64,
    /// Signals delivered per pass.
    pub signals: i64,
    /// Parent steps resolved per pass.
    pub children: i64,
    /// Leases reclaimed per pass.
    pub leases: i64,
    /// Cron schedules examined per pass.
    pub cron: i64,
    /// How long an unfinished blob reservation survives before collection.
    ///
    /// Generous on purpose: an app that reserved and then took a long time to
    /// upload is doing exactly what the two-phase protocol asks of it, and a
    /// window that expires mid-upload turns a slow network into data loss.
    pub blob_reservation_ttl: chrono::Duration,
    /// Cron ledger rows trimmed per pass.
    ///
    /// Separate from `cron` and much larger: trimming is cheap bulk deletion of
    /// rows nothing will read again, while a claimed schedule holds a row lock
    /// while its plan is computed and applied.
    pub cron_fires: i64,
}

impl Default for KeeperConfig {
    fn default() -> Self {
        // Deliberately bounded: an unbounded sweep holds locks for as long as the
        // backlog demands, and a backlog is exactly when the rest of the system
        // can least afford to wait behind it.
        Self {
            timers: 200,
            signals: 200,
            children: 200,
            leases: 100,
            // Smaller than the others on purpose. Each claimed schedule holds a
            // row lock while its plan is applied, and a schedule that misses one
            // pass is picked up on the next one a second later — whereas a
            // sweep that claims a thousand rows makes every other scheduler
            // replica wait behind it for no gain.
            cron: 50,
            cron_fires: 500,
            blob_reservation_ttl: chrono::Duration::hours(24),
        }
    }
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct KeeperStats {
    /// Runs woken by a timer.
    pub timers_fired: u64,
    /// Signals delivered.
    pub signals_delivered: u64,
    /// Parent steps resolved from finished children.
    pub children_resolved: u64,
    /// Leases returned to the queue.
    pub leases_reclaimed: u64,
    /// What the cron sweep did.
    pub cron: CronSweep,
    /// Cron ledger rows trimmed.
    pub cron_fires_trimmed: u64,
    /// Blobs collected.
    pub blobs_collected: u64,
}

impl KeeperStats {
    /// Whether the pass did anything at all.
    ///
    /// Measured by work *done*, not by the struct differing from its default.
    /// The cron sweep reports how many schedules it considered, and a pass that
    /// looked at fifty schedules and fired none has done nothing — reporting it
    /// as activity makes [`Housekeeper::run_until_idle`] spin until its pass
    /// budget runs out, every time, on any system with a cron schedule in it.
    pub fn is_idle(&self) -> bool {
        self.total() == 0
    }

    /// Total units of work.
    pub fn total(&self) -> u64 {
        self.timers_fired
            + self.signals_delivered
            + self.children_resolved
            + self.leases_reclaimed
            + self.cron.fired
            + self.cron.skipped
            // Pausing a broken schedule is a state change and belongs here.
            // `considered` and `duplicates` do not: looking is not work, and a
            // duplicate means another replica did it.
            + self.cron.unschedulable
            + self.cron_fires_trimmed
            + self.blobs_collected
    }
}

/// Runs the convergence sweeps.
pub struct Housekeeper<H, T, C> {
    keeping: Arc<H>,
    timers: Arc<T>,
    cron: Arc<C>,
    config: KeeperConfig,
}

impl<H, T, C> Housekeeper<H, T, C>
where
    H: Housekeeping,
    T: TimerStore,
    C: CronStore,
{
    /// Assemble from its components.
    pub fn new(keeping: Arc<H>, timers: Arc<T>, cron: Arc<C>, config: KeeperConfig) -> Self {
        Self {
            keeping,
            timers,
            cron,
            config,
        }
    }

    /// One pass of every sweep.
    ///
    /// A failure in one sweep must not stop the others: reclaiming leases matters
    /// most precisely when something else is broken, so each sweep is isolated
    /// and its error logged rather than propagated.
    pub async fn tick(&self) -> KeeperStats {
        let mut stats = KeeperStats::default();

        match self.timers.fire_due(Utc::now(), self.config.timers).await {
            Ok(n) => stats.timers_fired = n,
            Err(e) => warn!(error = %e, "timer sweep failed"),
        }
        match self.keeping.drain_signals(self.config.signals).await {
            Ok(n) => stats.signals_delivered = n,
            Err(e) => warn!(error = %e, "signal delivery failed"),
        }
        match self
            .keeping
            .resolve_finished_children(self.config.children)
            .await
        {
            Ok(n) => stats.children_resolved = n,
            Err(e) => warn!(error = %e, "child resolution sweep failed"),
        }
        match self
            .keeping
            .reclaim_expired_leases(self.config.leases)
            .await
        {
            Ok(n) => stats.leases_reclaimed = n,
            Err(e) => warn!(error = %e, "lease reclamation failed"),
        }

        // Cron lives here rather than in the dispatch loop for the reason at the
        // top of this file: a schedule must fire while the app is unreachable.
        // Firing creates a queued run; whether anything can execute it yet is a
        // separate question, and conflating them would mean an app outage
        // silently swallowed every occurrence it spanned rather than leaving a
        // backlog the operator can see and decide about.
        match self.cron.sweep(self.config.cron).await {
            Ok(s) => {
                if s.unschedulable > 0 {
                    // A paused schedule is a job that has stopped. It cannot be
                    // left at debug level next to the routine counters.
                    warn!(
                        count = s.unschedulable,
                        "cron schedules paused as unschedulable; they will not fire until fixed"
                    );
                }
                stats.cron = s;
            }
            Err(e) => warn!(error = %e, "cron sweep failed"),
        }
        match self.cron.trim_fires(self.config.cron_fires).await {
            Ok(n) => stats.cron_fires_trimmed = n,
            Err(e) => warn!(error = %e, "cron ledger trim failed"),
        }

        // Blob collection last. It deletes bytes, so it runs after everything
        // that might have recorded a reference to them in this same pass —
        // ordering that costs nothing and removes a window that would be
        // extremely hard to reproduce.
        match self
            .keeping
            .collect_blobs(Utc::now() - self.config.blob_reservation_ttl)
            .await
        {
            Ok(n) => {
                if n > 0 {
                    debug!(collected = n, "blob collection");
                }
                stats.blobs_collected = n;
            }
            Err(e) => warn!(error = %e, "blob collection failed"),
        }

        if !stats.is_idle() {
            debug!(?stats, "housekeeping pass");
        }
        stats
    }

    /// Sweep until a pass finds nothing to do, bounded so a test cannot spin.
    pub async fn run_until_idle(&self, max_passes: u32) -> Result<u32> {
        for i in 0..max_passes {
            if self.tick().await.is_idle() {
                return Ok(i + 1);
            }
        }
        Ok(max_passes)
    }
}
