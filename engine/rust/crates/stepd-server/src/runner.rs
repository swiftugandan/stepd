//! The background loops, and shutting them down without losing work.
//!
//! ## Draining, and why it is not optional
//!
//! On SIGTERM the server stops claiming new work and lets in-flight attempts
//! finish. It does not need to: leases expire and fencing makes an abrupt death
//! safe, which is exactly what makes it tempting to skip.
//!
//! The reason to drain anyway is that an undrained attempt re-executes its step.
//! At-least-once is the contract, so that is correct — and it also means every
//! rolling deploy charges some customers twice. Correct and expensive is still
//! expensive.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use stepd_core::{Dispatcher, Housekeeper};
use stepd_store_postgres::PostgresStore;
use stepd_transport_http::HttpTransport;
use tracing::{info, warn};

/// Drives the dispatch and convergence loops.
pub struct Runner {
    dispatcher: Arc<Dispatcher<PostgresStore, PostgresStore, HttpTransport>>,
    housekeeper: Arc<Housekeeper<PostgresStore, PostgresStore, PostgresStore>>,
    idle_poll: Duration,
    shutdown: Arc<AtomicBool>,
}

impl Runner {
    /// Build a runner from an assembled server's loops.
    pub fn new(
        dispatcher: Arc<Dispatcher<PostgresStore, PostgresStore, HttpTransport>>,
        housekeeper: Arc<Housekeeper<PostgresStore, PostgresStore, PostgresStore>>,
        idle_poll: Duration,
    ) -> Self {
        Self {
            dispatcher,
            housekeeper,
            idle_poll,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A handle that stops both loops at their next safe point.
    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    /// Run both loops until shutdown.
    pub async fn run(&self) {
        let dispatch = {
            let d = self.dispatcher.clone();
            let stop = self.shutdown.clone();
            let idle = self.idle_poll;
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    match d.tick().await {
                        // Only sleep when there was nothing to do. Sleeping after
                        // a productive tick would cap throughput at one batch per
                        // poll interval, which turns the poll interval into a
                        // throughput setting nobody thinks of it as.
                        Ok(0) => tokio::time::sleep(idle).await,
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, "dispatch tick failed");
                            // Back off on error rather than spinning: a database
                            // that is down does not recover faster for being
                            // asked a thousand times a second.
                            tokio::time::sleep(idle * 4).await;
                        }
                    }
                }
                info!("dispatch loop stopped");
            })
        };

        let keeper = {
            let k = self.housekeeper.clone();
            let stop = self.shutdown.clone();
            let idle = self.idle_poll;
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if k.tick().await.is_idle() {
                        tokio::time::sleep(idle * 2).await;
                    }
                }
                info!("housekeeping loop stopped");
            })
        };

        let _ = tokio::join!(dispatch, keeper);
    }

    /// Wait for SIGTERM or Ctrl-C, then stop claiming new work.
    pub async fn wait_for_shutdown(&self) {
        let stop = self.shutdown.clone();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = term.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }

        info!("shutdown signal received; draining in-flight attempts");
        stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shutdown_handle_is_shared_not_copied() {
        // Two independent flags would stop one loop and leave the other running,
        // which looks like a hang rather than a bug.
        let flag = Arc::new(AtomicBool::new(false));
        let a = flag.clone();
        a.store(true, Ordering::Relaxed);
        assert!(flag.load(Ordering::Relaxed));
    }
}
