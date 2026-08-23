//! # stepd engine
//!
//! The dispatch loop and load-protection policy, generic over the storage,
//! queue and transport interfaces in [`traits`].
//!
//! This crate must not depend on any concrete backend. A CI lint enforces it,
//! because the moment the engine reaches for `sqlx` directly, swapping a backing
//! service stops being a new crate and becomes a rewrite.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod cron;
pub mod dispatcher;
pub mod error;
pub mod housekeeper;
pub mod policy;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod traits;

pub use cron::{
    decide as cron_decide, CatchUp, CatchUpPolicy, CronDecision, CronError, Schedule, SkipReason,
};
pub use dispatcher::{DispatchConfig, DispatchStats, Dispatcher, TargetResolver};
pub use error::{Error, Result};
pub use housekeeper::{Housekeeper, KeeperConfig, KeeperStats};
pub use policy::{Circuit, CircuitBreaker, RetryPolicy};
pub use traits::*;
