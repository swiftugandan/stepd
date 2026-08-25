//! # stepd Rust SDK
//!
//! Write a durable workflow as an ordinary `async fn`, mount it on an HTTP
//! server, and let stepd drive it.
//!
//! ```no_run
//! use stepd_sdk::prelude::*;
//!
//! # #[derive(serde::Serialize, serde::Deserialize)] struct Receipt { tx: String }
//! async fn order_fulfilment(ctx: &Ctx) -> StepResult<Receipt> {
//!     let tx: String = ctx.step("charge", || async {
//!         Ok("ch_1".to_string())
//!     }).await?;
//!
//!     ctx.sleep("cooldown", chrono::Duration::days(1)).await?;
//!
//!     let approval: Option<serde_json::Value> =
//!         ctx.wait_event("approval", "order.approved")
//!            .timeout(chrono::Duration::days(7))
//!            .await?;
//!     let _ = approval;
//!
//!     Ok(Receipt { tx })
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let app = App::new("billing", "https://billing.internal/stepd")
//!     .signing_key(b"secret".to_vec())
//!     .function(
//!         Function::new("order-fulfilment")
//!             .on_event("order.created")
//!             .key("'order:' + string(event.data.order_id)")
//!             .run(order_fulfilment),
//!     );
//!
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
//! axum::serve(listener, app.router()).await?;
//! # Ok(()) }
//! ```
//!
//! ## What is worth knowing before writing a handler
//!
//! * **Everything outside a step may run many times.** Each attempt replays the
//!   handler from the top; only recorded steps are skipped. Side effects belong
//!   inside `ctx.step`.
//! * **Propagate step results with `?`.** Absorbing one — `let _ =`, `.ok()`,
//!   `.unwrap_or_default()` — also absorbs the control-flow signal, and the
//!   handler then runs against a state that does not exist. The SDK detects this
//!   and fails the attempt with `swallowed_halt` rather than committing a `done`
//!   for a run whose middle never happened.
//! * **Step ids are the identity.** Renaming one orphans its recorded result and
//!   re-executes the side effect. Adding, removing and reordering *around* a
//!   step is safe.
//! * **Concurrency is safe by construction.** `ctx.step` claims its hash when
//!   called, not when polled, so `join!` and friends cannot reorder hashes. See
//!   `stepd_sdk_core` for why that matters.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod executor;
pub mod function;
pub mod serve;
pub mod testing;

pub mod blobs;

pub use blobs::{Blob, BlobError, Blobs};
pub use executor::LocalExecutor;
pub use function::{App, CronOptions, Function, Trigger};
pub use serve::{verify_request, NonceCache, ServeError};

pub use stepd_sdk_core::wf;
pub use stepd_sdk_core::{
    workflow, BoxFut, Ctx, Halt, Handler, PassOutcome, StepError, StepResult,
};

/// The SDK's version string, sent in the `stepd-sdk` header.
pub const SDK_VERSION: &str = concat!("rust/", env!("CARGO_PKG_VERSION"));

/// Everything a handler module normally needs.
pub mod prelude {
    pub use crate::wf;
    pub use crate::{App, Blob, Blobs, CronOptions, Ctx, Function, StepError, StepResult};
}
