//! # stepd protocol
//!
//! Wire types for the stepd SDK protocol v1, plus the two algorithms every
//! implementation must agree on byte-for-byte: the step hash and the request
//! signature.
//!
//! This crate has **no I/O, no async runtime and no database**. That is a
//! load-bearing constraint, not an accident: it is what lets a third party build
//! an alternative server or SDK against the published spec without inheriting
//! our engine. A dependency lint in CI fails the build if that changes.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod hash;
pub mod manifest;
pub mod ops;
pub mod sig;
pub mod types;

pub use hash::{step_hash, OccurrenceCounter};
pub use manifest::*;
pub use ops::{Attempt, AttemptResponse, EnvelopeError, Op, RecordedStep, RunContext};
pub use sig::{sign, verify, SignatureError};
pub use types::*;

/// Protocol major version spoken by this crate.
pub const PROTOCOL_VERSION: &str = "1";

/// Header carrying the protocol version.
pub const HEADER_PROTOCOL: &str = "stepd-protocol";
/// Header carrying the HMAC signature.
pub const HEADER_SIGNATURE: &str = "stepd-signature";
/// Header carrying the per-request nonce (replay defence).
pub const HEADER_NONCE: &str = "stepd-nonce";
/// Header carrying the run id, for logging and routing.
pub const HEADER_RUN_ID: &str = "stepd-run-id";
/// Header carrying the attempt number.
pub const HEADER_ATTEMPT: &str = "stepd-attempt";
/// Header carrying the fencing token.
pub const HEADER_FENCE: &str = "stepd-fence";
/// Header carrying the SDK language and version.
pub const HEADER_SDK: &str = "stepd-sdk";
