//! Engine errors.

/// Engine result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Anything that can go wrong inside the engine or its backing services.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The backing store failed.
    #[error("store: {0}")]
    Store(String),

    /// The app could not be reached, or returned a transport-level failure.
    #[error("transport: {0}")]
    Transport(String),

    /// The app returned a body the protocol forbids.
    #[error("protocol: {0}")]
    Protocol(#[from] stepd_proto::EnvelopeError),

    /// A response arrived for an attempt that had already been superseded.
    #[error("stale fence for run {0}")]
    StaleFence(uuid::Uuid),

    /// Configuration was missing or invalid.
    #[error("config: {0}")]
    Config(String),

    /// Serialisation failure.
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    /// The thing asked for is not there, and the store is sure of it.
    ///
    /// Distinct from `Store`, which also covers "the store could not answer
    /// at all" (a connection failure, a query error). This variant is for the
    /// store having answered cleanly with "no such row" — a blob whose
    /// reservation was collected, or one that was never reserved. Callers
    /// that need to tell "the store is broken" from "the store said no" have
    /// had no way to since both arrived as `Store(String)`; a caller matching
    /// on message text to recover the distinction is a contract the producer
    /// can break by rewording a log line, with nothing to fail when it does.
    #[error("not found: {0}")]
    NotFound(String),

    /// A backing service answered, and what it answered rules the operation
    /// out for good.
    ///
    /// Distinct from `Store`, which covers "the store could not answer" and is
    /// therefore retryable. This variant is for the store answering perfectly
    /// well with something the engine cannot work with, where nothing about
    /// waiting and asking again changes the reply: an object stored without
    /// the checksum an S3 backend verifies digests from will not grow one.
    /// Classified as `Store` — which is what it was — such a condition is
    /// retried up to `quarantine_after` times, and each of those retries is a
    /// re-execution of a side effect the engine knows nothing about.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    /// Whether retrying the same operation could plausibly succeed.
    ///
    /// Protocol and config errors are the app's or operator's fault and will
    /// fail identically forever, so retrying them only burns dispatch capacity.
    /// A `NotFound` is the same shape: the row does not exist, and retrying
    /// the identical read does not change that. So is `Unsupported`: the
    /// service answered, and its answer is a standing property of that
    /// service, not a moment in it.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Store(_) | Error::Transport(_))
    }
}
