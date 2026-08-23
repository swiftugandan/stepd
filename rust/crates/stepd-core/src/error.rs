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
}

impl Error {
    /// Whether retrying the same operation could plausibly succeed.
    ///
    /// Protocol and config errors are the app's or operator's fault and will
    /// fail identically forever, so retrying them only burns dispatch capacity.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Store(_) | Error::Transport(_))
    }
}
