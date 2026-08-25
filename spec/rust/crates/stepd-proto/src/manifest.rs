//! Registration, blob-reserve and conformance documents — the rest of the wire.
//!
//! Attempt/op/event live in [`crate::ops`]. These types are the other messages
//! the published schemas describe: an app manifest, a function config, a blob
//! reservation, a Problem Details body, and a conformance manifest.
//!
//! No I/O. Validation that does not need a database — cron `singleton` requiring
//! `run_key`, at least one trigger — lives here so every consumer agrees.

use crate::iso8601_seconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Why a registration or reserve document is not well-formed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManifestError {
    /// A function with nothing that can start a run.
    #[error("function `{0}` has no triggers")]
    NoTriggers(String),
    /// Overlap control with nothing to be exclusive on.
    #[error(
        "function `{function}` cron trigger {index}: singleton requires run_key; \
         without one there is nothing for the schedule to be exclusive on"
    )]
    SingletonWithoutKey {
        /// Function id.
        function: String,
        /// Index in `triggers`.
        index: usize,
    },
    /// A duration this crate does not interpret.
    #[error("function `{function}`: `{field}` `{value}` is not an ISO 8601 duration of weeks, days, hours, minutes or seconds")]
    BadDuration {
        /// Function id.
        function: String,
        /// Field path.
        field: String,
        /// The value that did not parse.
        value: String,
    },
    /// `catchup_limit` outside the closed range the schema states.
    #[error("function `{function}` cron trigger {index}: catchup_limit {limit} is outside 1–1000")]
    CatchUpLimit {
        /// Function id.
        function: String,
        /// Index in `triggers`.
        index: usize,
        /// The value that was out of range.
        limit: i32,
    },
}

/// What an app registers with the server (protocol §3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppManifest {
    /// Protocol major version.
    pub protocol: String,
    /// Stable app identity within the namespace.
    pub app_id: String,
    /// Endpoint the server will call.
    pub url: String,
    /// `<language>/<version>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk: Option<String>,
    /// Digest over the functions array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// Deployment environment label, presentational.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Features this app implements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<Capability>>,
    /// Function configurations.
    #[serde(default)]
    pub functions: Vec<FunctionConfig>,
}

impl AppManifest {
    /// Reject a document the schemas would also reject for semantic reasons.
    pub fn validate(&self) -> Result<(), ManifestError> {
        for f in &self.functions {
            f.validate()?;
        }
        Ok(())
    }
}

/// A feature an app may declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Structured parallel ops.
    Parallel,
    /// Managed blobs.
    Blobs,
    /// `signal` ops.
    Signal,
    /// `invoke` ops.
    Invoke,
    /// Batched event triggers.
    Batch,
    /// Compensation via `on_cancel`.
    Cancel,
    /// Streaming attempt bodies.
    Streaming,
}

/// A function definition (protocol §3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    /// Stable identity.
    pub id: String,
    /// Opaque; informational.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Console label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Longer description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// How runs of this function start.
    pub triggers: Vec<Trigger>,
    /// CEL → string business key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
    /// CEL → ingest idempotency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_expr: Option<String>,
    /// CEL → integer priority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_expr: Option<String>,
    /// Concurrency limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<Vec<ConcurrencyLimit>>,
    /// Rate limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimit>,
    /// Debounce.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debounce: Option<Debounce>,
    /// Event batching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<Batch>,
    /// Retry policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<Retries>,
    /// Deadlines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeouts: Option<Timeouts>,
    /// Events that cancel an in-flight run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_on: Option<Vec<CancelOn>>,
    /// Function to invoke when this one fails terminally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<String>,
    /// At most one active run per key.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub singleton: bool,
    /// JSON Schema for invoke input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<serde_json::Value>,
    /// JSON Schema for output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<serde_json::Value>,
    /// Inbox bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox: Option<Inbox>,
    /// Engine limits for this function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<Limits>,
    /// Whether a cancelled run gets a compensation attempt.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub on_cancel: bool,
}

impl FunctionConfig {
    /// Semantic checks the JSON Schema cannot state as types.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.triggers.is_empty() {
            return Err(ManifestError::NoTriggers(self.id.clone()));
        }
        for (index, t) in self.triggers.iter().enumerate() {
            if let Trigger::Cron {
                singleton,
                run_key,
                misfire_window,
                catchup_limit,
                ..
            } = t
            {
                if *singleton && run_key.is_none() {
                    return Err(ManifestError::SingletonWithoutKey {
                        function: self.id.clone(),
                        index,
                    });
                }
                if let Some(limit) = catchup_limit {
                    if !(1..=1000).contains(limit) {
                        return Err(ManifestError::CatchUpLimit {
                            function: self.id.clone(),
                            index,
                            limit: *limit,
                        });
                    }
                }
                if let Some(w) = misfire_window {
                    self.check_duration("misfire_window", w)?;
                }
            }
        }
        if let Some(r) = &self.rate_limit {
            self.check_duration("rate_limit.period", &r.period)?;
        }
        if let Some(d) = &self.debounce {
            self.check_duration("debounce.period", &d.period)?;
            if let Some(m) = &d.max_delay {
                self.check_duration("debounce.max_delay", m)?;
            }
        }
        if let Some(b) = &self.batch {
            if let Some(t) = &b.timeout {
                self.check_duration("batch.timeout", t)?;
            }
        }
        if let Some(r) = &self.retries {
            if let Some(i) = &r.initial {
                self.check_duration("retries.initial", i)?;
            }
            if let Some(m) = &r.max {
                self.check_duration("retries.max", m)?;
            }
        }
        if let Some(t) = &self.timeouts {
            if let Some(a) = &t.attempt {
                self.check_duration("timeouts.attempt", a)?;
            }
            if let Some(r) = &t.run {
                self.check_duration("timeouts.run", r)?;
            }
            if let Some(s) = &t.start {
                self.check_duration("timeouts.start", s)?;
            }
        }
        Ok(())
    }

    fn check_duration(&self, field: &str, value: &str) -> Result<(), ManifestError> {
        iso8601_seconds(value)
            .filter(|&s| s > 0)
            .map(|_| ())
            .ok_or_else(|| ManifestError::BadDuration {
                function: self.id.clone(),
                field: field.into(),
                value: value.into(),
            })
    }

    /// Every CEL source in this config, named for error messages.
    pub fn cel_sources(&self) -> Vec<(&'static str, &str)> {
        let mut out = Vec::new();
        if let Some(k) = &self.key_expr {
            out.push(("key_expr", k.as_str()));
        }
        if let Some(k) = &self.idempotency_expr {
            out.push(("idempotency_expr", k.as_str()));
        }
        if let Some(k) = &self.priority_expr {
            out.push(("priority_expr", k.as_str()));
        }
        for t in &self.triggers {
            if let Trigger::Event { expr: Some(e), .. } = t {
                out.push(("trigger expr", e.as_str()));
            }
        }
        if let Some(cs) = &self.concurrency {
            for c in cs {
                if let Some(k) = &c.key_expr {
                    out.push(("concurrency.key_expr", k.as_str()));
                }
            }
        }
        if let Some(r) = &self.rate_limit {
            if let Some(k) = &r.key_expr {
                out.push(("rate_limit.key_expr", k.as_str()));
            }
        }
        if let Some(d) = &self.debounce {
            if let Some(k) = &d.key_expr {
                out.push(("debounce.key_expr", k.as_str()));
            }
        }
        if let Some(b) = &self.batch {
            if let Some(k) = &b.key_expr {
                out.push(("batch.key_expr", k.as_str()));
            }
        }
        if let Some(cs) = &self.cancel_on {
            for c in cs {
                if let Some(e) = &c.expr {
                    out.push(("cancel_on expr", e.as_str()));
                }
            }
        }
        out
    }
}

/// How a run of this function starts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// An ingested event.
    Event {
        /// CloudEvents `type`.
        event: String,
        /// CEL predicate over `event`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expr: Option<String>,
    },
    /// A schedule.
    Cron {
        /// Five-field cron expression.
        cron: String,
        /// IANA zone. Defaults to UTC.
        #[serde(default = "utc_tz", skip_serializing_if = "is_utc")]
        tz: String,
        /// Misfire policy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catchup: Option<CatchUp>,
        /// Cap on `all` recovery fires.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catchup_limit: Option<i32>,
        /// Occurrences older than this are never caught up.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        misfire_window: Option<String>,
        /// Skip a fire while `run_key` is still active.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        singleton: bool,
        /// Literal run key for fires from this schedule.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_key: Option<String>,
    },
    /// Called as a child by another function.
    Invoke,
}

fn utc_tz() -> String {
    "UTC".into()
}
fn is_utc(s: &str) -> bool {
    s == "UTC"
}

/// Misfire policy for a cron trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchUp {
    /// Fire once on recovery.
    One,
    /// Fire nothing missed.
    Skip,
    /// Fire every missed occurrence, capped by `catchup_limit`.
    All,
}

impl CatchUp {
    /// Wire form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Skip => "skip",
            Self::All => "all",
        }
    }
}

/// A concurrency gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyLimit {
    /// Maximum concurrent runs.
    pub limit: i32,
    /// CEL key; absent means the whole function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
    /// How widely the limit applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ConcurrencyScope>,
}

/// Breadth of a concurrency limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyScope {
    /// This function.
    Function,
    /// Every function in the namespace.
    Namespace,
}

/// A token bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    /// Tokens per period.
    pub limit: i32,
    /// Bucket period.
    pub period: String,
    /// CEL key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
    /// Burst size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst: Option<i32>,
}

/// Collapse rapid events into one run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Debounce {
    /// Quiet period.
    pub period: String,
    /// CEL key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
    /// Maximum wait before firing anyway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delay: Option<String>,
}

/// Batch inbound events into one run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    /// Maximum events per batch.
    pub max_size: i32,
    /// Flush after this long even if under `max_size`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    /// CEL key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
}

/// Retry policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retries {
    /// Attempts before the run fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i32>,
    /// Backoff shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<Backoff>,
    /// First interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial: Option<String>,
    /// Ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    /// Spread retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<bool>,
}

/// Backoff strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backoff {
    /// Exponential.
    Exponential,
    /// Linear.
    Linear,
    /// Constant.
    Constant,
}

/// Deadlines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timeouts {
    /// One attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<String>,
    /// The whole run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<String>,
    /// Time to start after the trigger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
}

/// An event that cancels an in-flight run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOn {
    /// Event type.
    pub event: String,
    /// CEL predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
    /// How long to listen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

/// Inbox bounds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inbox {
    /// Maximum buffered events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_entries: Option<i32>,
    /// What happens at the ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_overflow: Option<InboxOverflow>,
}

/// Inbox overflow policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboxOverflow {
    /// Drop the oldest entry.
    DropOldest,
    /// Fail the run.
    FailRun,
}

/// Per-function engine limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum invoke depth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invoke_depth: Option<i32>,
    /// Maximum live children.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invoke_fanout: Option<i32>,
    /// Maximum `continue_as_new` chain length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_length: Option<i32>,
    /// Maximum recorded steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<i32>,
}

/// `POST /v1/blobs:reserve` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobReserveRequest {
    /// Run the blob will belong to.
    pub run_id: Uuid,
    /// Step this will attach to. Informational.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    /// Declared size in bytes.
    pub size: i64,
    /// Lowercase hex SHA-256.
    pub sha256: String,
    /// Media type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Original filename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
}

/// `POST /v1/blobs:reserve` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobReserveResponse {
    /// Assigned id.
    pub blob_id: Uuid,
    /// True when the digest already exists; `upload_url` is then absent.
    pub deduplicated: bool,
    /// Write-scoped URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_url: Option<String>,
    /// Method for the upload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Headers the app must send verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, String>>,
    /// When the URL stops working.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// True when `upload_url` is the server's fallback relay.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub relay: bool,
}

/// RFC 9457 Problem Details, as stepd puts it on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProblemBody {
    /// Problem type URI.
    #[serde(rename = "type", default = "about_blank")]
    pub problem_type: String,
    /// Short summary.
    pub title: String,
    /// HTTP status.
    pub status: u16,
    /// Occurrence-specific detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Occurrence URI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Stable machine-readable code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Related run, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    /// Related op, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
}

fn about_blank() -> String {
    "about:blank".into()
}

/// `GET /.well-known/stepd-conformance`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConformanceManifest {
    /// Protocol major.
    pub protocol: String,
    /// Informational language/version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk: Option<String>,
    /// Suites this app implements.
    pub suites: Vec<ConformanceSuite>,
    /// Hazards made unrepresentable rather than detected at run time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statically_prevented: Vec<StaticHazard>,
}

/// A conformance suite (protocol §12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceSuite {
    /// Memoised steps are not re-executed.
    Memoization,
    /// Occurrence counters are stable across attempts.
    Loops,
    /// Hashes assigned in program order.
    Determinism,
    /// Parallel ops in one envelope.
    Parallel,
    /// Durable timers.
    Sleep,
    /// Event waits.
    Wait,
    /// Lost-signal race closed.
    EarlySignal,
    /// Child runs.
    Invoke,
    /// Cancellation cascade.
    Cascade,
    /// Successor with empty state.
    ContinueAsNew,
    /// Retryable vs terminal errors.
    Errors,
    /// Compensation path.
    Cancel,
    /// Abandoned steps re-execute once.
    Abandonment,
    /// Managed blobs.
    Blobs,
    /// External `$ref` pass-through.
    Refs,
    /// Stale fence discarded.
    Fencing,
    /// Request signatures.
    Signature,
    /// Paginated journals.
    Truncation,
    /// Cron schedules.
    Cron,
}

impl ConformanceSuite {
    /// Wire form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memoization => "memoization",
            Self::Loops => "loops",
            Self::Determinism => "determinism",
            Self::Parallel => "parallel",
            Self::Sleep => "sleep",
            Self::Wait => "wait",
            Self::EarlySignal => "early_signal",
            Self::Invoke => "invoke",
            Self::Cascade => "cascade",
            Self::ContinueAsNew => "continue_as_new",
            Self::Errors => "errors",
            Self::Cancel => "cancel",
            Self::Abandonment => "abandonment",
            Self::Blobs => "blobs",
            Self::Refs => "refs",
            Self::Fencing => "fencing",
            Self::Signature => "signature",
            Self::Truncation => "truncation",
            Self::Cron => "cron",
        }
    }
}

/// A hazard an implementation makes unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticHazard {
    /// Occurrence claimed off the sequential path.
    OffpathClaim,
}

impl StaticHazard {
    /// Wire form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OffpathClaim => "offpath_claim",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleton_without_run_key_is_refused() {
        let f: FunctionConfig = serde_json::from_value(serde_json::json!({
            "id": "nightly",
            "triggers": [{ "type": "cron", "cron": "0 3 * * *", "singleton": true }]
        }))
        .unwrap();
        assert!(matches!(
            f.validate(),
            Err(ManifestError::SingletonWithoutKey { .. })
        ));
    }

    #[test]
    fn a_function_with_no_triggers_is_refused() {
        let f: FunctionConfig = serde_json::from_value(serde_json::json!({
            "id": "orphan",
            "triggers": []
        }))
        .unwrap();
        assert!(matches!(f.validate(), Err(ManifestError::NoTriggers(_))));
    }

    #[test]
    fn a_catchup_limit_outside_the_schema_range_is_refused() {
        let f: FunctionConfig = serde_json::from_value(serde_json::json!({
            "id": "nightly",
            "triggers": [{ "type": "cron", "cron": "0 3 * * *", "catchup_limit": 0 }]
        }))
        .unwrap();
        assert!(matches!(
            f.validate(),
            Err(ManifestError::CatchUpLimit { limit: 0, .. })
        ));
    }
}
