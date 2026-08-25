//! Domain types shared by the server, SDKs and store implementations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifier for a workflow run.
pub type RunId = Uuid;
/// Fencing token. Monotonic per run; a response bearing a stale value is discarded.
pub type Fence = i64;

/// Lifecycle state of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Ready to be dispatched.
    Pending,
    /// An attempt is in flight.
    Running,
    /// Suspended on a durable timer.
    Sleeping,
    /// Suspended awaiting an event.
    Waiting,
    /// Finished successfully.
    Completed,
    /// Finished unsuccessfully.
    Failed,
    /// Terminated by an operator or a cascade.
    Cancelled,
    /// Failing identically and repeatedly; removed from dispatch pending a human.
    Quarantined,
}

impl RunStatus {
    /// Whether no further work will occur for this run.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// String form used by the database enum.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Sleeping => "sleeping",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Quarantined => "quarantined",
        }
    }
}

/// Outcome of a single step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Recorded but not yet resolved (a sleep or an unmatched wait).
    Pending,
    /// Resolved successfully.
    Completed,
    /// Resolved with a terminal error.
    Failed,
    /// Deadline elapsed before resolution.
    TimedOut,
    /// Cancelled, usually by a cascade or a join policy.
    Cancelled,
    /// An attempt was abandoned mid-step; the step will re-execute.
    Unknown,
}

/// Kind of operation a recorded step came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOp {
    /// Unit of work executed by the app.
    Step,
    /// Durable timer.
    Sleep,
    /// Suspension awaiting a correlated event.
    WaitEvent,
    /// Child run.
    Invoke,
    /// Directed event to another run.
    Signal,
}

/// A CloudEvents 1.0 envelope with the stepd extension attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// CloudEvents spec version. Always `1.0`.
    #[serde(default = "default_specversion")]
    pub specversion: String,
    /// Producer-assigned identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Producing context.
    pub source: String,
    /// Event type, e.g. `order.created`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Producer timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<DateTime<Utc>>,
    /// Payload.
    #[serde(default)]
    pub data: serde_json::Value,
    /// Extension: business key, overriding the function's `key_expr`.
    #[serde(default, rename = "stepdkey", skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Extension: ingest deduplication key.
    #[serde(
        default,
        rename = "stepdidempotency",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency: Option<String>,
}

fn default_specversion() -> String {
    "1.0".into()
}

impl Event {
    /// Build a minimal event.
    pub fn new(
        event_type: impl Into<String>,
        source: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        Self {
            specversion: default_specversion(),
            id: Some(Uuid::new_v4().to_string()),
            source: source.into(),
            event_type: event_type.into(),
            time: Some(Utc::now()),
            data,
            key: None,
            idempotency: None,
        }
    }
}

/// A payload value: inline JSON, a managed blob reference, or an external reference.
///
/// stepd is a control plane. Bulk data must not traverse the server, so anything
/// large is a reference rather than a value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload {
    /// Bytes stepd stores and refcounts.
    Blob {
        /// Reference body.
        #[serde(rename = "$blob")]
        blob: BlobRef,
    },
    /// A pointer to an object the application owns. Never fetched by the server.
    External {
        /// Reference body.
        #[serde(rename = "$ref")]
        reference: ExternalRef,
    },
    /// Ordinary JSON.
    Inline(serde_json::Value),
}

/// Reference to a stepd-managed blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobRef {
    /// Blob identifier.
    pub id: Uuid,
    /// Size in bytes.
    pub size: i64,
    /// Lowercase hex SHA-256 of the content.
    pub sha256: String,
    /// Media type, used by the console to choose a preview.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Original filename, presentational only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// Short-lived presigned read URL. Minted by the server; never persisted by SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Pointer to data the application stores itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalRef {
    /// Location, opaque to the server.
    pub uri: String,
    /// Size in bytes, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Content digest, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Media type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Application metadata, never interpreted by the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

/// A structured error from user code or the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Stable machine-readable code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable description.
    pub message: String,
    /// Optional stack or context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    /// Attempts made before giving up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<i32>,
}

impl ErrorBody {
    /// Build an error with just a message.
    pub fn msg(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
            stack: None,
            attempts: None,
        }
    }

    /// Build an error with a stable code.
    pub fn coded(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: Some(code.into()),
            message: message.into(),
            stack: None,
            attempts: None,
        }
    }

    /// A stable fingerprint used to detect a run failing identically and repeatedly.
    ///
    /// Over the **code** when there is one, and only over the message when there
    /// is not. Including the message alongside a code defeats the purpose:
    /// messages routinely embed a run id, an order number or a timestamp, so a
    /// hundred instances of one poison pill produce a hundred distinct
    /// signatures and group with nothing. A code is the thing the author chose
    /// to be stable, so it is the thing to fingerprint.
    pub fn signature(&self) -> String {
        use sha2::{Digest, Sha256};
        let basis = match self.code.as_deref() {
            Some(c) if !c.is_empty() => c,
            _ => self.message.as_str(),
        };
        hex::encode(&Sha256::digest(basis.as_bytes())[..8])
    }
}

/// Parse the ISO 8601 duration subset the protocol uses, in seconds.
///
/// Deliberately partial: weeks, days, hours, minutes and seconds, which is what
/// `common.schema.json#/$defs/duration` permits and what every SDK emits. Years
/// and months return `None` rather than a guess — a month is not a fixed number
/// of seconds, and a retry backoff or a misfire window silently interpreted as
/// thirty days when the author meant February is the kind of error that is only
/// noticed in a bill.
///
/// Lives here, in the crate that owns the wire format, because the SDK's lint
/// and the server's registration both need it and two parsers that disagree
/// about `P1M` would disagree about what a config means.
pub fn iso8601_seconds(s: &str) -> Option<i64> {
    let s = s.strip_prefix('P')?;
    let (date, time) = match s.split_once('T') {
        Some((d, t)) => (d, t),
        None => (s, ""),
    };
    let mut total = 0i64;
    let mut num = String::new();
    for c in date.chars() {
        match c {
            '0'..='9' => num.push(c),
            'D' => {
                total += num.parse::<i64>().ok()? * 86_400;
                num.clear();
            }
            'W' => {
                total += num.parse::<i64>().ok()? * 604_800;
                num.clear();
            }
            _ => return None,
        }
    }
    for c in time.chars() {
        match c {
            '0'..='9' => num.push(c),
            'H' => {
                total += num.parse::<i64>().ok()? * 3600;
                num.clear();
            }
            'M' => {
                total += num.parse::<i64>().ok()? * 60;
                num.clear();
            }
            'S' => {
                total += num.parse::<i64>().ok()?;
                num.clear();
            }
            _ => return None,
        }
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_states_are_exactly_three() {
        let all = [
            RunStatus::Pending,
            RunStatus::Running,
            RunStatus::Sleeping,
            RunStatus::Waiting,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
            RunStatus::Quarantined,
        ];
        assert_eq!(all.iter().filter(|s| s.is_terminal()).count(), 3);
        // Quarantined is deliberately NOT terminal: the run can be retried.
        assert!(!RunStatus::Quarantined.is_terminal());
    }

    #[test]
    fn payload_round_trips_all_three_shapes() {
        let inline: Payload = serde_json::from_str(r#"{"tx":1}"#).unwrap();
        assert!(matches!(inline, Payload::Inline(_)));

        let blob: Payload = serde_json::from_str(
            r#"{"$blob":{"id":"01926f5a-2200-7aaa-8000-0123456789ab","size":10,
                 "sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}"#,
        )
        .unwrap();
        assert!(matches!(blob, Payload::Blob { .. }));

        let ext: Payload = serde_json::from_str(r#"{"$ref":{"uri":"s3://b/k"}}"#).unwrap();
        assert!(matches!(ext, Payload::External { .. }));
    }

    #[test]
    fn error_signature_groups_one_poison_pill_into_one_signature() {
        // The whole purpose: a hundred runs failing the same way must produce one
        // signature an operator can act on in bulk. Messages embed run ids and
        // order numbers, so fingerprinting the message defeats that entirely.
        let a = ErrorBody::coded("gateway_down", "upstream 503 for order 4711");
        let b = ErrorBody::coded("gateway_down", "upstream 503 for order 4712");
        assert_eq!(a.signature(), b.signature(), "same code, same signature");

        let other = ErrorBody::coded("card_declined", "upstream 503 for order 4711");
        assert_ne!(
            a.signature(),
            other.signature(),
            "different codes must not group"
        );
    }

    #[test]
    fn an_uncoded_error_still_gets_a_signature() {
        let a = ErrorBody::msg("boom");
        assert_eq!(a.signature(), ErrorBody::msg("boom").signature());
        assert_ne!(a.signature(), ErrorBody::msg("bang").signature());
        assert_eq!(a.signature().len(), 16);
    }

    #[test]
    fn event_uses_cloudevents_field_names() {
        let mut e = Event::new("order.created", "/shop", serde_json::json!({"id": 1}));
        e.key = Some("order:4711".into());
        e.idempotency = Some("4711".into());
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["specversion"], "1.0");
        assert_eq!(v["type"], "order.created");
        assert!(v.get("event_type").is_none(), "must serialise as `type`");
        assert_eq!(v["stepdkey"], "order:4711");
        assert_eq!(v["stepdidempotency"], "4711");
        assert!(
            v.get("key").is_none(),
            "must serialise as `stepdkey`; a `key` field is ignored on ingest"
        );
        assert!(
            v.get("idempotency").is_none(),
            "must serialise as `stepdidempotency`"
        );
    }
}

#[cfg(test)]
mod duration_tests {
    use super::iso8601_seconds;

    #[test]
    fn the_forms_the_protocol_permits_all_parse() {
        assert_eq!(iso8601_seconds("PT60S"), Some(60));
        assert_eq!(iso8601_seconds("PT1H"), Some(3600));
        assert_eq!(iso8601_seconds("P30D"), Some(2_592_000));
        assert_eq!(iso8601_seconds("P1W"), Some(604_800));
        assert_eq!(iso8601_seconds("P1DT2H3M4S"), Some(86_400 + 7200 + 180 + 4));
    }

    #[test]
    fn months_and_years_are_refused_rather_than_approximated() {
        // A month is not a fixed number of seconds. Guessing thirty days makes a
        // misfire window written as `P1M` mean something different in February,
        // and the only place that difference shows up is an occurrence that
        // silently did or did not get caught up.
        assert_eq!(iso8601_seconds("P1M"), None);
        assert_eq!(iso8601_seconds("P1Y"), None);
    }

    #[test]
    fn anything_unrecognisable_is_refused_rather_than_partially_read() {
        assert_eq!(iso8601_seconds("banana"), None);
        assert_eq!(iso8601_seconds("1h"), None);
        assert_eq!(iso8601_seconds(""), None);
        // `PT1H` written without the T is a common slip and means something
        // else: reading it as one hour would be inventing intent.
        assert_eq!(iso8601_seconds("P1H"), None);
    }
}
