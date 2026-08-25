//! The attempt request and op envelope — the entire server↔app conversation.

use crate::types::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Server → app: everything the handler needs to replay and advance one step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    /// Protocol major version.
    pub protocol: String,
    /// Attempt counter, from 1.
    pub attempt: i32,
    /// Fencing token; echoed back on commit.
    pub fence: Fence,
    /// Deadline after which the SDK should stop starting new work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<DateTime<Utc>>,
    /// Run identity and context.
    pub run: RunContext,
    /// Triggering events. More than one only for batched triggers.
    #[serde(default)]
    pub events: Vec<Event>,
    /// Recorded steps, keyed by hash.
    ///
    /// Contains **only** completed or terminally-failed steps. Pending rows exist
    /// in the store but are never sent, or the handler would treat an unresolved
    /// sleep as already done.
    #[serde(default)]
    pub steps: HashMap<String, RecordedStep>,
    /// True when `steps` was paginated.
    #[serde(default)]
    pub state_truncated: bool,
}

/// Run identity carried on every attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunContext {
    /// Run identifier.
    pub id: RunId,
    /// Function being executed.
    pub function_id: String,
    /// Namespace.
    pub namespace: String,
    /// Business key, if the function is keyed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// When the run began.
    pub started_at: DateTime<Utc>,
    /// Input, for invoke-triggered runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    /// Stable across a `continue_as_new` chain.
    pub lineage_id: Uuid,
    /// Position in the chain; 0 for the first run.
    #[serde(default)]
    pub chain_position: i32,
    /// Set when the run is being cancelled, so the handler runs compensation only.
    #[serde(default)]
    pub cancelling: bool,
}

/// A step the server has already recorded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedStep {
    /// Developer-supplied id.
    pub id: String,
    /// Originating op kind.
    pub op: StepOp,
    /// Outcome.
    pub status: StepStatus,
    /// Result value, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Error, for failed steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

/// App → server: one control instruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// A unit of work that reached a terminal outcome.
    ///
    /// `error` is what makes this "terminal outcome" rather than "success". A
    /// step whose body raised non-retryably has an outcome, and the engine
    /// records it as `failed` so the journal says what happened. A *retryable*
    /// failure is not an outcome — it is a pause — and must never be recorded
    /// this way, or the memo would hand the same error back forever and the
    /// retry the app asked for would never run (§5.2.2, ADR-023).
    Step {
        /// Developer-supplied id.
        id: String,
        /// Step hash.
        hash: String,
        /// Result. Absent when `error` is present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<serde_json::Value>,
        /// Observability metadata (model, tokens, cost). Opaque to the engine.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<serde_json::Value>,
        /// Terminal failure of this step's body. Present ⇒ recorded `failed`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<ErrorBody>,
    },
    /// A durable timer.
    Sleep {
        /// Developer-supplied id.
        id: String,
        /// Step hash.
        hash: String,
        /// Absolute wake time.
        until: DateTime<Utc>,
    },
    /// Suspension awaiting a correlated event.
    WaitEvent {
        /// Developer-supplied id.
        id: String,
        /// Step hash.
        hash: String,
        /// Event type to match.
        event: String,
        /// Matching window. `run_start` by default, which closes the lost-signal race.
        #[serde(default = "default_since")]
        since: String,
        /// Optional deadline.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_at: Option<DateTime<Utc>>,
        /// Presentational block rendered as a pending decision in the console.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prompt: Option<serde_json::Value>,
    },
    /// A child run.
    Invoke {
        /// Developer-supplied id.
        id: String,
        /// Step hash.
        hash: String,
        /// Function to call.
        function: String,
        /// Input for the child.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<serde_json::Value>,
        /// Fire and forget: the child's lifecycle becomes independent.
        #[serde(default)]
        detach: bool,
    },
    /// A directed event to another run.
    Signal {
        /// Developer-supplied id.
        id: String,
        /// Step hash.
        hash: String,
        /// Target run.
        target_run: RunId,
        /// Event to deliver.
        event: Event,
    },
    /// Close this run and start a successor with empty state.
    ContinueAsNew {
        /// Developer-supplied id.
        id: String,
        /// Step hash.
        hash: String,
        /// Successor input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<serde_json::Value>,
    },
    /// The handler returned.
    Done {
        /// Final output.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<serde_json::Value>,
    },
    /// The handler raised.
    Error {
        /// Whether a retry could succeed.
        #[serde(default = "default_true")]
        retryable: bool,
        /// Offending step id, if known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step: Option<String>,
        /// Error body.
        error: ErrorBody,
    },
}

fn default_since() -> String {
    "run_start".into()
}
fn default_true() -> bool {
    true
}

impl Op {
    /// Hash of the step this op records, if it records one.
    pub fn hash(&self) -> Option<&str> {
        match self {
            Op::Step { hash, .. }
            | Op::Sleep { hash, .. }
            | Op::WaitEvent { hash, .. }
            | Op::Invoke { hash, .. }
            | Op::Signal { hash, .. }
            | Op::ContinueAsNew { hash, .. } => Some(hash),
            Op::Done { .. } | Op::Error { .. } => None,
        }
    }

    /// Developer-supplied step id, if any.
    pub fn step_id(&self) -> Option<&str> {
        match self {
            Op::Step { id, .. }
            | Op::Sleep { id, .. }
            | Op::WaitEvent { id, .. }
            | Op::Invoke { id, .. }
            | Op::Signal { id, .. }
            | Op::ContinueAsNew { id, .. } => Some(id),
            _ => None,
        }
    }

    /// Ops that end the run *successfully* and must therefore appear alone.
    ///
    /// `error` is deliberately not one of them. A pass can record work and then
    /// fail — a parallel group where one member raises is the ordinary case —
    /// and an envelope that could not carry both would have to drop one of them.
    /// Dropping the ops loses executed work; dropping the error loses the reason
    /// the run stopped. So `error` rides at the end of the batch instead
    /// (§5.2.2, ADR-023).
    ///
    /// `done` and `continue_as_new` stay exclusive for the opposite reason: a
    /// pass that recorded a step cannot also have returned, because recording a
    /// step ends the pass. Ops alongside them mean the handler swallowed a halt,
    /// and batching them would make that silent.
    pub fn is_exclusive(&self) -> bool {
        matches!(self, Op::Done { .. } | Op::ContinueAsNew { .. })
    }
}

/// App → server: the response to one attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptResponse {
    /// Protocol major version.
    pub protocol: String,
    /// One op, or a parallel batch.
    pub ops: Vec<Op>,
    /// Events published in the same transaction as the ops.
    #[serde(default)]
    pub emit: Vec<Event>,
    /// Recorded hashes this pass never encountered — a renamed or removed step.
    #[serde(default)]
    pub orphaned_steps: usize,

    /// Retired: the batch join policy (§5.2.1, ADR-023).
    ///
    /// Captured for the sole purpose of refusing it. §11 tells a receiver to
    /// ignore unknown fields, and that is right for a field added by a newer
    /// minor — one it has no opinion about. A *retired* field is different: it
    /// had a meaning, an app may still be sending it expecting that meaning, and
    /// ignoring it would let the app believe it had requested `all` or `any`
    /// when every batch resolves the one way. Being ignored is the state this
    /// field was removed for being in; it must not survive its own removal.
    ///
    /// Never serialised, so a well-formed envelope never carries one.
    #[serde(default, skip_serializing)]
    pub join: Option<serde_json::Value>,
}

/// Reasons an envelope is not well-formed.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EnvelopeError {
    /// No ops at all.
    #[error("envelope contains no ops")]
    Empty,
    /// A terminal op was batched with others.
    #[error("`{0}` must appear alone in an envelope")]
    NotAlone(&'static str),
    /// Two ops in one batch claimed the same hash.
    #[error("duplicate step hash `{0}` within one envelope")]
    DuplicateHash(String),
    /// Wrong protocol version.
    #[error("unsupported protocol version `{0}`")]
    BadVersion(String),
    /// A field the protocol has retired (§11).
    #[error(
        "`{0}` was retired from this protocol and is refused rather than ignored; \
         remove it from the envelope"
    )]
    RetiredField(&'static str),
    /// A terminal op appeared before the ops it was supposed to conclude.
    #[error("`{0}` must be the last op in an envelope")]
    MustBeLast(&'static str),
    /// A retryable error travelled with recorded ops.
    #[error(
        "a retryable `error` must appear alone; emit the recorded ops on their own \
         and re-raise on the next attempt (§5.2.2)"
    )]
    RetryableErrorBatched,
}

impl AttemptResponse {
    /// Build a single-op response.
    pub fn single(op: Op) -> Self {
        Self {
            protocol: crate::PROTOCOL_VERSION.into(),
            ops: vec![op],
            emit: Vec::new(),
            orphaned_steps: 0,
            join: None,
        }
    }

    /// Build a parallel batch.
    pub fn batch(ops: Vec<Op>) -> Self {
        Self {
            protocol: crate::PROTOCOL_VERSION.into(),
            ops,
            emit: Vec::new(),
            orphaned_steps: 0,
            join: None,
        }
    }

    /// Reject a malformed envelope before it reaches the engine.
    ///
    /// Validating here rather than in the store means every transport gets the
    /// same checks, and the store never has to defend against a shape the
    /// protocol forbids.
    pub fn validate(&self) -> Result<(), EnvelopeError> {
        if self.protocol != crate::PROTOCOL_VERSION {
            return Err(EnvelopeError::BadVersion(self.protocol.clone()));
        }
        if self.join.is_some() {
            return Err(EnvelopeError::RetiredField("join"));
        }
        if self.ops.is_empty() {
            return Err(EnvelopeError::Empty);
        }
        if self.ops.len() > 1 {
            for op in &self.ops {
                if op.is_exclusive() {
                    return Err(EnvelopeError::NotAlone(match op {
                        Op::Done { .. } => "done",
                        _ => "continue_as_new",
                    }));
                }
            }
            // An `error` anywhere but the end would have the engine fail the run
            // and then keep recording steps into it. Last is the only position
            // whose meaning is unambiguous: everything before it happened, and
            // then the run stopped.
            for op in &self.ops[..self.ops.len() - 1] {
                if let Op::Error { .. } = op {
                    return Err(EnvelopeError::MustBeLast("error"));
                }
            }
            // A retryable error means "re-execute me", and the retry policy
            // lives in the dispatcher, which does not commit. So the two cannot
            // travel together: an SDK holding recorded ops and a retryable
            // failure emits the ops and re-raises next attempt, where the work
            // is memoised and the error arrives alone.
            if let Some(Op::Error {
                retryable: true, ..
            }) = self.ops.last()
            {
                return Err(EnvelopeError::RetryableErrorBatched);
            }
        }
        let mut seen = std::collections::HashSet::new();
        for op in &self.ops {
            if let Some(h) = op.hash() {
                if !seen.insert(h) {
                    return Err(EnvelopeError::DuplicateHash(h.to_string()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(id: &str, hash: &str) -> Op {
        Op::Step {
            id: id.into(),
            hash: hash.into(),
            data: None,
            meta: None,
            error: None,
        }
    }

    #[test]
    fn single_op_envelope_is_valid() {
        assert!(AttemptResponse::single(step("a", "1111111111111111"))
            .validate()
            .is_ok());
    }

    #[test]
    fn empty_envelope_rejected() {
        let r = AttemptResponse::batch(vec![]);
        assert_eq!(r.validate(), Err(EnvelopeError::Empty));
    }

    #[test]
    fn done_must_appear_alone() {
        let r =
            AttemptResponse::batch(vec![step("a", "1111111111111111"), Op::Done { data: None }]);
        assert_eq!(r.validate(), Err(EnvelopeError::NotAlone("done")));
    }

    #[test]
    fn continue_as_new_must_appear_alone() {
        let r = AttemptResponse::batch(vec![
            step("a", "1111111111111111"),
            Op::ContinueAsNew {
                id: "c".into(),
                hash: "2222222222222222".into(),
                input: None,
            },
        ]);
        assert_eq!(
            r.validate(),
            Err(EnvelopeError::NotAlone("continue_as_new"))
        );
    }

    fn failed_step(id: &str, hash: &str) -> Op {
        Op::Step {
            id: id.into(),
            hash: hash.into(),
            data: None,
            meta: None,
            error: Some(ErrorBody::coded("boom", "it broke")),
        }
    }

    fn fatal_error() -> Op {
        Op::Error {
            retryable: false,
            step: None,
            error: ErrorBody::coded("boom", "it broke"),
        }
    }

    #[test]
    fn a_fatal_error_may_ride_at_the_end_of_a_batch() {
        // The reason `error` stopped being exclusive: this envelope is a
        // parallel group where one member raised. Refusing it would force the
        // SDK to drop either the two recorded results or the reason the run
        // stopped, and there is no acceptable choice between those.
        let r = AttemptResponse::batch(vec![
            step("ok-1", "1111111111111111"),
            failed_step("bad", "2222222222222222"),
            step("ok-2", "3333333333333333"),
            fatal_error(),
        ]);
        assert_eq!(r.validate(), Ok(()));
    }

    #[test]
    fn an_error_before_the_end_is_refused() {
        // Ops after an `error` would be recorded into a run the engine has
        // already failed. The position is the only thing that says which of the
        // two happened first.
        let r = AttemptResponse::batch(vec![fatal_error(), step("ok", "1111111111111111")]);
        assert_eq!(r.validate(), Err(EnvelopeError::MustBeLast("error")));
    }

    #[test]
    fn a_retryable_error_may_not_travel_with_recorded_ops() {
        // Retrying re-executes; committing is durable. The dispatcher decides
        // the first and never does the second, so an envelope asking for both
        // has no single owner. The SDK emits the ops and re-raises next attempt.
        let r = AttemptResponse::batch(vec![
            step("ok", "1111111111111111"),
            Op::Error {
                retryable: true,
                step: None,
                error: ErrorBody::coded("flaky", "try again"),
            },
        ]);
        assert_eq!(r.validate(), Err(EnvelopeError::RetryableErrorBatched));
    }

    #[test]
    fn a_retryable_error_alone_is_valid() {
        let r = AttemptResponse::single(Op::Error {
            retryable: true,
            step: None,
            error: ErrorBody::coded("flaky", "try again"),
        });
        assert_eq!(r.validate(), Ok(()));
    }

    #[test]
    fn duplicate_hash_in_batch_rejected() {
        let r = AttemptResponse::batch(vec![
            step("a", "1111111111111111"),
            step("b", "1111111111111111"),
        ]);
        assert!(matches!(r.validate(), Err(EnvelopeError::DuplicateHash(_))));
    }

    #[test]
    fn parallel_batch_with_distinct_hashes_is_valid() {
        let r = AttemptResponse::batch(vec![
            step("a", "1111111111111111"),
            step("b", "2222222222222222"),
        ]);
        assert!(r.validate().is_ok());
    }

    #[test]
    fn wrong_protocol_version_rejected() {
        let mut r = AttemptResponse::single(Op::Done { data: None });
        r.protocol = "2".into();
        assert!(matches!(r.validate(), Err(EnvelopeError::BadVersion(_))));
    }

    #[test]
    fn wait_event_defaults_to_run_start() {
        let op: Op = serde_json::from_str(
            r#"{"op":"wait_event","id":"a","hash":"1111111111111111","event":"x"}"#,
        )
        .unwrap();
        match op {
            Op::WaitEvent { since, .. } => assert_eq!(since, "run_start"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn ops_round_trip_through_json() {
        let ops = vec![
            step("a", "1111111111111111"),
            Op::Done {
                data: Some(serde_json::json!({"ok": true})),
            },
        ];
        for op in ops {
            let s = serde_json::to_string(&op).unwrap();
            let back: Op = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{op:?}"), format!("{back:?}"));
        }
    }
}

#[cfg(test)]
mod retirement_tests {
    use super::*;

    fn one_op() -> Op {
        Op::Step {
            id: "a".into(),
            hash: "3f2a91c4b70e1d55".into(),
            data: None,
            meta: None,
            error: None,
        }
    }

    #[test]
    fn an_envelope_carrying_a_retired_field_is_refused_not_ignored() {
        // §11 tells a receiver to ignore unknown fields, and that is right for a
        // field a newer minor added. `join` is different: an app sending it
        // believes it has requested `all` or `any`, and every batch resolves the
        // one way. Ignoring it would leave the app in exactly the state the
        // field was retired for being in.
        let raw = serde_json::json!({
            "protocol": crate::PROTOCOL_VERSION,
            "join": "any",
            "ops": [{ "op": "step", "id": "a", "hash": "3f2a91c4b70e1d55" }],
        });
        let envelope: AttemptResponse = serde_json::from_value(raw).unwrap();
        let err = envelope.validate().unwrap_err();
        assert_eq!(err, EnvelopeError::RetiredField("join"));
        // …and it names the field, so the sender learns what to remove rather
        // than that its envelope was somehow malformed.
        assert!(err.to_string().contains("join"), "got {err}");
    }

    #[test]
    fn a_genuinely_unknown_field_is_still_ignored() {
        // The other half of §11, and the reason retirement needs its own rule
        // rather than a blanket `deny_unknown_fields`: a field from a newer
        // minor must not break an older receiver.
        let raw = serde_json::json!({
            "protocol": crate::PROTOCOL_VERSION,
            "some_future_field": { "added": "in a later minor" },
            "ops": [{ "op": "step", "id": "a", "hash": "3f2a91c4b70e1d55" }],
        });
        let envelope: AttemptResponse = serde_json::from_value(raw).unwrap();
        assert!(envelope.validate().is_ok());
    }

    #[test]
    fn a_well_formed_envelope_never_serialises_a_retired_field() {
        let v = serde_json::to_value(AttemptResponse::batch(vec![one_op()])).unwrap();
        assert!(
            v.get("join").is_none(),
            "the retired field must not round-trip back out: {v}"
        );
    }
}
