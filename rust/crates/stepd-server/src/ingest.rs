//! Event ingest: dedupe, backpressure, trigger matching, correlation.
//!
//! One event can do two independent things, and both must happen: start runs of
//! every function whose trigger matches it, and resolve waits in runs that are
//! already parked on it. Doing only the first is the common shortcut and it is
//! how a workflow ends up waiting forever for an event that was ingested,
//! matched nothing, and was quietly filed away.

use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use stepd_core::traits::{EventLog, ExprEngine, NewRun, StateStore};
use tracing::{debug, warn};

use crate::auth::{Principal, Role};
use crate::problem::{ApiResult, Problem};
use crate::ServerState;

/// A CloudEvent as accepted on the wire.
#[derive(Debug, Clone, Deserialize)]
pub struct EventIn {
    /// CloudEvents spec version. Always `1.0`.
    #[serde(default = "specversion")]
    pub specversion: String,
    /// Producer-assigned identifier.
    #[serde(default)]
    pub id: Option<String>,
    /// Producing context.
    pub source: String,
    /// Event type, e.g. `order.created`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Producer timestamp.
    #[serde(default)]
    pub time: Option<chrono::DateTime<chrono::Utc>>,
    /// Payload.
    #[serde(default)]
    pub data: serde_json::Value,
    /// Extension: business key, overriding the function's `key_expr`.
    #[serde(default)]
    pub stepdkey: Option<String>,
    /// Extension: ingest deduplication key.
    #[serde(default)]
    pub stepdidempotency: Option<String>,
}

fn specversion() -> String {
    "1.0".into()
}

impl From<EventIn> for stepd_proto::Event {
    fn from(e: EventIn) -> Self {
        stepd_proto::Event {
            specversion: e.specversion,
            id: e.id,
            source: e.source,
            event_type: e.event_type,
            time: e.time,
            data: e.data,
            key: e.stepdkey,
            idempotency: e.stepdidempotency,
        }
    }
}

#[derive(Debug, Serialize)]
/// Outcome of one ingest call.
pub struct IngestResult {
    /// Events newly recorded.
    pub accepted: usize,
    /// Events already seen under the same idempotency key.
    pub deduplicated: usize,
    /// Runs started by a matching trigger.
    pub runs_started: usize,
    /// Parked waits resolved by correlation.
    pub waits_resolved: usize,
    /// Event ids, in the order supplied.
    pub ids: Vec<String>,
}

/// Queue depth above which a namespace is told to slow down (F-LP-8).
///
/// Accepting work the system cannot drain is not generosity: it converts a
/// visible rejection into an invisible backlog, and the first anyone hears of it
/// is a workflow that ran four hours late.
const BACKPRESSURE_THRESHOLD: i64 = 100_000;

/// `POST /v1/events`
pub async fn ingest(
    State(state): State<ServerState>,
    principal: Principal,
    Json(events): Json<Vec<EventIn>>,
) -> ApiResult<Json<IngestResult>> {
    principal.require(Role::Operator)?;

    let backlog: i64 =
        sqlx::query_scalar("SELECT count(*) FROM queue WHERE ns = $1 AND claimed_by IS NULL")
            .bind(&principal.namespace)
            .fetch_one(state.store.pool())
            .await
            .map_err(|e| Problem::internal(e.to_string()))?;

    if backlog > BACKPRESSURE_THRESHOLD {
        return Err(Problem::backpressure(
            5,
            format!(
                "namespace '{}' has {backlog} runs queued, over the {BACKPRESSURE_THRESHOLD} \
                 threshold",
                principal.namespace
            ),
        ));
    }

    let mut out = IngestResult {
        accepted: 0,
        deduplicated: 0,
        runs_started: 0,
        waits_resolved: 0,
        ids: Vec::new(),
    };

    for e in events {
        let event: stepd_proto::Event = e.into();
        let (id, duplicate) = state
            .store
            .append(&principal.namespace, &event)
            .await
            .map_err(Problem::from)?;
        out.ids.push(id.to_string());

        if duplicate {
            // A duplicate must not start a second run or resolve a second wait.
            // That is the entire purpose of the idempotency key, and it is why
            // dedupe happens before dispatch rather than after.
            out.deduplicated += 1;
            debug!(event = %id, "deduplicated on ingest");
            continue;
        }
        out.accepted += 1;

        out.runs_started += start_matching_runs(&state, &principal.namespace, &event, id).await?;
        out.waits_resolved += state
            .store
            .correlate(
                &principal.namespace,
                &event.event_type,
                event.key.as_deref(),
                &event.data,
            )
            .await
            .map_err(Problem::from)? as usize;
    }

    Ok(Json(out))
}

/// Start a run of every registered function whose trigger matches.
async fn start_matching_runs(
    state: &ServerState,
    namespace: &str,
    event: &stepd_proto::Event,
    event_id: uuid::Uuid,
) -> ApiResult<usize> {
    let rows = sqlx::query(
        r#"SELECT fn_id, config FROM functions
            WHERE ns = $1 AND archived_at IS NULL AND NOT paused"#,
    )
    .bind(namespace)
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let bindings = stepd_core::traits::Bindings {
        event: Some(serde_json::to_value(event).unwrap_or_default()),
        events: None,
        run: None,
        now: Some(chrono::Utc::now()),
    };

    let mut started = 0;
    for row in rows {
        let fn_id: String = row.get("fn_id");
        let config: serde_json::Value = row.get("config");

        if !trigger_matches(state, &config, event, &bindings, &fn_id) {
            continue;
        }

        // An explicit `stepdkey` on the event overrides the function's key
        // expression: a producer that knows the business key should not have to
        // encode it in a way the consumer's CEL can rediscover.
        let key = match &event.key {
            Some(k) => Some(k.clone()),
            None => evaluate_key(state, &config, &bindings, &fn_id),
        };

        let mut new = NewRun::root(namespace, &fn_id);
        new.key = key;
        new.trigger_event_id = Some(event_id);
        new.input = Some(event.data.clone());

        match state.store.create_run(new).await {
            Ok(Some(run)) => {
                started += 1;
                debug!(%run, %fn_id, "run started from event");
            }
            Ok(None) => {
                // Keyed exclusivity refused it: a run is already active on this
                // key. That is the singleton guarantee working, not an error.
                debug!(%fn_id, "run not started; the key already has an active run");
            }
            Err(e) => {
                warn!(%fn_id, error = %e, "failed to start a run from a matching event");
            }
        }
    }
    Ok(started)
}

fn trigger_matches(
    state: &ServerState,
    config: &serde_json::Value,
    event: &stepd_proto::Event,
    bindings: &stepd_core::traits::Bindings,
    fn_id: &str,
) -> bool {
    let Some(triggers) = config.get("triggers").and_then(|t| t.as_array()) else {
        return false;
    };
    triggers.iter().any(|t| {
        if t.get("type").and_then(|v| v.as_str()) != Some("event") {
            return false;
        }
        if t.get("event").and_then(|v| v.as_str()) != Some(event.event_type.as_str()) {
            return false;
        }
        match t.get("expr").and_then(|v| v.as_str()) {
            None => true,
            Some(src) => match state.expr.compile(src) {
                Ok(c) => state.expr.matches(&c, bindings),
                Err(e) => {
                    // §10: an evaluation error is a non-match reported as a
                    // function health warning, never an ingest failure. One
                    // tenant's bad predicate must not stop everyone's events.
                    warn!(%fn_id, expr = src, error = %e, "trigger expression did not compile");
                    false
                }
            },
        }
    })
}

fn evaluate_key(
    state: &ServerState,
    config: &serde_json::Value,
    bindings: &stepd_core::traits::Bindings,
    fn_id: &str,
) -> Option<String> {
    let src = config.get("key_expr")?.as_str()?;
    match state
        .expr
        .compile(src)
        .and_then(|c| state.expr.eval(&c, bindings))
    {
        Ok(serde_json::Value::String(s)) => Some(s),
        Ok(other) => Some(other.to_string().trim_matches('"').to_string()),
        Err(e) => {
            // Failing open — starting an unkeyed run — would silently drop the
            // singleton guarantee the key exists to provide, and two runs would
            // process the same order. Failing closed loses the run instead, which
            // is visible. Neither is good; the visible one is better.
            warn!(%fn_id, expr = src, error = %e, "key expression failed; run not started");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cloudevent_maps_onto_the_wire_type_without_losing_extensions() {
        let raw = serde_json::json!({
            "specversion": "1.0",
            "id": "e-1",
            "source": "/shop",
            "type": "order.created",
            "data": { "order_id": 4711 },
            "stepdkey": "order:4711",
            "stepdidempotency": "idem-1"
        });
        let e: EventIn = serde_json::from_value(raw).unwrap();
        let proto: stepd_proto::Event = e.into();
        assert_eq!(proto.event_type, "order.created");
        assert_eq!(proto.key.as_deref(), Some("order:4711"));
        assert_eq!(
            proto.idempotency.as_deref(),
            Some("idem-1"),
            "losing the idempotency key silently disables dedupe"
        );
    }

    #[test]
    fn an_event_without_the_optional_fields_still_parses() {
        let e: EventIn = serde_json::from_value(serde_json::json!({
            "source": "/x", "type": "t"
        }))
        .unwrap();
        assert_eq!(e.specversion, "1.0");
        assert!(e.stepdidempotency.is_none());
    }
}
