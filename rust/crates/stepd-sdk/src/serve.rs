//! The HTTP surface: discovery, attempt handling, signatures and replay defence.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use stepd_proto::{sig, Attempt, AttemptResponse, Op, PROTOCOL_VERSION};
use tracing::{debug, warn};

use crate::function::App;

/// Why a request was refused.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// No signature, and the app is not in development mode.
    #[error("unsigned request")]
    Unsigned,
    /// Signature failed verification.
    #[error("signature: {0}")]
    Signature(#[from] sig::SignatureError),
    /// Body was not a valid attempt request.
    #[error("malformed attempt request: {0}")]
    Malformed(String),
    /// No function with that id is registered here.
    #[error("unknown function '{0}'")]
    UnknownFunction(String),
}

impl ServeError {
    fn status(&self) -> StatusCode {
        match self {
            // 401, not 400: an unsigned or badly-signed request is an
            // authentication failure, and the server's documented reaction is to
            // alert and retry with backoff rather than to fail the run.
            Self::Unsigned | Self::Signature(_) => StatusCode::UNAUTHORIZED,
            // 400 fails the run non-retryably: a body we cannot parse will not
            // parse on the next attempt either.
            Self::Malformed(_) => StatusCode::BAD_REQUEST,
            // 404 marks the function unhealthy and retries — the usual cause is
            // a deploy in progress, which fixes itself.
            Self::UnknownFunction(_) => StatusCode::NOT_FOUND,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Unsigned => "unsigned",
            Self::Signature(_) => "bad_signature",
            Self::Malformed(_) => "malformed",
            Self::UnknownFunction(_) => "unknown_function",
        }
    }
}

impl IntoResponse for ServeError {
    fn into_response(self) -> Response {
        let status = self.status();
        // RFC 9457 Problem Details, the same shape the server produces, so an
        // operator reading a log does not have to know which end wrote it.
        let body = serde_json::json!({
            "type": "about:blank",
            "title": self.to_string(),
            "status": status.as_u16(),
            "code": self.code(),
        });
        (
            status,
            [("content-type", "application/problem+json")],
            body.to_string(),
        )
            .into_response()
    }
}

/// Bounded cache of recently-seen nonces.
///
/// The timestamp window alone permits replay of a captured body for its whole
/// width, which is why protocol §9 requires a nonce covered by the MAC *and* a
/// cache of what has been seen. Bounded to the window: entries older than the
/// tolerance can never be accepted again anyway, so keeping them is pure cost.
///
/// Per-replica is sufficient. A replay to a different replica still cannot
/// produce a duplicate recorded effect, because the fence has moved on.
pub struct NonceCache {
    seen: Mutex<VecDeque<(String, i64)>>,
    window_secs: i64,
    capacity: usize,
}

impl NonceCache {
    /// A cache sized to the signature tolerance window.
    pub fn new(window_secs: i64) -> Self {
        Self {
            seen: Mutex::new(VecDeque::new()),
            window_secs,
            capacity: 100_000,
        }
    }

    /// Record a nonce, returning `false` if it was already seen.
    pub fn check_and_insert(&self, nonce: &str, now: i64) -> bool {
        let mut seen = self.seen.lock().unwrap();
        while let Some((_, t)) = seen.front() {
            if now - *t > self.window_secs {
                seen.pop_front();
            } else {
                break;
            }
        }
        if seen.iter().any(|(n, _)| n == nonce) {
            return false;
        }
        // A hard cap as well as a time bound: an attacker who can send faster
        // than the window expires must not be able to grow this without limit.
        // Dropping the oldest is safe — it is already close to expiry.
        if seen.len() >= self.capacity {
            seen.pop_front();
        }
        seen.push_back((nonce.to_string(), now));
        true
    }
}

impl Default for NonceCache {
    fn default() -> Self {
        Self::new(sig::DEFAULT_TOLERANCE_SECS)
    }
}

/// Verify a request's signature and nonce.
///
/// Split out from the router so the same checks can be exercised directly, and
/// so an alternative transport (Lambda, a queue consumer) reuses them instead of
/// reimplementing the part that must not be got wrong.
pub fn verify_request(
    keys: &[Vec<u8>],
    dev_mode: bool,
    headers: &HeaderMap,
    body: &str,
    nonces: &NonceCache,
    now: i64,
) -> Result<(), ServeError> {
    let header = headers
        .get(stepd_proto::HEADER_SIGNATURE)
        .and_then(|v| v.to_str().ok());

    let Some(header) = header else {
        if dev_mode {
            return Ok(());
        }
        return Err(ServeError::Unsigned);
    };

    // Verify before touching the nonce cache: an attacker must not be able to
    // poison the cache with nonces they never had a valid signature for, which
    // would let them lock out the legitimate sender's next request.
    let nonce = sig::verify(keys, header, body, now, sig::DEFAULT_TOLERANCE_SECS)?;

    if !nonces.check_and_insert(&nonce, now) {
        return Err(ServeError::Signature(sig::SignatureError::Replay));
    }
    Ok(())
}

struct ServeState {
    app: App,
    nonces: NonceCache,
}

impl App {
    /// The axum router for this app.
    ///
    /// Two routes, both required by protocol §3: the attempt endpoint at the
    /// root, and the pull-discovery manifest.
    pub fn router(self) -> Router {
        let state = Arc::new(ServeState {
            app: self,
            nonces: NonceCache::default(),
        });

        // Lint findings are warnings, not start-up failures. A function with no
        // trigger is a mistake, but refusing to boot over it would take down an
        // app whose other forty functions are fine.
        for finding in state.app.lint() {
            warn!(target: "stepd::lint", "{finding}");
        }

        Router::new()
            .route("/", post(attempt))
            .route("/.well-known/stepd", get(manifest))
            .with_state(state)
    }
}

async fn manifest(State(state): State<Arc<ServeState>>) -> Json<serde_json::Value> {
    Json(state.app.manifest())
}

async fn attempt(
    State(state): State<Arc<ServeState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let raw = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => return ServeError::Malformed("body is not UTF-8".into()).into_response(),
    };

    let now = chrono::Utc::now().timestamp();
    if let Err(e) = verify_request(
        &state.app.keys,
        state.app.dev_mode,
        &headers,
        raw,
        &state.nonces,
        now,
    ) {
        return e.into_response();
    }

    let attempt: Attempt = match serde_json::from_str(raw) {
        Ok(a) => a,
        Err(e) => return ServeError::Malformed(e.to_string()).into_response(),
    };

    // Protocol §11: never emit ops above the negotiated version. A server
    // speaking a version we do not is a deployment error, not a run failure.
    if attempt.protocol != PROTOCOL_VERSION {
        return ServeError::Malformed(format!(
            "protocol version '{}' is not supported by {} (speaks '{}')",
            attempt.protocol,
            crate::SDK_VERSION,
            PROTOCOL_VERSION
        ))
        .into_response();
    }

    // §8.6: the SDK must fetch the rest of a paginated journal before replaying,
    // or fail non-retryably. Replaying against a partial journal would re-execute
    // steps whose results merely were not sent — silent double execution, which
    // is worse than a clear failure.
    if attempt.state_truncated {
        return ServeError::Malformed(
            "the attempt journal was truncated and this SDK build cannot yet page it; \
             raise STEPD_ATTEMPT_STATE_LIMIT on the server or split the run with \
             continue_as_new (protocol §8.6)"
                .into(),
        )
        .into_response();
    }

    let function_id = attempt.run.function_id.clone();
    let Some(handler) = state.app.handler(&function_id) else {
        return ServeError::UnknownFunction(function_id).into_response();
    };

    let run_id = attempt.run.id;
    let deadline = attempt.deadline;

    // The pass runs on a detached local worker. If this request future is
    // dropped — client disconnect, server-side timeout — the pass carries on to
    // completion and the response is simply abandoned. Protocol §7.1.1: the
    // server treats no response as `unknown` and retries; it must never be able
    // to leave a step half-executed.
    let Some((outcome, extras)) = state.app.executor.run(handler, attempt).await else {
        return ServeError::Malformed("handler pool unavailable".into()).into_response();
    };

    if let Some(d) = deadline {
        if chrono::Utc::now() > d {
            // Worth saying out loud: the run is fine, but the operator is paying
            // for repeated work every time this happens.
            warn!(
                run = %run_id,
                "attempt overran its deadline; the response will be discarded as a stale \
                 fence and the step re-executed. Raise timeouts.attempt above the slowest step."
            );
        }
    }

    let mut response = match outcome {
        stepd_sdk_core::PassOutcome::Done(data) => {
            AttemptResponse::single(Op::Done { data: Some(data) })
        }
        stepd_sdk_core::PassOutcome::Yield(ops) => AttemptResponse::batch(ops),
        stepd_sdk_core::PassOutcome::Error {
            mut ops,
            retryable,
            error,
        } => {
            // The error goes last, after whatever the pass managed to record.
            // A group with two successful members and one that raised commits
            // all three outcomes and then fails the run; sending the error on
            // its own would fail a run having thrown away work it did (§5.2.2).
            ops.push(Op::Error {
                retryable,
                step: None,
                error,
            });
            AttemptResponse::batch(ops)
        }
    };
    response.emit = extras.emit;
    response.orphaned_steps = extras.orphaned;

    if extras.orphaned > 0 {
        // Answers "why did my step re-run?" without the developer having to
        // reconstruct it from two deploys' worth of source (F-DX-2, F-DX-4).
        warn!(
            run = %run_id,
            orphaned = extras.orphaned,
            "recorded steps were not encountered this pass — a step id was renamed or removed, \
             and its side effect will re-execute under the new id"
        );
    }
    debug!(run = %run_id, ops = response.ops.len(), "attempt complete");

    // Validate our own envelope before sending it. An SDK that emits a malformed
    // batch fails the run at the server with a message about the server; failing
    // here names the SDK, which is where the bug is.
    if let Err(e) = response.validate() {
        return ServeError::Malformed(format!("SDK produced an invalid envelope: {e}"))
            .into_response();
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        stepd_proto::HEADER_PROTOCOL,
        PROTOCOL_VERSION.parse().unwrap(),
    );
    headers.insert(stepd_proto::HEADER_SDK, crate::SDK_VERSION.parse().unwrap());

    // Sign the response: signing is required in both directions (protocol §9).
    let body = serde_json::to_string(&response).unwrap_or_default();
    if let Some(key) = state.app.keys.first() {
        let nonce = uuid::Uuid::new_v4().to_string();
        let header = sig::sign(key, &body, now, &nonce);
        headers.insert(stepd_proto::HEADER_SIGNATURE, header.parse().unwrap());
        headers.insert(stepd_proto::HEADER_NONCE, nonce.parse().unwrap());
    }

    (StatusCode::OK, headers, body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(sig_header: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(stepd_proto::HEADER_SIGNATURE, sig_header.parse().unwrap());
        h
    }

    #[test]
    fn an_unsigned_request_is_refused_unless_dev_mode_is_explicit() {
        let cache = NonceCache::default();
        let keys = vec![b"k".to_vec()];
        assert!(matches!(
            verify_request(&keys, false, &HeaderMap::new(), "{}", &cache, 1000),
            Err(ServeError::Unsigned)
        ));
        assert!(verify_request(&keys, true, &HeaderMap::new(), "{}", &cache, 1000).is_ok());
    }

    #[test]
    fn a_replayed_nonce_is_refused() {
        let cache = NonceCache::default();
        let keys = vec![b"k".to_vec()];
        let h = headers_with(&sig::sign(b"k", "{}", 1000, "n1"));
        assert!(verify_request(&keys, false, &h, "{}", &cache, 1000).is_ok());
        assert!(
            matches!(
                verify_request(&keys, false, &h, "{}", &cache, 1000),
                Err(ServeError::Signature(sig::SignatureError::Replay))
            ),
            "the timestamp window alone permits replay for its whole width"
        );
    }

    #[test]
    fn a_bad_signature_cannot_poison_the_nonce_cache() {
        // If the cache were populated before verification, an attacker could
        // burn the nonces a legitimate sender is about to use.
        let cache = NonceCache::default();
        let keys = vec![b"real".to_vec()];
        let forged = headers_with(&sig::sign(b"wrong", "{}", 1000, "n1"));
        assert!(verify_request(&keys, false, &forged, "{}", &cache, 1000).is_err());

        let genuine = headers_with(&sig::sign(b"real", "{}", 1000, "n1"));
        assert!(
            verify_request(&keys, false, &genuine, "{}", &cache, 1000).is_ok(),
            "a rejected request must not consume the nonce"
        );
    }

    #[test]
    fn the_nonce_cache_forgets_what_can_no_longer_be_replayed() {
        let cache = NonceCache::new(300);
        assert!(cache.check_and_insert("n1", 1000));
        assert!(!cache.check_and_insert("n1", 1000));
        // Past the window the signature itself is expired, so keeping the nonce
        // buys nothing and costs memory.
        assert!(cache.check_and_insert("n1", 2000));
    }

    #[test]
    fn the_nonce_cache_is_bounded_even_under_a_flood() {
        let cache = NonceCache {
            seen: Mutex::new(VecDeque::new()),
            window_secs: 300,
            capacity: 8,
        };
        for i in 0..100 {
            cache.check_and_insert(&format!("n{i}"), 1000);
        }
        assert!(cache.seen.lock().unwrap().len() <= 8);
    }

    #[test]
    fn error_statuses_match_the_servers_documented_reactions() {
        // These are load-bearing: the server fails a run on 400 and merely
        // retries on 401/404, so a mis-mapped status turns a deploy blip into a
        // permanently failed run.
        assert_eq!(ServeError::Unsigned.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            ServeError::Malformed("x".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServeError::UnknownFunction("f".into()).status(),
            StatusCode::NOT_FOUND
        );
    }
}
