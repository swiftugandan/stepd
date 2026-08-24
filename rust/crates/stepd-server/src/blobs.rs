//! The managed-blob transfer endpoints (protocol §8.3).
//!
//! Two routes, and the reason there are only two is the point of the design:
//! **bytes never traverse the control plane on the way in**. The app reserves,
//! uploads directly to wherever bytes live, and hands back a reference. §8.5
//! forbids the server from interpreting payload content at all.
//!
//! The filesystem backend shipped here is the exception that proves it. It has
//! no presigned-URL service of its own, so §8.3.2's compatibility fallback
//! applies: the server relays the bytes. That is a fallback, it is warned about,
//! and an S3-backed store implementing the same trait removes this route from
//! the path entirely without anything above it changing.
//!
//! ## What authorises a transfer
//!
//! Not the caller's token. A capability in the URL, signed with an HMAC over the
//! blob id, the direction, the declared size and the expiry.
//!
//! An upload URL is handed to an app that may hold no console token at all, and
//! it must be usable exactly once, for one blob, up to one size. A capability
//! expresses that; a session does not. And because the signature covers the
//! direction and the size, a leaked write URL cannot be turned into a read URL,
//! cannot be pointed at another blob, and cannot store more than was reserved.
//!
//! Content substitution is separately impossible: `commit_blob` verifies the
//! digest before the blob becomes readable, so even a stolen URL used in time
//! can only store bytes that hash to what the app already declared.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use stepd_core::traits::{BlobSpec, BlobStore, Reservation};
use tracing::warn;
use uuid::Uuid;

use crate::auth::{Principal, Role};
use crate::problem::{ApiResult, Problem};
use crate::ServerState;

/// Mount the transfer endpoints.
///
/// `relay` mounts §8.3.2's compatibility route. It is passed rather than
/// assumed because a backend that presigns has no use for it, and an unused
/// route that accepts bytes is a second way in that nothing warns about.
pub fn router(relay: bool) -> Router<ServerState> {
    let r = Router::new().route("/v1/blobs:reserve", post(reserve));
    if !relay {
        return r;
    }
    r.route(
        "/v1/blobs/{id}/content",
        get(read_content).put(write_content),
    )
}

/// `POST /v1/blobs:reserve` — phase one of the two-phase upload.
#[derive(Debug, Deserialize)]
pub struct ReserveRequest {
    /// The run the blob will belong to. Scopes the reservation to a namespace.
    pub run_id: Uuid,
    /// Step this will be attached to. Informational.
    #[serde(default)]
    pub step_id: Option<String>,
    /// Declared size in bytes.
    pub size: i64,
    /// Lowercase hex SHA-256 of the content.
    pub sha256: String,
    /// Media type.
    #[serde(default)]
    pub content_type: Option<String>,
    /// Original filename, presentational only.
    #[serde(default)]
    pub filename: Option<String>,
}

/// The reservation, as `blob-reserve.schema.json#/$defs/response`.
#[derive(Debug, Serialize)]
pub struct ReserveResponse {
    /// Assigned id.
    pub blob_id: Uuid,
    /// True when the digest already exists here and the app must skip the upload.
    pub deduplicated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Write-scoped URL. Absent when deduplicated.
    pub upload_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Method to use for the upload.
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Headers the app must send verbatim.
    pub headers: Option<std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// When the URL stops working.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn reserve(
    State(state): State<ServerState>,
    principal: Principal,
    Json(req): Json<ReserveRequest>,
) -> ApiResult<(StatusCode, Json<ReserveResponse>)> {
    principal.require(Role::Operator)?;
    let blobs = state.blobs()?;

    // The run must be in the caller's namespace. Without this check a token for
    // one namespace could reserve against another's run id and land a blob in
    // its dedupe pool — which is also a probe for whether a digest exists there.
    let owned: bool = sqlx::query_scalar("SELECT exists(SELECT 1 FROM runs WHERE id=$1 AND ns=$2)")
        .bind(req.run_id)
        .bind(&principal.namespace)
        .fetch_one(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;
    if !owned {
        // Deliberately the same answer as a run that does not exist. Telling the
        // caller which of the two it is says whether a run id is real in another
        // namespace.
        return Err(Problem::not_found(
            "no_such_run",
            "no such run in this namespace",
        ));
    }

    if req.sha256.len() != 64 || !req.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Problem::bad_request(
            "bad_digest",
            "sha256 must be 64 lowercase hex characters",
        ));
    }

    let spec = BlobSpec {
        size: req.size,
        sha256: req.sha256,
        content_type: req.content_type,
        filename: req.filename,
    };

    match blobs.reserve(&principal.namespace, spec).await {
        Ok(Reservation::Deduplicated { id }) => Ok((
            StatusCode::OK,
            Json(ReserveResponse {
                blob_id: id,
                deduplicated: true,
                upload_url: None,
                method: None,
                headers: None,
                expires_at: None,
            }),
        )),
        Ok(Reservation::Upload {
            id,
            url,
            method,
            headers,
            expires_at,
        }) => Ok((
            StatusCode::CREATED,
            Json(ReserveResponse {
                blob_id: id,
                deduplicated: false,
                upload_url: Some(url),
                method: Some(method),
                headers: Some(headers.into_iter().collect()),
                expires_at: Some(expires_at),
            }),
        )),
        // The ceiling is a `payload_too_large`, and it says what to do instead:
        // an external `$ref` for data the application already stores.
        Err(e) if e.to_string().contains("payload_too_large") => {
            Err(Problem::bad_request("payload_too_large", e.to_string()))
        }
        Err(e) => Err(Problem::bad_request("bad_reservation", e.to_string())),
    }
}

/// The capability, as it arrives on the transfer URL.
#[derive(Debug, Deserialize)]
pub struct Capability {
    dir: String,
    size: i64,
    exp: i64,
    sig: String,
}

/// `PUT /v1/blobs/{id}/content` — the relay fallback (§8.3.2).
async fn write_content(
    State(state): State<ServerState>,
    Path(id): Path<Uuid>,
    Query(cap): Query<Capability>,
    body: Bytes,
) -> Result<Response, Problem> {
    let blobs = state.blobs()?;

    // No `Principal` extractor on this route, deliberately. The capability *is*
    // the authorisation, and requiring a token as well would mean an app that
    // holds only an upload URL — which is the whole point of handing one out —
    // could not use it.
    if cap.dir != "write" {
        return Err(Problem::bad_request(
            "wrong_direction",
            "this capability is not scoped for writing",
        ));
    }
    let declared = blobs
        .capability()
        .verify(id, "write", cap.size, cap.exp, &cap.sig)
        .map_err(|e| Problem::bad_request("bad_capability", e.to_string()))?;

    // The declared size is in the signature for exactly this check: a valid
    // upload URL must not become a way to store an arbitrary amount of data.
    if body.len() as i64 != declared {
        return Err(Problem::bad_request(
            "size_mismatch",
            format!(
                "the capability reserved {declared} bytes and the body is {}",
                body.len()
            ),
        ));
    }

    warn!(
        blob = %id, bytes = body.len(),
        "blob bytes relayed through the server (protocol §8.3.2 fallback); a store that \
         can presign keeps payload bytes out of the control plane entirely"
    );

    blobs
        .put_bytes(id, &body)
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

    // Commit here rather than at op-commit time. Verification is what makes the
    // blob readable, and doing it now means a digest mismatch is reported to the
    // uploader — who can retry — instead of surfacing later as a failed run
    // whose cause is two systems away.
    let blob = blobs.commit_blob(id).await.map_err(commit_blob_problem)?;

    Ok((StatusCode::CREATED, Json(blob)).into_response())
}

/// Turn a `commit_blob` failure into the right response.
///
/// `commit_blob` returns `Error::Config` only for a real mismatch — the store
/// answered and the bytes it holds do not match what was declared — which is
/// what §8.3.2 reserves `blob_digest_mismatch` for. Anything else
/// (`Error::Store`, `Error::Transport`) is the store failing to answer at all:
/// no checksum in the object's metadata, or a HEAD that never landed. Telling
/// the app `blob_digest_mismatch` for that says the upload was corrupt when
/// the truth is the backend could not be asked. A free function so this
/// mapping is testable without a database or an HTTP request.
fn commit_blob_problem(e: stepd_core::Error) -> Problem {
    match e {
        stepd_core::Error::Config(msg) => Problem::bad_request("blob_digest_mismatch", msg),
        other => Problem::bad_gateway(
            "blob_backend_unavailable",
            format!("could not verify the upload against the blob store: {other}"),
        ),
    }
}

/// `GET /v1/blobs/{id}/content` — read, with `Range` (§8.3.3).
async fn read_content(
    State(state): State<ServerState>,
    Path(id): Path<Uuid>,
    Query(cap): Query<Capability>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let blobs = state.blobs()?;

    if cap.dir != "read" {
        return Err(Problem::bad_request(
            "wrong_direction",
            "this capability is not scoped for reading",
        ));
    }
    let size = blobs
        .capability()
        .verify(id, "read", cap.size, cap.exp, &cap.sig)
        .map_err(|e| Problem::bad_request("bad_capability", e.to_string()))?;

    // `Range` is required by §8.3.3 so a step can read a file header without
    // pulling the whole object — which is the difference between a workflow that
    // inspects a hundred-megabyte upload and one that downloads it forty times
    // over forty replays.
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_range(v, size));

    let bytes = blobs
        .get_bytes(id, range)
        .await
        .map_err(|_| Problem::not_found("no_such_blob", "no such blob"))?;

    let mut res = Response::builder().header(header::CONTENT_LENGTH, bytes.len());
    let status = match range {
        Some((from, to)) => {
            res = res.header(header::CONTENT_RANGE, format!("bytes {from}-{to}/{size}"));
            StatusCode::PARTIAL_CONTENT
        }
        None => {
            res = res.header(header::ACCEPT_RANGES, "bytes");
            StatusCode::OK
        }
    };

    res.status(status)
        .body(axum::body::Body::from(bytes))
        .map_err(|e| Problem::internal(e.to_string()))
}

/// Parse a single `bytes=` range, clamped to the object.
///
/// Deliberately narrow: one range, no multipart. A multi-range response is a
/// MIME document the server would have to assemble, and §8.3.3 asks for enough
/// to read a header cheaply, not for a general HTTP range implementation.
fn parse_range(header: &str, size: i64) -> Option<(u64, u64)> {
    let spec = header.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (from, to) = spec.split_once('-')?;
    let last = (size - 1).max(0) as u64;

    let out = match (from.trim(), to.trim()) {
        // `bytes=-N`: the final N bytes. Reading a trailer, typically.
        ("", n) => {
            let n: u64 = n.parse().ok()?;
            (size as u64 - n.min(size as u64), last)
        }
        (a, "") => (a.parse().ok()?, last),
        (a, b) => (a.parse().ok()?, b.parse::<u64>().ok()?.min(last)),
    };
    (out.0 <= out.1).then_some(out)
}

impl ServerState {
    /// The blob store, or a problem explaining why there is not one.
    ///
    /// A 501 rather than a 500: a server with no blob signing key is correctly
    /// configured for a deployment that does not use managed blobs, and the
    /// caller needs to know it is a configuration answer rather than a fault.
    fn blobs(&self) -> Result<&stepd_store_postgres::PostgresBlobStore, Problem> {
        self.blobs.as_deref().ok_or_else(|| {
            Problem::not_implemented(
                "blobs_disabled",
                "managed blobs are not configured on this server; set \
                 STEPD_BLOB_SIGNING_KEY, or use external $ref values for payloads the \
                 application already stores",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{commit_blob_problem, parse_range};
    use axum::http::StatusCode;

    #[test]
    fn a_real_mismatch_is_reported_as_a_digest_mismatch() {
        // §8.3.2 reserves this code for the store having answered and the
        // bytes it holds not matching what was declared.
        let p = commit_blob_problem(stepd_core::Error::Config(
            "blob_digest_mismatch: content does not match the declared sha256".into(),
        ));
        assert_eq!(p.status, StatusCode::BAD_REQUEST);
        assert_eq!(p.code, "blob_digest_mismatch");
    }

    #[test]
    fn a_store_that_could_not_answer_is_not_reported_as_a_digest_mismatch() {
        // The two conditions Task 4 added: no checksum in the object's
        // metadata, or a HEAD that never landed. Neither is the app's fault,
        // and telling it "blob_digest_mismatch" says its upload was corrupt
        // when the truth is the backend could not be asked.
        for e in [
            stepd_core::Error::Store("the object store reports no checksum".into()),
            stepd_core::Error::Transport("HeadObject for blob: connection refused".into()),
        ] {
            let p = commit_blob_problem(e);
            assert_eq!(p.status, StatusCode::BAD_GATEWAY, "got code {}", p.code);
            assert_ne!(
                p.code, "blob_digest_mismatch",
                "a backend failure must not read as a claim about the app's bytes"
            );
        }
    }

    #[test]
    fn a_bounded_range_is_clamped_to_the_object() {
        assert_eq!(parse_range("bytes=0-9", 100), Some((0, 9)));
        // Past the end is clamped rather than refused: a client that asks for
        // more than exists wants what exists, and 416 would make reading the
        // tail of an object require knowing its length first.
        assert_eq!(parse_range("bytes=90-999", 100), Some((90, 99)));
    }

    #[test]
    fn an_open_ended_range_reads_to_the_end() {
        assert_eq!(parse_range("bytes=50-", 100), Some((50, 99)));
    }

    #[test]
    fn a_suffix_range_reads_the_final_bytes() {
        // `bytes=-10` is the last ten bytes, not the first ten. Getting this
        // backwards returns plausible-looking data from the wrong end.
        assert_eq!(parse_range("bytes=-10", 100), Some((90, 99)));
    }

    #[test]
    fn a_multipart_or_malformed_range_is_ignored_rather_than_guessed_at() {
        // Returning `None` means the whole object is served, which is always a
        // correct answer. Guessing which of two ranges was meant is not.
        assert_eq!(parse_range("bytes=0-9,20-29", 100), None);
        assert_eq!(parse_range("items=0-9", 100), None);
        assert_eq!(parse_range("bytes=abc", 100), None);
        assert_eq!(parse_range("bytes=90-10", 100), None);
    }
}
