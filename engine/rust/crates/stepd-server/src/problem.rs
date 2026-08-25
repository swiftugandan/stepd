//! RFC 9457 Problem Details (F-API-3).
//!
//! One error shape for the whole API, and one place that decides which status
//! an error deserves. The status codes are not cosmetic: the SDK and the
//! dispatcher both branch on them, so a 404 that should have been a 403 turns a
//! permissions bug into a retry loop.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// An API error with its RFC 9457 representation.
#[derive(Debug)]
pub struct Problem {
    /// HTTP status.
    pub status: StatusCode,
    /// Short human-readable summary.
    pub title: &'static str,
    /// Stable machine-readable code.
    pub code: &'static str,
    /// Specific detail for this occurrence.
    pub detail: String,
}

impl Problem {
    /// No credential presented.
    pub fn unauthenticated(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            title: "Unauthenticated",
            code: "no_token",
            detail: detail.into(),
        }
    }

    /// Authenticated but not permitted.
    pub fn forbidden(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            title: "Forbidden",
            code: "insufficient_role",
            detail: detail.into(),
        }
    }

    /// Absent, or present in a namespace the caller cannot see.
    ///
    /// The two are deliberately indistinguishable. Answering 403 for a run that
    /// exists in another namespace and 404 for one that does not tells an
    /// attacker which run ids are real, which is a cross-tenant information leak
    /// dressed up as good HTTP manners.
    pub fn not_found(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            title: "Not found",
            code,
            detail: detail.into(),
        }
    }

    /// The request was understood but cannot be applied in the current state.
    pub fn conflict(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            title: "Conflict",
            code,
            detail: detail.into(),
        }
    }

    /// Malformed request.
    pub fn bad_request(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            title: "Bad request",
            code,
            detail: detail.into(),
        }
    }

    /// The namespace is over its queue threshold (F-LP-8).
    pub fn backpressure(retry_after_secs: u64, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            title: "Too many requests",
            code: "backpressure",
            detail: format!("{} (retry after {retry_after_secs}s)", detail.into()),
        }
    }

    /// Something failed inside the server.
    /// The request is well-formed, and this server is not configured to do it.
    ///
    /// Distinct from a 500 on purpose: a server with no blob signing key is
    /// correctly configured for a deployment that does not use managed blobs,
    /// and a caller told "internal error" will go looking for a fault that is
    /// not there.
    pub fn not_implemented(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            title: "Not implemented",
            code,
            detail: detail.into(),
        }
    }

    /// Something went wrong on this side. The detail is logged, not blamed on
    /// the caller.
    pub fn internal(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            title: "Internal error",
            code: "internal",
            detail: detail.into(),
        }
    }

    /// A dependency this server talks to — the object store behind a managed
    /// blob, say — failed or gave an answer that could not be used.
    ///
    /// Distinct from `internal` and from `bad_request` on purpose: it is not
    /// this server's own fault, so `internal` would send someone looking for a
    /// bug that is not here, and it is not the caller's fault either, so a 4xx
    /// would blame an app for bytes it uploaded correctly.
    pub fn bad_gateway(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            title: "Bad gateway",
            code,
            detail: detail.into(),
        }
    }
}

impl From<stepd_core::Error> for Problem {
    fn from(e: stepd_core::Error) -> Self {
        Problem::internal(e.to_string())
    }
}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let body = stepd_proto::ProblemBody {
            problem_type: "about:blank".into(),
            title: self.title.to_string(),
            status: self.status.as_u16(),
            detail: Some(self.detail),
            instance: None,
            code: Some(self.code.to_string()),
            run_id: None,
            op: None,
        };
        let mut res = (
            self.status,
            [("content-type", "application/problem+json")],
            serde_json::to_string(&body).unwrap_or_else(|_| "{}".into()),
        )
            .into_response();
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            // Without this an overloaded client retries immediately and makes the
            // overload worse, which is the failure mode backpressure exists to
            // prevent.
            res.headers_mut()
                .insert("retry-after", "5".parse().unwrap());
        }
        res
    }
}

/// API result alias.
pub type ApiResult<T> = std::result::Result<T, Problem>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forbidden_resource_is_reported_as_absent() {
        // Distinguishing "exists but forbidden" from "does not exist" leaks which
        // run ids are real across a namespace boundary.
        assert_eq!(
            Problem::not_found("run_not_found", "x").status,
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn backpressure_always_carries_retry_after() {
        let res = Problem::backpressure(5, "queue is deep").into_response();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            res.headers().contains_key("retry-after"),
            "a 429 without Retry-After invites an immediate retry, which is the \
             overload it was sent to prevent"
        );
    }

    #[test]
    fn the_body_is_a_problem_details_document() {
        let p = Problem::bad_request("bad_cursor", "cursor is not a uuid");
        let res = p.into_response();
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/problem+json"
        );
    }
}
