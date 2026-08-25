//! Namespace-scoped authorisation (F-SEC-1).
//!
//! Promoted to v1 because a namespace is a security boundary, not a filter. The
//! rule that follows from that, and which the query layer enforces rather than
//! the response layer: **a token for one namespace must not be able to observe
//! that another namespace exists.** Filtering results after the fact satisfies
//! neither half — it leaks through counts, through cursors, and through the
//! difference between "forbidden" and "absent".

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::problem::Problem;
use crate::ServerState;

/// Roles, in increasing order of authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Read-only.
    Viewer,
    /// May cancel, retry and resolve waits.
    Operator,
    /// May also manage tokens and namespaces.
    Admin,
}

impl Role {
    /// Parse the database's text form.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "viewer" => Some(Self::Viewer),
            "operator" => Some(Self::Operator),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    /// The text form stored in the database.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Operator => "operator",
            Self::Admin => "admin",
        }
    }
}

/// An authenticated caller.
#[derive(Debug, Clone)]
pub struct Principal {
    /// The one namespace this token can see.
    pub namespace: String,
    /// What it may do there.
    pub role: Role,
}

impl Principal {
    /// Require at least `role`, or fail.
    pub fn require(&self, role: Role) -> Result<(), Problem> {
        if self.role >= role {
            Ok(())
        } else {
            Err(Problem::forbidden(format!(
                "this token has role '{}' and the operation requires '{}'",
                self.role.as_str(),
                role.as_str()
            )))
        }
    }
}

/// Hash a bearer token for storage and lookup.
///
/// Only the hash is stored. A database dump — a backup, a replica, a support
/// export — must not be a set of working credentials.
pub fn token_hash(raw: &str) -> Vec<u8> {
    Sha256::digest(raw.as_bytes()).to_vec()
}

impl FromRequestParts<ServerState> for Principal {
    type Rejection = Problem;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServerState,
    ) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();

        let Some(raw) = header.strip_prefix("Bearer ") else {
            return Err(Problem::unauthenticated("a Bearer token is required"));
        };

        let row = sqlx::query(
            r#"SELECT ns, role FROM tokens
                WHERE token_hash = $1 AND revoked_at IS NULL
                  AND (expires_at IS NULL OR expires_at > now())"#,
        )
        .bind(token_hash(raw))
        .fetch_optional(state.store.pool())
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

        let Some(row) = row else {
            return Err(Problem::unauthenticated(
                "unknown, expired or revoked token",
            ));
        };

        let role_text: String = row.get("role");
        Ok(Principal {
            namespace: row.get("ns"),
            role: Role::parse(&role_text)
                .ok_or_else(|| Problem::internal(format!("unknown role '{role_text}'")))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_ordered_so_a_single_comparison_expresses_the_rule() {
        assert!(Role::Admin > Role::Operator);
        assert!(Role::Operator > Role::Viewer);
    }

    #[test]
    fn a_viewer_cannot_perform_an_operator_action() {
        let p = Principal {
            namespace: "prod".into(),
            role: Role::Viewer,
        };
        assert!(p.require(Role::Operator).is_err());
        assert!(p.require(Role::Viewer).is_ok());
    }

    #[test]
    fn an_admin_can_perform_every_lesser_action() {
        let p = Principal {
            namespace: "prod".into(),
            role: Role::Admin,
        };
        for r in [Role::Viewer, Role::Operator, Role::Admin] {
            assert!(p.require(r).is_ok());
        }
    }

    #[test]
    fn only_the_hash_of_a_token_is_ever_stored() {
        let raw = "secret-token-value";
        let h = token_hash(raw);
        assert_eq!(h.len(), 32);
        assert!(
            !String::from_utf8_lossy(&h).contains(raw),
            "a database dump must not be a set of working credentials"
        );
        assert_eq!(
            token_hash(raw),
            token_hash(raw),
            "lookup must be deterministic"
        );
        assert_ne!(token_hash(raw), token_hash("other"));
    }

    #[test]
    fn unknown_roles_do_not_silently_become_viewers() {
        // Defaulting an unrecognised role to the least privilege sounds safe and
        // is not: it turns a migration mistake into an operator who quietly loses
        // the ability to cancel a run mid-incident, with no error to point at.
        assert_eq!(Role::parse("superuser"), None);
    }
}
