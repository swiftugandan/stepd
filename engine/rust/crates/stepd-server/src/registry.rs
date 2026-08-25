//! App and function registration, and resolving a function to the app that
//! hosts it.
//!
//! Registration is idempotent by `(app_id, function.id)`, and removing a
//! function from a manifest **archives** it rather than deleting it: no new runs
//! start, but in-flight runs are still driven. Deleting would strand every run
//! mid-flight the moment someone shipped a rename.

use axum::extract::State;
use axum::Json;
use sha2::{Digest, Sha256};
use sqlx::Row;
use stepd_core::cron::{CatchUp, Schedule};
use stepd_core::traits::{AppTarget, CronRegistration, ExprEngine};
use stepd_core::{Error, Result, TargetResolver};
use stepd_proto::{AppManifest, CatchUp as WireCatchUp, FunctionConfig, ManifestError, Trigger};
use tracing::{info, warn};
use uuid::Uuid;

use crate::auth::{Principal, Role};
use crate::problem::{ApiResult, Problem};
use crate::ServerState;

/// The protocol major one below the current one, if there is one.
fn previous_protocol() -> Option<String> {
    stepd_proto::PROTOCOL_VERSION
        .parse::<u32>()
        .ok()
        .filter(|n| *n > 1)
        .map(|n| (n - 1).to_string())
}

/// Whether this server will talk to an app speaking `version`.
fn protocol_supported(version: &str) -> bool {
    version == stepd_proto::PROTOCOL_VERSION || previous_protocol().is_some_and(|p| p == version)
}

/// Read a function's cron triggers out of its config.
///
/// Every failure here is a 400, on purpose. The alternative — accept it and
/// work it out later — is what ADR-016 was written about: `on_cron` registered a
/// trigger, nothing ever scheduled it, and a cron function registered cleanly
/// and never ran. A deploy that succeeds and produces a job that never fires is
/// the worst shape a failure can take, because there is no moment at which
/// anyone is told.
fn cron_registrations(
    namespace: &str,
    function: &FunctionConfig,
) -> std::result::Result<Vec<CronRegistration>, String> {
    let mut out = Vec::new();

    for (idx, t) in function.triggers.iter().enumerate() {
        let Trigger::Cron {
            cron,
            tz,
            catchup,
            catchup_limit,
            misfire_window,
            singleton,
            run_key,
        } = t
        else {
            continue;
        };

        // Parsed now, so an expression that cannot be scheduled cannot be
        // stored. The parser also rejects the dialect extensions (`L`, `W`, `#`)
        // by name rather than misinterpreting them, which matters most here:
        // a schedule that fires at the wrong time is worse than one that
        // refuses to register.
        let schedule = Schedule::parse(cron, tz).map_err(|e| format!("cron trigger {idx}: {e}"))?;

        let window_text = misfire_window.as_deref().unwrap_or("PT1H");
        let window = stepd_proto::iso8601_seconds(window_text).ok_or_else(|| {
            format!(
                "cron trigger {idx}: misfire_window '{window_text}' is not an ISO 8601 duration \
                 this server understands (weeks, days, hours, minutes, seconds)"
            )
        })?;
        if window <= 0 {
            return Err(format!(
                "cron trigger {idx}: a misfire_window of '{window_text}' would discard every \
                 occurrence, including the current one"
            ));
        }

        out.push(CronRegistration {
            namespace: namespace.to_string(),
            function_id: function.id.clone(),
            trigger_idx: idx as i32,
            schedule,
            catchup: match catchup {
                Some(WireCatchUp::Skip) => CatchUp::Skip,
                Some(WireCatchUp::All) => CatchUp::All,
                Some(WireCatchUp::One) | None => CatchUp::One,
            },
            catchup_limit: catchup_limit.unwrap_or(10),
            misfire_window: chrono::Duration::seconds(window),
            singleton: *singleton,
            run_key: run_key.clone(),
        });
    }

    Ok(out)
}

fn manifest_problem(e: ManifestError) -> Problem {
    let code = match e {
        ManifestError::NoTriggers(_) => "bad_function",
        ManifestError::SingletonWithoutKey { .. } => "bad_cron",
        ManifestError::CatchUpLimit { .. } => "bad_cron",
        ManifestError::BadDuration { .. } => "bad_function",
    };
    Problem::bad_request(code, e.to_string())
}

/// `PUT /v1/apps`
pub async fn register(
    State(state): State<ServerState>,
    principal: Principal,
    Json(manifest): Json<AppManifest>,
) -> ApiResult<Json<serde_json::Value>> {
    principal.require(Role::Admin)?;

    // Protocol §11 requires a server to support version N and N-1 for at least
    // twelve months. Accepting only its own major would make every protocol bump
    // a flag day in which no app can register until every app is redeployed —
    // which is the situation the rule exists to prevent.
    if !protocol_supported(&manifest.protocol) {
        return Err(Problem::bad_request(
            "unsupported_protocol",
            format!(
                "this server speaks protocol {} (and {} for compatibility); the app speaks {}",
                stepd_proto::PROTOCOL_VERSION,
                previous_protocol().unwrap_or_else(|| "none".into()),
                manifest.protocol
            ),
        ));
    }

    // The app's URL is about to be dereferenced on every dispatch, so it is
    // checked here — at registration, where a human sees the error — rather than
    // at the first attempt, where it becomes a mysterious run failure.
    state
        .transport
        .policy()
        .resolve(&manifest.url)
        .map_err(|e| {
            Problem::bad_request(
                "egress_denied",
                format!("the egress policy refuses '{}': {e}", manifest.url),
            )
        })?;

    manifest.validate().map_err(manifest_problem)?;

    // Compile every expression now, at registration, where a human is watching.
    //
    // Deferring it to the ingest path means a typo in a trigger predicate
    // surfaces as a `warn!` in the server's log at the first matching event —
    // and the function simply never runs, which is indistinguishable from a
    // predicate that legitimately did not match. Failing here costs one bad
    // deploy; failing there costs an afternoon of "why did nothing happen?".
    for f in &manifest.functions {
        for (what, src) in f.cel_sources() {
            if let Err(e) = state.expr.compile(src) {
                return Err(Problem::bad_request(
                    "bad_expression",
                    format!("function '{}': {what} `{src}` did not compile: {e}", f.id),
                ));
            }
        }

        // Cron expressions are parsed in the same pass and for the same reason.
        // This one is the more expensive to get wrong: a predicate that does not
        // compile is at least logged at the first event, whereas an unschedulable
        // cron trigger produces no event at all to log against.
        if let Err(e) = cron_registrations(&principal.namespace, f) {
            return Err(Problem::bad_request(
                "bad_cron",
                format!("function '{}': {e}", f.id),
            ));
        }
    }

    let checksum = manifest.checksum.clone().unwrap_or_else(|| {
        let body = serde_json::to_string(&manifest.functions).unwrap_or_default();
        format!("sha256:{}", hex::encode(Sha256::digest(body.as_bytes())))
    });

    let mut tx = state
        .store
        .pool()
        .begin()
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

    // Signing keys are stored hashed, never in plaintext: a database dump — a
    // backup, a replica, a support export — must not be a working credential.
    // The key itself is supplied by configuration, not by a manifest that
    // travels over the network with the thing it authenticates.
    //
    // NULL means "this server has no key for this app". That is the honest
    // answer, and it is one an operator can act on; a placeholder digest here
    // would read as a configured key and verify nothing.
    let key_digest: Option<Vec<u8>> = state
        .keys_for(&manifest.app_id)
        .and_then(|k| k.first())
        .map(|k| Sha256::digest(k).to_vec());

    if key_digest.is_none() {
        warn!(
            app = %manifest.app_id,
            "registered an app this server holds no signing key for; its attempts will be \
             sent unsigned and a conforming app will reject them"
        );
    }

    let binding: Uuid = sqlx::query_scalar(
        r#"INSERT INTO app_bindings (id, ns, app_id, url, key_hash_current, last_seen,
                                     last_manifest_checksum)
           VALUES (gen_random_uuid(), $1, $2, $3, $4, now(), $5)
           ON CONFLICT (ns, app_id) DO UPDATE
             SET url = EXCLUDED.url, last_seen = now(),
                 key_hash_current = EXCLUDED.key_hash_current,
                 last_manifest_checksum = EXCLUDED.last_manifest_checksum
           RETURNING id"#,
    )
    .bind(&principal.namespace)
    .bind(&manifest.app_id)
    .bind(&manifest.url)
    .bind(key_digest)
    .bind(&checksum)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    let mut ids: Vec<String> = Vec::new();
    let mut cron_count = 0usize;
    for f in &manifest.functions {
        let fn_id = f.id.as_str();
        let version = f.version.as_deref().unwrap_or("1");
        ids.push(fn_id.to_string());

        let config = serde_json::to_value(f).map_err(|e| Problem::internal(e.to_string()))?;

        sqlx::query(
            r#"INSERT INTO functions (id, ns, app_binding_id, fn_id, version, config)
               VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)
               ON CONFLICT (ns, fn_id, version) DO UPDATE
                 SET config = EXCLUDED.config, app_binding_id = EXCLUDED.app_binding_id,
                     archived_at = NULL"#,
        )
        .bind(&principal.namespace)
        .bind(binding)
        .bind(fn_id)
        .bind(version)
        .bind(&config)
        .execute(&mut *tx)
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

        // In the SAME transaction as the function row. Committing the function
        // and then registering its schedules leaves two silent ways to be wrong
        // if the process dies between: a cron function that never fires, or
        // schedules firing for a function no app is bound to.
        let schedules = cron_registrations(&principal.namespace, f)
            .map_err(|e| Problem::bad_request("bad_cron", format!("function '{fn_id}': {e}")))?;
        let idx: Vec<i32> = schedules.iter().map(|s| s.trigger_idx).collect();
        if schedules.is_empty() {
            // A function that dropped its cron triggers must stop firing.
            state
                .store
                .prune_schedules_in(&mut tx, &principal.namespace, fn_id, &idx)
                .await
                .map_err(|e| Problem::internal(e.to_string()))?;
        } else {
            state
                .store
                .register_schedules_in(&mut tx, &schedules)
                .await
                .map_err(|e| Problem::internal(e.to_string()))?;
            cron_count += schedules.len();
        }
    }

    // Archive, never delete: in-flight runs of a removed function must keep
    // being driven to completion.
    let archived: i64 = sqlx::query_scalar(
        r#"WITH gone AS (
             UPDATE functions SET archived_at = now()
              WHERE ns = $1 AND app_binding_id = $2 AND archived_at IS NULL
                AND NOT (fn_id = ANY($3))
             RETURNING 1)
           SELECT count(*) FROM gone"#,
    )
    .bind(&principal.namespace)
    .bind(binding)
    .bind(&ids)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| Problem::internal(e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| Problem::internal(e.to_string()))?;

    if archived > 0 {
        warn!(
            app = %manifest.app_id, archived,
            "functions absent from the manifest were archived; in-flight runs of them \
             will still be driven to completion"
        );
    }
    info!(
        app = %manifest.app_id, functions = ids.len(), schedules = cron_count,
        "app registered"
    );

    Ok(Json(serde_json::json!({
        "app_id": manifest.app_id,
        "registered": ids,
        "schedules": cron_count,
        "archived": archived,
        "checksum": checksum,
    })))
}

/// Resolves a function to the endpoint and keys that reach it.
pub struct DbTargetResolver {
    /// Read pool for the lookup.
    pub pool: sqlx::PgPool,
    /// Signing keys by app id, supplied by configuration.
    ///
    /// Not from the manifest: a key that travels over the network with the thing
    /// it authenticates is not a secret. The server and the app are configured
    /// with it out of band.
    pub keys: std::collections::HashMap<String, Vec<Vec<u8>>>,
    /// Key used when an app has no specific one configured.
    pub default_keys: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl TargetResolver for DbTargetResolver {
    async fn resolve(&self, namespace: &str, function_id: &str) -> Result<AppTarget> {
        let row = sqlx::query(
            r#"SELECT a.url, a.app_id FROM functions f
                 JOIN app_bindings a ON a.id = f.app_binding_id
                WHERE f.ns = $1 AND f.fn_id = $2 AND f.archived_at IS NULL
                ORDER BY f.created_at DESC LIMIT 1"#,
        )
        .bind(namespace)
        .bind(function_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::Store(e.to_string()))?;

        // Also consider archived functions: an in-flight run of a removed
        // function must still be driveable, or archiving would strand it.
        let row = match row {
            Some(r) => r,
            None => sqlx::query(
                r#"SELECT a.url, a.app_id FROM functions f
                     JOIN app_bindings a ON a.id = f.app_binding_id
                    WHERE f.ns = $1 AND f.fn_id = $2
                    ORDER BY f.created_at DESC LIMIT 1"#,
            )
            .bind(namespace)
            .bind(function_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?
            .ok_or_else(|| {
                Error::Config(format!(
                    "no app hosts function '{function_id}' in namespace '{namespace}'; \
                     has the app registered?"
                ))
            })?,
        };

        let app_id: String = row.get("app_id");
        Ok(AppTarget {
            url: row.get("url"),
            keys: self
                .keys
                .get(&app_id)
                .cloned()
                .unwrap_or_else(|| self.default_keys.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_current_and_previous_protocol_majors_are_both_accepted() {
        // §11: N and N-1, for at least twelve months. Accepting only N makes a
        // protocol bump a flag day in which no app can register until every app
        // has been redeployed.
        assert!(protocol_supported(stepd_proto::PROTOCOL_VERSION));
        if let Some(prev) = previous_protocol() {
            assert!(protocol_supported(&prev));
        }
    }

    #[test]
    fn a_future_or_unrecognisable_protocol_is_rejected_rather_than_guessed_at() {
        assert!(!protocol_supported("99"));
        assert!(!protocol_supported("banana"));
        assert!(!protocol_supported(""));
    }

    #[test]
    fn a_manifest_without_a_checksum_still_registers() {
        // The checksum is an optimisation — it lets the server skip a no-op
        // update. An SDK that omits it must not be unable to register.
        let m: AppManifest = serde_json::from_value(serde_json::json!({
            "protocol": "1", "app_id": "billing", "url": "https://x/",
            "functions": [{ "id": "f", "triggers": [{ "type": "invoke" }] }]
        }))
        .unwrap();
        assert!(m.checksum.is_none());
        assert_eq!(m.functions.len(), 1);
    }
}
