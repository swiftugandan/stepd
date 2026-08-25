//! Declaring functions and assembling them into an app.
//!
//! A [`Function`] is a workflow's *configuration* — triggers, key, retries,
//! timeouts — plus the handler that runs it. An [`App`] is a set of functions
//! plus the deployment facts that must not live in code: the URL stepd reaches
//! it on and the key it signs with.
//!
//! That split is deliberate. A `FunctionConfig` travels to the server on every
//! registration and is stored there; if it carried URLs or credentials, moving a
//! function between environments would mean editing the workflow, and a
//! staging manifest overwriting production's endpoint would be one deploy away.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use stepd_proto::iso8601_seconds;
use stepd_sdk_core::{BoxFut, Ctx, Handler};

use crate::executor::{ErasedHandler, LocalExecutor};

/// What starts a run.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// An ingested event of this type, optionally filtered.
    Event {
        /// CloudEvents `type` to match.
        event: String,
        /// CEL predicate over `event`.
        #[serde(skip_serializing_if = "Option::is_none")]
        expr: Option<String>,
    },
    /// A schedule.
    ///
    /// Every field beyond `cron` and `tz` answers a question that only comes up
    /// when the server was down over a fire time — which is exactly when nobody
    /// wants to discover what the default was (ADR-016). They are here rather
    /// than server-side defaults only so the answer is written next to the
    /// schedule it applies to.
    Cron {
        /// Five-field cron expression.
        cron: String,
        /// IANA zone. Scheduling is from database time, never an app clock.
        tz: String,
        /// Misfire policy: `one` (default), `skip` or `all`.
        #[serde(skip_serializing_if = "Option::is_none")]
        catchup: Option<String>,
        /// Cap on occurrences fired in one recovery, for `all`.
        #[serde(skip_serializing_if = "Option::is_none")]
        catchup_limit: Option<u32>,
        /// Occurrences older than this are never caught up. ISO 8601.
        #[serde(skip_serializing_if = "Option::is_none")]
        misfire_window: Option<String>,
        /// Skip a fire while a run on `run_key` is still active.
        #[serde(skip_serializing_if = "Option::is_none")]
        singleton: Option<bool>,
        /// The run key for fires from this schedule.
        #[serde(skip_serializing_if = "Option::is_none")]
        run_key: Option<String>,
    },
    /// Called as a child run by another function.
    Invoke,
}

/// How a schedule behaves when the server was down over a fire time.
///
/// Separate from the function's own `singleton`, which is about event-triggered
/// runs sharing a key. A cron fire has no event to derive a key from, so its
/// exclusivity key is a literal stated here.
#[derive(Debug, Clone, Default)]
pub struct CronOptions {
    catchup: Option<String>,
    catchup_limit: Option<u32>,
    misfire_window: Option<String>,
    run_key: Option<String>,
}

impl CronOptions {
    /// Fire once on recovery, however many occurrences were missed. The default.
    pub fn catchup_one(mut self) -> Self {
        self.catchup = Some("one".into());
        self
    }

    /// Fire nothing that was missed — but still fire the current occurrence.
    ///
    /// For work that is worthless if late: a cache warm, a dashboard refresh.
    pub fn catchup_skip(mut self) -> Self {
        self.catchup = Some("skip".into());
        self
    }

    /// Fire every missed occurrence, up to `limit`.
    ///
    /// For work where each occurrence means something — a billing tick, a
    /// per-window aggregation. The limit is not optional: `all` with no cap
    /// turns a weekend outage of a per-minute schedule into ten thousand runs
    /// arriving at once, precisely when the system is least healthy.
    pub fn catchup_all(mut self, limit: u32) -> Self {
        self.catchup = Some("all".into());
        self.catchup_limit = Some(limit);
        self
    }

    /// Never catch up an occurrence older than this ISO 8601 duration.
    ///
    /// An age cap, not a count cap, and it beats the catch-up policy. A count
    /// cap alone still lets a five-hour-old nightly report fire into a business
    /// day where nobody expects it.
    pub fn misfire_window(mut self, iso8601: impl Into<String>) -> Self {
        self.misfire_window = Some(iso8601.into());
        self
    }

    /// Skip a fire while a previous one is still running, under this key.
    ///
    /// The skip is counted, not silent: a schedule that can never keep up and
    /// one that is working look identical otherwise.
    pub fn singleton(mut self, run_key: impl Into<String>) -> Self {
        self.run_key = Some(run_key.into());
        self
    }
}

/// Retry policy for a function's attempts.
#[derive(Debug, Clone, Serialize)]
pub struct Retries {
    /// Attempts before the run fails.
    pub max_attempts: u32,
    /// Backoff strategy name.
    pub backoff: String,
    /// First interval, ISO 8601.
    pub initial: String,
    /// Ceiling, ISO 8601.
    pub max: String,
    /// Whether to spread retries.
    pub jitter: bool,
}

impl Default for Retries {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            backoff: "exponential".into(),
            initial: "PT10S".into(),
            max: "PT1H".into(),
            jitter: true,
        }
    }
}

/// Deadlines for a function.
#[derive(Debug, Clone, Serialize)]
pub struct Timeouts {
    /// How long one attempt may take.
    pub attempt: String,
    /// How long the whole run may take.
    pub run: String,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            attempt: "PT60S".into(),
            run: "P30D".into(),
        }
    }
}

/// A workflow: its configuration and its handler.
pub struct Function {
    id: String,
    name: Option<String>,
    version: String,
    triggers: Vec<Trigger>,
    key_expr: Option<String>,
    singleton: bool,
    on_cancel: bool,
    retries: Retries,
    timeouts: Timeouts,
    handler: Option<ErasedHandler>,
}

impl Function {
    /// Declare a function. The id is the stable identity used by every run.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            version: "1".into(),
            triggers: Vec::new(),
            key_expr: None,
            singleton: false,
            on_cancel: false,
            retries: Retries::default(),
            timeouts: Timeouts::default(),
            handler: None,
        }
    }

    /// Human-readable name for the console.
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    /// Opaque version string. Informational: identity is the id.
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.version = v.into();
        self
    }

    /// Start a run for every event of this type.
    pub fn on_event(mut self, event: impl Into<String>) -> Self {
        self.triggers.push(Trigger::Event {
            event: event.into(),
            expr: None,
        });
        self
    }

    /// Start a run for events of this type that satisfy a CEL predicate.
    pub fn on_event_where(mut self, event: impl Into<String>, expr: impl Into<String>) -> Self {
        self.triggers.push(Trigger::Event {
            event: event.into(),
            expr: Some(expr.into()),
        });
        self
    }

    /// Start a run on a schedule, with the server's defaults: catch up once on
    /// recovery, within an hour, overlapping freely.
    pub fn on_cron(self, cron: impl Into<String>, tz: impl Into<String>) -> Self {
        self.on_cron_with(cron, tz, CronOptions::default())
    }

    /// Start a run on a schedule, stating the recovery behaviour explicitly.
    ///
    /// Worth doing for any schedule whose occurrences each mean something. The
    /// default catches up once, which is right for "this should have run
    /// recently" and wrong for a billing tick, where a missed occurrence is
    /// money rather than a stale cache.
    pub fn on_cron_with(
        mut self,
        cron: impl Into<String>,
        tz: impl Into<String>,
        opts: CronOptions,
    ) -> Self {
        self.triggers.push(Trigger::Cron {
            cron: cron.into(),
            tz: tz.into(),
            catchup: opts.catchup,
            catchup_limit: opts.catchup_limit,
            misfire_window: opts.misfire_window,
            singleton: opts.run_key.is_some().then_some(true),
            run_key: opts.run_key,
        });
        self
    }

    /// Declare that this function has a compensation path (§7.4).
    ///
    /// Without it the server finishes a cancelled run immediately and the
    /// handler is never told. With it, the run is dispatched again with
    /// `ctx.run().cancelling` set, and keeps its business key until the undo
    /// finishes — so nothing else starts on that key while this run is still
    /// releasing what it reserved.
    pub fn on_cancel(mut self) -> Self {
        self.on_cancel = true;
        self
    }

    /// Allow this function to be invoked as a child run.
    pub fn on_invoke(mut self) -> Self {
        self.triggers.push(Trigger::Invoke);
        self
    }

    /// CEL expression producing the business key.
    ///
    /// A key gives the function keyed ordering: at most one active run per key,
    /// enforced by a database invariant rather than by application logic.
    pub fn key(mut self, expr: impl Into<String>) -> Self {
        self.key_expr = Some(expr.into());
        self
    }

    /// Skip a trigger whose key already has an active run.
    pub fn singleton(mut self) -> Self {
        self.singleton = true;
        self
    }

    /// Override the retry policy.
    pub fn retries(mut self, r: Retries) -> Self {
        self.retries = r;
        self
    }

    /// Override the deadlines.
    ///
    /// The attempt timeout must exceed the slowest single step. It is the
    /// server's patience, and the SDK will not cut a step short to fit inside it
    /// — a step that overruns is retried, not abandoned mid-effect.
    pub fn timeouts(mut self, t: Timeouts) -> Self {
        self.timeouts = t;
        self
    }

    /// Attach the handler.
    ///
    /// Takes an `async fn(&Ctx) -> StepResult<T>`. For a closure, wrap it in
    /// [`wf!`](crate::wf) so the compiler can infer the higher-ranked signature.
    pub fn run<H, T>(mut self, handler: H) -> Self
    where
        H: for<'a> Handler<'a, T> + Send + Sync + 'static,
        T: Serialize + 'static,
    {
        // Erase the result type once, here, so the whole serve stack is not
        // monomorphised per workflow return type.
        let h = Arc::new(handler);
        self.handler = Some(Arc::new(
            move |ctx: &Ctx| -> BoxFut<'_, serde_json::Value> {
                let h = h.clone();
                Box::pin(async move {
                    let v = h.call(ctx).await?;
                    serde_json::to_value(v).map_err(|e| {
                        stepd_sdk_core::StepError::fatal(format!(
                            "the handler's return value could not be serialised: {e}"
                        ))
                    })
                })
            },
        ));
        self
    }

    /// The id this function is registered under.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The `FunctionConfig` sent to the server.
    pub fn config(&self) -> serde_json::Value {
        let mut v = serde_json::json!({
            "id": self.id,
            "version": self.version,
            "triggers": self.triggers,
            "retries": self.retries,
            "timeouts": self.timeouts,
        });
        if let Some(n) = &self.name {
            v["name"] = serde_json::json!(n);
        }
        if let Some(k) = &self.key_expr {
            v["key_expr"] = serde_json::json!(k);
        }
        if self.singleton {
            v["singleton"] = serde_json::json!(true);
        }
        if self.on_cancel {
            v["on_cancel"] = serde_json::json!(true);
        }
        v
    }

    /// Problems worth refusing to start over (F-DX-6).
    ///
    /// Reported at start-up rather than at the first attempt, because every one
    /// of these produces a run that looks fine until it does not: a function
    /// with no trigger simply never runs, and nothing ever says so.
    pub fn lint(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.handler.is_none() {
            out.push(format!(
                "function '{}' has no handler; it will be registered and every attempt will 404",
                self.id
            ));
        }
        if self.triggers.is_empty() {
            out.push(format!(
                "function '{}' has no triggers; nothing will ever start a run of it",
                self.id
            ));
        }
        if self.singleton && self.key_expr.is_none() {
            out.push(format!(
                "function '{}' is singleton but has no key; singleton skipping is defined \
                 per key, so it has no effect",
                self.id
            ));
        }
        if let (Some(a), Some(r)) = (
            iso8601_seconds(&self.timeouts.attempt),
            iso8601_seconds(&self.timeouts.run),
        ) {
            if a > r {
                out.push(format!(
                    "function '{}' has an attempt timeout ({}) longer than its run timeout ({}); \
                     the run will be killed before its first attempt can finish",
                    self.id, self.timeouts.attempt, self.timeouts.run
                ));
            }
        }
        out
    }
}

/// A set of functions served at one URL.
pub struct App {
    pub(crate) app_id: String,
    pub(crate) url: String,
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) functions: HashMap<String, Function>,
    pub(crate) executor: LocalExecutor,
    pub(crate) dev_mode: bool,
}

impl App {
    /// Declare an app. `url` is the endpoint stepd will call.
    pub fn new(app_id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            url: url.into(),
            keys: Vec::new(),
            functions: HashMap::new(),
            executor: LocalExecutor::default(),
            dev_mode: false,
        }
    }

    /// Add a signing key. Call twice during rotation: both are accepted, and the
    /// first is used to sign.
    pub fn signing_key(mut self, key: Vec<u8>) -> Self {
        self.keys.push(key);
        self
    }

    /// Register a function.
    pub fn function(mut self, f: Function) -> Self {
        self.functions.insert(f.id.clone(), f);
        self
    }

    /// Accept unsigned requests.
    ///
    /// Loopback development only. Production apps must reject unsigned requests
    /// (protocol §9), so this is a separate explicit call rather than something
    /// that happens by default when no key is configured — a missing key must
    /// fail closed.
    pub fn dev_mode(mut self) -> Self {
        self.dev_mode = true;
        self
    }

    /// Use a specific handler pool instead of the default.
    pub fn executor(mut self, e: LocalExecutor) -> Self {
        self.executor = e;
        self
    }

    /// The `AppManifest` this app registers with.
    pub fn manifest(&self) -> serde_json::Value {
        let mut fns: Vec<serde_json::Value> = self.functions.values().map(|f| f.config()).collect();
        // Sorted so the checksum is a function of content, not of hash-map
        // iteration order — otherwise every restart looks like a config change
        // and the server re-registers for nothing.
        fns.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));

        let body = serde_json::to_string(&fns).unwrap_or_default();
        let checksum = {
            use sha2::{Digest, Sha256};
            format!("sha256:{}", hex::encode(Sha256::digest(body.as_bytes())))
        };

        serde_json::json!({
            "protocol": stepd_proto::PROTOCOL_VERSION,
            "app_id": self.app_id,
            "url": self.url,
            "sdk": crate::SDK_VERSION,
            "checksum": checksum,
            "functions": fns,
        })
    }

    /// Every lint finding across the app's functions.
    pub fn lint(&self) -> Vec<String> {
        let mut out: Vec<String> = self.functions.values().flat_map(|f| f.lint()).collect();
        if self.keys.is_empty() && !self.dev_mode {
            out.push(
                "no signing key configured and dev mode is off; every request will be rejected"
                    .into(),
            );
        }
        out.sort();
        out
    }

    pub(crate) fn handler(&self, function_id: &str) -> Option<ErasedHandler> {
        self.functions
            .get(function_id)
            .and_then(|f| f.handler.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_durations_parse_or_abstain() {
        assert_eq!(iso8601_seconds("PT60S"), Some(60));
        assert_eq!(iso8601_seconds("PT1H"), Some(3600));
        assert_eq!(iso8601_seconds("P30D"), Some(2_592_000));
        assert_eq!(iso8601_seconds("P1DT2H3M4S"), Some(86_400 + 7200 + 180 + 4));
        // Abstain rather than guess: a lint that invents findings gets ignored.
        assert_eq!(iso8601_seconds("P1Y"), None);
        assert_eq!(iso8601_seconds("banana"), None);
    }

    #[test]
    fn lint_catches_a_function_nothing_can_start() {
        let f = Function::new("orphan").run(|_: &Ctx| async { Ok(1) });
        let findings = f.lint();
        assert!(
            findings.iter().any(|s| s.contains("no triggers")),
            "a function with no trigger never runs and nothing says so: {findings:?}"
        );
    }

    #[test]
    fn lint_catches_an_attempt_timeout_that_outlives_the_run() {
        let f = Function::new("f")
            .on_event("x")
            .timeouts(Timeouts {
                attempt: "PT2H".into(),
                run: "PT1H".into(),
            })
            .run(|_: &Ctx| async { Ok(1) });
        assert!(f
            .lint()
            .iter()
            .any(|s| s.contains("longer than its run timeout")));
    }

    #[test]
    fn lint_catches_singleton_without_a_key() {
        let f = Function::new("f")
            .on_event("x")
            .singleton()
            .run(|_: &Ctx| async { Ok(1) });
        assert!(f.lint().iter().any(|s| s.contains("singleton")));
    }

    #[test]
    fn an_app_without_a_key_fails_closed() {
        let app = App::new("a", "http://x").function(
            Function::new("f")
                .on_event("x")
                .run(|_: &Ctx| async { Ok(1) }),
        );
        assert!(
            app.lint().iter().any(|s| s.contains("no signing key")),
            "a missing key must be reported, not silently treated as dev mode"
        );
    }

    #[test]
    fn the_manifest_checksum_is_stable_across_restarts() {
        let build = || {
            App::new("a", "http://x")
                .signing_key(b"k".to_vec())
                .function(
                    Function::new("b")
                        .on_event("e")
                        .run(|_: &Ctx| async { Ok(1) }),
                )
                .function(
                    Function::new("a")
                        .on_event("e")
                        .run(|_: &Ctx| async { Ok(1) }),
                )
                .manifest()["checksum"]
                .clone()
        };
        // Hash-map iteration order differs between runs; the checksum must not,
        // or the server re-registers on every restart for no reason.
        assert_eq!(build(), build());
    }

    #[test]
    fn the_manifest_carries_no_credentials() {
        let m = App::new("a", "http://x")
            .signing_key(b"super-secret".to_vec())
            .function(
                Function::new("f")
                    .on_event("e")
                    .run(|_: &Ctx| async { Ok(1) }),
            )
            .manifest();
        let s = serde_json::to_string(&m).unwrap();
        assert!(
            !s.contains("super-secret"),
            "the signing key must never travel in a manifest"
        );
    }
}
