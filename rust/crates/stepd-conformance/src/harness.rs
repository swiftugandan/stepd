//! Standing up a server, pointing it at the app under test, and driving runs.
//!
//! The runner brings its own server. That is a deliberate limitation and worth
//! stating plainly: this battery tests an **app**, and it does so *through* a
//! known-good server, so a failure is attributed to the app. It cannot be turned
//! round to test somebody else's server — protocol §12.4 says so, and pretending
//! otherwise would let a defect in either side be blamed on the other.
//!
//! Everything the harness observes falls into two channels:
//!
//! * **Server state** — run status, the journal, the queue. Authoritative for
//!   what was *recorded*.
//! * **The app's effect log** (§12.1) — authoritative for what was *executed*.
//!
//! Both are needed, and the interesting assertions are the ones that compare
//! them. "The step was recorded once" and "the step body ran once" are different
//! statements, and memoisation is precisely the claim that the second follows
//! from the first.

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use stepd_core::traits::{EventLog, StateStore};
use stepd_proto::RunStatus;
use stepd_server::{Config, Server};
use uuid::Uuid;

/// How the runner was told to reach things.
#[derive(Debug, Clone)]
pub struct Options {
    /// The app under test.
    pub app_url: String,
    /// Database for the server the runner stands up.
    pub database_url: String,
    /// Shared signing key. Both sides must hold it, exactly as in production:
    /// a key that travelled with the manifest would not be a secret.
    pub signing_key: Vec<u8>,
    /// How long a single case may wait for a run to settle.
    pub case_timeout: Duration,
    /// Run only these suites, if non-empty.
    pub only: Vec<String>,
    /// Called once the runner's server is serving, with its API base URL and an
    /// operator token for this run's namespace.
    ///
    /// Whoever started the app under test finishes configuring it here. The
    /// bundled reference app needs it to point its blob client at the server,
    /// and cannot be told sooner: the app has to be serving before this runner
    /// will read its manifest, and the API it must call does not exist until
    /// after that. A third-party app is configured by whoever launched it and
    /// leaves this `None`.
    pub on_ready: Option<OnReady>,
}

/// What a readiness callback is handed: the API base URL, and an operator token.
type ReadyFn = dyn Fn(&str, &str) + Send + Sync;

/// A callback invoked with `(api_base, operator_token)` once the runner is up.
#[derive(Clone)]
pub struct OnReady(Arc<ReadyFn>);

impl OnReady {
    /// Wrap a callback.
    pub fn new(f: impl Fn(&str, &str) + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Invoke it.
    pub fn call(&self, api_base: &str, token: &str) {
        (self.0)(api_base, token)
    }
}

impl std::fmt::Debug for OnReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OnReady(..)")
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            app_url: "http://127.0.0.1:9944".into(),
            database_url: String::new(),
            signing_key: b"stepd-conformance".to_vec(),
            // Generous: the sleep and abandonment suites are about real time
            // passing, and a runner that times out under a slow CI machine
            // reports a defect that is not there.
            case_timeout: Duration::from_secs(90),
            only: Vec::new(),
            on_ready: None,
        }
    }
}

/// What the app said it implements (§12.1).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConformanceManifest {
    /// Protocol major.
    pub protocol: String,
    /// Informational language/version string.
    #[serde(default)]
    pub sdk: Option<String>,
    /// Suites the app claims.
    pub suites: Vec<String>,
    /// Hazards the app says it makes unrepresentable rather than detectable.
    #[serde(default)]
    pub statically_prevented: Vec<String>,
}

/// A server, an app, and the means to drive one against the other.
pub struct Harness {
    /// The server the runner stands up.
    pub server: Arc<Server>,
    /// Isolated namespace for this run.
    pub namespace: String,
    /// Options as given.
    pub options: Options,
    /// What the app declared.
    pub manifest: ConformanceManifest,
    /// Address the server's own API is bound to, for the registration call.
    api_addr: String,
    http: reqwest::Client,
}

impl Harness {
    /// Build the server, read the app's conformance manifest, register it.
    pub async fn start(options: Options) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        // The conformance manifest first. If the app does not serve one it is
        // not an app under test, and saying so here beats nineteen suites all
        // failing to connect.
        let url = format!(
            "{}/.well-known/stepd-conformance",
            options.app_url.trim_end_matches('/')
        );
        let manifest: ConformanceManifest = http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("the app under test did not answer {url}"))?
            .json()
            .await
            .context(
                "the conformance manifest did not parse (schemas/conformance-manifest.schema.json)",
            )?;

        if manifest.protocol != stepd_proto::PROTOCOL_VERSION {
            return Err(anyhow!(
                "the app speaks protocol {} and this battery is for {}",
                manifest.protocol,
                stepd_proto::PROTOCOL_VERSION
            ));
        }

        // The socket is bound before the server is built, because the blob
        // capability minter needs the address the app will actually reach. A
        // capability signed against the bind address is useless on any real
        // deployment, where that is `0.0.0.0`.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let api_addr = listener.local_addr()?.to_string();

        let config = Config {
            database_url: options.database_url.clone(),
            default_keys: vec![options.signing_key.clone()],
            // The app under test is normally on this machine, and refusing to
            // reach it would make the tool unusable for the person developing an
            // SDK. The policy still denies cloud metadata addresses; its own
            // tests assert that.
            egress: stepd_transport_http::EgressPolicy::development(),
            lease: chrono::Duration::seconds(30),
            // No jitter. A conformance failure that appears one run in ten
            // because a timer was spread is worse than a slightly unrealistic
            // timer.
            timer_jitter: chrono::Duration::zero(),
            // Short enough that the abandonment suite finishes, long enough that
            // a slow-but-healthy app is not reclaimed mid-attempt.
            attempt_timeout: Duration::from_secs(8),
            // Managed blobs, into a temporary root. A key must be set or the
            // endpoints refuse every request — which is the correct behaviour
            // for a server that does not use them, and would make the `blobs`
            // suite fail for a configuration reason rather than a protocol one.
            blob_key: options.signing_key.clone(),
            blob_backend: stepd_server::BlobBackendConfig::Filesystem {
                root: std::env::temp_dir().join(format!(
                    "stepd-conformance-blobs-{}",
                    Uuid::new_v4().simple()
                )),
            },
            blob_base_url: format!("http://{api_addr}"),
            ..Default::default()
        };

        let server = Arc::new(Server::build(config).await?);
        server.migrate().await?;

        let namespace = format!("conf-{}", &Uuid::new_v4().simple().to_string()[..8]);
        server.state.store.ensure_namespace(&namespace).await?;

        // Serving starts here. Registration then goes over that real socket
        // rather than straight into the handler: it is the one exchange where an
        // SDK's output is judged by the server, and a manifest that serialises
        // wrongly should fail in the runner's own setup, with a clear message,
        // rather than as nineteen suites finding no registered function.
        let router = server.router();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let h = Self {
            server,
            namespace,
            options,
            manifest,
            api_addr,
            http,
        };

        // §12.1: clear the effect log once, before anything, so a rerun does not
        // inherit the previous one and read as a memoisation failure.
        h.reset_effects().await?;
        h.register().await?;
        Ok(h)
    }

    /// Whether the app declared `suite`.
    pub fn declares(&self, suite: &str) -> bool {
        self.manifest.suites.iter().any(|s| s == suite)
    }

    /// Whether the app says it makes `hazard` unrepresentable (§12.1).
    pub fn prevents(&self, hazard: &str) -> bool {
        self.manifest
            .statically_prevented
            .iter()
            .any(|s| s == hazard)
    }

    /// Pull the app's `AppManifest` and register it through the real API path.
    async fn register(&self) -> Result<()> {
        let url = format!(
            "{}/.well-known/stepd",
            self.options.app_url.trim_end_matches('/')
        );
        let manifest: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("the app under test did not answer {url}"))?
            .json()
            .await
            .context("the app manifest did not parse")?;

        let token = self.mint("admin").await?;
        let res = self
            .http
            .put(format!("{}/v1/apps", self.api_base()))
            .bearer_auth(&token)
            .json(&manifest)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(anyhow!(
                "registering the app under test failed: {}",
                res.text().await.unwrap_or_default()
            ));
        }
        Ok(())
    }

    /// Base URL of the server this runner stood up.
    pub fn api_base(&self) -> String {
        format!("http://{}", self.api_addr)
    }

    /// Issue a token in this run's namespace.
    pub async fn mint(&self, role: &str) -> Result<String> {
        let raw = Uuid::new_v4().simple().to_string();
        sqlx::query(
            "INSERT INTO tokens (id, ns, role, token_hash, name)
             VALUES (gen_random_uuid(), $1, $2, $3, 'conformance')",
        )
        .bind(&self.namespace)
        .bind(role)
        .bind(stepd_server::auth::token_hash(&raw))
        .execute(self.server.state.store.pool())
        .await?;
        Ok(raw)
    }

    // ------------------------------------------------------------ driving

    /// Ingest an event and return the run it started, if any.
    pub async fn fire(&self, event_type: &str, data: serde_json::Value) -> Result<Uuid> {
        self.fire_keyed(event_type, data, None).await
    }

    /// Ingest an event carrying an explicit business key.
    ///
    /// Through the real `POST /v1/events`, not straight into the event log.
    /// Writing the row directly would skip trigger matching, key evaluation and
    /// the backpressure check — so every case would be testing a path no
    /// production event ever takes, which is the sort of shortcut that makes a
    /// green suite meaningless.
    pub async fn fire_keyed(
        &self,
        event_type: &str,
        data: serde_json::Value,
        key: Option<String>,
    ) -> Result<Uuid> {
        let before = self.run_ids().await?;

        let mut event = serde_json::json!({
            "specversion": "1.0",
            "id": Uuid::new_v4().to_string(),
            "source": "/conformance",
            "type": event_type,
            "time": Utc::now(),
            "data": data,
        });
        if let Some(k) = key {
            event["stepdkey"] = serde_json::Value::String(k);
        }

        let token = self.mint("operator").await?;
        let res = self
            .http
            .post(format!("{}/v1/events", self.api_base()))
            .bearer_auth(&token)
            .json(&serde_json::json!([event]))
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(anyhow!(
                "ingesting '{event_type}' failed: {}",
                res.text().await.unwrap_or_default()
            ));
        }

        // The dispatcher's tick is what turns an ingested event into a run.
        self.tick().await?;

        let after = self.run_ids().await?;
        after
            .into_iter()
            .find(|id| !before.contains(id))
            .ok_or_else(|| {
                anyhow!(
                    "no run started for event '{event_type}'; is a function registered \
                     with a matching trigger?"
                )
            })
    }

    /// Every run id in this namespace.
    pub async fn run_ids(&self) -> Result<Vec<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM runs WHERE ns = $1 ORDER BY started_at")
                .bind(&self.namespace)
                .fetch_all(self.server.state.store.pool())
                .await?,
        )
    }

    /// One dispatch pass plus one housekeeping pass over this namespace.
    ///
    /// Namespace-scoped so that a conformance run against a shared database does
    /// not drive somebody else's work — and so that two cases running in the
    /// same battery cannot drive each other's.
    pub async fn tick(&self) -> Result<()> {
        self.server
            .dispatcher
            .tick_namespace(&self.namespace)
            .await?;
        self.server.housekeeper.tick().await;
        Ok(())
    }

    /// Drive until `run` reaches a terminal state, or the case budget runs out.
    pub async fn settle(&self, run: Uuid) -> Result<RunStatus> {
        self.settle_until(run, |s| {
            matches!(
                s,
                RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
            )
        })
        .await
    }

    /// Drive until `run`'s status satisfies `done`, or the budget runs out.
    pub async fn settle_until(
        &self,
        run: Uuid,
        done: impl Fn(RunStatus) -> bool,
    ) -> Result<RunStatus> {
        let deadline = std::time::Instant::now() + self.options.case_timeout;
        let mut last = RunStatus::Pending;
        while std::time::Instant::now() < deadline {
            self.tick().await?;
            if let Some(s) = self.server.state.store.run_status(run).await? {
                last = s;
                if done(s) {
                    return Ok(s);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(anyhow!(
            "run {run} did not settle within {:?}; last status {last:?}",
            self.options.case_timeout
        ))
    }

    // ------------------------------------------------------------ observing

    /// The app's effect log for a run: what the handler actually executed.
    ///
    /// The other half of every interesting assertion. Server state says what was
    /// recorded; only this says what ran, and memoisation is the claim that
    /// recording something stops it running again.
    pub async fn effects(&self, run: Uuid) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct Body {
            effects: Vec<String>,
        }
        let url = format!(
            "{}/_conformance/effects?run={run}",
            self.options.app_url.trim_end_matches('/')
        );
        let body: Body = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("the app did not answer {url} (protocol §12.1)"))?
            .json()
            .await
            .context("the effect log did not parse")?;
        Ok(body.effects)
    }

    async fn reset_effects(&self) -> Result<()> {
        let url = format!(
            "{}/_conformance/reset",
            self.options.app_url.trim_end_matches('/')
        );
        self.http
            .post(&url)
            .send()
            .await
            .with_context(|| format!("the app did not answer {url} (protocol §12.1)"))?;
        Ok(())
    }

    /// The journal for a run: `(step_id, op, status)` in hash order.
    pub async fn journal(&self, run: Uuid) -> Result<Vec<(String, String, String)>> {
        Ok(sqlx::query_as(
            "SELECT step_id, op::text, status::text FROM run_steps
              WHERE run_id = $1 ORDER BY step_hash",
        )
        .bind(run)
        .fetch_all(self.server.state.store.pool())
        .await?)
    }

    /// Step hashes in the order they were recorded.
    pub async fn hashes(&self, run: Uuid) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT step_hash FROM run_steps WHERE run_id = $1
              ORDER BY started_at NULLS LAST, step_hash",
        )
        .bind(run)
        .fetch_all(self.server.state.store.pool())
        .await?)
    }

    /// A run's recorded output.
    pub async fn output(&self, run: Uuid) -> Result<Option<serde_json::Value>> {
        Ok(sqlx::query_scalar("SELECT output FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(self.server.state.store.pool())
            .await?)
    }

    /// A run's recorded error.
    pub async fn error(&self, run: Uuid) -> Result<Option<serde_json::Value>> {
        Ok(sqlx::query_scalar("SELECT error FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(self.server.state.store.pool())
            .await?)
    }

    /// How many attempts a run has taken.
    pub async fn attempts(&self, run: Uuid) -> Result<i32> {
        Ok(
            sqlx::query_scalar("SELECT attempt_no FROM runs WHERE id = $1")
                .bind(run)
                .fetch_one(self.server.state.store.pool())
                .await?,
        )
    }

    /// Deliver an event straight to a run's inbox, bypassing correlation.
    pub async fn signal(&self, run: Uuid, event_type: &str, data: serde_json::Value) -> Result<()> {
        self.server
            .state
            .store
            .deliver(run, event_type, &data, None)
            .await?;
        Ok(())
    }
}
