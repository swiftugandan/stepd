//! # stepd server
//!
//! One binary: the HTTP API, the operations console, the dispatch loop and the
//! convergence loop, over one Postgres database. Deploying stepd is deploying
//! this and running `stepd migrate`.
//!
//! ## The two loops, and why they are two
//!
//! [`Dispatcher`](stepd_core::Dispatcher) drives runs by calling apps.
//! [`Housekeeper`](stepd_core::Housekeeper) fires timers, delivers relayed
//! signals, resolves finished children and reclaims expired leases.
//!
//! They are separate because everything the housekeeper does must still happen
//! when no app is reachable at all. Folding it into dispatch would mean a total
//! app outage also stops the system healing itself, and the backlog to clear
//! afterwards would be far larger than the outage caused.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod api;
pub mod auth;
pub mod blobs;
pub mod console;
pub mod ingest;
pub mod problem;
pub mod registry;
pub mod runner;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use stepd_blobs_s3::S3Config;
use stepd_core::traits::{BlobBackend, RelayBytes};
use stepd_core::{DispatchConfig, Dispatcher, Housekeeper, KeeperConfig};
use stepd_expr_cel::CelEngine;
use stepd_store_postgres::PostgresStore;
use stepd_transport_http::{EgressPolicy, HttpTransport};
use url::Url;

pub use registry::DbTargetResolver;
pub use runner::Runner;

/// How the server is configured.
///
/// Environment only (twelve-factor III). Nothing here is read from the database
/// or from a manifest, because a value that arrives over the network is a value
/// a tenant can influence.
#[derive(Debug, Clone)]
pub struct Config {
    /// Postgres connection URL.
    pub database_url: String,
    /// Address to listen on.
    pub bind: String,
    /// Connection pool size.
    pub max_connections: u32,
    /// Worker identity recorded on leases.
    pub worker: String,
    /// Runs claimed per namespace per tick.
    pub batch: i64,
    /// Lease duration.
    pub lease: chrono::Duration,
    /// How long to wait for an app to answer one attempt.
    pub attempt_timeout: Duration,
    /// Pause between dispatch ticks when there was nothing to do.
    pub idle_poll: Duration,
    /// What the server may connect to.
    pub egress: EgressPolicy,
    /// Signing keys per app id.
    pub app_keys: HashMap<String, Vec<Vec<u8>>>,
    /// Signing key used for apps with no specific one.
    pub default_keys: Vec<Vec<u8>>,
    /// Spread applied to sleep wake-ups so a million midnight timers do not
    /// arrive at once.
    pub timer_jitter: chrono::Duration,

    /// Where managed blob bytes live, and which `BlobBackend` serves them.
    ///
    /// Filesystem by default, because that is what makes `stepd dev` work with
    /// no cloud account and what CI uses. `Server::build` builds exactly one
    /// backend from this and shares the `Arc` between the store and the blob
    /// index; nothing above either knows which variant is in force.
    pub blob_backend: BlobBackendConfig,
    /// Base URL the transfer capabilities are minted against.
    ///
    /// Not derived from `bind`: a server behind a load balancer binds to
    /// `0.0.0.0:8080` and is reached at something else entirely, and a capability
    /// minted against the bind address is one the app cannot use.
    pub blob_base_url: String,
    /// Key the transfer capabilities are signed with.
    ///
    /// Separate from the attempt signing key. They authenticate different things
    /// to different parties — one says "this server sent this attempt", the other
    /// says "the bearer may write these exact bytes once" — and one key doing
    /// both means rotating either forces rotating both.
    pub blob_key: Vec<u8>,
    /// Ceiling on a single managed blob (protocol §8.2).
    pub blob_max_size: i64,
    /// How long a reservation that was never uploaded survives before collection.
    pub blob_reservation_ttl: chrono::Duration,
}

/// Where managed blob bytes live and which [`BlobBackend`] serves them.
///
/// `Filesystem` carries no [`stepd_store_postgres::Capability`]: the transfer
/// capability is built from `Config::blob_base_url` and `Config::blob_key`,
/// which are not backend-specific, and `Server::build` supplies it to whichever
/// backend this selects. Carrying a second copy here would just be a second
/// place for it to drift from the one actually used.
#[derive(Debug, Clone)]
pub enum BlobBackendConfig {
    /// Bytes on a local filesystem root, relayed through this process
    /// (protocol §8.3.2). What makes `stepd dev` work with no cloud account
    /// and what CI uses.
    Filesystem {
        /// Root directory blob bytes are stored under.
        root: std::path::PathBuf,
    },
    /// Bytes in an S3-compatible object store, reached with presigned URLs
    /// (see [`stepd_blobs_s3`]).
    ///
    /// [`S3ConfigInput`], not [`S3Config`] directly: this holds whatever was
    /// read from the environment, which may be incomplete, and
    /// [`S3ConfigInput::resolve`] is the one place that turns it into a
    /// usable `S3Config` or names what is missing.
    S3(S3ConfigInput),
}

/// S3 configuration as read from the environment, before completeness
/// validation.
///
/// `endpoint` is `Option<Url>`, not a sentinel URL string. An earlier version
/// used `"http://unset.invalid"` and compared `Url::as_str()` against it —
/// WHATWG URL normalisation gives a special-scheme URL an empty path of `/`,
/// so the parsed sentinel read back as `"http://unset.invalid/"` and the
/// comparison never matched. `STEPD_BLOB_BACKEND=s3` with no endpoint set
/// built and served regardless, presigning uploads against a hostname RFC
/// 2606 guarantees will never resolve — exactly the failure the completeness
/// check exists to prevent. `Option` makes "not configured" a state the type
/// carries, not a string that has to survive being re-parsed and compared.
///
/// [`Debug`] is implemented by hand, like [`S3Config`]'s, so the secret key
/// does not reach a log the first time someone prints a `Config`.
#[derive(Clone)]
pub struct S3ConfigInput {
    /// Base URL of the service, if `STEPD_BLOB_S3_ENDPOINT` was set and parsed.
    pub endpoint: Option<Url>,
    /// Region name used in the signature.
    pub region: String,
    /// Bucket holding the objects.
    pub bucket: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Address the bucket as a path segment rather than a hostname.
    pub path_style: bool,
}

impl std::fmt::Debug for S3ConfigInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ConfigInput")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("path_style", &self.path_style)
            .finish_non_exhaustive()
    }
}

impl S3ConfigInput {
    /// Resolve into a usable [`S3Config`], or name every `STEPD_BLOB_S3_*`
    /// variable that is still missing.
    ///
    /// Called twice on the way to a running server: once by
    /// [`Config::validate_blob_backend`], so an incomplete config fails
    /// before a database connection is even opened, and again when the
    /// backend is actually built in `Server::build`. Both calls run the
    /// identical check; the second is not trusted to have already passed,
    /// because trusting an invariant across two function calls is how it
    /// silently stops holding. Public so `stepd doctor` (built as its own
    /// binary, in `stepd-cli`) can resolve the same value it is diagnosing
    /// rather than reimplementing this check.
    pub fn resolve(&self) -> anyhow::Result<S3Config> {
        let mut missing = Vec::new();
        if self.endpoint.is_none() {
            missing.push("STEPD_BLOB_S3_ENDPOINT");
        }
        if self.bucket.is_empty() {
            missing.push("STEPD_BLOB_S3_BUCKET");
        }
        if self.access_key.is_empty() {
            missing.push("STEPD_BLOB_S3_ACCESS_KEY");
        }
        if self.secret_key.is_empty() {
            missing.push("STEPD_BLOB_S3_SECRET_KEY");
        }
        anyhow::ensure!(
            missing.is_empty(),
            "STEPD_BLOB_BACKEND=s3 but {} unset; a half-configured S3 backend would hand \
             apps upload URLs pointing at nothing",
            missing.join(", ")
        );
        Ok(S3Config {
            endpoint: self
                .endpoint
                .clone()
                .expect("checked not-None immediately above"),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            path_style: self.path_style,
        })
    }
}

/// Default filesystem root for managed blob bytes, absent `STEPD_BLOB_ROOT`
/// or an explicit override.
const DEFAULT_BLOB_ROOT: &str = "/var/lib/stepd/blobs";

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/stepd".into(),
            bind: "0.0.0.0:8080".into(),
            max_connections: 16,
            worker: hostname_or("worker"),
            batch: 16,
            // The lease must outlast the attempt, with room for the response to
            // come back. Equal values mean a slow-but-healthy app is reclaimed
            // mid-attempt and its step re-executed.
            lease: chrono::Duration::seconds(150),
            attempt_timeout: Duration::from_secs(60),
            idle_poll: Duration::from_millis(250),
            egress: EgressPolicy::default(),
            app_keys: HashMap::new(),
            default_keys: Vec::new(),
            timer_jitter: chrono::Duration::seconds(60),
            blob_backend: BlobBackendConfig::Filesystem {
                root: std::path::PathBuf::from(DEFAULT_BLOB_ROOT),
            },
            blob_base_url: "http://127.0.0.1:8080".into(),
            // Empty by default, and `validate` refuses to serve blobs without
            // one rather than inventing a key. A capability signed with a
            // predictable key is not a capability, and the failure would be
            // silent: every URL would verify, including the ones an attacker
            // minted.
            blob_key: Vec::new(),
            blob_max_size: 100 * 1024 * 1024,
            blob_reservation_ttl: chrono::Duration::hours(24),
        }
    }
}

impl Config {
    /// Read configuration from the environment.
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("STEPD_DATABASE_URL") {
            c.database_url = v;
        }
        if let Ok(v) = std::env::var("STEPD_BIND") {
            c.bind = v;
        }
        if let Ok(v) = std::env::var("STEPD_WORKER") {
            c.worker = v;
        }
        if let Ok(v) = std::env::var("STEPD_MAX_CONNECTIONS") {
            if let Ok(n) = v.parse() {
                c.max_connections = n;
            }
        }
        if let Ok(v) = std::env::var("STEPD_SIGNING_KEY") {
            c.default_keys.push(v.into_bytes());
        }
        // Two keys live at once during rotation, so a key change is not a flag
        // day. Both are accepted; the first is used to sign.
        if let Ok(v) = std::env::var("STEPD_SIGNING_KEY_PREVIOUS") {
            c.default_keys.push(v.into_bytes());
        }
        if std::env::var("STEPD_ALLOW_PRIVATE_EGRESS").is_ok() {
            c.egress.allow_private = true;
        }
        if std::env::var("STEPD_ALLOW_LOOPBACK_EGRESS").is_ok() {
            c.egress.allow_loopback = true;
        }
        if let Ok(v) = std::env::var("STEPD_EGRESS_ALLOWLIST") {
            c.egress.allowlist = v.split(',').map(|s| s.trim().to_string()).collect();
        }

        // Dispatch tuning. These were previously reachable only by editing the
        // struct, which makes them useless during an incident — the one time
        // anybody wants to change them.
        if let Some(n) = env_parse("STEPD_BATCH") {
            c.batch = n;
        }
        if let Some(n) = env_parse::<i64>("STEPD_LEASE_SECONDS") {
            c.lease = chrono::Duration::seconds(n);
        }
        if let Some(n) = env_parse::<u64>("STEPD_ATTEMPT_TIMEOUT_SECONDS") {
            c.attempt_timeout = Duration::from_secs(n);
        }
        if let Some(n) = env_parse::<u64>("STEPD_IDLE_POLL_MS") {
            c.idle_poll = Duration::from_millis(n);
        }
        if let Some(n) = env_parse::<i64>("STEPD_TIMER_JITTER_SECONDS") {
            c.timer_jitter = chrono::Duration::seconds(n);
        }

        // The parsing itself lives in `blob_backend_from`, a pure function of
        // a lookup closure rather than of `std::env::var` calls sprinkled
        // through this method — which is what lets it be tested without
        // mutating the process environment. See its doc comment.
        c.blob_backend = blob_backend_from(|name| std::env::var(name).ok());

        if let Ok(v) = std::env::var("STEPD_BLOB_BASE_URL") {
            c.blob_base_url = v;
        }
        if let Ok(v) = std::env::var("STEPD_BLOB_SIGNING_KEY") {
            c.blob_key = v.into_bytes();
        }
        if let Some(n) = env_parse::<i64>("STEPD_BLOB_MAX_SIZE") {
            c.blob_max_size = n;
        }
        if let Some(n) = env_parse::<i64>("STEPD_BLOB_RESERVATION_TTL_HOURS") {
            c.blob_reservation_ttl = chrono::Duration::hours(n);
        }

        c.validate();
        c
    }

    /// Warn about configurations that are legal and wrong.
    ///
    /// Not fatal: an operator raising a timeout under pressure should not be
    /// stopped by a start-up check. But a lease shorter than the attempt it
    /// covers produces duplicate step execution under perfectly ordinary load,
    /// and that is worth saying out loud rather than discovering from a bill.
    pub fn validate(&self) {
        let lease = self.lease.num_seconds();
        let attempt = self.attempt_timeout.as_secs() as i64;
        if lease <= attempt {
            tracing::warn!(
                lease_seconds = lease,
                attempt_timeout_seconds = attempt,
                "the lease is not longer than the attempt timeout; an app that uses its \
                 whole budget will have its run reclaimed mid-attempt and its step \
                 re-executed. Raise STEPD_LEASE_SECONDS above STEPD_ATTEMPT_TIMEOUT_SECONDS."
            );
        }
        if self.blob_key.is_empty() {
            tracing::warn!(
                "no STEPD_BLOB_SIGNING_KEY is set; the managed-blob endpoints will refuse \
                 every request. Set one, or use external $ref values for payloads the \
                 application already stores."
            );
        }
    }

    /// Refuse a managed-blob backend that is only partly configured.
    ///
    /// Deliberately not folded into `validate`, which only ever warns. That is
    /// right for a missing blob signing key — a server without one still runs
    /// every other path, just not the transfer endpoints — but wrong here: a
    /// bucketless S3 config still answers `:reserve` with `201` and an upload
    /// URL pointing at nothing, and the app finds out on its own next deploy,
    /// not on this server's. `Server::build` calls this before it opens a
    /// database connection, so a deployment with the database reachable and
    /// the blob config wrong still fails at startup naming the missing
    /// variable, rather than at the first upload naming a run.
    pub fn validate_blob_backend(&self) -> anyhow::Result<()> {
        match &self.blob_backend {
            BlobBackendConfig::Filesystem { .. } => Ok(()),
            BlobBackendConfig::S3(s3) => s3.resolve().map(|_| ()),
        }
    }

    /// Whether managed blobs are usable on this server.
    ///
    /// A missing key disables the endpoints rather than defaulting to one. A
    /// capability signed with a predictable key verifies for anybody who guesses
    /// it, and the failure is silent: every URL checks out, including the ones an
    /// attacker minted.
    pub fn blobs_enabled(&self) -> bool {
        !self.blob_key.is_empty()
    }
}

fn env_parse<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok()?.parse().ok()
}

/// Parse a boolean **by value**, not by presence, from an already-read value.
///
/// Unlike the egress flags, which are read with `.is_ok()` — already a
/// documented trap, since `STEPD_ALLOW_PRIVATE_EGRESS=0` still enables private
/// egress — this one looks at what was written. A variable next to those three
/// that reads the opposite way is how an operator writes `=0` expecting
/// something off and gets it on.
///
/// Takes `Option<String>` rather than a variable name and reading the
/// environment itself, so it can be exercised — like `blob_backend_from`,
/// which calls it — without `std::env::set_var`/`remove_var`. `name` is only
/// used to label the warning if `v` is `Some` but not a recognised spelling.
fn parse_bool_by_value(name: &str, v: Option<String>) -> Option<bool> {
    match v?.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        other => {
            tracing::warn!(name, value = other, "not a recognised boolean; ignoring");
            None
        }
    }
}

/// Parse the blob-backend selection from `lookup`.
///
/// A pure function of its input, not of `std::env::var` calls sprinkled
/// through `Config::from_env`, specifically so it is testable without two
/// tests racing over the same process-global environment variable —
/// `std::env::set_var`/`remove_var` affect the whole process, this crate pulls
/// in no `serial_test`, and CLAUDE.md already names "two tests interfering
/// through shared mutable state" as a recurring failure shape here. Tests call
/// this directly against a closure over a fixed map; `Config::from_env` is the
/// only real caller and supplies `std::env::var` at the edge.
fn blob_backend_from(lookup: impl Fn(&str) -> Option<String>) -> BlobBackendConfig {
    match lookup("STEPD_BLOB_BACKEND").as_deref() {
        Some("s3") => BlobBackendConfig::S3(S3ConfigInput {
            endpoint: lookup("STEPD_BLOB_S3_ENDPOINT").and_then(|v| v.parse().ok()),
            // Most self-hosted S3-compatible servers ignore the region but
            // still require it to match what was signed; `us-east-1` is the
            // one every one of them accepts.
            region: lookup("STEPD_BLOB_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
            bucket: lookup("STEPD_BLOB_S3_BUCKET").unwrap_or_default(),
            access_key: lookup("STEPD_BLOB_S3_ACCESS_KEY").unwrap_or_default(),
            secret_key: lookup("STEPD_BLOB_S3_SECRET_KEY").unwrap_or_default(),
            path_style: parse_bool_by_value(
                "STEPD_BLOB_S3_PATH_STYLE",
                lookup("STEPD_BLOB_S3_PATH_STYLE"),
            )
            .unwrap_or(false),
        }),
        other => {
            // Unset, or explicitly "fs", is the ordinary default. Anything
            // else ("minio", "aws", a trailing space) is almost certainly a
            // typo rather than a considered choice: silently keeping the
            // filesystem default would mean every `STEPD_BLOB_S3_*` variable
            // the operator set is ignored with no diagnostic at all —
            // `parse_bool_by_value` above already warns on an unrecognised
            // value, so this matches that precedent rather than being the one
            // place in this function that guesses silently.
            if let Some(v) = other {
                if v != "fs" {
                    tracing::warn!(
                        value = v,
                        "STEPD_BLOB_BACKEND must be \"fs\" or \"s3\"; using the filesystem \
                         backend, but every STEPD_BLOB_S3_* variable that was set is being \
                         ignored"
                    );
                }
            }
            let root = lookup("STEPD_BLOB_ROOT")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_BLOB_ROOT));
            BlobBackendConfig::Filesystem { root }
        }
    }
}

fn hostname_or(fallback: &str) -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| fallback.to_string())
}

/// Shared state every handler sees.
#[derive(Clone)]
pub struct ServerState {
    /// The store, queue, event log, timers and housekeeping.
    pub store: Arc<PostgresStore>,
    /// Managed blob index and bytes, when a signing key is configured.
    pub blobs: Option<Arc<stepd_store_postgres::PostgresBlobStore>>,
    /// How long a presigned read URL lives. Minted per attempt, never persisted.
    pub blob_read_ttl: chrono::Duration,
    /// The expression engine used for triggers and keys.
    pub expr: Arc<CelEngine>,
    /// The push transport, used here for its egress policy at registration time.
    pub transport: Arc<HttpTransport>,
    /// Signing keys per app id, with the fallback last.
    ///
    /// Held here so registration can record the digest of the key the server
    /// will actually sign that app's attempts with. An earlier version stored a
    /// hash of the app *id* in `key_hash_current`, which read like a key digest,
    /// verified nothing, and would have let an operator conclude from the column
    /// that a key was configured when none was.
    pub signing_keys: Arc<HashMap<String, Vec<Vec<u8>>>>,
    /// Key used for apps with no specific one.
    pub default_keys: Arc<Vec<Vec<u8>>>,
}

impl ServerState {
    /// The keys this server will sign `app_id`'s attempts with, if any.
    pub fn keys_for(&self, app_id: &str) -> Option<&Vec<Vec<u8>>> {
        self.signing_keys
            .get(app_id)
            .or(if self.default_keys.is_empty() {
                None
            } else {
                Some(&self.default_keys)
            })
    }
}

/// The assembled server.
pub struct Server {
    /// Effective configuration.
    pub config: Config,
    /// Shared state.
    pub state: ServerState,
    /// The dispatch loop.
    pub dispatcher: Arc<Dispatcher<PostgresStore, PostgresStore, HttpTransport>>,
    /// The convergence loop.
    pub housekeeper: Arc<Housekeeper<PostgresStore, PostgresStore, PostgresStore>>,
    /// Whether `router()` has already logged the relay start-up warning.
    ///
    /// Per-instance, not a `static`: two `Server`s in one process (as the test
    /// fixtures build) must not share this. `router()` builds a fresh `Router`
    /// on every call and nothing stops a caller invoking it more than once on
    /// the same instance; this keeps the warning to one line per instance
    /// regardless, rather than becoming per-call noise.
    blob_relay_warned: AtomicBool,
}

impl Server {
    /// Connect, assemble, and return a server ready to serve and to run.
    pub async fn build(config: Config) -> anyhow::Result<Self> {
        // Checked before anything touches the network: a half-configured S3
        // backend is not something a database connection or a migration can
        // fix, and failing here names the missing variable instead of a run.
        config.validate_blob_backend()?;

        let mut store =
            PostgresStore::connect(&config.database_url, config.max_connections).await?;
        // Managed blobs are optional. A server with no blob signing key still
        // runs every other path; it simply refuses the two transfer endpoints,
        // which is honest and is what the doctor reports.
        //
        // The backend is built exactly once here and the same `Arc` is handed
        // to both the store (which mints read URLs for `$blob` values on their
        // way to an attempt) and the blob index below. Building two backends
        // from the same configuration would work today and diverge the moment
        // one took a different code path — the store minting reads against one
        // bucket while uploads land in another is exactly the silent-corruption
        // shape this project keeps finding.
        let blobs = if config.blobs_enabled() {
            let caps = stepd_store_postgres::Capability::new(
                config.blob_base_url.clone(),
                config.blob_key.clone(),
            );
            // `backend` and `relay` come from one construction per variant, so
            // there is exactly one place that could hand the store and the
            // blob index different objects — and it can't, because both are
            // built from the same `Arc` below.
            let (backend, relay): (Arc<dyn BlobBackend>, Option<Arc<dyn RelayBytes>>) =
                match &config.blob_backend {
                    BlobBackendConfig::Filesystem { root } => {
                        let fs = Arc::new(stepd_store_postgres::blobs::FilesystemBackend::new(
                            root.clone(),
                            caps.clone(),
                        ));
                        (
                            fs.clone() as Arc<dyn BlobBackend>,
                            Some(fs as Arc<dyn RelayBytes>),
                        )
                    }
                    BlobBackendConfig::S3(s3_input) => {
                        // No `RelayBytes`: a backend that presigns has nothing
                        // to relay, which is what keeps §8.3.2's fallback route
                        // unmounted for it (see `stepd_blobs_s3`'s module docs).
                        // `resolve()` re-runs the same check
                        // `validate_blob_backend` already ran above; see its
                        // doc comment for why that is not redundant.
                        let s3 = Arc::new(stepd_blobs_s3::S3Backend::new(s3_input.resolve()?)?);
                        (s3 as Arc<dyn BlobBackend>, None)
                    }
                };
            store = store.with_blob_backend(backend.clone());
            let blob_store = Arc::new(
                stepd_store_postgres::PostgresBlobStore::with_backend_and_relay(
                    store.pool().clone(),
                    backend,
                    relay,
                    caps,
                )
                .with_max_size(config.blob_max_size),
            );
            // The relay start-up warning is logged from `router()`, not here:
            // `build()` runs for every subcommand (`migrate`, `doctor`,
            // `token`, `namespace`, `run`, `limits`), most of which never
            // mount an HTTP route at all, and a warning here would tell an
            // operator running `doctor` that bytes are being relayed through
            // a process that never serves a request.
            Some(blob_store)
        } else {
            None
        };
        if let Some(b) = &blobs {
            store = store.with_blob_collector(b.clone());
        }
        let store = Arc::new(store);
        let transport = Arc::new(HttpTransport::new(
            config.attempt_timeout,
            config.egress.clone(),
        )?);
        let expr = Arc::new(CelEngine::new());

        let targets = Arc::new(DbTargetResolver {
            pool: store.pool().clone(),
            keys: config.app_keys.clone(),
            default_keys: config.default_keys.clone(),
        });

        let dispatcher = Arc::new(Dispatcher::new(
            store.clone(),
            store.clone(),
            transport.clone(),
            targets,
            DispatchConfig {
                worker: config.worker.clone(),
                batch: config.batch,
                lease: config.lease,
                timer_jitter: config.timer_jitter,
                ..Default::default()
            },
        ));

        // Managed blobs are optional. A server with no blob signing key still
        // runs every other path; it simply refuses the two transfer endpoints,
        // which is honest and is what the doctor reports.

        let housekeeper = Arc::new(Housekeeper::new(
            store.clone(),
            store.clone(),
            store.clone(),
            KeeperConfig {
                blob_reservation_ttl: config.blob_reservation_ttl,
                ..Default::default()
            },
        ));

        Ok(Self {
            state: ServerState {
                blobs,
                blob_read_ttl: chrono::Duration::seconds(300),
                store,
                expr,
                transport,
                signing_keys: Arc::new(config.app_keys.clone()),
                default_keys: Arc::new(config.default_keys.clone()),
            },
            config,
            dispatcher,
            housekeeper,
            blob_relay_warned: AtomicBool::new(false),
        })
    }

    /// Apply the bundled migrations.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        self.state.store.migrate().await?;
        Ok(())
    }

    /// The HTTP router.
    pub fn router(&self) -> Router {
        // `None` when managed blobs are switched off entirely: no backend, so
        // nothing to relay for and nothing to name.
        let backend = self
            .state
            .blobs
            .as_ref()
            .map(|b| (b.backend_name(), !b.can_presign()));
        let relay = backend.map(|(_, r)| r).unwrap_or(false);

        // Once per serving process, naming the backend — not per request,
        // where this warning already fires too (`blobs.rs`'s
        // `write_content`). A start-up line alone would not tell an operator
        // the fallback is still in use at three in the morning; a
        // per-request line alone would not tell them their deployment is on
        // the fallback path at all. §8.3.2 calls the relay a compatibility
        // fallback. The name comes from the backend rather than a literal
        // because the guard beside it is already dynamic: an S3-compatible
        // store that could not presign would be mounted here and logged as
        // "filesystem", and nothing in the build would catch it. Guarded so a
        // caller building more than one `Router` from the same `Server` still
        // gets one line, not one per call.
        if let Some((name, true)) = backend {
            if self
                .blob_relay_warned
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                tracing::warn!(
                    backend = name,
                    "managed-blob backend cannot presign; mounting the protocol §8.3.2 relay \
                     route, which puts payload bytes through this process on every transfer"
                );
            }
        }

        Router::new()
            .route("/", get(console::serve))
            .merge(api::router())
            .merge(blobs::router(relay))
            .with_state(self.state.clone())
            .layer(tower_http::trace::TraceLayer::new_for_http())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_egress_policy_fails_closed() {
        // A deployment that needs private addressing says so. One that forgets
        // gets an error at registration, not a credential leak.
        let c = Config::default();
        assert!(!c.egress.allow_private);
        assert!(!c.egress.allow_loopback);
    }

    #[test]
    fn rotation_accepts_two_keys_at_once() {
        std::env::set_var("STEPD_SIGNING_KEY", "new");
        std::env::set_var("STEPD_SIGNING_KEY_PREVIOUS", "old");
        let c = Config::from_env();
        assert_eq!(
            c.default_keys.len(),
            2,
            "a key change must not require a flag day"
        );
        assert_eq!(
            c.default_keys[0],
            b"new".to_vec(),
            "the first key is the one used to sign"
        );
        std::env::remove_var("STEPD_SIGNING_KEY");
        std::env::remove_var("STEPD_SIGNING_KEY_PREVIOUS");
    }

    #[test]
    fn the_lease_outlasts_the_attempt_it_covers() {
        // Equal values mean a slow-but-healthy app races its own lease and its
        // steps execute twice under ordinary load.
        let c = Config::default();
        assert!(
            c.lease.num_seconds() > c.attempt_timeout.as_secs() as i64,
            "lease {}s must exceed attempt timeout {}s",
            c.lease.num_seconds(),
            c.attempt_timeout.as_secs()
        );
    }

    #[test]
    fn dispatch_tuning_is_reachable_from_the_environment() {
        // These are the knobs somebody reaches for during an incident. A value
        // that can only be changed by editing the struct cannot be changed then.
        std::env::set_var("STEPD_BATCH", "64");
        std::env::set_var("STEPD_LEASE_SECONDS", "300");
        let c = Config::from_env();
        assert_eq!(c.batch, 64);
        assert_eq!(c.lease.num_seconds(), 300);
        std::env::remove_var("STEPD_BATCH");
        std::env::remove_var("STEPD_LEASE_SECONDS");
    }

    #[test]
    fn configuration_comes_only_from_the_environment() {
        std::env::set_var("STEPD_BIND", "127.0.0.1:9999");
        assert_eq!(Config::from_env().bind, "127.0.0.1:9999");
        std::env::remove_var("STEPD_BIND");
    }

    #[test]
    fn the_default_blob_backend_is_the_filesystem() {
        // What makes `stepd dev` work with no cloud account, and what the
        // default `docker compose up` stack ships.
        assert!(matches!(
            Config::default().blob_backend,
            BlobBackendConfig::Filesystem { .. }
        ));
    }

    fn probe_s3_config() -> S3ConfigInput {
        S3ConfigInput {
            endpoint: Some("http://127.0.0.1:9000".parse().unwrap()),
            region: "us-east-1".into(),
            bucket: "stepd".into(),
            access_key: "probe".into(),
            secret_key: "probeprobe".into(),
            path_style: true,
        }
    }

    #[tokio::test]
    async fn selecting_s3_without_a_bucket_is_refused_rather_than_defaulted() {
        // A half-configured S3 backend that starts hands apps upload URLs
        // pointing at a bucket that is not there. Failing at startup names
        // the missing variable; failing later names a run. This must not
        // need a database: `Server::build` checks this before it connects.
        let c = Config {
            blob_key: vec![1, 2, 3],
            blob_backend: BlobBackendConfig::S3(S3ConfigInput {
                bucket: String::new(),
                ..probe_s3_config()
            }),
            ..Default::default()
        };
        // `Server` implements no `Debug`, so `expect_err` (which would print
        // the `Ok` value on failure) cannot be used here.
        let err = Server::build(c)
            .await
            .err()
            .expect("a bucketless S3 config must not build");
        assert!(
            err.to_string().contains("STEPD_BLOB_S3_BUCKET"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn selecting_s3_with_no_endpoint_is_refused_rather_than_silently_accepted() {
        // The bug this guards: a sentinel URL survived WHATWG normalisation
        // differently than it was compared against
        // (`http://unset.invalid` parsed back as `http://unset.invalid/`), so
        // this branch was previously dead — `STEPD_BLOB_S3_ENDPOINT` unset
        // built and served regardless, presigning against a hostname RFC 2606
        // guarantees will never resolve. `endpoint: Option<Url>` removes the
        // sentinel-comparison class of bug rather than fixing the comparison.
        let c = Config {
            blob_key: vec![1, 2, 3],
            blob_backend: BlobBackendConfig::S3(S3ConfigInput {
                endpoint: None,
                ..probe_s3_config()
            }),
            ..Default::default()
        };
        let err = Server::build(c)
            .await
            .err()
            .expect("an endpointless S3 config must not build");
        assert!(
            err.to_string().contains("STEPD_BLOB_S3_ENDPOINT"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn selecting_s3_with_no_credentials_is_also_refused() {
        // The bucket is not the only identity an upload URL depends on; empty
        // keys are just as much "pointing at nothing" as an empty bucket.
        let c = Config {
            blob_key: vec![1, 2, 3],
            blob_backend: BlobBackendConfig::S3(S3ConfigInput {
                access_key: String::new(),
                secret_key: String::new(),
                ..probe_s3_config()
            }),
            ..Default::default()
        };
        let err = Server::build(c)
            .await
            .err()
            .expect("a keyless S3 config must not build");
        assert!(
            err.to_string().contains("STEPD_BLOB_S3_ACCESS_KEY")
                && err.to_string().contains("STEPD_BLOB_S3_SECRET_KEY"),
            "got {err}"
        );
    }

    #[test]
    fn a_fully_configured_s3_backend_validates() {
        let c = Config {
            blob_backend: BlobBackendConfig::S3(probe_s3_config()),
            ..Default::default()
        };
        assert!(c.validate_blob_backend().is_ok());
    }

    // The four tests below exercise `blob_backend_from` directly, against a
    // closure over a fixed map, rather than `Config::from_env` against
    // `std::env::set_var`/`remove_var`. Two earlier versions of these tests
    // raced over the process-global `STEPD_BLOB_BACKEND` variable — no
    // `serial_test`, no `--test-threads=1` anywhere in this workspace, so
    // they ran concurrently in one process and could flake in both
    // directions. This is the shape CLAUDE.md already names: "two tests
    // interfering through shared mutable state, and nothing in the build
    // catches the next one." Calling the pure function directly removes the
    // shared state rather than serialising around it.
    fn lookup(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn s3_path_style_is_read_by_value_not_by_presence() {
        // The three egress flags are read with `.is_ok()`, where setting one
        // to `0` still turns it on — a documented trap. This flag must not
        // repeat it: `=0` has to mean off.
        const BASE: &[(&str, &str)] = &[
            ("STEPD_BLOB_BACKEND", "s3"),
            ("STEPD_BLOB_S3_ENDPOINT", "http://127.0.0.1:9000"),
            ("STEPD_BLOB_S3_BUCKET", "stepd"),
            ("STEPD_BLOB_S3_ACCESS_KEY", "probe"),
            ("STEPD_BLOB_S3_SECRET_KEY", "probeprobe"),
            ("STEPD_BLOB_S3_PATH_STYLE", "0"),
        ];
        match blob_backend_from(lookup(BASE)) {
            BlobBackendConfig::S3(s3) => assert!(
                !s3.path_style,
                "STEPD_BLOB_S3_PATH_STYLE=0 must mean path_style is off"
            ),
            other => panic!("expected an S3 backend, got {other:?}"),
        }

        const ON: &[(&str, &str)] = &[
            ("STEPD_BLOB_BACKEND", "s3"),
            ("STEPD_BLOB_S3_ENDPOINT", "http://127.0.0.1:9000"),
            ("STEPD_BLOB_S3_BUCKET", "stepd"),
            ("STEPD_BLOB_S3_ACCESS_KEY", "probe"),
            ("STEPD_BLOB_S3_SECRET_KEY", "probeprobe"),
            ("STEPD_BLOB_S3_PATH_STYLE", "1"),
        ];
        match blob_backend_from(lookup(ON)) {
            BlobBackendConfig::S3(s3) => assert!(
                s3.path_style,
                "STEPD_BLOB_S3_PATH_STYLE=1 must mean path_style is on"
            ),
            other => panic!("expected an S3 backend, got {other:?}"),
        }
    }

    #[test]
    fn an_unset_blob_backend_keeps_the_filesystem_default() {
        // `STEPD_BLOB_BACKEND` unset, or set to `fs`, must not disable the
        // filesystem-plus-relay default that makes `stepd dev` and CI work
        // with no object store.
        assert!(matches!(
            blob_backend_from(lookup(&[])),
            BlobBackendConfig::Filesystem { .. }
        ));
        assert!(matches!(
            blob_backend_from(lookup(&[("STEPD_BLOB_BACKEND", "fs")])),
            BlobBackendConfig::Filesystem { .. }
        ));
    }

    #[test]
    fn an_unrecognised_blob_backend_falls_back_to_the_filesystem_rather_than_hanging() {
        // "minio", "aws", a trailing space — none of these should silently
        // drop every STEPD_BLOB_S3_* variable an operator set with no
        // diagnostic. This asserts only the fallback; the warning itself
        // needs a tracing subscriber this test binary does not install.
        assert!(matches!(
            blob_backend_from(lookup(&[("STEPD_BLOB_BACKEND", "minio")])),
            BlobBackendConfig::Filesystem { .. }
        ));
    }

    #[test]
    fn blob_root_still_selects_the_filesystem_root() {
        // An upgrading operator's existing `STEPD_BLOB_ROOT` must keep
        // selecting the same root it always did.
        match blob_backend_from(lookup(&[("STEPD_BLOB_ROOT", "/tmp/custom-blobs")])) {
            BlobBackendConfig::Filesystem { root } => {
                assert_eq!(root, std::path::PathBuf::from("/tmp/custom-blobs"));
            }
            other => panic!("expected filesystem, got {other:?}"),
        }
    }
}
