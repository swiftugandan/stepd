//! # `stepd`
//!
//! One binary for the whole product: run the server, apply migrations, diagnose
//! a deployment, mint a token, inspect and unstick runs.
//!
//! Admin operations are separate commands rather than flags on `serve`
//! (twelve-factor XII): `stepd migrate` is a release step that must be able to
//! run without starting a server, and `stepd doctor` must be runnable against a
//! database whose server will not start.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use stepd_server::{Config, Runner, Server};

mod doctor;

#[derive(Parser)]
#[command(
    name = "stepd",
    version,
    about = "Durable workflow engine — server, migrations and operations"
)]
struct Cli {
    /// Postgres connection URL. Defaults to $STEPD_DATABASE_URL.
    #[arg(long, global = true, env = "STEPD_DATABASE_URL")]
    database_url: Option<String>,

    /// Log level.
    #[arg(long, global = true, env = "STEPD_LOG", default_value = "info")]
    log: String,

    /// Emit logs as JSON.
    #[arg(long, global = true, env = "STEPD_LOG_JSON")]
    log_json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the server: API, console, dispatch and convergence loops.
    Serve {
        /// Address to bind.
        #[arg(long, env = "STEPD_BIND", default_value = "0.0.0.0:8080")]
        bind: String,
        /// Apply migrations before starting.
        #[arg(long)]
        migrate: bool,
    },

    /// Apply pending migrations and exit.
    Migrate,

    /// Diagnose a deployment.
    Doctor,

    /// Development server: loopback egress, migrations applied, a token minted.
    Dev {
        /// Address to bind.
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        /// Namespace to create.
        #[arg(long, default_value = "dev")]
        namespace: String,
    },

    /// Mint an API token.
    Token {
        /// Namespace the token can see.
        #[arg(long)]
        namespace: String,
        /// Role: viewer, operator or admin.
        #[arg(long, default_value = "operator")]
        role: String,
        /// Label shown in the token list.
        #[arg(long, default_value = "")]
        name: String,
    },

    /// Create a namespace.
    Namespace {
        /// Name.
        name: String,
    },

    /// Show a run and its journal.
    Run {
        /// Run id.
        id: uuid::Uuid,
    },

    /// Print the engine limits currently in force.
    Limits,

    /// Drive an app through the protocol conformance battery (protocol §12).
    ///
    /// Tests an **app**, through a server this command stands up. It cannot be
    /// turned round to test somebody else's server: the mirror battery — a fixed
    /// app that reports what it was sent — is specified nowhere and does not
    /// exist, and pretending otherwise would let a defect on either side be
    /// blamed on the other.
    Conformance {
        /// Base URL of the app under test.
        #[arg(long)]
        app: String,
        /// Shared signing key. Both sides must already hold it.
        #[arg(long, env = "STEPD_SIGNING_KEY", default_value = "stepd-conformance")]
        key: String,
        /// Run only these suites. Repeatable.
        #[arg(long = "suite")]
        suites: Vec<String>,
        /// Seconds a single case may wait for a run to settle.
        #[arg(long, default_value_t = 90)]
        case_timeout: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log, cli.log_json);

    let database_url = cli
        .database_url
        .clone()
        .unwrap_or_else(|| Config::default().database_url);

    match cli.command {
        Command::Serve { bind, migrate } => serve(database_url, bind, migrate, false).await,
        Command::Migrate => {
            let server = build(database_url, None).await?;
            server.migrate().await?;
            println!("migrations applied");
            Ok(())
        }
        Command::Doctor => {
            let server = build(database_url, None).await?;
            println!("\nstepd doctor\n");
            let findings =
                doctor::run(server.state.store.pool(), &server.config.blob_backend).await;
            if doctor::report(&findings) {
                // A non-zero exit so this is usable as a deployment gate rather
                // than something a human has to read and interpret.
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Dev { bind, namespace } => dev(database_url, bind, namespace).await,
        Command::Token {
            namespace,
            role,
            name,
        } => {
            let server = build(database_url, None).await?;
            let raw = mint_token(&server, &namespace, &role, &name).await?;
            println!("{raw}");
            eprintln!(
                "\nThis is the only time the token is shown; only its hash is stored.\n\
                 Open the console at /?token={raw}"
            );
            Ok(())
        }
        Command::Namespace { name } => {
            let server = build(database_url, None).await?;
            server.state.store.ensure_namespace(&name).await?;
            println!("namespace '{name}' ready");
            Ok(())
        }
        Command::Run { id } => show_run(database_url, id).await,
        Command::Limits => {
            let server = build(database_url, None).await?;
            let rows = sqlx::query_as::<_, (String, i64, Option<String>)>(
                "SELECT name, value, note FROM engine_limits ORDER BY name",
            )
            .fetch_all(server.state.store.pool())
            .await?;
            for (name, value, note) in rows {
                println!("  {name:<20} {value:>10}   {}", note.unwrap_or_default());
            }
            Ok(())
        }
        Command::Conformance {
            app,
            key,
            suites,
            case_timeout,
        } => {
            let report = stepd_conformance::run(stepd_conformance::Options {
                app_url: app,
                database_url,
                signing_key: key.into_bytes(),
                case_timeout: std::time::Duration::from_secs(case_timeout),
                only: suites,
                // The app under test is somebody else's process, configured by
                // whoever started it.
                on_ready: None,
            })
            .await?;

            println!("{report}");

            // Non-zero on a failure, an error, or a suite this runner did not
            // implement — but not on one the app merely did not declare. A
            // smaller implementation reporting itself accurately should not fail
            // a build, or the manifest becomes something to lie in.
            if report.should_fail() {
                std::process::exit(1);
            }
            Ok(())
        }
    }
}

fn init_tracing(level: &str, json: bool) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("stepd={level},tower_http=warn")));
    // Unbuffered to stdout (twelve-factor XI). JSON in production so a log
    // aggregator does not have to parse prose.
    if json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

async fn build(database_url: String, bind: Option<String>) -> Result<Server> {
    let mut config = Config::from_env();
    config.database_url = database_url;
    if let Some(b) = bind {
        config.bind = b;
    }
    // Two independent things can fail here now: the database connection, and
    // (checked first, so it fails without touching the network) a
    // half-configured blob backend. A single "check STEPD_DATABASE_URL"
    // context would send an operator chasing the database for a bucket name
    // that was never set, so the underlying error — which already names
    // exactly what is wrong — is left to speak for itself instead of being
    // captioned with a guess.
    Server::build(config)
        .await
        .context("could not build the server")
}

async fn serve(database_url: String, bind: String, migrate: bool, dev_mode: bool) -> Result<()> {
    let server = build(database_url, Some(bind.clone())).await?;
    if migrate {
        server.migrate().await?;
    }

    // Refuse to start unconfigured rather than starting and rejecting every
    // attempt with a signature error nobody connects to the missing key.
    if server.config.default_keys.is_empty() && !dev_mode {
        anyhow::bail!(
            "no signing key configured. Set STEPD_SIGNING_KEY, or use `stepd dev` for \
             loopback development. Every attempt would otherwise be sent unsigned, and \
             a conforming app rejects unsigned requests (protocol §9)."
        );
    }

    run_forever(server, bind).await
}

/// `stepd dev` — everything configured for a laptop, and nothing for production.
async fn dev(database_url: String, bind: String, namespace: String) -> Result<()> {
    let mut config = Config::from_env();
    config.database_url = database_url;
    config.bind = bind.clone();
    // Loopback and private ranges, because the app under development is on this
    // machine. Cloud metadata stays denied even here.
    config.egress = stepd_transport_http::EgressPolicy::development();
    if config.default_keys.is_empty() {
        config.default_keys.push(b"dev-signing-key".to_vec());
    }

    let server = Server::build(config)
        .await
        .context("could not connect to the database")?;
    server.migrate().await?;
    server.state.store.ensure_namespace(&namespace).await?;

    let token = mint_token(&server, &namespace, "admin", "dev").await?;

    println!("\n  stepd dev");
    println!("  ─────────────────────────────────────────────");
    println!("  console    http://{bind}/?token={token}");
    println!("  api        http://{bind}/v1");
    println!("  namespace  {namespace}");
    println!("  signing    dev-signing-key   (override with STEPD_SIGNING_KEY)");
    println!("  egress     loopback and private ranges allowed; metadata still denied");
    println!();

    run_forever(server, bind).await
}

async fn run_forever(server: Server, bind: String) -> Result<()> {
    let runner = Runner::new(
        server.dispatcher.clone(),
        server.housekeeper.clone(),
        server.config.idle_poll,
    );
    let shutdown = runner.shutdown_handle();
    let signals = Runner::new(
        server.dispatcher.clone(),
        server.housekeeper.clone(),
        server.config.idle_poll,
    );

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("cannot bind {bind}"))?;
    tracing::info!(%bind, worker = %server.config.worker, "stepd listening");
    println!("stepd listening on http://{bind}  (console at /)");

    let router = server.router();
    let loops = tokio::spawn(async move { runner.run().await });

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            signals.wait_for_shutdown().await;
            // Stop claiming, then let in-flight attempts finish. An undrained
            // attempt re-executes its step: correct under at-least-once, and it
            // still means every rolling deploy charges some customers twice.
            shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        })
        .await?;

    let _ = loops.await;
    tracing::info!("stopped");
    Ok(())
}

async fn mint_token(server: &Server, namespace: &str, role: &str, name: &str) -> Result<String> {
    anyhow::ensure!(
        ["viewer", "operator", "admin"].contains(&role),
        "role must be viewer, operator or admin"
    );
    server.state.store.ensure_namespace(namespace).await?;

    // 24 bytes of randomness. Only the hash is stored, so this is the one moment
    // the plaintext exists anywhere.
    let raw = {
        use rand::RngCore;
        let mut b = [0u8; 24];
        rand::rngs::OsRng.fill_bytes(&mut b);
        hex::encode(b)
    };

    sqlx::query(
        "INSERT INTO tokens (id, ns, role, token_hash, name)
         VALUES (gen_random_uuid(), $1, $2, $3, $4)",
    )
    .bind(namespace)
    .bind(role)
    .bind(stepd_server::auth::token_hash(&raw))
    .bind(name)
    .execute(server.state.store.pool())
    .await?;

    Ok(raw)
}

async fn show_run(database_url: String, id: uuid::Uuid) -> Result<()> {
    use sqlx::Row;
    let server = build(database_url, None).await?;
    let pool = server.state.store.pool();

    let run = sqlx::query(
        "SELECT ns, fn_id, status::text AS status, key, started_at, ended_at,
                attempt_no, chain_position, error, output, (restored_at IS NOT NULL) AS restored
           FROM runs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("no run {id}"))?;

    println!("\n  run       {id}");
    println!("  function  {}", run.get::<String, _>("fn_id"));
    println!("  namespace {}", run.get::<String, _>("ns"));
    println!("  status    {}", run.get::<String, _>("status"));
    if let Some(k) = run.get::<Option<String>, _>("key") {
        println!("  key       {k}");
    }
    println!("  attempt   {}", run.get::<i32, _>("attempt_no"));
    if run.get::<bool, _>("restored") {
        // The restore paradox (C3): committed steps un-commit and side effects
        // re-execute. An operator reading a run must not have to remember this.
        println!(
            "\n  ⚠ THIS RUN WAS REWOUND BY A POINT-IN-TIME RESTORE.\n    \
             Steps recorded after the restore point were un-recorded and will re-execute.\n    \
             Their side effects have already happened once. See docs/runbooks/restore-hazard.md"
        );
    }
    if let Some(e) = run.get::<Option<serde_json::Value>, _>("error") {
        println!("  error     {e}");
    }
    if let Some(o) = run.get::<Option<serde_json::Value>, _>("output") {
        println!("  output    {o}");
    }

    let steps = sqlx::query(
        "SELECT step_id, op::text AS op, status::text AS status, step_hash, ended_at
           FROM run_steps WHERE run_id = $1 ORDER BY ended_at NULLS LAST, step_hash",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;

    println!("\n  steps");
    for s in &steps {
        println!(
            "    {:<12} {:<14} {:<10} {}",
            s.get::<String, _>("op"),
            s.get::<String, _>("step_id"),
            s.get::<String, _>("status"),
            s.get::<String, _>("step_hash"),
        );
    }

    let waits = sqlx::query(
        "SELECT event_type, expires_at FROM waits WHERE run_id = $1 AND resolved_at IS NULL",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    if !waits.is_empty() {
        println!("\n  waiting for");
        for w in &waits {
            println!(
                "    {}  {}",
                w.get::<String, _>("event_type"),
                w.get::<Option<chrono::DateTime<chrono::Utc>>, _>("expires_at")
                    .map(|t| format!("until {t}"))
                    .unwrap_or_else(|| "no timeout".into())
            );
        }
    }
    println!();
    Ok(())
}
