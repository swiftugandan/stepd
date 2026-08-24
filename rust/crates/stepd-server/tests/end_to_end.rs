//! The whole stack, end to end.
//!
//! A real `stepd-sdk` app on a real TCP socket, a real server with a real
//! Postgres, real signed HTTP between them, and a workflow driven from an
//! ingested event to a completed run.
//!
//! Everything else in this workspace tests one layer against fakes. This is the
//! only test that can catch the failures that live *between* layers — a header
//! the transport sends and the SDK does not read, a status code one side means
//! differently from the other, an envelope that validates on one side and not
//! the other. Those are exactly the failures that a well-tested set of
//! components still ships with.
//!
//! Skipped, loudly, when `STEPD_TEST_DATABASE_URL` is unset.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::serve::Listener;
use stepd_sdk::prelude::*;
use stepd_server::{BlobBackendConfig, Config, Server};
use stepd_transport_http::EgressPolicy;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use uuid::Uuid;

/// Executions per (run, step).
///
/// Keyed by run id rather than a bare counter: every test in this file shares
/// one process, several drive the same workflow, and they run concurrently. A
/// global counter would make "the charge step ran once" a statement about the
/// whole test binary, which is both wrong and intermittently wrong — the worst
/// combination, because it fails on someone else's change.
static EXECUTIONS: OnceLock<Mutex<HashMap<(Uuid, &'static str), usize>>> = OnceLock::new();

fn record(run: Uuid, step: &'static str) {
    *EXECUTIONS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry((run, step))
        .or_insert(0) += 1;
}

fn executions(run: Uuid, step: &'static str) -> usize {
    EXECUTIONS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(&(run, step))
        .copied()
        .unwrap_or(0)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct Receipt {
    tx: String,
    carrier: String,
    approved_by: String,
}

/// The workflow under test: a step, a parallel pair, a wait, another step.
async fn order_fulfilment(ctx: &Ctx) -> StepResult<Receipt> {
    let run_id = ctx.run().id;
    let tx: String = ctx
        .step("charge", || async move {
            record(run_id, "charge");
            Ok("ch_1".to_string())
        })
        .await?;

    // A parallel pair, to prove the whole batch commits atomically and costs one
    // attempt rather than two.
    let invoice = ctx.step::<String, _, _>("fetch-invoice", || async { Ok("inv-9".into()) });
    let risk = ctx.step::<i32, _, _>("score-risk", || async { Ok(17) });
    let (_invoice, _risk) = ctx.join((invoice, risk)).await?;

    let approval: Option<serde_json::Value> = ctx.wait_event("approval", "order.approved").await?;

    let carrier: String = ctx
        .step("ship", || async move {
            record(run_id, "ship");
            Ok("dhl".to_string())
        })
        .await?;

    Ok(Receipt {
        tx,
        carrier,
        approved_by: approval
            .and_then(|v| v["by"].as_str().map(str::to_string))
            .unwrap_or_else(|| "nobody".into()),
    })
}

/// A cron-triggered workflow. Reads its occurrence from the run input, which is
/// where a catch-up fire's *intended* time lives — `started_at` is when recovery
/// happened, not what the schedule meant.
async fn nightly_report(ctx: &Ctx) -> StepResult<String> {
    let run_id = ctx.run().id;
    let occurrence = ctx
        .run()
        .input
        .as_ref()
        .and_then(|v| v["cron"]["occurrence_at"].as_str())
        .unwrap_or("missing")
        .to_string();

    let out: String = ctx
        .step("summarise", move || {
            let occurrence = occurrence.clone();
            async move {
                record(run_id, "summarise");
                Ok(format!("report for {occurrence}"))
            }
        })
        .await?;
    Ok(out)
}

struct Fixture {
    server: Arc<Server>,
    namespace: String,
    token: String,
    base: String,
    _app: tokio::task::JoinHandle<()>,
}

const SIGNING_KEY: &[u8] = b"end-to-end-signing-key";

async fn fixture(label: &str) -> Option<Fixture> {
    let database_url = std::env::var("STEPD_TEST_DATABASE_URL").ok()?;

    // The app first: it needs a port before the server can be told about it.
    let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = app_listener.local_addr().unwrap();
    let app_url = format!("http://{app_addr}");

    let app = App::new("billing", app_url.clone())
        .signing_key(SIGNING_KEY.to_vec())
        .function(
            Function::new("order-fulfilment")
                .on_event("order.created")
                .key("'order:' + string(event.data.order_id)")
                .run(order_fulfilment),
        )
        .function(
            Function::new("nightly-report")
                .on_cron_with(
                    "0 3 * * *",
                    "Europe/London",
                    CronOptions::default()
                        .catchup_all(3)
                        .misfire_window("PT6H")
                        .singleton("nightly-report"),
                )
                .run(nightly_report),
        );
    let manifest = app.manifest();
    let app_task = tokio::spawn(async move {
        let _ = axum::serve(app_listener, app.router()).await;
    });

    let mut config = Config::from_env();
    config.database_url = database_url;
    // Managed blobs, into a per-fixture temporary root. The key must be set or
    // the endpoints refuse everything, which is correct for a deployment that
    // does not use them and useless for a test of the ones that do.
    config.blob_key = SIGNING_KEY.to_vec();
    config.blob_backend = BlobBackendConfig::Filesystem {
        root: std::env::temp_dir().join(format!("stepd-e2e-blobs-{}", Uuid::new_v4().simple())),
    };
    // Loopback, because the app under test is on this machine. Cloud metadata
    // stays denied even here — the policy's own tests assert that.
    config.egress = EgressPolicy::development();
    config.default_keys = vec![SIGNING_KEY.to_vec()];
    config.lease = chrono::Duration::seconds(30);
    // No jitter: a test that sometimes waits an extra minute is a flaky test.
    config.timer_jitter = chrono::Duration::zero();

    // Bound before the server is built: a blob capability has to be signed
    // against the address the app will really reach, and `bind` is `0.0.0.0` on
    // any deployment that matters.
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = api_listener.local_addr().unwrap();
    config.blob_base_url = format!("http://{api_addr}");

    let server = Arc::new(Server::build(config).await.expect("server"));
    server.migrate().await.expect("migrate");

    let namespace = format!("e2e-{label}-{}", &Uuid::new_v4().simple().to_string()[..8]);
    server
        .state
        .store
        .ensure_namespace(&namespace)
        .await
        .unwrap();

    let token = mint(&server, &namespace, "admin").await;

    let router = server.router();
    tokio::spawn(async move {
        let _ = axum::serve(api_listener, router).await;
    });

    let base = format!("http://{api_addr}");

    // Register the app through the real API, exactly as an SDK would at start-up.
    let res = reqwest::Client::new()
        .put(format!("{base}/v1/apps"))
        .bearer_auth(&token)
        .json(&manifest)
        .send()
        .await
        .expect("register");
    assert_eq!(
        res.status(),
        200,
        "registration failed: {:?}",
        res.text().await
    );

    Some(Fixture {
        server,
        namespace,
        token,
        base,
        _app: app_task,
    })
}

async fn mint(server: &Server, namespace: &str, role: &str) -> String {
    let raw = Uuid::new_v4().simple().to_string();
    sqlx::query(
        "INSERT INTO tokens (id, ns, role, token_hash, name)
         VALUES (gen_random_uuid(), $1, $2, $3, 'e2e')",
    )
    .bind(namespace)
    .bind(role)
    .bind(stepd_server::auth::token_hash(&raw))
    .execute(server.state.store.pool())
    .await
    .unwrap();
    raw
}

macro_rules! fixture_or_skip {
    ($label:expr) => {
        match fixture($label).await {
            Some(f) => f,
            None => {
                eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the end-to-end tests");
                return;
            }
        }
    };
}

/// A `BlobBackend` that presigns, so it has no use for §8.3.2's relay.
///
/// Deliberately does not implement `RelayBytes` — a backend that can presign
/// has nothing to relay, which is exactly the property that must make
/// `blobs::router` leave the content route unmounted. Never exercised beyond
/// `can_presign`: reserving or transferring a real blob through it is not what
/// this fixture is for.
struct PresigningBackend;

#[async_trait::async_trait]
impl stepd_core::traits::BlobBackend for PresigningBackend {
    async fn upload_target(
        &self,
        id: Uuid,
        _spec: &stepd_core::traits::BlobSpec,
        ttl: chrono::Duration,
    ) -> stepd_core::Result<stepd_core::traits::UploadTarget> {
        Ok(stepd_core::traits::UploadTarget {
            url: format!("https://example-object-store.test/{id}"),
            method: "PUT".to_string(),
            headers: Vec::new(),
            expires_at: chrono::Utc::now() + ttl,
        })
    }

    fn read_url(&self, id: Uuid, size: i64, _ttl: chrono::Duration) -> stepd_core::Result<String> {
        Ok(format!(
            "https://example-object-store.test/{id}?size={size}"
        ))
    }

    async fn stored(
        &self,
        _id: Uuid,
    ) -> stepd_core::Result<Option<stepd_core::traits::StoredObject>> {
        Ok(None)
    }

    async fn delete(&self, _id: Uuid) -> stepd_core::Result<()> {
        Ok(())
    }

    fn can_presign(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "presigning-stub"
    }
}

/// A running server whose managed-blob backend is [`PresigningBackend`].
///
/// Not `fixture()`: that one always builds a `FilesystemBackend`, which is the
/// case that keeps the relay mounted. `Server::build` has no configuration
/// knob for swapping the backend — nothing outside a test should want one, a
/// deployment picks the filesystem fallback or a real presigning backend, not
/// a stub — so this fixture builds a server the normal way and then replaces
/// `state.blobs` before the router (and the mount decision it makes) is built.
///
/// Per-instance state throughout, never a `static`: the server, listener and
/// base URL all live on the returned struct, not in shared global state.
struct PresigningFixture {
    base: String,
    _server: Arc<Server>,
}

async fn fixture_with_presigning_backend() -> Option<PresigningFixture> {
    let database_url = std::env::var("STEPD_TEST_DATABASE_URL").ok()?;

    let mut config = Config::from_env();
    config.database_url = database_url;
    config.blob_key = SIGNING_KEY.to_vec();
    // Never read: `PresigningBackend` touches no filesystem. `Server::build`
    // still builds a `FilesystemBackend` internally before it is replaced
    // below, and that construction needs a path even though nothing is
    // written under it.
    config.blob_backend = BlobBackendConfig::Filesystem {
        root: std::env::temp_dir().join(format!("stepd-e2e-presign-{}", Uuid::new_v4().simple())),
    };
    config.egress = EgressPolicy::development();

    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = api_listener.local_addr().unwrap();
    config.blob_base_url = format!("http://{api_addr}");

    let blob_base_url = config.blob_base_url.clone();
    let blob_key = config.blob_key.clone();

    let mut server = Server::build(config).await.expect("server");
    server.migrate().await.expect("migrate");

    // Swap the `FilesystemBackend` `Server::build` wired up for one that
    // presigns — the case this fixture exists to cover.
    server.state.blobs = Some(Arc::new(
        stepd_store_postgres::PostgresBlobStore::with_backend(
            server.state.store.pool().clone(),
            Arc::new(PresigningBackend) as Arc<dyn stepd_core::traits::BlobBackend>,
            stepd_store_postgres::Capability::new(blob_base_url, blob_key),
        ),
    ));

    let server = Arc::new(server);
    let router = server.router();
    tokio::spawn(async move {
        let _ = axum::serve(api_listener, router).await;
    });

    Some(PresigningFixture {
        base: format!("http://{api_addr}"),
        _server: server,
    })
}

/// Drive `namespace` on `server` until `run` finishes or the budget runs out.
///
/// Shared by every fixture in this file that needs a drive loop: it takes
/// only `Server` and a namespace, not a fixture type, so `Fixture` and
/// `S3Fixture` — which otherwise share no common type — can both call it
/// without either duplicating the loop or forcing the other to gain fields it
/// does not use.
async fn drive(server: &Server, namespace: &str, run: Uuid, max_ticks: u32) -> String {
    use stepd_core::traits::StateStore;
    for _ in 0..max_ticks {
        server.dispatcher.tick_namespace(namespace).await.unwrap();
        server.housekeeper.tick().await;
        if let Some(s) = server.store_status(run).await {
            if s.is_terminal() {
                return format!("{s:?}").to_lowercase();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let s = server.state.store.run_status(run).await.unwrap();
    panic!("run {run} did not finish; last status {s:?}");
}

/// Drive the namespace until the run finishes or the budget runs out.
async fn drive_until_done(f: &Fixture, run: Uuid, max_ticks: u32) -> String {
    drive(&f.server, &f.namespace, run, max_ticks).await
}

/// Convenience for the status poll above.
trait StatusExt {
    async fn store_status(&self, run: Uuid) -> Option<stepd_proto::RunStatus>;
}

impl StatusExt for Server {
    async fn store_status(&self, run: Uuid) -> Option<stepd_proto::RunStatus> {
        use stepd_core::traits::StateStore;
        self.state.store.run_status(run).await.ok().flatten()
    }
}

async fn ingest(f: &Fixture, event_type: &str, data: serde_json::Value) -> serde_json::Value {
    reqwest::Client::new()
        .post(format!("{}/v1/events", f.base))
        .bearer_auth(&f.token)
        .json(&serde_json::json!([{
            "specversion": "1.0",
            "source": "/shop",
            "type": event_type,
            "data": data,
        }]))
        .send()
        .await
        .expect("ingest")
        .json()
        .await
        .expect("ingest body")
}

async fn only_run(f: &Fixture) -> Uuid {
    sqlx::query_scalar::<_, Uuid>("SELECT id FROM runs WHERE ns = $1 ORDER BY started_at LIMIT 1")
        .bind(&f.namespace)
        .fetch_one(f.server.state.store.pool())
        .await
        .expect("a run was created")
}

// ---------------------------------------------------------------- the test

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_drives_a_real_sdk_workflow_to_completion() {
    let f = fixture_or_skip!("full");

    let res = ingest(&f, "order.created", serde_json::json!({ "order_id": 4711 })).await;
    assert_eq!(res["accepted"], 1);
    assert_eq!(
        res["runs_started"], 1,
        "the trigger expression matched and started a run"
    );

    let run = only_run(&f).await;

    // The key came from the function's CEL expression, evaluated at ingest.
    let key: Option<String> = sqlx::query_scalar("SELECT key FROM runs WHERE id = $1")
        .bind(run)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(key.as_deref(), Some("order:4711"));

    // Drive to the wait.
    for _ in 0..10 {
        f.server
            .dispatcher
            .tick_namespace(&f.namespace)
            .await
            .unwrap();
        f.server.housekeeper.tick().await;
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM waits WHERE run_id = $1 AND resolved_at IS NULL",
        )
        .bind(run)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
        if waiting > 0 {
            break;
        }
    }

    // Resolve it through the operator command, the same path the console uses.
    let res = reqwest::Client::new()
        .post(format!("{}/v1/runs/{run}/resolve-wait", f.base))
        .bearer_auth(&f.token)
        .json(&serde_json::json!({
            "event_type": "order.approved",
            "data": { "by": "priya" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{:?}", res.text().await);

    let status = drive_until_done(&f, run, 40).await;
    assert_eq!(status, "completed");

    let output: serde_json::Value = sqlx::query_scalar("SELECT output FROM runs WHERE id = $1")
        .bind(run)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    let receipt: Receipt = serde_json::from_value(output).unwrap();
    assert_eq!(
        receipt,
        Receipt {
            tx: "ch_1".into(),
            carrier: "dhl".into(),
            approved_by: "priya".into()
        }
    );

    // The property the whole system exists to provide.
    assert_eq!(
        executions(run, "charge"),
        1,
        "the charge step executed more than once across the run's attempts"
    );
    assert_eq!(executions(run, "ship"), 1);

    // Four recorded steps, and the parallel pair really was one envelope: five
    // steps would mean the join degraded into sequential awaits.
    let steps: Vec<String> =
        sqlx::query_scalar("SELECT step_id FROM run_steps WHERE run_id = $1 ORDER BY step_id")
            .bind(run)
            .fetch_all(f.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(
        steps,
        vec!["approval", "charge", "fetch-invoice", "score-risk", "ship"]
    );

    let attempts: i32 = sqlx::query_scalar("SELECT attempt_no FROM runs WHERE id = $1")
        .bind(run)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        attempts, 5,
        "expected charge, the parallel pair, the wait, ship, done — one attempt each"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_arriving_before_the_wait_still_resolves_it() {
    // The early-signal race, over the real stack: the approval is ingested while
    // the run is still on its first step, long before `wait_event` registers.
    let f = fixture_or_skip!("early");

    ingest(&f, "order.created", serde_json::json!({ "order_id": 5000 })).await;
    let run = only_run(&f).await;

    // Deliver the approval immediately, before a single attempt has run.
    use stepd_core::traits::EventLog;
    let delivery = f
        .server
        .state
        .store
        .deliver(
            run,
            "order.approved",
            &serde_json::json!({ "by": "early" }),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        delivery,
        stepd_core::traits::Delivery::Buffered,
        "no wait is registered yet, so the event must be buffered, not dropped"
    );

    let status = drive_until_done(&f, run, 40).await;
    assert_eq!(status, "completed");

    let output: serde_json::Value = sqlx::query_scalar("SELECT output FROM runs WHERE id = $1")
        .bind(run)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        output["approved_by"], "early",
        "the buffered event resolved the wait when it was finally registered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_event_does_not_start_a_second_run() {
    let f = fixture_or_skip!("dedupe");

    let body = serde_json::json!([{
        "specversion": "1.0", "source": "/shop", "type": "order.created",
        "data": { "order_id": 6000 }, "stepdidempotency": "order-6000"
    }]);
    let client = reqwest::Client::new();
    for _ in 0..3 {
        let res: serde_json::Value = client
            .post(format!("{}/v1/events", f.base))
            .bearer_auth(&f.token)
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let _ = res;
    }

    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE ns = $1")
        .bind(&f.namespace)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        runs, 1,
        "an idempotency key must survive the whole ingest path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_token_cannot_see_another_namespace() {
    // F-SEC-1: namespace isolation is enforced in the query, so a token for one
    // namespace cannot observe that another exists — not even by run id.
    let f = fixture_or_skip!("isolation");
    ingest(&f, "order.created", serde_json::json!({ "order_id": 7000 })).await;
    let run = only_run(&f).await;

    let other_ns = format!("other-{}", &Uuid::new_v4().simple().to_string()[..8]);
    f.server
        .state
        .store
        .ensure_namespace(&other_ns)
        .await
        .unwrap();
    let other_token = mint(&f.server, &other_ns, "admin").await;

    let client = reqwest::Client::new();

    let res = client
        .get(format!("{}/v1/runs/{run}", f.base))
        .bearer_auth(&other_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        404,
        "a foreign run must look absent, not forbidden — 403 would confirm the id is real"
    );

    let res = client
        .post(format!("{}/v1/runs/{run}/cancel", f.base))
        .bearer_auth(&other_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        404,
        "and it must not be cancellable across the boundary"
    );

    let listed: serde_json::Value = client
        .get(format!("{}/v1/runs", f.base))
        .bearer_auth(&other_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        listed["items"].as_array().unwrap().len(),
        0,
        "the list must be empty, not filtered after the fact"
    );

    // …and the legitimate token still works.
    let res = client
        .get(format!("{}/v1/runs/{run}", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unauthenticated_request_is_refused_but_health_is_not() {
    let f = fixture_or_skip!("auth");
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{}/v1/runs", f.base))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .get(format!("{}/v1/runs", f.base))
            .bearer_auth("not-a-real-token")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    // A health check that needs a token is one more thing to get wrong in a
    // deployment, and it reveals nothing.
    assert_eq!(
        client
            .get(format!("{}/v1/health", f.base))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_viewer_can_read_but_not_command() {
    let f = fixture_or_skip!("roles");
    ingest(&f, "order.created", serde_json::json!({ "order_id": 8000 })).await;
    let run = only_run(&f).await;

    let viewer = mint(&f.server, &f.namespace, "viewer").await;
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{}/v1/runs/{run}", f.base))
            .bearer_auth(&viewer)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{}/v1/runs/{run}/cancel", f.base))
            .bearer_auth(&viewer)
            .send()
            .await
            .unwrap()
            .status(),
        403,
        "here 403 is right: the caller can see the run, they just may not cancel it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_command_is_audited() {
    // An audit log with exceptions answers "who cancelled this run?" with
    // "someone".
    let f = fixture_or_skip!("audit");
    ingest(&f, "order.created", serde_json::json!({ "order_id": 9000 })).await;
    let run = only_run(&f).await;

    reqwest::Client::new()
        .post(format!("{}/v1/runs/{run}/cancel", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap();

    let logged: Vec<String> =
        sqlx::query_scalar("SELECT command FROM commands_audit WHERE ns = $1 AND target = $2")
            .bind(&f.namespace)
            .bind(run.to_string())
            .fetch_all(f.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(logged, vec!["cancel_run"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_is_served_with_a_strict_csp() {
    let f = fixture_or_skip!("console");
    let res = reqwest::Client::new().get(&f.base).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let csp = res
        .headers()
        .get("content-security-policy")
        .expect("the console must carry a CSP: it renders arbitrary payload JSON")
        .to_str()
        .unwrap()
        .to_string();
    assert!(csp.contains("script-src 'nonce-"));
    assert!(!csp.contains("script-src 'unsafe-inline'"));
    assert!(res.text().await.unwrap().contains("stepd console"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_app_manifest_is_discoverable_and_registration_is_idempotent() {
    let f = fixture_or_skip!("discovery");

    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM functions WHERE ns = $1")
        .bind(&f.namespace)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        before, 2,
        "the fixture registers an event and a cron function"
    );

    // Re-register the same manifest. Registration is idempotent by
    // (app_id, function.id); a second call must not duplicate the function.
    let app = App::new("billing", "http://127.0.0.1:1/")
        .signing_key(SIGNING_KEY.to_vec())
        .function(
            Function::new("order-fulfilment")
                .on_event("order.created")
                .key("'order:' + string(event.data.order_id)")
                .run(order_fulfilment),
        )
        .function(
            Function::new("nightly-report")
                .on_cron_with(
                    "0 3 * * *",
                    "Europe/London",
                    CronOptions::default()
                        .catchup_all(3)
                        .misfire_window("PT6H")
                        .singleton("nightly-report"),
                )
                .run(nightly_report),
        );
    let res = reqwest::Client::new()
        .put(format!("{}/v1/apps", f.base))
        .bearer_auth(&f.token)
        .json(&app.manifest())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM functions WHERE ns = $1")
        .bind(&f.namespace)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(after, 2, "re-registration must update, not duplicate");

    // …and the same for schedules. An upsert that inserted instead would give
    // the function two identical schedules and fire it twice per occurrence,
    // gaining one more on every deploy.
    let schedules: i64 = sqlx::query_scalar("SELECT count(*) FROM cron_schedules WHERE ns = $1")
        .bind(&f.namespace)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(schedules, 1, "re-registration must not duplicate schedules");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registering_an_app_the_egress_policy_refuses_fails_at_registration() {
    // At registration, where a human sees the error — not at the first attempt,
    // where it becomes a mysterious run failure.
    let f = fixture_or_skip!("egress");

    let manifest = serde_json::json!({
        "protocol": "1",
        "app_id": "evil",
        "url": "http://169.254.169.254/",
        "functions": [{ "id": "f", "triggers": [{ "type": "event", "event": "x" }] }]
    });
    let res = reqwest::Client::new()
        .put(format!("{}/v1/apps", f.base))
        .bearer_auth(&f.token)
        .json(&manifest)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["code"], "egress_denied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_expression_is_rejected_at_registration_not_at_the_first_event() {
    // Deferring expression compilation to the ingest path makes a typo in a
    // predicate surface as a log line at the first matching event — and the
    // function simply never runs, which is indistinguishable from a predicate
    // that legitimately did not match.
    let f = fixture_or_skip!("expr");

    let manifest = serde_json::json!({
        "protocol": "1",
        "app_id": "billing",
        "url": "http://127.0.0.1:1/",
        "functions": [{
            "id": "broken",
            "triggers": [{ "type": "event", "event": "x",
                           "expr": "event.data.tags.all(t, t == 'a')" }]
        }]
    });
    let res = reqwest::Client::new()
        .put(format!("{}/v1/apps", f.base))
        .bearer_auth(&f.token)
        .json(&manifest)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["code"], "bad_expression");
    assert!(
        body["detail"].as_str().unwrap().contains("all"),
        "the error must name the unsupported construct, not just say 'invalid': {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_recorded_key_digest_reflects_a_key_the_server_actually_holds() {
    // The column previously held a hash of the app *id*: a value shaped exactly
    // like a key digest that verified nothing, from which an operator would
    // reasonably conclude a key was configured when none was.
    let f = fixture_or_skip!("keydigest");

    let stored: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT key_hash_current FROM app_bindings WHERE ns = $1 AND app_id = 'billing'",
    )
    .bind(&f.namespace)
    .fetch_one(f.server.state.store.pool())
    .await
    .unwrap();

    use sha2::{Digest, Sha256};
    assert_eq!(
        stored,
        Some(Sha256::digest(SIGNING_KEY).to_vec()),
        "the digest must be of the signing key in force, not of anything else"
    );
    assert_ne!(
        stored,
        Some(Sha256::digest(b"billing").to_vec()),
        "and specifically not a hash of the app id"
    );
}

#[tokio::test]
async fn a_cron_function_registers_schedules_and_runs_when_they_fire() {
    let f = fixture_or_skip!("cron");

    // Registration happened in the fixture, through the real API. The first
    // assertion is that it produced a schedule at all: for the whole life of
    // this project `on_cron` registered a trigger and nothing scheduled it, and
    // every test passed, because a function that never fires fails nothing.
    let listed: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/schedules", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let schedules = listed["schedules"].as_array().unwrap();
    assert_eq!(schedules.len(), 1, "the cron trigger produced a schedule");
    let s = &schedules[0];
    assert_eq!(s["fn_id"], "nightly-report");
    assert_eq!(s["cron"], "0 3 * * *");
    assert_eq!(s["tz"], "Europe/London");
    // The options the SDK set survived the round trip through the manifest,
    // the JSON schema and the database. ADR-016 noted these existed in the
    // protocol with no way for the Rust SDK to set them.
    assert_eq!(s["catchup"], "all");
    assert_eq!(s["catchup_limit"], 3);
    assert_eq!(s["misfire_window_secs"], 21_600);
    assert_eq!(s["singleton"], true);
    assert!(!s["paused"].as_bool().unwrap());

    // Bring the fire time forward, as an outage would.
    sqlx::query(
        "UPDATE cron_schedules SET next_fire_at = now() - interval '10 seconds' WHERE ns = $1",
    )
    .bind(&f.namespace)
    .execute(f.server.state.store.pool())
    .await
    .unwrap();

    // The housekeeping pass fires it; the dispatcher executes it. Both through
    // the real loops, against the real app, over signed HTTP.
    f.server.housekeeper.tick().await;

    let run: Uuid =
        sqlx::query_scalar("SELECT id FROM runs WHERE ns = $1 AND fn_id = 'nightly-report'")
            .bind(&f.namespace)
            .fetch_one(f.server.state.store.pool())
            .await
            .expect("the schedule created a run");

    let status = drive_until_done(&f, run, 60).await;
    assert_eq!(status, "completed", "the cron run executed and completed");
    assert_eq!(
        executions(run, "summarise"),
        1,
        "the step ran exactly once, as for any other run"
    );

    // The run knows which occurrence it is for. A handler that read the wall
    // clock instead would misdate a catch-up fire's output with nothing to
    // notice it by (ADR-016, "what we accept").
    let output: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT output FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(f.server.state.store.pool())
            .await
            .unwrap();
    let text = output.unwrap().as_str().unwrap_or_default().to_string();
    assert!(
        text.starts_with("report for 20"),
        "the handler read its occurrence from the run input, got {text:?}"
    );
}

#[tokio::test]
async fn a_bad_cron_expression_is_refused_at_registration() {
    let f = fixture_or_skip!("badcron");

    // The point of the whole exercise. A cron expression that cannot be
    // scheduled must fail the deploy, where a person is watching — not register
    // cleanly and produce a function that never runs, which is what happened
    // before and which nothing anywhere reports.
    for (expr, why) in [
        ("0 3 * * 9", "day-of-week out of range"),
        ("0 0 L * *", "a dialect extension"),
        ("0 3 * *", "four fields"),
    ] {
        let manifest = serde_json::json!({
            "protocol": "1", "app_id": "billing", "url": "http://127.0.0.1:9/",
            "functions": [{
                "id": "broken",
                "triggers": [{ "type": "cron", "cron": expr }]
            }]
        });
        let res = reqwest::Client::new()
            .put(format!("{}/v1/apps", f.base))
            .bearer_auth(&f.token)
            .json(&manifest)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 400, "'{expr}' ({why}) should be refused");
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body["code"], "bad_cron", "and say which thing was wrong");
    }

    // An unknown zone too: falling back to UTC would keep the schedule firing,
    // at a time nobody chose.
    let manifest = serde_json::json!({
        "protocol": "1", "app_id": "billing", "url": "http://127.0.0.1:9/",
        "functions": [{
            "id": "broken",
            "triggers": [{ "type": "cron", "cron": "0 3 * * *", "tz": "Mars/Olympus_Mons" }]
        }]
    });
    let res = reqwest::Client::new()
        .put(format!("{}/v1/apps", f.base))
        .bearer_auth(&f.token)
        .json(&manifest)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400, "an unknown time zone is refused");
}

#[tokio::test]
async fn a_paused_schedule_can_be_resumed_through_the_api() {
    let f = fixture_or_skip!("resume");

    // Corrupt the row the way a vanished tzdata zone would, and let the sweep
    // find it.
    sqlx::query(
        "UPDATE cron_schedules SET expr = 'not a cron', \
                next_fire_at = now() - interval '1 minute' WHERE ns = $1",
    )
    .bind(&f.namespace)
    .execute(f.server.state.store.pool())
    .await
    .unwrap();
    f.server.housekeeper.tick().await;

    let listed: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/schedules", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let s = &listed["schedules"][0];
    assert_eq!(s["paused"], true);
    assert!(
        s["last_error"].is_string(),
        "the endpoint says why, so an operator need not read the logs"
    );
    let id = s["id"].as_str().unwrap().to_string();

    // Resuming while still broken must fail, and say so. Reporting success and
    // pausing again on the next sweep would be the worst of both.
    let res = reqwest::Client::new()
        .post(format!("{}/v1/schedules/{id}/resume", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400, "resuming a still-broken schedule fails");

    sqlx::query("UPDATE cron_schedules SET expr = '0 3 * * *' WHERE ns = $1")
        .bind(&f.namespace)
        .execute(f.server.state.store.pool())
        .await
        .unwrap();

    let res = reqwest::Client::new()
        .post(format!("{}/v1/schedules/{id}/resume", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "a fixed schedule resumes");

    let listed: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/schedules", f.base))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["schedules"][0]["paused"], false);
    assert!(listed["schedules"][0]["last_error"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_round_trips_through_the_real_transfer_endpoints() {
    // The two routes that existed only as a store and a capability minter until
    // now. Everything beneath them was built and tested; nothing mounted them,
    // so a `$blob` was a shape an app could not actually produce.
    let f = fixture_or_skip!("blobs");

    let run: Uuid = sqlx::query_scalar(
        "INSERT INTO runs (id, ns, fn_id, lineage_id, status)
         VALUES (gen_random_uuid(), $1, 'blob-holder', gen_random_uuid(), 'pending')
         RETURNING id",
    )
    .bind(&f.namespace)
    .fetch_one(f.server.state.store.pool())
    .await
    .unwrap();

    let payload = b"conformance blob payload".to_vec();
    let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&payload));
    let http = reqwest::Client::new();

    // Phase one: reserve.
    let res = http
        .post(format!("{}/v1/blobs:reserve", f.base))
        .bearer_auth(&f.token)
        .json(&serde_json::json!({
            "run_id": run, "size": payload.len(), "sha256": digest,
            "content_type": "text/plain"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{:?}", res.text().await);
    let reservation: serde_json::Value = res.json().await.unwrap();
    assert_eq!(reservation["deduplicated"], false);
    let upload_url = reservation["upload_url"].as_str().unwrap().to_string();
    let blob_id = reservation["blob_id"].as_str().unwrap().to_string();

    // Phase two: upload. No bearer token — the capability in the URL is the
    // authorisation, which is the whole reason an app that holds only an upload
    // URL can use it.
    let up = http
        .put(&upload_url)
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(up.status(), 201, "{:?}", up.text().await);

    // Reserving the identical digest again must skip the upload entirely.
    let again: serde_json::Value = http
        .post(format!("{}/v1/blobs:reserve", f.base))
        .bearer_auth(&f.token)
        .json(&serde_json::json!({
            "run_id": run, "size": payload.len(), "sha256": digest
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(again["deduplicated"], true);
    assert_eq!(again["blob_id"], blob_id);
    assert!(
        again["upload_url"].is_null(),
        "a deduplicated reservation must carry no upload URL; an app that saw one \
         would upload bytes that already exist"
    );

    // Read it back, in full and by range, through the presigned URL.
    use stepd_core::traits::BlobStore;
    let read_url = f
        .server
        .state
        .blobs
        .as_ref()
        .expect("blobs enabled")
        .presign_read(blob_id.parse().unwrap(), chrono::Duration::seconds(60))
        .await
        .unwrap();

    let whole = http.get(&read_url).send().await.unwrap();
    assert_eq!(whole.status(), 200);
    assert_eq!(whole.bytes().await.unwrap().to_vec(), payload);

    let head = http
        .get(&read_url)
        .header("range", "bytes=0-10")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.status(),
        206,
        "§8.3.3 requires Range so a step can read a header without pulling the object"
    );
    assert_eq!(head.bytes().await.unwrap().to_vec(), payload[0..11]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_capability_cannot_be_repurposed() {
    // The signature covers the direction and the size, so a leaked write URL is
    // not a read URL and not a licence to store more than was reserved. Both
    // failures would be silent: the transfer would simply succeed.
    let f = fixture_or_skip!("blobcap");

    let run: Uuid = sqlx::query_scalar(
        "INSERT INTO runs (id, ns, fn_id, lineage_id, status)
         VALUES (gen_random_uuid(), $1, 'blob-holder', gen_random_uuid(), 'pending')
         RETURNING id",
    )
    .bind(&f.namespace)
    .fetch_one(f.server.state.store.pool())
    .await
    .unwrap();

    let payload = b"eight!!!".to_vec();
    let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&payload));
    let http = reqwest::Client::new();

    let reservation: serde_json::Value = http
        .post(format!("{}/v1/blobs:reserve", f.base))
        .bearer_auth(&f.token)
        .json(&serde_json::json!({
            "run_id": run, "size": payload.len(), "sha256": digest
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let upload_url = reservation["upload_url"].as_str().unwrap().to_string();

    // More bytes than were reserved.
    let too_big = http
        .put(&upload_url)
        .body(b"considerably more than eight bytes".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(
        too_big.status(),
        400,
        "a write capability must not accept more than the size it was signed for"
    );

    // The same capability, pointed at reading.
    let repurposed = upload_url.replace("dir=write", "dir=read");
    let read = http.get(&repurposed).send().await.unwrap();
    assert!(
        !read.status().is_success(),
        "a write capability must not be usable for reading; the direction is inside \
         the signature precisely so this cannot be edited"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_presigning_backend_does_not_expose_the_relay_route() {
    // The relay is a fallback (§8.3.2). Leaving it mounted next to a backend
    // that presigns leaves a second, unwarned path to the same bytes — and the
    // capability it accepts is signed with a different key than the one the
    // object store checks.
    let f = match fixture_with_presigning_backend().await {
        Some(f) => f,
        None => {
            eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the end-to-end tests");
            return;
        }
    };

    let http = reqwest::Client::new();
    let res = http
        .put(format!(
            "{}/v1/blobs/{}/content?dir=write&size=1&exp=1&sig=x",
            f.base,
            Uuid::now_v7()
        ))
        .body("x")
        .send()
        .await
        .expect("the server answered");
    assert_eq!(res.status(), 404, "the relay route must not exist here");

    // A bare 404 is not enough: a run that does not exist, a mistyped path, or
    // an auth redirect could all produce one just as well, and none of those
    // would mean the route is absent. Every error this server's handlers raise
    // is a Problem Details document (`application/problem+json`, a `code`
    // field) — see `Problem::into_response`. Axum's own fallback for an
    // unmatched route carries neither, because no handler ever ran to build
    // one. That is what actually distinguishes "this route does not exist"
    // from "this route exists and refused you".
    assert_ne!(
        res.headers().get("content-type").map(|v| v.as_bytes()),
        Some(b"application/problem+json".as_slice()),
        "a Problem Details response means a handler ran and refused the request; a route \
         that was never mounted never reaches one"
    );
    let body = res.bytes().await.expect("body");
    assert!(
        body.is_empty(),
        "axum's fallback for an unmatched route has an empty body; a non-empty body would \
         mean some handler produced this 404, i.e. the route exists after all"
    );

    // And the server itself is up, and still mounts the other blob route: the
    // 404 above is about the content route specifically, not a fixture that
    // failed to start or a base URL that is wrong.
    let reserve_status = http
        .post(format!("{}/v1/blobs:reserve", f.base))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("the server answered")
        .status();
    assert_ne!(
        reserve_status, 404,
        "the reserve route must still exist; only the relay content route is conditional"
    );
}

// ------------------------------------------------------ S3: no bytes reach the server
//
// BR-19's claim, proven end to end rather than about a backend in isolation:
// bulk payload data must never traverse the control plane. Everything below
// exists to drive one real run, whose step result carries a `$blob`, against
// a real S3-compatible object store, and to say what that run touching this
// server's own socket would have to look like if the claim ever stopped
// being true.

/// A `TcpStream` that adds every byte it moves, in either direction, to a
/// shared counter.
///
/// Per-fixture, never a `static`: CLAUDE.md is explicit that per-instance
/// state landing in a `static` has cost this codebase three prior
/// recurrences, each found by two tests interfering through shared mutable
/// state. Two [`S3Fixture`]s built by two tests in this binary must not be
/// able to move each other's count, so the counter lives on the struct each
/// test owns and nowhere else.
struct CountingStream {
    inner: tokio::net::TcpStream,
    bytes: Arc<AtomicU64>,
}

impl AsyncRead for CountingStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if poll.is_ready() {
            let added = buf.filled().len() - before;
            this.bytes.fetch_add(added as u64, Ordering::Relaxed);
        }
        poll
    }
}

impl AsyncWrite for CountingStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            this.bytes.fetch_add(*n as u64, Ordering::Relaxed);
        }
        poll
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Wraps a bound listener so every accepted connection is a [`CountingStream`]
/// sharing one counter: the total bytes, in both directions, that have ever
/// crossed this server's own client-facing socket.
///
/// Only the API listener is ever wrapped in this file, never the app's or the
/// dispatcher's outbound leg to it — both are also control plane, and neither
/// is counted here. What stands in for that leg is the test's separate
/// assertion on the run's output: `Blob` carries no bytes, only a size and a
/// digest, so an app step that tried to return content inline could not
/// produce the shape the test checks for at all, counter or no counter.
struct CountingListener {
    inner: tokio::net::TcpListener,
    bytes: Arc<AtomicU64>,
}

impl Listener for CountingListener {
    type Io = CountingStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // Matches `TcpListener`'s own `Listener` impl: retry on a transient
        // accept error rather than stopping the server over it.
        loop {
            if let Ok((stream, addr)) = self.inner.accept().await {
                return (
                    CountingStream {
                        inner: stream,
                        bytes: self.bytes.clone(),
                    },
                    addr,
                );
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Bytes the S3 no-relay test uploads and reads back.
///
/// Large enough that a full relay through the control plane and a few
/// kilobytes of JSON are not the same order of magnitude, which is what makes
/// the byte-count assertion below able to tell them apart. The content itself
/// is arbitrary.
const S3_TEST_PAYLOAD: &[u8] = &[0x5A; 256 * 1024];

/// The workflow this test drives: one step that puts bytes through the SDK's
/// own blob client, exactly as an application would, and returns the
/// reference as its result.
///
/// The base URL and token the step needs to reach this server come off
/// `ctx.run().input` — the same trick `nightly_report` above uses for its
/// occurrence — because a bare `fn(&Ctx)` has no other way to receive them.
async fn s3_blob_upload(ctx: &Ctx) -> StepResult<Blob> {
    let input = ctx.run().input.clone().unwrap_or_default();
    let base = input["stepd_base"].as_str().unwrap_or_default().to_string();
    let token = input["stepd_token"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let run_id = ctx.run().id;

    ctx.step("upload", move || {
        let base = base.clone();
        let token = token.clone();
        async move {
            Blobs::new(base, token)
                .put(run_id, S3_TEST_PAYLOAD)
                .content_type("application/octet-stream")
                .send()
                .await
                .map_err(|e| StepError::fatal(format!("blob upload failed: {e}")))
        }
    })
    .await
}

/// Configuration for the live S3-compatible object store, or `None` with a
/// loud reason.
///
/// Mirrors `stepd-blobs-s3/tests/live.rs`'s `config()` exactly — same env
/// vars, same defaults — because both suites have to agree on how to reach
/// the same test server. Not shared as library code between the crates: that
/// crate's own docs reject reaching for app-side or store-side code to save a
/// few lines, and the same reasoning applies here in the other direction.
fn s3_test_config() -> Option<stepd_blobs_s3::S3Config> {
    let endpoint = std::env::var("STEPD_TEST_S3_ENDPOINT").ok()?;
    let var = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    Some(stepd_blobs_s3::S3Config {
        endpoint: endpoint.parse().expect("STEPD_TEST_S3_ENDPOINT is a URL"),
        region: var("STEPD_TEST_S3_REGION", "us-east-1"),
        bucket: var("STEPD_TEST_S3_BUCKET", "stepd"),
        access_key: var("STEPD_TEST_S3_ACCESS_KEY", "probe"),
        secret_key: var("STEPD_TEST_S3_SECRET_KEY", "probeprobe"),
        path_style: true,
    })
}

/// Create the test bucket, tolerating one that already exists.
///
/// `S3Backend` never creates a bucket itself (see its module docs), so
/// whoever tests it has to. Duplicated from
/// `stepd-blobs-s3/tests/live.rs::ensure_bucket` rather than shared, for the
/// same layering reason `s3_test_config` gives.
async fn ensure_bucket(cfg: &stepd_blobs_s3::S3Config) {
    use rusty_s3::actions::CreateBucket;
    use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

    let bucket = Bucket::new(
        cfg.endpoint.clone(),
        UrlStyle::Path,
        cfg.bucket.clone(),
        cfg.region.clone(),
    )
    .expect("a usable bucket url");
    let credentials = Credentials::new(cfg.access_key.clone(), cfg.secret_key.clone());
    let url = CreateBucket::new(&bucket, &credentials).sign(std::time::Duration::from_secs(60));
    let res = reqwest::Client::new()
        .put(url)
        .send()
        .await
        .expect("the test object store answers");
    assert!(
        res.status().is_success() || res.status() == reqwest::StatusCode::CONFLICT,
        "creating the test bucket returned {}",
        res.status()
    );
}

/// A running server whose managed-blob backend is the real S3 backend
/// (`stepd-blobs-s3`), plus what the no-bytes test needs to observe it: the
/// resolved object-store config, for an out-of-band readback, and a
/// per-instance counter of everything that has crossed the API socket.
struct S3Fixture {
    server: Arc<Server>,
    namespace: String,
    token: String,
    base: String,
    s3: stepd_blobs_s3::S3Config,
    server_bytes: Arc<AtomicU64>,
    _app: tokio::task::JoinHandle<()>,
}

async fn fixture_with_s3(label: &str) -> Option<S3Fixture> {
    let Ok(database_url) = std::env::var("STEPD_TEST_DATABASE_URL") else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the end-to-end tests");
        return None;
    };
    let Some(s3) = s3_test_config() else {
        eprintln!(
            "SKIPPED: set STEPD_TEST_S3_ENDPOINT to run the S3 blob-backend \
             end-to-end test. Without it nothing in this build proves BR-19 \
             against a real object store on a real run — only against fakes."
        );
        return None;
    };
    ensure_bucket(&s3).await;

    // Bound before the server is built, exactly as `fixture()` does: a blob
    // capability has to be signed against the address the app will really
    // reach.
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = api_listener.local_addr().unwrap();
    let base = format!("http://{api_addr}");

    let mut config = Config::from_env();
    config.database_url = database_url;
    config.blob_key = SIGNING_KEY.to_vec();
    config.blob_backend = BlobBackendConfig::S3(stepd_server::S3ConfigInput {
        endpoint: Some(s3.endpoint.clone()),
        region: s3.region.clone(),
        bucket: s3.bucket.clone(),
        access_key: s3.access_key.clone(),
        secret_key: s3.secret_key.clone(),
        path_style: s3.path_style,
    });
    config.blob_base_url = base.clone();
    config.egress = EgressPolicy::development();
    config.default_keys = vec![SIGNING_KEY.to_vec()];
    config.lease = chrono::Duration::seconds(30);
    config.timer_jitter = chrono::Duration::zero();

    let server = Arc::new(Server::build(config).await.expect("server"));
    server.migrate().await.expect("migrate");

    let namespace = format!("e2e-{label}-{}", &Uuid::new_v4().simple().to_string()[..8]);
    server
        .state
        .store
        .ensure_namespace(&namespace)
        .await
        .unwrap();
    let token = mint(&server, &namespace, "admin").await;

    // The app: a real SDK function whose one step puts bytes through
    // `Blobs::put`, the same call an application would make.
    let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = app_listener.local_addr().unwrap();
    let app_url = format!("http://{app_addr}");
    let app = App::new("s3-blobs", app_url)
        .signing_key(SIGNING_KEY.to_vec())
        .function(
            Function::new("s3-blob-upload")
                .on_event("blob.created")
                .run(s3_blob_upload),
        );
    let manifest = app.manifest();
    let app_task = tokio::spawn(async move {
        let _ = axum::serve(app_listener, app.router()).await;
    });

    // The API listener is wrapped, the app's is not: see `CountingListener`'s
    // doc comment for why only this socket is the one the test needs a count
    // of.
    let server_bytes = Arc::new(AtomicU64::new(0));
    let counted = CountingListener {
        inner: api_listener,
        bytes: server_bytes.clone(),
    };
    let router = server.router();
    tokio::spawn(async move {
        let _ = axum::serve(counted, router).await;
    });

    let res = reqwest::Client::new()
        .put(format!("{base}/v1/apps"))
        .bearer_auth(&token)
        .json(&manifest)
        .send()
        .await
        .expect("register");
    assert_eq!(
        res.status(),
        200,
        "registration failed: {:?}",
        res.text().await
    );

    Some(S3Fixture {
        server,
        namespace,
        token,
        base,
        s3,
        server_bytes,
        _app: app_task,
    })
}

/// Drive an `S3Fixture`'s run to completion via the shared `drive` loop.
async fn drive_s3_until_done(f: &S3Fixture, run: Uuid, max_ticks: u32) -> String {
    drive(&f.server, &f.namespace, run, max_ticks).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_object_bytes_reach_the_server_on_the_s3_path() {
    // BR-19's whole point, driven through a real run rather than asserted
    // about a backend in isolation: a step result carries a `$blob`, the app
    // put the bytes straight into the object store, and it is the
    // dispatcher's op-commit verification (`Dispatcher::commit`, immediately
    // before `store.commit` — there is no separate commit endpoint) that
    // makes the reference readable. A `commit_blob` that downloaded the
    // object to verify it would pass every other assertion in this file and
    // fail only this one.
    let Some(f) = fixture_with_s3("s3blob").await else {
        return;
    };

    let baseline = f.server_bytes.load(Ordering::SeqCst);
    assert!(
        baseline > 0,
        "app registration is real traffic through this socket; if the \
         counter had not already moved before the run even started, it would \
         not be wired to anything and the assertion below would prove nothing"
    );

    let ingested: serde_json::Value = reqwest::Client::new()
        .post(format!("{}/v1/events", f.base))
        .bearer_auth(&f.token)
        .json(&serde_json::json!([{
            "specversion": "1.0",
            "source": "/blobs",
            "type": "blob.created",
            "data": { "stepd_base": f.base, "stepd_token": f.token },
        }]))
        .send()
        .await
        .expect("ingest")
        .json()
        .await
        .expect("ingest body");
    assert_eq!(ingested["runs_started"], 1);

    let run: Uuid =
        sqlx::query_scalar("SELECT id FROM runs WHERE ns = $1 ORDER BY started_at LIMIT 1")
            .bind(&f.namespace)
            .fetch_one(f.server.state.store.pool())
            .await
            .expect("a run was created");

    let status = drive_s3_until_done(&f, run, 40).await;
    assert_eq!(status, "completed");

    let output: serde_json::Value = sqlx::query_scalar("SELECT output FROM runs WHERE id = $1")
        .bind(run)
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    let blob: Blob = serde_json::from_value(output).expect("run output is a $blob reference");
    assert_eq!(blob.size(), S3_TEST_PAYLOAD.len() as i64);

    // The property this test exists to prove: the run's own commit is what
    // made the blob readable, not a call this test made. `Dispatcher::commit`
    // verifies and commits a referenced blob in the same transaction as the
    // run's ops (§8.3.2), so a run that reports "completed" has already left
    // its blob rows `committed` — there is no other moment that could have
    // done it, and nothing here calls one directly.
    let state: String = sqlx::query_scalar("SELECT state::text FROM blobs WHERE id = $1")
        .bind(blob.inner.id)
        .fetch_one(f.server.state.store.pool())
        .await
        .expect("the blob row exists");
    assert_eq!(
        state, "committed",
        "the run reported completed, so its blob reference must already be \
         committed as a consequence of that commit"
    );

    // Readable, independent of anything this test asked the server to do: a
    // presigned GET straight to the object store, signed locally against the
    // same config the fixture gave the server. This does not test the same
    // thing the byte counter below does — it would still pass even if
    // `commit_blob` downloaded the object to verify it — it is here so that
    // "committed but the bytes are not actually retrievable" is a distinct,
    // separately-caught failure from "committed but unverified".
    use stepd_core::traits::BlobBackend;
    let backend = stepd_blobs_s3::S3Backend::new(f.s3.clone()).expect("a backend");
    let read_url = backend
        .read_url(blob.inner.id, blob.size(), chrono::Duration::seconds(60))
        .expect("a read url");
    let read = reqwest::Client::new()
        .get(&read_url)
        .send()
        .await
        .expect("the object store answers");
    assert_eq!(read.status(), 200);
    assert_eq!(read.bytes().await.unwrap().to_vec(), S3_TEST_PAYLOAD);

    // The relay route (§8.3.2's fallback) must be absent for a real S3
    // backend too, not only for `PresigningBackend`'s stub in
    // `a_presigning_backend_does_not_expose_the_relay_route` above.
    let relay = reqwest::Client::new()
        .put(format!(
            "{}/v1/blobs/{}/content?dir=write&size=1&exp=1&sig=x",
            f.base,
            Uuid::now_v7()
        ))
        .body("x")
        .send()
        .await
        .expect("the server answered");
    assert_eq!(
        relay.status(),
        404,
        "the relay route must not exist for an S3 backend"
    );

    // The load-bearing assertion. Across app registration, ingest, the
    // reservation call the app's own step made through the SDK, and the
    // whole drive loop, the total bytes that ever crossed this server's own
    // client-facing socket — both directions, everything `CountingListener`
    // saw — stayed in the noise. Actual control-plane traffic here is a few
    // kilobytes of JSON; the ceiling below is `CONTROL_PLANE_TRAFFIC_CEILING`
    // (32 KiB), comfortably above that and comfortably below
    // `S3_TEST_PAYLOAD.len()` (256 KiB), so a regression that routed the
    // payload — or any large fraction of it — through this socket instead of
    // straight to the object store would clear it.
    //
    // What this does NOT catch: a `commit_blob` that downloaded the object
    // from the object store to hash it would add no bytes here at all — that
    // traffic runs between this server process and the object store, never
    // touching the socket this counter watches. Today that path is closed
    // structurally rather than by a test: `commit_blob`
    // (`stepd-store-postgres/src/blobs/mod.rs`) only downloads and hashes
    // when `BlobBackend::stored` answers `sha256: None`, and `S3Backend::stored`
    // never does — it errors instead of returning `None` when the object
    // store reports no checksum. Nothing here or in `stepd-blobs-s3` asserts
    // that a *regressed* `S3Backend::stored` returning `None` would still be
    // caught; `stepd-blobs-s3`'s own
    // `a_committed_object_reports_its_digest_without_transferring_it` only
    // checks that `stored` reports the right digest today, and its own doc
    // comment says a backend that downloaded to hash it would pass that
    // assertion too. As of this test, neither suite runs in CI — both exist
    // and pass locally with `STEPD_TEST_DATABASE_URL` and `STEPD_TEST_S3_*`
    // set, but the CI lane that was meant to run them was reverted pending
    // separate review of how it starts an S3-compatible service, so BR-19 is
    // not yet proven by anything CI runs.
    let total = f.server_bytes.load(Ordering::SeqCst);
    const CONTROL_PLANE_TRAFFIC_CEILING: usize = 32 * 1024;
    assert!(
        (total as usize) < CONTROL_PLANE_TRAFFIC_CEILING,
        "control-plane traffic totalled {total} bytes, over the {}-byte \
         ceiling; that is not explainable by JSON control chatter alone and \
         is consistent with the payload itself having crossed this socket",
        CONTROL_PLANE_TRAFFIC_CEILING
    );
}
