//! Run the whole battery against the reference app.
//!
//! This is the T2 lane's conformance step, and it is what keeps the suite from
//! becoming decorative. The project's own record on that is unambiguous: a
//! coverage check once showed cascade cancellation hit zero times across five
//! hundred green seeds, and the tick said nothing was wrong.
//!
//! What a pass here means is bounded, and the bound is worth restating because
//! it is easy to over-read. The battery and the Rust SDK were written together,
//! so they can agree on a shared misreading of the protocol and nothing here
//! would surface it. What it does prove is that every assertion is reachable,
//! that the §12.2 contract is implementable, and that an SDK change which breaks
//! a protocol guarantee fails a build.
//!
//! Skipped, loudly, without `STEPD_TEST_DATABASE_URL`.

use stepd_conformance::{reference, Options, Report};

async fn serve_reference() -> Option<(String, reference::State, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
    let addr = listener.local_addr().ok()?;
    let url = format!("http://{addr}");
    let app = reference::app(&url, b"stepd-conformance".to_vec());
    // This instance's state, kept so the battery can finish configuring it once
    // the runner's API exists. Every concurrent battery holds its own.
    let state = app.state.clone();
    let router = reference::router(app);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Some((url, state, handle))
}

/// A fresh database for one battery run.
///
/// Not tidiness. Blob collection is server-wide by design — an unreferenced blob
/// is unreferenced whatever namespace it is in — so two batteries sharing a
/// database have one deleting the other's in-flight reservations. Namespacing
/// the runs is not enough, and making the collector namespace-scoped to suit a
/// test would be the test dictating the product.
async fn fresh_database() -> Option<String> {
    let base = std::env::var("STEPD_TEST_DATABASE_URL").ok()?;
    let name = format!(
        "stepd_conf_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let admin = sqlx::PgPool::connect(&base).await.ok()?;
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .ok()?;
    let (prefix, _) = base.rsplit_once('/')?;
    Some(format!("{prefix}/{name}"))
}

async fn run_battery(only: &[&str]) -> Option<Report> {
    let database_url = fresh_database().await?;
    let (app_url, state, _app) = serve_reference().await?;
    let report = stepd_conformance::run(Options {
        app_url,
        database_url,
        only: only.iter().map(|s| s.to_string()).collect(),
        on_ready: Some(stepd_conformance::OnReady::sync(move |api_base, token| {
            state.configure(api_base, token)
        })),
        ..Default::default()
    })
    .await
    .expect("the battery ran");
    Some(report)
}

macro_rules! battery_or_skip {
    ($only:expr) => {
        match run_battery($only).await {
            Some(r) => r,
            None => {
                eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
                return;
            }
        }
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reference_app_passes_every_case_the_runner_can_drive() {
    let report = battery_or_skip!(&[]);
    eprintln!("{report}");

    // Every case that actually ran must pass. A failure here is a real
    // divergence between the Rust SDK and the protocol.
    let failures: Vec<_> = report
        .cases
        .iter()
        .filter(|c| {
            matches!(
                c.status,
                stepd_conformance::Status::Failed(_) | stepd_conformance::Status::Errored(_)
            )
        })
        .collect();
    assert!(
        failures.is_empty(),
        "the battery found real divergences:\n{report}"
    );

    // Level 2: every suite in protocol §12 declared, driven and passed.
    //
    // This assertion used to say level 1, because `blobs` could not be driven
    // while the server's blob routes were missing. That is the report type doing
    // its job — the runner refused to certify around its own gap, and closing
    // the gap is what changed the answer.
    assert_eq!(
        report.level(),
        Some(2),
        "unaccounted for level 2: {:?}",
        report.unaccounted(2)
    );
    assert!(
        report.unaccounted(2).is_empty(),
        "nothing should stand between this run and level 2"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_declared_suite_actually_ran_a_case() {
    // The vacuity check. A suite that is declared, dispatched, and quietly
    // produces no cases would show as passing in every summary that counts
    // failures — which is precisely the way a conformance tool lies.
    let report = battery_or_skip!(&[]);
    for suite in &report.declared {
        let n = report.cases.iter().filter(|c| c.suite == suite).count();
        assert!(n > 0, "suite '{suite}' was declared but ran no cases");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_suite_in_the_protocol_is_driven_by_this_runner() {
    // The runner must have no gaps left, and must be able to say so from its own
    // data rather than from this list being kept up to date by hand.
    //
    // The predecessor of this test asserted the opposite — that `blobs` reported
    // itself unimplemented — and it failed the moment the gap was closed, which
    // is exactly what it was written to do. A test that quietly kept passing
    // through both states would have said nothing about either.
    let report = battery_or_skip!(&[]);
    let unimplemented: Vec<_> = report
        .cases
        .iter()
        .filter(|c| matches!(c.status, stepd_conformance::Status::NotImplemented(_)))
        .collect();
    assert!(
        unimplemented.is_empty(),
        "these suites are specified and not driven: {:?}",
        unimplemented.iter().map(|c| c.suite).collect::<Vec<_>>()
    );

    for suite in stepd_conformance::ALL_SUITES {
        assert!(
            report.cases.iter().any(|c| c.suite == *suite),
            "suite '{suite}' produced no case at all"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_app_configured_over_http_reaches_the_suites_that_need_a_callback() {
    // The path a non-Rust SDK takes, driven against the app we have.
    //
    // Every other test here configures the reference app in-process, through a
    // closure over its `State`. No app in another process can be configured that
    // way, and until `--app-configure-url` existed there was no other way at
    // all: the runner's API binds to an ephemeral port and mints its own token,
    // so `blobs` and `truncation` were unreachable for every app not written in
    // Rust. That is not a limitation anyone would find by reading the CLI — it
    // reads as though the app's own launcher supplies these — so it needs a test
    // that goes over the wire rather than round it.
    let Some(database_url) = fresh_database().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };
    let Some((app_url, _state, _app)) = serve_reference().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };

    // Deliberately no in-process handle: `_state` is dropped unused, so if the
    // HTTP round trip does not configure the app, nothing else will.
    let report = stepd_conformance::run(Options {
        app_url: app_url.clone(),
        database_url,
        only: vec!["blobs".into(), "truncation".into()],
        on_ready: Some(stepd_conformance::OnReady::post_to(format!(
            "{app_url}/_conformance/configure"
        ))),
        ..Default::default()
    })
    .await
    .expect("the battery ran");

    eprintln!("{report}");
    assert!(report.suite_passed("blobs"), "{report}");
    assert!(report.suite_passed("truncation"), "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fixed_api_bind_is_honoured() {
    // For the app that has to know the address before it starts — a container
    // with the URL already in its environment, which cannot be told afterwards.
    // The default stays ephemeral, so this is the only thing that proves the
    // option is wired to the listener rather than ignored.
    let Some(database_url) = fresh_database().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };
    let Some((app_url, state, _app)) = serve_reference().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };

    // Ask the OS for a free port and hand it back, rather than picking a number
    // that CI might already be using.
    let port = {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        probe.local_addr().unwrap().port()
    };

    let report = stepd_conformance::run(Options {
        app_url,
        database_url,
        only: vec!["memoization".into()],
        api_bind: format!("127.0.0.1:{port}"),
        on_ready: Some(stepd_conformance::OnReady::sync(move |api_base, token| {
            state.configure(api_base, token)
        })),
        ..Default::default()
    })
    .await
    .expect("the battery ran on the requested port");

    // The suite passing is what says the server really came up there: every
    // attempt in it was signed, pushed and committed through that socket.
    assert!(report.suite_passed("memoization"), "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_any_callback_the_suites_that_need_one_fail() {
    // The control for the test above. If `blobs` passed unconfigured, that test
    // would prove nothing about the HTTP round trip — it would only prove the
    // suite is insensitive to whether the callback happened at all.
    //
    // It also pins the behaviour that matters for an SDK author: declaring a
    // suite you cannot support has to fail. The manifest's whole design punishes
    // over-declaring, and a `blobs` that quietly passed with no blob client
    // would be the runner certifying around its own gap.
    let Some(database_url) = fresh_database().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };
    let Some((app_url, _state, _app)) = serve_reference().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };

    let report = stepd_conformance::run(Options {
        app_url,
        database_url,
        only: vec!["blobs".into(), "truncation".into()],
        on_ready: None,
        // Short, because the truncation case here cannot settle and waiting the
        // default 90s for that is dead time in every CI run.
        //
        // It cannot settle because of a defect this test found and does not fix.
        // The SDK correctly answers 400 — it has nowhere to page from — and
        // protocol §2.2 says a 400 fails the run non-retryably. The dispatcher
        // does not do that: `Dispatcher::drive` funnels every transport error
        // into `handle_failure` with the code `transport`, which backs off and
        // retries, and never consults `Error::is_retryable` even though
        // `Error::Config` (which is what a 400 becomes) returns false from it.
        // So the run sits Pending between retries instead of failing.
        //
        // Nothing had exercised the path: an app returning 400 is the one thing
        // no suite could produce while `truncation` could not truncate. Filed as
        // https://github.com/swiftugandan/stepd/issues/30.
        case_timeout: std::time::Duration::from_secs(20),
        ..Default::default()
    })
    .await
    .expect("the battery ran");
    eprintln!("{report}");

    assert!(
        !report.suite_passed("blobs"),
        "blobs passed with no blob client configured, so the callback proves nothing:\n{report}"
    );
    // And this is what says the harness's low `attempt_state_limit` really
    // truncates. Unconfigured, the SDK has nowhere to page from and must refuse
    // the attempt; if `truncation` passed here, `state_truncated` was never set
    // and the suite is asserting nothing about §8.6 — which is exactly the state
    // it was in at the 2000-step default.
    assert!(
        !report.suite_passed("truncation"),
        "truncation passed with nowhere to page from, so the journal was never \
         truncated and the suite tests nothing:\n{report}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configure_url_that_does_not_answer_stops_the_run() {
    // The failure has to be loud and attributed to configuration. The quiet
    // alternative is worse than useless: `blobs` fails, the report says the app
    // diverges from the protocol, and the actual cause is a typo'd URL. An SDK
    // author would go looking in their blob client.
    let Some(database_url) = fresh_database().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };
    let Some((app_url, _state, _app)) = serve_reference().await else {
        eprintln!("SKIPPED: set STEPD_TEST_DATABASE_URL to run the conformance battery");
        return;
    };

    let err = stepd_conformance::run(Options {
        app_url: app_url.clone(),
        database_url,
        only: vec!["blobs".into()],
        on_ready: Some(stepd_conformance::OnReady::post_to(format!(
            "{app_url}/_conformance/no-such-route"
        ))),
        ..Default::default()
    })
    .await
    .expect_err("a configure URL that 404s must fail the run, not the blobs suite");

    let text = format!("{err:#}");
    assert!(
        text.contains("did not accept its configuration"),
        "the error must name configuration as the cause, got: {text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_errors_suite_passes_in_isolation() {
    // Kept as its own test because the two error cases are the ones most likely
    // to interact with something earlier in the battery: a deliberate failure is
    // exactly the input load protection is built to react to, and a case that
    // only fails when it runs eleventh is the hardest kind to read.
    let report = battery_or_skip!(&["errors"]);
    eprintln!("{report}");
    assert!(report.suite_passed("errors"), "{report}");
}
