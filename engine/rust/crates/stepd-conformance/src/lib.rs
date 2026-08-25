//! The stepd protocol conformance battery (protocol §12).
//!
//! `stepd conformance --app <url>` points this at an app and produces a claim
//! about it: conformant at level 1, at level 2, or not conformant, with the
//! failing assertion named.
//!
//! ## What makes this tool trustworthy or not
//!
//! A conformance runner is judged on one thing: whether its verdict can be
//! believed without reading its source. The failure mode specific to the genre
//! is certifying around its own gaps — printing LEVEL 2 when four of the suites
//! were never written, which reads exactly like a real pass.
//!
//! So the outcomes are four, not two ([`Status`]): passed, failed, *not declared
//! by the app*, and *not implemented by this runner*. The last two are unknowns,
//! they are printed as loudly as failures, and either one bars the level it
//! belongs to. `stepd conformance` will tell you what it did not check.
//!
//! ## What it tests
//!
//! An **app**, driven through a known-good server this runner stands up. Not a
//! server: a second implementation of the server side is equally within the
//! protocol's claim and would need the mirror image of this battery — a fixed
//! app that reports what it was sent. Protocol §12.4 says so; so does this
//! module, because a tool whose scope is only stated in the specification will
//! be used outside it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod harness;
pub mod reference;
pub mod report;
pub mod suites;

pub use harness::{ConformanceManifest, Harness, OnReady, Options};
pub use report::{CaseResult, Report, Status, ALL_SUITES, LEVEL_1};

/// Run the battery and return what it concluded.
pub async fn run(options: Options) -> anyhow::Result<Report> {
    use anyhow::Context as _;

    let harness = Harness::start(options).await?;
    // The `blobs` suite needs the app to be able to call back, which needs an
    // address and a token that do not exist until the harness is up. Every app
    // is configured by whoever started it — including the bundled reference one,
    // which the test starts. The runner signals readiness and holds no opinion
    // about which app is listening.
    if let Some(on_ready) = harness.options.on_ready.clone() {
        let token = harness.mint("operator").await?;
        on_ready
            .call(&harness.api_base(), &token)
            .await
            .context("the app under test did not accept its configuration")?;
    }
    let mut report = Report {
        declared: harness
            .manifest
            .suites
            .iter()
            .map(|s| s.as_str().to_string())
            .collect(),
        sdk: harness.manifest.sdk.clone(),
        ..Default::default()
    };
    suites::run_all(&harness, &mut report).await;
    Ok(report)
}
