//! The level-1 gate (protocol §12).
//!
//! An implementation that passes these is safe to use for work that matters:
//! recorded steps do not re-execute, hashes are stable across attempts, sleeps
//! and errors behave as specified, an abandoned attempt does not lose a step,
//! and a forged request is refused.
//!
//! `determinism` is in this tier rather than the next because non-deterministic
//! hashing corrupts *silently*. Everything else on this list fails loudly when it
//! fails; that one succeeds while doing the wrong thing.

use super::record;
use crate::harness::Harness;
use crate::report::{Report, Status};
use anyhow::Result;
use stepd_proto::RunStatus;

/// `memoization`: a recorded step is not executed again.
pub async fn memoization(h: &Harness, report: &mut Report) {
    record(
        report,
        "memoization",
        "a recorded step is not re-executed on a later attempt",
        memoization_case(h).await,
    );
}

async fn memoization_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.memoize", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }

    let attempts = h.attempts(run).await?;
    if attempts < 2 {
        return Ok(Err(format!(
            "the run took {attempts} attempt(s); the case needs at least two for \
             memoisation to mean anything — does the app honour ctx.attempt()?"
        )));
    }

    // The assertion the whole protocol exists for, and it is only visible here.
    // Server state is identical whether the body ran once or twice; the effect
    // log is the only witness.
    let effects = h.effects(run).await?;
    let works = effects.iter().filter(|e| *e == "work").count();
    if works != 1 {
        return Ok(Err(format!(
            "step 'work' executed {works} times across {attempts} attempts; \
             expected exactly once. Effects: {effects:?}"
        )));
    }
    if !effects.contains(&"after".to_string()) {
        return Ok(Err(format!(
            "step 'after' never executed, so the run did not get past the retry: {effects:?}"
        )));
    }
    Ok(Ok(()))
}

/// `loops`: generated ids produce a stable hash sequence across attempts.
pub async fn loops(h: &Harness, report: &mut Report) {
    record(
        report,
        "loops",
        "a loop's hash sequence is identical across attempts",
        loops_case(h).await,
    );
}

async fn loops_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.loops", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }

    let journal = h.journal(run).await?;
    let ids: Vec<&str> = journal.iter().map(|(id, _, _)| id.as_str()).collect();
    for i in 0..5 {
        let want = format!("item-{i}");
        if !ids.contains(&want.as_str()) {
            return Ok(Err(format!(
                "step '{want}' is missing from the journal: {ids:?}"
            )));
        }
    }

    // Each iteration's body must have run exactly once, despite the forced retry
    // partway through. If the hash sequence shifted between attempts, the first
    // three would re-execute and show up twice here — which is the silent
    // corruption this suite is really about.
    let effects = h.effects(run).await?;
    for i in 0..5 {
        let want = format!("item-{i}");
        let n = effects.iter().filter(|e| **e == want).count();
        if n != 1 {
            return Ok(Err(format!(
                "'{want}' executed {n} times; expected once. A count above one means \
                 the hash sequence moved between attempts. Effects: {effects:?}"
            )));
        }
    }
    Ok(Ok(()))
}

/// `determinism`: program-order hashing, off-path claims, ambiguous ids.
pub async fn determinism(h: &Harness, report: &mut Report) {
    record(
        report,
        "determinism",
        "hashes follow program order",
        program_order(h).await,
    );

    // §12.1: an SDK that makes the hazard unrepresentable declares it and omits
    // the function. Demanding a runtime failure would score the strongest
    // defence below a weaker one.
    if h.prevents("offpath_claim") {
        report.push(
            "determinism",
            "a step cannot be claimed off the sequential pass",
            Status::PassedByConstruction(
                "the app declares offpath_claim unrepresentable, so there is no program to drive",
            ),
        );
    } else {
        record(
            report,
            "determinism",
            "a step cannot be claimed off the sequential pass",
            offpath(h).await,
        );
    }

    record(
        report,
        "determinism",
        "a repeated id in one parallel group raises ambiguous_step_id",
        ambiguous(h).await,
    );
}

async fn program_order(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.order", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }
    let effects = h.effects(run).await?;
    if effects != vec!["a", "b", "c"] {
        return Ok(Err(format!(
            "steps executed in the order {effects:?}, not the program order [a, b, c]"
        )));
    }
    Ok(Ok(()))
}

async fn offpath(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.offpath", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Failed {
        return Ok(Err(format!(
            "the run ended {status:?}; an off-path claim must fail non-retryably"
        )));
    }
    let attempts = h.attempts(run).await?;
    if attempts > 1 {
        return Ok(Err(format!(
            "the run was retried {attempts} times; an off-path claim is not retryable, \
             because retrying cannot make a concurrent claim sequential"
        )));
    }
    Ok(Ok(()))
}

async fn ambiguous(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.ambiguous", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Failed {
        return Ok(Err(format!(
            "the run ended {status:?}; a repeated id in one parallel group must fail"
        )));
    }
    let err = h.error(run).await?.unwrap_or(serde_json::Value::Null);
    let text = err.to_string();
    if !text.contains("ambiguous_step_id") {
        return Ok(Err(format!(
            "the run failed but not with ambiguous_step_id (§6.1); it said: {text}"
        )));
    }
    Ok(Ok(()))
}

/// `sleep`: timer accuracy, replay after wake, and a `null` result.
pub async fn sleep(h: &Harness, report: &mut Report) {
    record(
        report,
        "sleep",
        "a sleep parks the run, wakes it, and records a null result",
        sleep_case(h).await,
    );
}

async fn sleep_case(h: &Harness) -> Result<Result<(), String>> {
    let started = std::time::Instant::now();
    let run = h.fire("conf.sleep", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }

    // The timer must not fire early. Late is a scheduling question; early is a
    // correctness one, because a workflow that says "wait a day before charging
    // the card" means it.
    let elapsed = started.elapsed();
    if elapsed < std::time::Duration::from_secs(2) {
        return Ok(Err(format!(
            "the run completed in {elapsed:?}, before its two-second sleep could elapse"
        )));
    }

    let journal = h.journal(run).await?;
    match journal.iter().find(|(id, _, _)| id == "nap") {
        None => return Ok(Err("no 'nap' step was recorded".into())),
        Some((_, op, _)) if op != "sleep" => {
            return Ok(Err(format!("'nap' was recorded as op '{op}', not 'sleep'")))
        }
        _ => {}
    }

    let result: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT result FROM run_steps WHERE run_id = $1 AND step_id = 'nap'")
            .bind(run)
            .fetch_one(h.server.state.store.pool())
            .await?;
    if !matches!(result, None | Some(serde_json::Value::Null)) {
        return Ok(Err(format!(
            "the sleep recorded a result of {result:?}; §5.2 says a sleep resolves to null"
        )));
    }

    // …and the steps either side each ran once, so the replay after waking did
    // not re-execute the work before the sleep.
    let effects = h.effects(run).await?;
    if effects != vec!["before", "after"] {
        return Ok(Err(format!(
            "expected [before, after] once each; the replay after waking executed {effects:?}"
        )));
    }
    Ok(Ok(()))
}

/// `errors`: retryable versus terminal.
pub async fn errors(h: &Harness, report: &mut Report) {
    record(
        report,
        "errors",
        "a retryable error is retried until it succeeds",
        retryable(h).await,
    );
    record(
        report,
        "errors",
        "a non-retryable error is not retried",
        terminal(h).await,
    );
}

async fn retryable(h: &Harness) -> Result<Result<(), String>> {
    let run = h
        .fire("conf.errors_retryable", serde_json::json!({}))
        .await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!(
            "the run ended {status:?}; a retryable error must be retried, not fatal"
        )));
    }
    let attempts = h.attempts(run).await?;
    if attempts < 3 {
        return Ok(Err(format!(
            "the run completed after {attempts} attempt(s); the app fails twice first"
        )));
    }
    Ok(Ok(()))
}

async fn terminal(h: &Harness) -> Result<Result<(), String>> {
    let run = h
        .fire("conf.errors_terminal", serde_json::json!({}))
        .await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Failed {
        return Ok(Err(format!("the run ended {status:?}, not failed")));
    }
    let attempts = h.attempts(run).await?;
    if attempts != 1 {
        return Ok(Err(format!(
            "the run was attempted {attempts} times; a non-retryable error means the \
             next attempt would do the same thing, so retrying only delays the failure"
        )));
    }
    Ok(Ok(()))
}

/// `abandonment`: an attempt that never answers is retried, and its step is
/// recorded once (§7.1.1).
pub async fn abandonment(h: &Harness, report: &mut Report) {
    record(
        report,
        "abandonment",
        "an abandoned attempt re-executes its step and records it once",
        abandonment_case(h).await,
    );
}

async fn abandonment_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.abandon", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!(
            "the run ended {status:?}; an abandoned attempt must be retried, not failed"
        )));
    }

    // Both halves of §7.1.1, and they pull in opposite directions. The step
    // *executed* twice — the first attempt ran it and then went silent, so its
    // effect happened and nothing recorded it. It is *recorded* once, because
    // the second attempt's commit is the only one the server accepted. This is
    // exactly why at-least-once is the contract, and why a step whose effect is
    // not naturally idempotent needs an idempotency key.
    let effects = h.effects(run).await?;
    let slows = effects.iter().filter(|e| e.starts_with("slow-")).count();
    if slows < 2 {
        return Ok(Err(format!(
            "the step executed {slows} time(s); the first attempt should have run it and \
             then abandoned the response, forcing a second execution. Effects: {effects:?}"
        )));
    }

    let journal = h.journal(run).await?;
    let recorded = journal.iter().filter(|(id, _, _)| id == "slow").count();
    if recorded != 1 {
        return Ok(Err(format!(
            "the step was recorded {recorded} times; the abandoned attempt's response \
             must be discarded, not committed alongside the retry's"
        )));
    }
    Ok(Ok(()))
}

/// `signature`: a forged, stale or replayed request is refused (§9).
///
/// The only suite that does not drive a run. It speaks to the app directly,
/// because what is under test is the app's *rejection* of requests the server
/// would never send — and a correctly behaving server cannot produce one.
pub async fn signature(h: &Harness, report: &mut Report) {
    record(
        report,
        "signature",
        "a request with a bad MAC is refused",
        bad_mac(h).await,
    );
    record(
        report,
        "signature",
        "a request with an expired timestamp is refused",
        stale_timestamp(h).await,
    );
    record(
        report,
        "signature",
        "a replayed nonce is refused",
        replayed_nonce(h).await,
    );
}

fn sign(key: &[u8], ts: i64, nonce: &str, body: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("hmac key");
    mac.update(format!("{ts}\n{nonce}\n{body}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn probe_body() -> String {
    // A syntactically valid attempt for a function that exists. It must be
    // rejected for its signature, before anything looks at what it asks for — so
    // the case cannot pass by accident because the function was missing.
    serde_json::json!({
        "protocol": stepd_proto::PROTOCOL_VERSION,
        "attempt": 1,
        "fence": "conformance-probe",
        "run": {
            "id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10",
            "function_id": "conf-memoize",
            "namespace": "conformance",
            "started_at": "2026-01-01T00:00:00Z",
            "lineage_id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10"
        },
        "steps": {}
    })
    .to_string()
}

async fn post_signed(
    h: &Harness,
    ts: i64,
    nonce: &str,
    signature: &str,
) -> Result<reqwest::StatusCode> {
    let res = reqwest::Client::new()
        .post(h.options.app_url.trim_end_matches('/'))
        .header("content-type", "application/json")
        .header("stepd-timestamp", ts.to_string())
        .header("stepd-nonce", nonce)
        .header("stepd-signature", signature)
        .body(probe_body())
        .send()
        .await?;
    Ok(res.status())
}

async fn bad_mac(h: &Harness) -> Result<Result<(), String>> {
    let ts = chrono::Utc::now().timestamp();
    let status = post_signed(h, ts, "conf-badmac", &"0".repeat(64)).await?;
    if status.is_success() {
        return Ok(Err(format!(
            "the app answered {status} to a request with a forged MAC; anyone who can \
             reach the app can then start runs on it"
        )));
    }
    Ok(Ok(()))
}

async fn stale_timestamp(h: &Harness) -> Result<Result<(), String>> {
    // Correctly signed, but an hour old. Without a freshness window a captured
    // request is replayable forever, and the valid MAC is what makes it
    // convincing.
    let ts = chrono::Utc::now().timestamp() - 3600;
    let sig = sign(&h.options.signing_key, ts, "conf-stale", &probe_body());
    let status = post_signed(h, ts, "conf-stale", &sig).await?;
    if status.is_success() {
        return Ok(Err(format!(
            "the app answered {status} to a correctly-signed request an hour old; §9 \
             requires a freshness window"
        )));
    }
    Ok(Ok(()))
}

async fn replayed_nonce(h: &Harness) -> Result<Result<(), String>> {
    // The one an implementation is most likely to miss: the MAC is valid and the
    // timestamp is fresh, because it is a byte-for-byte copy of a request that
    // was. Only a nonce cache catches it.
    let ts = chrono::Utc::now().timestamp();
    let nonce = format!("conf-replay-{}", uuid::Uuid::new_v4().simple());
    let sig = sign(&h.options.signing_key, ts, &nonce, &probe_body());

    let first = post_signed(h, ts, &nonce, &sig).await?;
    let second = post_signed(h, ts, &nonce, &sig).await?;

    if second.is_success() {
        return Ok(Err(format!(
            "the app answered {first} then {second} to the same signed request; a captured \
             attempt can then be replayed inside the freshness window (§9)"
        )));
    }
    Ok(Ok(()))
}
