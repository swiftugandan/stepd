//! The reference conformance app (protocol §12.2).
//!
//! Every function the battery drives, built on `stepd-sdk`, plus the effect-log
//! probe of §12.1.
//!
//! ## What passing against this proves, and what it does not
//!
//! This app ships in the same crate as the runner, so a green run is the Rust
//! SDK agreeing with a battery written alongside it. That is weaker evidence
//! than an independent implementation passing, and it is worth saying so where
//! someone will read it: a suite and an implementation written together can
//! agree on a shared misreading of the specification, and no amount of green
//! ticks will surface that.
//!
//! What it does establish is that the battery *runs* — that every assertion is
//! reachable, that the contract in §12.2 is implementable, and that a future
//! change to the SDK which breaks a protocol guarantee fails a build. The
//! project's own record is that a suite nobody has executed is worth about as
//! much as one that does not exist.
//!
//! It is also the worked example. An SDK author in another language has the
//! specification, and this, and nothing else should be needed.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use stepd_sdk::prelude::*;
use uuid::Uuid;

// ---------------------------------------------------------------- blob client

/// The blob client the `conf-blobs` handler uses.
///
/// A process global because the handler is a plain `async fn` with no place to
/// carry one, and because the app is built before the server it talks to has an
/// address. An application would hold an `Arc<Blobs>` and capture it in a
/// closure; this is the shape a fixed-signature reference app is left with.
///
/// Replaceable rather than a bare `OnceLock`: several batteries run in one test
/// process, each against its own server on its own port, and a client fixed by
/// whichever ran first would point every later one at a closed socket.
static BLOBS: OnceLock<Mutex<Option<stepd_sdk::Blobs>>> = OnceLock::new();

fn blob_client() -> Option<stepd_sdk::Blobs> {
    BLOBS.get()?.lock().unwrap().clone()
}

/// Point the reference app's blob client at a server.
///
/// Called by the runner once its API socket is bound. Without it `conf-blobs`
/// fails loudly rather than silently skipping, which is the behaviour the suite
/// is checking for everywhere else.
pub fn configure_blobs(stepd_url: &str, token: &str) {
    *BLOBS.get_or_init(Default::default).lock().unwrap() =
        Some(stepd_sdk::Blobs::new(stepd_url, token));
}

// ---------------------------------------------------------------- effect log

/// What each run's handler actually executed, in order.
///
/// The whole reason §12.1 requires the effect-log endpoint: "the step body did
/// not run a second time" is invisible in server state, because the journal
/// after one execution and after two is byte-identical. Only the app can report
/// it.
fn log() -> &'static Mutex<HashMap<Uuid, Vec<String>>> {
    static LOG: OnceLock<Mutex<HashMap<Uuid, Vec<String>>>> = OnceLock::new();
    LOG.get_or_init(Default::default)
}

fn record(run: Uuid, effect: impl Into<String>) {
    log()
        .lock()
        .unwrap()
        .entry(run)
        .or_default()
        .push(effect.into());
}

/// Effects recorded for a run.
pub fn effects_of(run: Uuid) -> Vec<String> {
    log().lock().unwrap().get(&run).cloned().unwrap_or_default()
}

/// Clear every log.
pub fn reset_effects() {
    log().lock().unwrap().clear();
}

// ---------------------------------------------------------------- handlers

/// `conf-memoize`: a step, a forced retry, another step.
///
/// The assertion is that `work` appears once in the effect log across two
/// attempts. Branching on `ctx.attempt()` rather than racing a timeout is what
/// makes the case deterministic — and it is only possible because the SDK now
/// surfaces the attempt number the protocol has always sent (§4).
async fn memoize(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let a: String = ctx
        .step("work", || async move {
            record(run, "work");
            Ok("did-the-work".to_string())
        })
        .await?;

    if ctx.attempt() == 1 {
        return Err(StepError::retryable("conformance: forced retry"));
    }

    let b: String = ctx
        .step("after", || async move {
            record(run, "after");
            Ok("after".to_string())
        })
        .await?;

    Ok(serde_json::json!({ "work": a, "after": b }))
}

/// `conf-loops`: five generated ids with a forced retry part-way.
///
/// The point is that the *hash sequence* is identical across attempts, which is
/// what makes a loop replayable at all. A discriminator in the id is what makes
/// it so — the occurrence counter alone would tie the hash to iteration order.
async fn loops(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let mut out = Vec::new();
    for i in 0..5 {
        let id = format!("item-{i}");
        let v: i64 = ctx
            .step(&id, || async move {
                record(run, format!("item-{i}"));
                Ok(i as i64)
            })
            .await?;
        out.push(v);

        if i == 2 && ctx.attempt() == 1 {
            return Err(StepError::retryable("conformance: forced retry mid-loop"));
        }
    }
    Ok(serde_json::json!(out))
}

/// `conf-order`: three steps whose program order the journal must reflect.
async fn order(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    for id in ["a", "b", "c"] {
        ctx.step::<String, _, _>(id, || async move {
            record(run, id);
            Ok(id.to_string())
        })
        .await?;
    }
    Ok(serde_json::json!("ordered"))
}

/// `conf-ambiguous`: the same id twice inside one parallel group.
///
/// Reachable at run time, unlike the off-path claim: the group check is a
/// runtime check because two identical ids in one `join` is a program that
/// compiles perfectly well and means something ambiguous.
async fn ambiguous(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let a = ctx.step::<i64, _, _>("dup", || async move {
        record(run, "dup-a");
        Ok(1)
    });
    let b = ctx.step::<i64, _, _>("dup", || async move {
        record(run, "dup-b");
        Ok(2)
    });
    let (x, y) = ctx.join((a, b)).await?;
    Ok(serde_json::json!([x, y]))
}

/// `conf-parallel`: three steps in one envelope.
async fn parallel(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let a = ctx.step::<i64, _, _>("p-a", || async move {
        record(run, "p-a");
        Ok(1)
    });
    let b = ctx.step::<i64, _, _>("p-b", || async move {
        record(run, "p-b");
        Ok(2)
    });
    let c = ctx.step::<i64, _, _>("p-c", || async move {
        record(run, "p-c");
        Ok(3)
    });
    let (x, y, z) = ctx.join3((a, b, c)).await?;
    Ok(serde_json::json!([x, y, z]))
}

/// `conf-parallel-partial`: a batch where one member fails.
///
/// The property the retired join policies were gesturing at, stated as the one
/// rule that actually holds: a failing member neither cancels its siblings nor
/// hides their outcomes. Every body runs, every outcome is recorded, and the
/// handler is the thing that decides what a failure means.
async fn parallel_partial(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let a = ctx.step::<i64, _, _>("q-ok-1", || async move {
        record(run, "q-ok-1");
        Ok(1)
    });
    let b = ctx.step::<i64, _, _>("q-bad", || async move {
        record(run, "q-bad");
        Err(StepError::fatal_coded(
            "conformance_member_failed",
            "this member fails on purpose",
        ))
    });
    let c = ctx.step::<i64, _, _>("q-ok-2", || async move {
        record(run, "q-ok-2");
        Ok(3)
    });

    // The group surfaces the failure and the handler propagates it, which is
    // the only thing an app is ever asked to do. Everything the runner checks
    // happens on the way out: the siblings' results and the failing member's
    // outcome all reach the journal in the envelope that carries the error.
    let (x, y, z) = ctx.join3((a, b, c)).await?;
    Ok(serde_json::json!({ "unexpected_success": [x, y, z] }))
}

/// `conf-sleep`: a step, a sleep, a step.
async fn sleeper(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    ctx.step::<String, _, _>("before", || async move {
        record(run, "before");
        Ok("before".into())
    })
    .await?;

    ctx.sleep("nap", chrono::Duration::seconds(2)).await?;

    ctx.step::<String, _, _>("after", || async move {
        record(run, "after");
        Ok("after".into())
    })
    .await?;
    Ok(serde_json::json!("slept"))
}

/// `conf-wait`: waits for `conf.signal` and returns the token it carried.
async fn waiter(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let got: Option<serde_json::Value> = ctx
        .wait_event("await-signal", "conf.signal")
        .timeout(chrono::Duration::seconds(10))
        .await?;
    Ok(serde_json::json!({ "token": got.map(|v| v["token"].clone()) }))
}

/// `conf-wait-timeout`: waits for an event that never comes.
async fn wait_timeout(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let got: Option<serde_json::Value> = ctx
        .wait_event("never", "conf.never")
        .timeout(chrono::Duration::seconds(2))
        .await?;
    Ok(serde_json::json!({ "timed_out": got.is_none() }))
}

/// `conf-early-signal`: a step first, so the runner can deliver before the wait.
///
/// The case this exists for is the one that used to lose events entirely: the
/// signal arrives while the handler is still working and the wait has not been
/// registered. The durable inbox is what makes it resolve anyway.
async fn early_signal(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    ctx.step::<String, _, _>("settle", || async move {
        record(run, "settle");
        Ok("settled".into())
    })
    .await?;

    let got: Option<serde_json::Value> = ctx
        .wait_event("await-early", "conf.signal")
        .timeout(chrono::Duration::seconds(10))
        .await?;
    Ok(serde_json::json!({ "token": got.map(|v| v["token"].clone()) }))
}

/// `conf-invoke`: calls a child and returns its result.
async fn invoker(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let out: serde_json::Value = ctx
        .invoke("child", "conf-invoke-child", serde_json::json!({ "n": 7 }))
        .await?;
    Ok(serde_json::json!({ "child": out }))
}

/// `conf-invoke-child`: echoes its input after one step.
async fn invoke_child(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let input = ctx.run().input.unwrap_or(serde_json::Value::Null);
    let v: serde_json::Value = ctx
        .step("child-work", || async move {
            record(run, "child-work");
            Ok(input)
        })
        .await?;
    Ok(v)
}

/// `conf-cascade`: an attached child, a detached child, then a wait.
///
/// Cancelling the parent must cancel the first and leave the second running.
/// The asymmetry is the whole point of `detach`, and it is the kind of rule that
/// is easy to implement in one direction and forget in the other.
async fn cascade(ctx: &Ctx) -> StepResult<serde_json::Value> {
    // The detached one first, and awaited: a detached invoke records as complete
    // immediately, so the pass resumes on the next attempt with it memoised.
    // Doing it the other way round would park on the attached child and the
    // detached one would never be created.
    let detached: serde_json::Value = ctx
        .invoke("detached", "conf-cascade-detached", serde_json::json!({}))
        .detach()
        .await?;

    // This one parks the parent until the child finishes — which it will not,
    // within the case, because the child sleeps for thirty seconds. That is the
    // state the runner needs: a parent with one attached and one detached child
    // both live, so cancelling it tests both halves of the rule at once.
    let attached: serde_json::Value = ctx
        .invoke("attached", "conf-cascade-child", serde_json::json!({}))
        .await?;

    Ok(serde_json::json!({ "attached": attached, "detached": detached }))
}

/// `conf-cascade-child` and `conf-cascade-detached`: long enough to still be
/// alive when the parent is cancelled.
async fn cascade_child(ctx: &Ctx) -> StepResult<serde_json::Value> {
    ctx.sleep("long", chrono::Duration::seconds(30)).await?;
    Ok(serde_json::json!("finished"))
}

/// `conf-continue`: three ticks across a `continue_as_new` chain.
async fn continuer(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let n = ctx
        .run()
        .input
        .as_ref()
        .and_then(|v| v["n"].as_i64())
        .unwrap_or(0);

    ctx.step::<i64, _, _>("tick", || async move {
        record(run, format!("tick-{n}"));
        Ok(n)
    })
    .await?;

    if n < 2 {
        return Err(ctx.continue_as_new("go-again", serde_json::json!({ "n": n + 1 })));
    }
    Ok(serde_json::json!({ "ticks": n + 1 }))
}

/// `conf-cancel`: works, then waits; its compensation path runs once.
async fn cancellable(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;

    // Protocol §7.4: a cancelled run still gets one attempt so the handler can
    // compensate, and it is told which mode it is in.
    if ctx.run().cancelling {
        ctx.step::<String, _, _>("compensate", || async move {
            record(run, "cancelled");
            Ok("compensated".into())
        })
        .await?;
        return Ok(serde_json::json!("compensated"));
    }

    ctx.step::<String, _, _>("work", || async move {
        record(run, "work");
        Ok("worked".into())
    })
    .await?;

    let _: Option<serde_json::Value> = ctx
        .wait_event("park", "conf.signal")
        .timeout(chrono::Duration::seconds(60))
        .await?;
    Ok(serde_json::json!("finished"))
}

/// `conf-errors-retryable`: fails twice, succeeds on the third attempt.
async fn errors_retryable(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    record(run, format!("attempt-{}", ctx.attempt()));
    if ctx.attempt() < 3 {
        return Err(StepError::retryable("conformance: retry me"));
    }
    Ok(serde_json::json!({ "attempts": ctx.attempt() }))
}

/// `conf-errors-terminal`: fails non-retryably the first time.
async fn errors_terminal(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    record(run, format!("attempt-{}", ctx.attempt()));
    Err(StepError::fatal_coded(
        "conformance_terminal",
        "this must not be retried",
    ))
}

/// `conf-abandon`: on the first attempt, runs a step and never answers.
///
/// The hazard behind §7.1.1: an SDK that dropped the running future to meet its
/// deadline would leave the step's external effect in an indeterminate state.
/// The rule is to abandon the *response* instead — the step finishes, the server
/// hears nothing, and the attempt is retried.
async fn abandon(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let attempt = ctx.attempt();

    // The hang is INSIDE the step body, which is the only place it demonstrates
    // anything. A hang after the step would prove nothing: the SDK halts the
    // pass as soon as a new step is recorded, so the code after it does not run
    // on that attempt, the envelope commits normally, and the step is memoised
    // on the next one. The case has to abandon a response for work that has
    // already happened.
    ctx.step::<String, _, _>("slow", move || async move {
        record(run, format!("slow-{attempt}"));
        if attempt == 1 {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
        Ok("slow".into())
    })
    .await?;

    Ok(serde_json::json!("finished"))
}

/// `conf-refs`: an external reference passes through untouched.
///
/// §8.3: the server must never dereference a `$ref`. It is a pointer to
/// something in the app's own storage, and a server that fetched it would be
/// reaching into a system it has no credentials for and no business in.
async fn refs(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let value = serde_json::json!({ "$ref": "s3://conformance/opaque-object" });
    let out: serde_json::Value = ctx.step("passthrough", || async move { Ok(value) }).await?;
    Ok(out)
}

/// `conf-blobs`: a payload larger than the inline limit, round-tripped.
///
/// Exercises every clause of §8.3 the suite can observe: two-phase upload,
/// content addressing, lazy read, `Range`, and — through the second, identical
/// upload — that a duplicate digest skips the transfer entirely.
async fn blobs_case(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let client = blob_client().ok_or_else(|| {
        StepError::fatal("the conformance app's blob client was never configured")
    })?;

    // Deterministic content, and large enough that no inline path could carry
    // it. Random bytes would make the dedupe assertion below untestable, since
    // the second upload would have a different digest.
    let payload: Vec<u8> = (0..64_000u32).map(|i| (i % 251) as u8).collect();

    let first: stepd_sdk::Blob = ctx
        .step("upload", || async {
            record(run, "upload");
            client
                .put(run, &payload)
                .content_type("application/octet-stream")
                .filename("conformance.bin")
                .await
                .map_err(|e| StepError::fatal(e.to_string()))
        })
        .await?;

    // A second upload of identical bytes. Content addressing means the server
    // returns the existing id and the app skips the transfer — which is what
    // makes replaying an event or retrying a run cheap rather than merely
    // correct.
    let second: stepd_sdk::Blob = ctx
        .step("upload-again", || async {
            record(run, "upload-again");
            client
                .put(run, &payload)
                .content_type("application/octet-stream")
                .await
                .map_err(|e| StepError::fatal(e.to_string()))
        })
        .await?;

    // Read back, in full and by range. Both go through the URL the server
    // minted for *this* attempt.
    let whole = client
        .read(&first)
        .await
        .map_err(|e| StepError::fatal(format!("full read failed: {e}")))?;
    let head = client
        .read_range(&first, 0, 15)
        .await
        .map_err(|e| StepError::fatal(format!("range read failed: {e}")))?;

    Ok(serde_json::json!({
        "size": first.size(),
        "sha256": first.sha256(),
        "read_len": whole.len(),
        "head_len": head.len(),
        "head_matches": head == payload[0..16],
        "content_matches": whole == payload,
        "deduplicated": second.inner.id == first.inner.id,
    }))
}

/// `conf-truncation`: more steps than the server ships inline.
async fn truncation(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let mut total = 0i64;
    for i in 0..40 {
        let id = format!("t-{i}");
        let v: i64 = ctx
            .step(&id, || async move {
                record(run, format!("t-{i}"));
                Ok(i as i64)
            })
            .await?;
        total += v;
    }
    Ok(serde_json::json!({ "total": total }))
}

/// `conf-cron`: records the occurrence it was fired for.
async fn cron(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    let occurrence = ctx
        .run()
        .input
        .as_ref()
        .and_then(|v| v["cron"]["occurrence_at"].as_str())
        .unwrap_or("missing")
        .to_string();

    let out: String = ctx
        .step("tick", move || {
            let occurrence = occurrence.clone();
            async move {
                record(run, "tick");
                Ok(occurrence)
            }
        })
        .await?;
    Ok(serde_json::json!({ "occurrence_at": out }))
}

/// `conf-fencing`: one step, then done.
async fn fencing(ctx: &Ctx) -> StepResult<serde_json::Value> {
    let run = ctx.run().id;
    ctx.step::<String, _, _>("work", || async move {
        record(run, "work");
        Ok("worked".into())
    })
    .await?;
    Ok(serde_json::json!("done"))
}

// ---------------------------------------------------------------- assembly

/// Suites this app implements, for the conformance manifest (§12.1).
///
/// `determinism` is here even though `conf-offpath` is absent, because the Rust
/// SDK makes that hazard unrepresentable rather than detectable — see
/// [`STATICALLY_PREVENTED`].
pub const SUITES: &[&str] = &[
    "memoization",
    "loops",
    "determinism",
    "parallel",
    "sleep",
    "wait",
    "early_signal",
    "invoke",
    "cascade",
    "continue_as_new",
    "errors",
    "cancel",
    "abandonment",
    "blobs",
    "refs",
    "fencing",
    "signature",
    "truncation",
    "cron",
];

/// Hazards this SDK makes unrepresentable (§12.1).
///
/// `Ctx` is `!Send` and `!Sync`, so a handler that tries to claim a step from a
/// spawned task does not compile. There is no runtime failure to demonstrate,
/// because there is no such program.
pub const STATICALLY_PREVENTED: &[&str] = &["offpath_claim"];

/// Build the app under test.
pub fn app(url: &str, signing_key: Vec<u8>) -> App {
    App::new("stepd-conformance", url)
        .signing_key(signing_key)
        .function(
            Function::new("conf-memoize")
                .on_event("conf.memoize")
                .run(memoize),
        )
        .function(
            Function::new("conf-loops")
                .on_event("conf.loops")
                .run(loops),
        )
        .function(
            Function::new("conf-order")
                .on_event("conf.order")
                .run(order),
        )
        .function(
            Function::new("conf-ambiguous")
                .on_event("conf.ambiguous")
                .run(ambiguous),
        )
        .function(
            Function::new("conf-parallel")
                .on_event("conf.parallel")
                .run(parallel),
        )
        .function(
            Function::new("conf-parallel-partial")
                .on_event("conf.parallel_partial")
                .run(parallel_partial),
        )
        .function(
            Function::new("conf-sleep")
                .on_event("conf.sleep")
                .run(sleeper),
        )
        .function(Function::new("conf-wait").on_event("conf.wait").run(waiter))
        .function(
            Function::new("conf-wait-timeout")
                .on_event("conf.wait_timeout")
                .run(wait_timeout),
        )
        .function(
            Function::new("conf-early-signal")
                .on_event("conf.early_signal")
                .run(early_signal),
        )
        .function(
            Function::new("conf-invoke")
                .on_event("conf.invoke")
                .run(invoker),
        )
        .function(
            Function::new("conf-invoke-child")
                .on_invoke()
                .run(invoke_child),
        )
        .function(
            Function::new("conf-cascade")
                .on_event("conf.cascade")
                .run(cascade),
        )
        .function(
            Function::new("conf-cascade-child")
                .on_invoke()
                .run(cascade_child),
        )
        .function(
            Function::new("conf-cascade-detached")
                .on_invoke()
                .run(cascade_child),
        )
        .function(
            Function::new("conf-continue")
                .on_event("conf.continue")
                .key("'conf-continue'")
                .run(continuer),
        )
        .function(
            Function::new("conf-cancel")
                .on_event("conf.cancel")
                .on_cancel()
                .run(cancellable),
        )
        .function(
            Function::new("conf-errors-retryable")
                .on_event("conf.errors_retryable")
                .run(errors_retryable),
        )
        .function(
            Function::new("conf-errors-terminal")
                .on_event("conf.errors_terminal")
                .run(errors_terminal),
        )
        .function(
            Function::new("conf-abandon")
                .on_event("conf.abandon")
                .run(abandon),
        )
        .function(Function::new("conf-refs").on_event("conf.refs").run(refs))
        .function(
            Function::new("conf-blobs")
                .on_event("conf.blobs")
                .run(blobs_case),
        )
        .function(
            Function::new("conf-truncation")
                .on_event("conf.truncation")
                .run(truncation),
        )
        .function(
            Function::new("conf-fencing")
                .on_event("conf.fencing")
                .run(fencing),
        )
        .function(
            Function::new("conf-cron")
                .on_cron("*/5 * * * *", "UTC")
                .run(cron),
        )
}

/// The app's HTTP surface, including the conformance probe of §12.1.
///
/// The probe routes are added here rather than in `App::router` on purpose. They
/// take no authentication and expose execution detail, and an SDK that made them
/// a default route would ship a debug endpoint on every production app anyone
/// ever wrote with it.
pub fn router(app: App) -> axum::Router {
    use axum::extract::Query;
    use axum::routing::{get, post};

    #[derive(Deserialize)]
    struct RunQuery {
        run: Uuid,
    }

    app.router()
        .route(
            "/_conformance/effects",
            get(|Query(q): Query<RunQuery>| async move {
                axum::Json(serde_json::json!({ "effects": effects_of(q.run) }))
            }),
        )
        .route(
            "/_conformance/reset",
            post(|| async {
                reset_effects();
                axum::Json(serde_json::json!({ "reset": true }))
            }),
        )
        .route(
            "/.well-known/stepd-conformance",
            get(|| async {
                axum::Json(serde_json::json!({
                    "protocol": stepd_proto::PROTOCOL_VERSION,
                    "sdk": concat!("rust/", env!("CARGO_PKG_VERSION")),
                    "suites": SUITES,
                    "statically_prevented": STATICALLY_PREVENTED,
                }))
            }),
        )
}
