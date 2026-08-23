//! Level 2 (protocol §12): everything beyond the gate.
//!
//! `early_signal` is the level-2 gate for the same reason `determinism` is the
//! level-1 one — it is the other failure mode that corrupts quietly. An event
//! that arrives before the wait is registered and is dropped produces a run that
//! waits forever, and "waiting" is indistinguishable from "not finished yet"
//! until someone goes looking.

use super::record;
use crate::harness::Harness;
use crate::report::Report;
use anyhow::Result;
use stepd_proto::RunStatus;

/// `parallel`: several ops in one envelope, committed atomically.
pub async fn parallel(h: &Harness, report: &mut Report) {
    record(
        report,
        "parallel",
        "a parallel group commits atomically and each member runs once",
        parallel_case(h).await,
    );
    record(
        report,
        "parallel",
        "a failing member neither cancels its siblings nor hides their outcomes",
        parallel_partial_case(h).await,
    );
}

/// The rule that replaced the `all` and `any` join policies (ADR-023).
///
/// Those were defined in terms of cancelling siblings, and by the time a batch
/// reaches the server every body has already run — so there is nothing to
/// cancel and the only honest thing to do is record what happened. This asserts
/// that: every member executed, every outcome is in the journal, and the
/// successful siblings were not thrown away because one of them failed.
async fn parallel_partial_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h
        .fire("conf.parallel_partial", serde_json::json!({}))
        .await?;
    h.settle(run).await?;

    let effects = h.effects(run).await?;
    for id in ["q-ok-1", "q-bad", "q-ok-2"] {
        if effects.iter().filter(|e| *e == id).count() != 1 {
            return Ok(Err(format!(
                "member '{id}' executed {} times, expected once. A member skipped \
                 because a sibling failed is work the batch was asked for and did not \
                 do; one executed twice is work recorded and repeated. Effects: {effects:?}",
                effects.iter().filter(|e| *e == id).count()
            )));
        }
    }

    let journal = h.journal(run).await?;
    for (id, want) in [
        ("q-ok-1", "completed"),
        ("q-bad", "failed"),
        ("q-ok-2", "completed"),
    ] {
        match journal.iter().find(|(s, _, _)| s == id) {
            None => {
                return Ok(Err(format!(
                    "member '{id}' is missing from the journal. Its body ran, so a \
                     missing record means the engine executed work and forgot it — the \
                     one thing a durable engine must never do"
                )))
            }
            Some((_, _, status)) if status != want => {
                return Ok(Err(format!(
                    "member '{id}' is recorded as '{status}', expected '{want}'; a \
                     sibling's outcome must not be rewritten by another member's failure"
                )))
            }
            _ => {}
        }
    }
    Ok(Ok(()))
}

async fn parallel_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.parallel", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }

    let journal = h.journal(run).await?;
    for id in ["p-a", "p-b", "p-c"] {
        if !journal.iter().any(|(s, _, _)| s == id) {
            return Ok(Err(format!("step '{id}' is missing from the journal")));
        }
    }

    // If the group were committed one op at a time, a crash between two of them
    // would leave the batch half-recorded — which is what "atomic" is protecting
    // against, and it is invisible on a happy path.
    let effects = h.effects(run).await?;
    let mut sorted = effects.clone();
    sorted.sort();
    if sorted != vec!["p-a", "p-b", "p-c"] {
        return Ok(Err(format!(
            "expected each member to execute exactly once; got {effects:?}"
        )));
    }
    Ok(Ok(()))
}

/// `wait`: a match resolves, and a timeout resolves to `null`.
pub async fn wait(h: &Harness, report: &mut Report) {
    record(
        report,
        "wait",
        "a matching event resolves the wait and binds its payload",
        wait_match(h).await,
    );
    record(
        report,
        "wait",
        "a wait that times out resolves to null rather than failing",
        wait_timeout(h).await,
    );
}

async fn wait_match(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.wait", serde_json::json!({})).await?;

    // Let it reach the wait first. This case is about the ordinary path; the
    // other order is `early_signal`, and conflating them would mean a failure
    // could not tell you which of the two broke.
    h.settle_until(run, |s| s == RunStatus::Waiting).await?;
    h.signal(run, "conf.signal", serde_json::json!({ "token": "abc123" }))
        .await?;

    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }
    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);
    if out["token"] != serde_json::json!("abc123") {
        return Ok(Err(format!(
            "the wait resolved but the payload did not reach the handler: {out}"
        )));
    }
    Ok(Ok(()))
}

async fn wait_timeout(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.wait_timeout", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!(
            "the run ended {status:?}; a wait that times out is not an error — the \
             handler is meant to decide what a missing approval means"
        )));
    }
    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);
    if out["timed_out"] != serde_json::json!(true) {
        return Ok(Err(format!(
            "the handler did not see a null result from the timed-out wait: {out}"
        )));
    }
    Ok(Ok(()))
}

/// `early_signal`: an event delivered before the wait exists still resolves it.
pub async fn early_signal(h: &Harness, report: &mut Report) {
    record(
        report,
        "early_signal",
        "an event delivered before the wait is registered still resolves it",
        early_signal_case(h).await,
    );
}

async fn early_signal_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.early_signal", serde_json::json!({})).await?;

    // Deliver immediately, without driving the run to its wait. The handler has
    // not reached `wait_event` yet, so there is nothing for the event to match —
    // and the durable inbox is what stops it being dropped on the floor.
    h.signal(run, "conf.signal", serde_json::json!({ "token": "early" }))
        .await?;

    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!(
            "the run ended {status:?}; an event delivered before the wait was registered \
             was lost, so the run waits forever — which looks exactly like a run that is \
             merely slow (§7.6)"
        )));
    }
    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);
    if out["token"] != serde_json::json!("early") {
        return Ok(Err(format!(
            "the run completed but the early event's payload did not reach it: {out}"
        )));
    }
    Ok(Ok(()))
}

/// `invoke`: a child run, and its result memoised into the parent.
pub async fn invoke(h: &Harness, report: &mut Report) {
    record(
        report,
        "invoke",
        "a child run's result resolves the parent's step",
        invoke_case(h).await,
    );
}

async fn invoke_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.invoke", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the parent ended {status:?}, not completed")));
    }

    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);
    if out["child"]["n"] != serde_json::json!(7) {
        return Ok(Err(format!(
            "the child's result did not reach the parent: {out}"
        )));
    }

    let children: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE parent_run_id = $1")
        .bind(run)
        .fetch_one(h.server.state.store.pool())
        .await?;
    if children != 1 {
        return Ok(Err(format!(
            "{children} child runs were created; the parent invokes once, and a second \
             child means the invoke was re-executed after being recorded"
        )));
    }
    Ok(Ok(()))
}

/// `cascade`: cancelling a parent cancels attached children and spares detached.
pub async fn cascade(h: &Harness, report: &mut Report) {
    record(
        report,
        "cascade",
        "cancelling a parent cancels attached children and leaves detached ones running",
        cascade_case(h).await,
    );
}

async fn cascade_case(h: &Harness) -> Result<Result<(), String>> {
    use stepd_core::traits::StateStore;

    let run = h.fire("conf.cascade", serde_json::json!({})).await?;

    // Wait until both children exist and the parent is parked on the attached
    // one. Cancelling earlier would test nothing — there would be no descendant
    // for the cascade to reach.
    let deadline = std::time::Instant::now() + h.options.case_timeout;
    loop {
        h.tick().await?;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE parent_run_id = $1")
            .bind(run)
            .fetch_one(h.server.state.store.pool())
            .await?;
        if n >= 2 {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Ok(Err(format!(
                "only {n} child run(s) were created; the case needs an attached and a \
                 detached one both live"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.server.state.store.cancel_run(&h.namespace, run).await?;
    for _ in 0..40 {
        h.tick().await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let rows: Vec<(String, bool, String)> =
        sqlx::query_as("SELECT fn_id, detached, status::text FROM runs WHERE parent_run_id = $1")
            .bind(run)
            .fetch_all(h.server.state.store.pool())
            .await?;

    match rows.iter().find(|(_, d, _)| !*d) {
        Some((_, _, s)) if s == "cancelled" => {}
        Some((f, _, s)) => {
            return Ok(Err(format!(
                "the attached child '{f}' is {s}, not cancelled; a cancelled parent that \
                 leaves live descendants is how work outlives the thing that asked for \
                 it (§7.5)"
            )))
        }
        None => return Ok(Err("no attached child was created".into())),
    }

    match rows.iter().find(|(_, d, _)| *d) {
        Some((_, _, s)) if s != "cancelled" => {}
        Some((f, _, _)) => {
            return Ok(Err(format!(
                "the detached child '{f}' was cancelled; `detach` means the child's \
                 lifecycle is independent in both directions, and cancelling it anyway \
                 makes the flag mean nothing"
            )))
        }
        None => return Ok(Err("no detached child was created".into())),
    }
    Ok(Ok(()))
}

/// `continue_as_new`: the successor keeps the key and lineage, with empty state.
pub async fn continue_as_new(h: &Harness, report: &mut Report) {
    record(
        report,
        "continue_as_new",
        "the successor keeps key and lineage, starts empty, and nothing interleaves",
        continue_case(h).await,
    );
}

async fn continue_case(h: &Harness) -> Result<Result<(), String>> {
    let first = h
        .fire_keyed(
            "conf.continue",
            serde_json::json!({ "n": 0 }),
            Some("conf-continue".into()),
        )
        .await?;

    let deadline = std::time::Instant::now() + h.options.case_timeout;
    loop {
        h.tick().await?;
        // Wait for a run that completed *without* continuing — the end of the
        // chain. Waiting for "any completed run" stops at the first link, which
        // completes the moment it hands over, and the case would then assert
        // about a chain that is still growing.
        let done: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runs r JOIN runs f ON f.id = $1
              WHERE r.lineage_id = f.lineage_id AND r.status = 'completed'
                AND r.chain_position = 2",
        )
        .bind(first)
        .fetch_one(h.server.state.store.pool())
        .await?;
        if done > 0 {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Ok(Err("no run in the chain ever completed".into()));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let chain: Vec<(uuid::Uuid, i32, Option<String>)> = sqlx::query_as(
        "SELECT r.id, r.chain_position, r.key FROM runs r JOIN runs f ON f.id = $1
          WHERE r.lineage_id = f.lineage_id ORDER BY r.chain_position",
    )
    .bind(first)
    .fetch_all(h.server.state.store.pool())
    .await?;

    if chain.len() != 3 {
        return Ok(Err(format!(
            "the chain has {} run(s); the app continues twice and then completes",
            chain.len()
        )));
    }
    for (i, (_, pos, key)) in chain.iter().enumerate() {
        if *pos != i as i32 {
            return Ok(Err(format!(
                "chain position {pos} at index {i}; positions must be contiguous, or a \
                 gap is indistinguishable from a lost successor"
            )));
        }
        if key.as_deref() != Some("conf-continue") {
            return Ok(Err(format!(
                "the run at position {pos} carries key {key:?}; the successor must keep \
                 the key, or another run can start on it mid-chain"
            )));
        }
    }

    // Each successor starts with an empty journal, so `tick` executes once per
    // link rather than being memoised across the chain — which is the point of
    // continuing rather than looping.
    let mut ticks = Vec::new();
    for (id, ..) in &chain {
        ticks.extend(h.effects(*id).await?);
    }
    for n in 0..3 {
        let want = format!("tick-{n}");
        if ticks.iter().filter(|t| **t == want).count() != 1 {
            return Ok(Err(format!(
                "expected '{want}' exactly once across the chain; got {ticks:?}"
            )));
        }
    }
    Ok(Ok(()))
}

/// `cancel`: the compensation path runs exactly once.
pub async fn cancel(h: &Harness, report: &mut Report) {
    record(
        report,
        "cancel",
        "the on_cancel path executes exactly once",
        cancel_case(h).await,
    );
}

async fn cancel_case(h: &Harness) -> Result<Result<(), String>> {
    use stepd_core::traits::StateStore;

    let run = h.fire("conf.cancel", serde_json::json!({})).await?;
    h.settle_until(run, |s| s == RunStatus::Waiting).await?;

    h.server.state.store.cancel_run(&h.namespace, run).await?;

    // Keep driving: `cancelled` is where the run ends up, not where it goes
    // first. Stopping at the status would mean asserting about the compensation
    // phase before it had been dispatched — and the case would pass against an
    // engine that never ran it.
    let deadline = std::time::Instant::now() + h.options.case_timeout;
    while std::time::Instant::now() < deadline {
        h.tick().await?;
        let compensating: bool = sqlx::query_scalar("SELECT compensating FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(h.server.state.store.pool())
            .await?;
        if !compensating {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }

    // §7.4: a cancelled run gets a compensation phase so it can undo. Running
    // that path twice double-refunds; not running it leaks whatever the run had
    // reserved. Exactly once is the only acceptable answer.
    let effects = h.effects(run).await?;
    let n = effects.iter().filter(|e| *e == "cancelled").count();
    if n != 1 {
        return Ok(Err(format!(
            "the compensation path ran {n} times; expected exactly once. Effects: {effects:?}"
        )));
    }
    Ok(Ok(()))
}

/// `cron`: a registered schedule fires and the run knows its occurrence.
pub async fn cron(h: &Harness, report: &mut Report) {
    record(
        report,
        "cron",
        "a registered cron trigger produces a schedule that fires",
        cron_case(h).await,
    );
}

async fn cron_case(h: &Harness) -> Result<Result<(), String>> {
    use stepd_core::traits::CronStore;

    let schedules: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cron_schedules WHERE ns = $1 AND fn_id = 'conf-cron'",
    )
    .bind(&h.namespace)
    .fetch_one(h.server.state.store.pool())
    .await?;
    if schedules != 1 {
        return Ok(Err(format!(
            "registering a cron trigger produced {schedules} schedules; a cron function \
             that registers cleanly and never fires is the worst shape of missing"
        )));
    }

    sqlx::query(
        "UPDATE cron_schedules SET next_fire_at = now() - interval '10 seconds' WHERE ns = $1",
    )
    .bind(&h.namespace)
    .execute(h.server.state.store.pool())
    .await?;

    let swept = h
        .server
        .state
        .store
        .sweep_namespace(&h.namespace, 10)
        .await?;
    if swept.fired != 1 {
        return Ok(Err(format!(
            "the due schedule fired {} run(s), expected 1",
            swept.fired
        )));
    }

    let run: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM runs WHERE ns = $1 AND fn_id = 'conf-cron'")
            .bind(&h.namespace)
            .fetch_one(h.server.state.store.pool())
            .await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the cron run ended {status:?}, not completed")));
    }

    // §3.1 accepts that `started_at` is the recovery time, so the occurrence has
    // to reach the handler by another route — otherwise a catch-up fire computes
    // the wrong window with nothing to notice it by.
    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);
    if out["occurrence_at"] == serde_json::json!("missing") {
        return Ok(Err(
            "the handler could not read the occurrence it was fired for".into(),
        ));
    }
    Ok(Ok(()))
}

/// `fencing`: a stale fence is rejected and changes nothing.
pub async fn fencing(h: &Harness, report: &mut Report) {
    record(
        report,
        "fencing",
        "a commit carrying a stale fence is rejected and records nothing",
        fencing_case(h).await,
    );
}

async fn fencing_case(h: &Harness) -> Result<Result<(), String>> {
    use stepd_core::traits::OpCommit;
    use stepd_core::traits::{CommitOutcome, StateStore};
    use stepd_proto::Op;

    let run = h.fire("conf.fencing", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }

    let fence: i64 = sqlx::query_scalar("SELECT fence_token FROM runs WHERE id = $1")
        .bind(run)
        .fetch_one(h.server.state.store.pool())
        .await?;

    // A response from an attempt that was superseded. Accepting it would let a
    // worker everyone has given up on write into a run that moved on without it,
    // which is the whole reason a fence exists (§7.3).
    let outcome = h
        .server
        .state
        .store
        .commit(
            run,
            fence - 1,
            OpCommit {
                ops: vec![Op::Step {
                    id: "ghost".into(),
                    hash: "0123456789abcdef".into(),
                    data: Some(serde_json::json!("from a stale attempt")),
                    meta: None,
                    error: None,
                }],
                emit: vec![],
            },
        )
        .await?;

    if matches!(outcome, CommitOutcome::Committed) {
        return Ok(Err(
            "a commit carrying a stale fence was accepted; a superseded attempt can then \
             overwrite the work of the one that replaced it (§7.3)"
                .into(),
        ));
    }

    if h.journal(run)
        .await?
        .into_iter()
        .any(|(id, _, _)| id == "ghost")
    {
        return Ok(Err(
            "the stale commit was rejected but its step was recorded anyway".into(),
        ));
    }
    Ok(Ok(()))
}

/// `refs`: a `$ref` passes through and is never dereferenced.
pub async fn refs(h: &Harness, report: &mut Report) {
    record(
        report,
        "refs",
        "a $ref value passes through the server untouched",
        refs_case(h).await,
    );
}

async fn refs_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.refs", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }
    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);

    // The server stores the pointer and nothing else. Dereferencing it would mean
    // reaching into storage it has no credentials for and no business in — and
    // the app chose a `$ref` precisely to keep that payload out of the engine.
    if out["$ref"] != serde_json::json!("s3://conformance/opaque-object") {
        return Ok(Err(format!(
            "the $ref did not survive the round trip unchanged: {out}"
        )));
    }
    Ok(Ok(()))
}

/// `truncation`: a journal too large to ship inline is paged correctly.
pub async fn truncation(h: &Harness, report: &mut Report) {
    record(
        report,
        "truncation",
        "a journal larger than the inline limit replays without re-executing",
        truncation_case(h).await,
    );
}

async fn truncation_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.truncation", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        return Ok(Err(format!("the run ended {status:?}, not completed")));
    }

    let journal = h.journal(run).await?;
    if journal.len() < 40 {
        return Ok(Err(format!(
            "only {} steps were recorded; the case needs 40",
            journal.len()
        )));
    }

    // The failure this guards is subtle and expensive: a truncated journal the
    // SDK does not page means the handler cannot see steps that *are* recorded,
    // so it re-executes them. Every side effect happens twice and the run still
    // completes successfully (§8.6).
    let effects = h.effects(run).await?;
    for i in 0..40 {
        let want = format!("t-{i}");
        let n = effects.iter().filter(|e| **e == want).count();
        if n != 1 {
            return Ok(Err(format!(
                "'{want}' executed {n} times; a step the SDK could not see in a truncated \
                 journal re-executes, and the run still completes (§8.6)"
            )));
        }
    }
    Ok(Ok(()))
}

/// `blobs`: two-phase upload, content addressing, lazy read, `Range`.
pub async fn blobs(h: &Harness, report: &mut Report) {
    record(
        report,
        "blobs",
        "a large payload round-trips, dedupes, and reads by range",
        blobs_case(h).await,
    );
    record(
        report,
        "blobs",
        "a blob referenced by a live run is not collected",
        blob_refs_case(h).await,
    );
    record(
        report,
        "blobs",
        "content that does not match its declared digest is refused",
        digest_mismatch(h).await,
    );
}

async fn blobs_case(h: &Harness) -> Result<Result<(), String>> {
    let run = h.fire("conf.blobs", serde_json::json!({})).await?;
    let status = h.settle(run).await?;
    if status != RunStatus::Completed {
        let err = h.error(run).await?.unwrap_or(serde_json::Value::Null);
        return Ok(Err(format!(
            "the run ended {status:?}, not completed: {err}"
        )));
    }

    let out = h.output(run).await?.unwrap_or(serde_json::Value::Null);
    if out["content_matches"] != serde_json::json!(true) {
        return Ok(Err(format!(
            "the bytes read back are not the bytes uploaded: {out}"
        )));
    }
    if out["head_matches"] != serde_json::json!(true) || out["head_len"] != serde_json::json!(16) {
        return Ok(Err(format!(
            "a Range read did not return the requested 16 bytes: {out}. §8.3.3 requires \
             Range so a step can read a file header without pulling the whole object"
        )));
    }
    if out["deduplicated"] != serde_json::json!(true) {
        return Ok(Err(format!(
            "uploading identical bytes a second time produced a different blob: {out}. \
             Content addressing is what makes replaying an event cheap rather than \
             merely correct (§8.3.2)"
        )));
    }
    Ok(Ok(()))
}

async fn blob_refs_case(h: &Harness) -> Result<Result<(), String>> {
    use stepd_core::traits::Housekeeping;

    let run = h.fire("conf.blobs", serde_json::json!({})).await?;
    h.settle(run).await?;

    let blob: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT (s.result -> '$blob' ->> 'id')::uuid FROM run_steps s
          WHERE s.run_id = $1 AND s.step_id = 'upload'",
    )
    .bind(run)
    .fetch_optional(h.server.state.store.pool())
    .await?
    .flatten();

    let Some(blob) = blob else {
        return Ok(Err("the upload step recorded no blob reference".into()));
    };

    // Collect with a cutoff far in the future, so the only thing keeping these
    // bytes alive is the reference. This is the case that was silently broken:
    // nothing wrote `blob_refs`, so a committed blob was collectable the moment
    // it existed, and the run failed on its next replay with a missing object
    // hours after the collection that caused it.
    h.server
        .state
        .store
        .collect_blobs(chrono::Utc::now() + chrono::Duration::days(1))
        .await?;

    let alive: bool = sqlx::query_scalar("SELECT exists(SELECT 1 FROM blobs WHERE id = $1)")
        .bind(blob)
        .fetch_one(h.server.state.store.pool())
        .await?;
    if !alive {
        return Ok(Err(
            "the collector deleted a blob a completed run still references (§8.3.4)".into(),
        ));
    }
    Ok(Ok(()))
}

async fn digest_mismatch(h: &Harness) -> Result<Result<(), String>> {
    use stepd_core::traits::{BlobSpec, BlobStore, Reservation};

    // Straight at the store, because no conforming app can produce this: the SDK
    // hashes what it is about to send. The case exists because a *compromised*
    // upload URL is exactly the thing that can, and verification before the blob
    // becomes readable is what stops it substituting content for a reference
    // already committed into a step result.
    let Some(blobs) = h.server.state.blobs.as_ref() else {
        return Ok(Err("the server has managed blobs disabled".into()));
    };

    let claimed = "0".repeat(64);
    let reservation = blobs
        .reserve(
            &h.namespace,
            BlobSpec {
                size: 4,
                sha256: claimed,
                content_type: None,
                filename: None,
            },
        )
        .await?;
    let Reservation::Upload { id, .. } = reservation else {
        return Ok(Err("a fresh digest was reported as already stored".into()));
    };

    blobs.put_bytes(id, b"nope").await?;
    match blobs.commit_blob(id).await {
        Ok(_) => Ok(Err(
            "the server committed a blob whose content does not match its declared \
             digest; a stolen upload URL could then substitute content for a reference \
             already recorded in a step result (§8.3.2)"
                .into(),
        )),
        Err(e) if e.to_string().contains("blob_digest_mismatch") => Ok(Ok(())),
        Err(e) => Ok(Err(format!(
            "the commit was refused, but not as a digest mismatch: {e}"
        ))),
    }
}
