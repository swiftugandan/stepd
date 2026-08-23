//! The suites themselves, one module each, named as protocol §12 names them.

use crate::harness::Harness;
use crate::report::{Report, Status, ALL_SUITES};

mod level1;
mod level2;

/// Run every suite the app declared, and record an honest outcome for the rest.
pub async fn run_all(h: &Harness, report: &mut Report) {
    for suite in ALL_SUITES {
        if !h.options.only.is_empty() && !h.options.only.iter().any(|s| s == suite) {
            continue;
        }
        // The runner's own gaps are reported FIRST, before the app's
        // declaration is consulted. The other order hides them: a suite this
        // battery has not implemented would show as "not declared" whenever no
        // app happens to declare it, and the tool's own hole becomes invisible
        // exactly when nothing else would reveal it.
        if let Some(why) = UNIMPLEMENTED
            .iter()
            .find(|(s, _)| s == suite)
            .map(|(_, w)| *w)
        {
            report.push(
                suite,
                "implemented by this runner",
                Status::NotImplemented(why),
            );
            continue;
        }
        if !h.declares(suite) {
            // Not a failure: a level-1 implementation should be reported as
            // level 1, not as twelve failures it never claimed. It still bars
            // certification of the level containing it.
            report.push(suite, "declared by the app", Status::NotDeclared);
            continue;
        }
        dispatch(suite, h, report).await;
    }
}

/// Suites this battery specifies and does not yet drive, with the reason.
///
/// Stated as data rather than left as a gap in a `match`, so the report can name
/// them without the runner having to be asked. Emptying this list is the goal;
/// hiding it would be the failure.
const UNIMPLEMENTED: &[(&str, &str)] = &[];

async fn dispatch(suite: &'static str, h: &Harness, report: &mut Report) {
    match suite {
        "memoization" => level1::memoization(h, report).await,
        "loops" => level1::loops(h, report).await,
        "determinism" => level1::determinism(h, report).await,
        "sleep" => level1::sleep(h, report).await,
        "errors" => level1::errors(h, report).await,
        "abandonment" => level1::abandonment(h, report).await,
        "signature" => level1::signature(h, report).await,

        "parallel" => level2::parallel(h, report).await,
        "wait" => level2::wait(h, report).await,
        "early_signal" => level2::early_signal(h, report).await,
        "invoke" => level2::invoke(h, report).await,
        "cascade" => level2::cascade(h, report).await,
        "continue_as_new" => level2::continue_as_new(h, report).await,
        "cancel" => level2::cancel(h, report).await,
        "cron" => level2::cron(h, report).await,
        "fencing" => level2::fencing(h, report).await,
        "refs" => level2::refs(h, report).await,
        "truncation" => level2::truncation(h, report).await,
        "blobs" => level2::blobs(h, report).await,

        other => report.push(
            ALL_SUITES
                .iter()
                .find(|s| **s == other)
                .copied()
                .unwrap_or("unknown"),
            "known to this runner",
            Status::NotImplemented("no case is registered for this suite"),
        ),
    }
}

/// Record a case from a fallible body, turning an error into `Errored`.
///
/// Errors and failures are kept apart on purpose: "the assertion did not hold"
/// is a statement about the app, "the run never settled" is usually a statement
/// about the harness or the machine, and conflating them sends an SDK author
/// looking in the wrong place.
pub(crate) fn record<T>(
    report: &mut Report,
    suite: &'static str,
    case: &'static str,
    outcome: anyhow::Result<Result<T, String>>,
) {
    let status = match outcome {
        Ok(Ok(_)) => Status::Passed,
        Ok(Err(why)) => Status::Failed(why),
        Err(e) => Status::Errored(e.to_string()),
    };
    report.push(suite, case, status);
}
