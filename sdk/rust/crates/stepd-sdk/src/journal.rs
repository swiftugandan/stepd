//! Paging a truncated journal (protocol §8.6).
//!
//! A run whose journal outgrows the server's inline ceiling arrives with
//! `state_truncated: true` and only part of its steps. Replaying against that
//! partial journal is the worst available outcome: every step the app cannot see
//! re-executes, the run still completes, and nothing anywhere errors. It is the
//! same failure mode as an unstable step hash, reached by a different route.
//!
//! So there are exactly two acceptable behaviours, and this module implements the
//! first: fetch the rest before replaying, or fail the attempt non-retryably. An
//! SDK that has no way to fetch must still refuse.
//!
//! Refusing means answering 400, which §2.2 says fails the run non-retryably.
//! The engine does not currently honour that — it retries with backoff
//! ([#30](https://github.com/swiftugandan/stepd/issues/30)) — so an app that
//! reaches this path unconfigured stalls rather than failing cleanly. The SDK
//! side is right either way; the note is here so the next reader does not
//! conclude the refusal is what is broken.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use stepd_proto::RecordedStep;
use uuid::Uuid;

/// Where a truncated journal is paged from.
///
/// A handle rather than a value, because the address is often not known when the
/// app is built. A conformance app is told it after it is already serving
/// (§12.1), and a deployment reading it from the environment can set it at
/// construction; both go through the same `configure`.
#[derive(Clone, Default)]
pub struct JournalSource(Arc<Mutex<Option<Endpoint>>>);

#[derive(Clone)]
struct Endpoint {
    base: String,
    token: String,
}

impl std::fmt::Debug for JournalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the token. This type is inside `App`, and `App` ends up in
        // enough error paths that a derived Debug would eventually print a
        // credential into somebody's logs.
        match self.0.lock().unwrap().as_ref() {
            Some(e) => write!(f, "JournalSource({})", e.base),
            None => f.write_str("JournalSource(unconfigured)"),
        }
    }
}

impl JournalSource {
    /// Point it at a stepd server. `token` needs the operator role.
    pub fn configure(&self, base_url: &str, token: &str) {
        *self.0.lock().unwrap() = Some(Endpoint {
            base: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
        });
    }

    /// Whether an address has been supplied.
    pub fn is_configured(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }

    fn endpoint(&self) -> Option<Endpoint> {
        self.0.lock().unwrap().clone()
    }

    /// Fetch every step the attempt did not carry, and return them merged.
    ///
    /// `have` is what arrived inline. The server orders by hash and sends the
    /// lowest `n`, so the cursor is the highest hash already held — an unordered
    /// page boundary would drop or repeat steps between requests, and either one
    /// corrupts a replay silently.
    pub(crate) async fn fetch_remaining(
        &self,
        run_id: Uuid,
        have: &HashMap<String, RecordedStep>,
    ) -> Result<HashMap<String, RecordedStep>, String> {
        let Some(endpoint) = self.endpoint() else {
            return Err(
                "the attempt journal was truncated and this app has no server address to \
                 page it from; call `App::journal_source` (protocol §8.6)"
                    .into(),
            );
        };

        let http = reqwest::Client::new();
        let mut merged = have.clone();
        let mut cursor = have.keys().max().cloned();

        // Bounded so a server that always answers `next` cannot spin here
        // forever. 10 000 steps is the engine's per-run ceiling and the page
        // size is 500, so twenty times that is far past any real journal.
        for _ in 0..400 {
            let url = format!("{}/v1/runs/{run_id}/steps", endpoint.base);
            let mut request = http
                .get(&url)
                .bearer_auth(&endpoint.token)
                .query(&[("limit", "500")]);
            if let Some(after) = &cursor {
                request = request.query(&[("after", after)]);
            }

            let response = request
                .send()
                .await
                .map_err(|e| format!("could not page the journal from {url}: {e}"))?;
            if !response.status().is_success() {
                return Err(format!(
                    "the server answered {} paging the journal from {url}",
                    response.status()
                ));
            }

            #[derive(serde::Deserialize)]
            struct Page {
                #[serde(default)]
                steps: HashMap<String, RecordedStep>,
                #[serde(default)]
                next: Option<String>,
            }
            let page: Page = response
                .json()
                .await
                .map_err(|e| format!("the journal page from {url} did not parse: {e}"))?;

            merged.extend(page.steps);
            match page.next {
                // The cursor must advance. A server echoing the same `next`
                // would otherwise be an infinite loop that looks like a hang
                // rather than a fault.
                Some(next) if Some(&next) != cursor.as_ref() => cursor = Some(next),
                Some(_) => {
                    return Err(format!(
                        "the server repeated the page cursor while paging {url}"
                    ))
                }
                None => return Ok(merged),
            }
        }

        Err(format!(
            "gave up paging the journal for run {run_id} after 400 pages"
        ))
    }
}
