//! Step identity.
//!
//! A step is identified by `(function_id, step_id, occurrence)`, hashed to 64
//! bits. Identity is deliberately *not* positional: inserting, removing or
//! reordering steps around an existing one leaves its hash untouched, which is
//! what makes it safe to change workflow code while runs are in flight.

use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Separator between hash components. `0x1F` (unit separator) cannot appear in a
/// function or step id, so `("a", "bc")` and `("ab", "c")` can never collide.
const SEP: u8 = 0x1F;

/// `sha256(function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ occurrence)[0..8]`, hex.
///
/// `function_id` is included so that a child run of a different function cannot
/// collide with its parent. 64 bits is ample given the 10 000-steps-per-run cap:
/// at that size the birthday probability is on the order of 1e-11.
///
/// ```
/// # use stepd_proto::step_hash;
/// assert_eq!(step_hash("f", "charge", 0).len(), 16);
/// assert_ne!(step_hash("f", "charge", 0), step_hash("f", "charge", 1));
/// assert_ne!(step_hash("a", "charge", 0), step_hash("b", "charge", 0));
/// ```
pub fn step_hash(function_id: &str, step_id: &str, occurrence: u32) -> String {
    let mut h = Sha256::new();
    h.update(function_id.as_bytes());
    h.update([SEP]);
    h.update(step_id.as_bytes());
    h.update([SEP]);
    h.update(occurrence.to_string().as_bytes());
    hex::encode(&h.finalize()[..8])
}

/// Assigns occurrence numbers within a single replay pass.
///
/// # Why this is not just a counter
///
/// Occurrence must be claimed **in program order at the point of the call**, on a
/// sequential pass. If two concurrent tasks race to claim occurrences, they will
/// swap between attempts, the hashes will differ, and completed work will
/// silently re-execute — the worst failure mode in the design, because it
/// corrupts quietly rather than erroring.
///
/// SDKs built on this type must therefore claim eagerly (when the step function
/// is *called*) rather than lazily (when its future is first polled). See
/// `stepd-sdk` for the enforcement.
#[derive(Debug, Default)]
pub struct OccurrenceCounter {
    function_id: String,
    counts: HashMap<String, u32>,
    /// Ids seen this pass, in order. Used to detect a handler whose step
    /// sequence changed between attempts.
    sequence: Vec<String>,
}

impl OccurrenceCounter {
    /// Start a fresh pass for `function_id`. Counters reset every attempt.
    pub fn new(function_id: impl Into<String>) -> Self {
        Self {
            function_id: function_id.into(),
            counts: HashMap::new(),
            sequence: Vec::new(),
        }
    }

    /// Claim the next occurrence for `step_id` and return its hash.
    pub fn claim(&mut self, step_id: &str) -> String {
        let n = self.counts.entry(step_id.to_string()).or_insert(0);
        let occurrence = *n;
        *n += 1;
        self.sequence.push(step_id.to_string());
        step_hash(&self.function_id, step_id, occurrence)
    }

    /// The step ids encountered this pass, in order.
    pub fn sequence(&self) -> &[String] {
        &self.sequence
    }

    /// Number of distinct step ids seen.
    pub fn distinct(&self) -> usize {
        self.counts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_and_sized() {
        assert_eq!(step_hash("fn", "s", 0), step_hash("fn", "s", 0));
        assert_eq!(step_hash("fn", "s", 0).len(), 16);
    }

    #[test]
    fn separator_prevents_component_confusion() {
        // Without a separator these would hash the same input bytes.
        assert_ne!(step_hash("a", "bc", 0), step_hash("ab", "c", 0));
    }

    #[test]
    fn occurrence_distinguishes_loop_iterations() {
        let mut c = OccurrenceCounter::new("fn");
        let a = c.claim("item");
        let b = c.claim("item");
        let d = c.claim("other");
        assert_ne!(a, b);
        assert_ne!(a, d);
        assert_eq!(c.sequence(), &["item", "item", "other"]);
    }

    #[test]
    fn counters_are_per_step_id() {
        let mut c = OccurrenceCounter::new("fn");
        c.claim("a");
        let b0 = c.claim("b");
        // "b" starts at 0 even though "a" was claimed first
        assert_eq!(b0, step_hash("fn", "b", 0));
    }

    #[test]
    fn same_ids_across_passes_produce_identical_hashes() {
        let run = |_: ()| {
            let mut c = OccurrenceCounter::new("fn");
            (0..3).map(|_| c.claim("item")).collect::<Vec<_>>()
        };
        assert_eq!(run(()), run(()));
    }
}
