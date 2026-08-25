//! Load protection: circuit breaking, recovery ramping and retry backoff.
//!
//! These types are deliberately synchronous and clock-injected so they can be
//! tested exhaustively without a runtime or sleeping.

use chrono::{DateTime, Duration, Utc};

/// Circuit state for one app endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Circuit {
    /// Normal operation.
    Closed,
    /// Dispatch suspended after repeated failures.
    Open,
    /// Probing recovery with a limited budget.
    HalfOpen,
}

/// Per-app circuit breaker.
///
/// Two decisions here came out of testing rather than design:
///
/// 1. **Closing is not purely success-driven.** A breaker that only closes after
///    observing N successes stays half-open forever once traffic stops, so the
///    next burst is throttled long after the app recovered. Closing happens on
///    consecutive successes *or* a quiet period, whichever comes first.
///
/// 2. **Half-open admission is a deterministic budget, not a probability.**
///    Probabilistic admission makes recovery time something operators cannot
///    reason about and tests cannot pin down — it produced a flaky test, which
///    was the symptom of a real design flaw.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    /// Consecutive failures before opening.
    pub threshold: u32,
    /// How long to stay open before probing.
    pub cooldown: Duration,
    /// Consecutive successes in half-open before closing.
    pub close_after: u32,
    /// Failure-free interval in half-open that also closes the circuit.
    pub quiet_period: Duration,

    state: Circuit,
    failures: u32,
    successes: u32,
    opened_at: Option<DateTime<Utc>>,
    last_failure: Option<DateTime<Utc>>,
    budget: u32,
    round: u32,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self {
            threshold: 5,
            cooldown: Duration::seconds(15),
            close_after: 5,
            quiet_period: Duration::seconds(60),
            state: Circuit::Closed,
            failures: 0,
            successes: 0,
            opened_at: None,
            last_failure: None,
            budget: 0,
            round: 1,
        }
    }
}

impl CircuitBreaker {
    /// Build with an explicit threshold and cooldown.
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold,
            cooldown,
            ..Default::default()
        }
    }

    /// Current state.
    pub fn state(&self) -> Circuit {
        self.state
    }

    /// Consecutive failures recorded.
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// Whether a dispatch may proceed now.
    pub fn allow(&mut self, now: DateTime<Utc>) -> bool {
        if self.state == Circuit::Open {
            if self.opened_at.is_some_and(|t| now - t >= self.cooldown) {
                self.half_open();
                // Fall through: the transitioning probe consumes a token like any
                // other, so the budget means exactly what it says.
            } else {
                return false;
            }
        }
        match self.state {
            Circuit::Closed => true,
            Circuit::Open => false,
            Circuit::HalfOpen => {
                if self
                    .last_failure
                    .is_some_and(|t| now - t >= self.quiet_period)
                {
                    self.close();
                    return true;
                }
                if self.budget > 0 {
                    self.budget -= 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful dispatch.
    pub fn record_success(&mut self) {
        self.failures = 0;
        if self.state == Circuit::HalfOpen {
            self.successes += 1;
            self.round = (self.round * 2).min(64);
            self.budget += self.round;
            if self.successes >= self.close_after {
                self.close();
            }
        }
    }

    /// Record a failed dispatch.
    pub fn record_failure(&mut self, now: DateTime<Utc>) {
        self.failures += 1;
        self.successes = 0;
        self.last_failure = Some(now);
        // A failure while probing sends us straight back to open: the app is
        // not ready, and continuing to probe just prolongs the outage for it.
        if self.state == Circuit::HalfOpen || self.failures >= self.threshold {
            self.state = Circuit::Open;
            self.opened_at = Some(now);
            self.budget = 0;
        }
    }

    fn half_open(&mut self) {
        self.state = Circuit::HalfOpen;
        self.successes = 0;
        self.round = 1;
        self.budget = 1;
    }

    fn close(&mut self) {
        self.state = Circuit::Closed;
        self.failures = 0;
        self.budget = 0;
        self.round = 1;
    }
}

/// Retry backoff for a failing run.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Attempts before the run is failed.
    pub max_attempts: i32,
    /// First backoff interval.
    pub initial: Duration,
    /// Ceiling on the backoff interval.
    pub max: Duration,
    /// Whether to spread retries to avoid synchronised herds.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial: Duration::seconds(10),
            max: Duration::hours(1),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Backoff before attempt number `attempt` (1-based).
    ///
    /// Jitter is full-spread over the interval rather than a small percentage:
    /// when a shared dependency fails, every run backs off from the same instant,
    /// and a narrow jitter band still produces a synchronised retry spike.
    pub fn backoff(&self, attempt: i32, rand01: f64) -> Duration {
        // `clamp` rather than `max().min()`: the two read the same and only one
        // of them says what it means.
        let exp = attempt.clamp(1, 20) - 1;
        let base = self
            .initial
            .num_milliseconds()
            .saturating_mul(1i64 << exp.min(30));
        let capped = base.min(self.max.num_milliseconds()).max(1);
        let ms = if self.jitter {
            // Never below a tenth of the interval, so a burst cannot collapse to ~0.
            let lo = capped / 10;
            lo + ((capped - lo) as f64 * rand01) as i64
        } else {
            capped
        };
        Duration::milliseconds(ms)
    }

    /// Whether another attempt is permitted.
    pub fn should_retry(&self, attempts_so_far: i32) -> bool {
        attempts_so_far < self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn opens_after_threshold_failures() {
        let mut cb = CircuitBreaker::new(3, Duration::seconds(10));
        assert!(cb.allow(t(0)));
        cb.record_failure(t(0));
        cb.record_failure(t(1));
        assert_eq!(cb.state(), Circuit::Closed, "must not open early");
        cb.record_failure(t(2));
        assert_eq!(cb.state(), Circuit::Open);
        assert!(!cb.allow(t(3)), "open circuit must stop dispatch");
    }

    #[test]
    fn probes_after_cooldown_then_closes_on_sustained_success() {
        let mut cb = CircuitBreaker::new(1, Duration::seconds(10));
        cb.record_failure(t(0));
        assert!(!cb.allow(t(5)));
        assert!(cb.allow(t(10)), "cooldown elapsed, must probe");
        assert_eq!(cb.state(), Circuit::HalfOpen);
        for _ in 0..cb.close_after {
            cb.record_success();
        }
        assert_eq!(cb.state(), Circuit::Closed);
    }

    #[test]
    fn half_open_budget_is_deterministic_not_random() {
        let mut cb = CircuitBreaker::new(1, Duration::seconds(10));
        cb.record_failure(t(0));
        // First probe consumes the single starting token.
        assert!(cb.allow(t(10)));
        assert!(
            !cb.allow(t(11)),
            "budget exhausted until a success widens it"
        );
        cb.record_success();
        // Budget widened by 2, so exactly two more admissions.
        assert!(cb.allow(t(12)));
        assert!(cb.allow(t(13)));
        assert!(!cb.allow(t(14)));
    }

    #[test]
    fn closes_after_a_quiet_period_with_no_traffic() {
        // The regression that a purely success-driven breaker fails: traffic
        // stops while half-open, so no successes ever arrive to close it.
        let mut cb = CircuitBreaker::new(1, Duration::seconds(10));
        cb.quiet_period = Duration::seconds(30);
        cb.record_failure(t(0));
        assert!(cb.allow(t(10)));
        assert_eq!(cb.state(), Circuit::HalfOpen);
        assert!(cb.allow(t(100)), "quiet period elapsed");
        assert_eq!(cb.state(), Circuit::Closed);
    }

    #[test]
    fn failure_while_probing_reopens_immediately() {
        let mut cb = CircuitBreaker::new(3, Duration::seconds(10));
        for i in 0..3 {
            cb.record_failure(t(i));
        }
        assert!(
            cb.allow(t(12)),
            "cooldown runs from the third failure at t(2)"
        );
        assert_eq!(cb.state(), Circuit::HalfOpen);
        cb.record_failure(t(13));
        assert_eq!(cb.state(), Circuit::Open, "one probe failure is enough");
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let p = RetryPolicy {
            jitter: false,
            ..Default::default()
        };
        assert_eq!(p.backoff(1, 0.0), Duration::seconds(10));
        assert_eq!(p.backoff(2, 0.0), Duration::seconds(20));
        assert_eq!(p.backoff(3, 0.0), Duration::seconds(40));
        assert_eq!(p.backoff(30, 0.0), Duration::hours(1), "capped");
    }

    #[test]
    fn jitter_spreads_but_never_collapses_to_zero() {
        let p = RetryPolicy::default();
        let lo = p.backoff(5, 0.0);
        let hi = p.backoff(5, 1.0);
        assert!(lo < hi, "jitter must actually spread");
        assert!(lo >= Duration::milliseconds(1), "must never be zero");
        // Lower bound is a tenth of the interval, so a herd cannot collapse.
        assert!(lo >= Duration::seconds(16));
    }

    #[test]
    fn backoff_never_panics_on_extreme_attempts() {
        let p = RetryPolicy::default();
        for a in [i32::MIN, -1, 0, 1, 1000, i32::MAX] {
            let d = p.backoff(a, 0.5);
            assert!(d > Duration::zero() && d <= p.max);
        }
    }

    #[test]
    fn retry_limit_respected() {
        let p = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };
        assert!(p.should_retry(2));
        assert!(!p.should_retry(3));
    }
}
