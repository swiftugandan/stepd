//! What a conformance run concluded, and what it declined to conclude.
//!
//! The design constraint here is a single sentence from protocol §12.3: a runner
//! must distinguish "the app does not claim this suite" from "I did not test this
//! suite", and must not certify a level containing either.
//!
//! That is not pedantry. A conformance tool exists to be pointed at somebody
//! else's implementation and produce a claim about it, and the failure mode
//! specific to such a tool is certifying around its own gaps — reporting LEVEL 2
//! when four of its suites were never written. The result reads identically to a
//! real pass, which makes it worse than no tool at all: the whole value is that
//! the answer can be trusted without reading the runner's source.
//!
//! So [`Status::NotImplemented`] is a first-class outcome, printed as loudly as a
//! failure, and [`Report::level`] returns `None` rather than a level when any
//! suite in that level is unaccounted for.

use std::collections::BTreeMap;
use std::fmt;

/// How one case came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// The assertion held.
    Passed,
    /// The assertion did not hold, with what was expected and what happened.
    Failed(String),
    /// The app declared the hazard unrepresentable, so there is nothing to drive.
    ///
    /// Relayed, not verified: the runner cannot check that a program fails to
    /// compile. It certifies — otherwise an SDK whose type system prevents the
    /// hazard outright would score worse than one that merely detects it, which
    /// is a test rewarding the wrong thing — but it prints differently, because
    /// a claim the tool checked and a claim it passed on are not the same
    /// evidence and the report should not blur them.
    PassedByConstruction(&'static str),
    /// The app's conformance manifest did not declare this suite.
    ///
    /// Not a failure. An implementation that supports only level 1 should be
    /// reported as level 1, not as eleven failures it never claimed.
    NotDeclared,
    /// This runner has not implemented the case.
    ///
    /// The honest outcome for a suite the battery specifies and the runner does
    /// not yet cover. It bars certification of its level, which is the point.
    NotImplemented(&'static str),
    /// The case could not run for a reason that is not the app's fault.
    Errored(String),
}

impl Status {
    /// Whether this outcome permits certifying the level it belongs to.
    ///
    /// Only `Passed` does. `NotDeclared` and `NotImplemented` are both unknowns,
    /// and an unknown is not a pass however sympathetic its reason.
    pub fn certifies(&self) -> bool {
        matches!(self, Status::Passed | Status::PassedByConstruction(_))
    }

    fn marker(&self) -> &'static str {
        match self {
            Status::Passed => "pass",
            Status::PassedByConstruction(_) => "pass (asserted)",
            Status::Failed(_) => "FAIL",
            Status::NotDeclared => "not declared",
            Status::NotImplemented(_) => "NOT IMPLEMENTED",
            Status::Errored(_) => "ERROR",
        }
    }
}

/// One assertion's outcome.
#[derive(Debug, Clone)]
pub struct CaseResult {
    /// Suite this case belongs to, as named in protocol §12.
    pub suite: &'static str,
    /// What the case asserts, in words, so a failure names the property rather
    /// than the mechanism.
    pub case: &'static str,
    /// How it came out.
    pub status: Status,
}

/// The suites that make up level 1 (protocol §12).
///
/// `determinism` is in here because it guards non-deterministic step hashing,
/// which corrupts silently: completed work re-executes and nothing raises.
pub const LEVEL_1: &[&str] = &[
    "memoization",
    "loops",
    "determinism",
    "sleep",
    "errors",
    "abandonment",
    "signature",
];

/// Every suite in the battery. Level 2 is all of these.
pub const ALL_SUITES: &[&str] = &[
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

/// The conclusion of a run.
#[derive(Debug, Default)]
pub struct Report {
    /// Every case, in the order it ran.
    pub cases: Vec<CaseResult>,
    /// What the app said it implements.
    pub declared: Vec<String>,
    /// The app's self-reported SDK string, for the header.
    pub sdk: Option<String>,
}

impl Report {
    /// Record a case.
    pub fn push(&mut self, suite: &'static str, case: &'static str, status: Status) {
        self.cases.push(CaseResult {
            suite,
            case,
            status,
        });
    }

    /// Cases grouped by suite, in `ALL_SUITES` order.
    pub fn by_suite(&self) -> BTreeMap<&'static str, Vec<&CaseResult>> {
        let mut out: BTreeMap<&'static str, Vec<&CaseResult>> = BTreeMap::new();
        for c in &self.cases {
            out.entry(c.suite).or_default().push(c);
        }
        out
    }

    /// Whether every case in `suite` passed, and there was at least one.
    ///
    /// The "at least one" clause matters: a suite with no cases has vacuously
    /// no failures, and reporting that as a pass is the exact shape of lie this
    /// type exists to prevent.
    pub fn suite_passed(&self, suite: &str) -> bool {
        let mut seen = false;
        for c in self.cases.iter().filter(|c| c.suite == suite) {
            seen = true;
            if !c.status.certifies() {
                return false;
            }
        }
        seen
    }

    /// The highest level this run establishes, if any.
    ///
    /// `None` means the run does not establish a level — because something
    /// failed, or because something was never checked. The two are different in
    /// the printed report and identical here, deliberately: neither is a pass.
    pub fn level(&self) -> Option<u8> {
        if ALL_SUITES.iter().all(|s| self.suite_passed(s)) {
            Some(2)
        } else if LEVEL_1.iter().all(|s| self.suite_passed(s)) {
            Some(1)
        } else {
            None
        }
    }

    /// Suites in `level` that this run cannot vouch for, and why.
    pub fn unaccounted(&self, level: u8) -> Vec<(&'static str, String)> {
        let suites = if level == 1 { LEVEL_1 } else { ALL_SUITES };
        suites
            .iter()
            .filter(|s| !self.suite_passed(s))
            .map(|s| {
                let why = self
                    .cases
                    .iter()
                    .filter(|c| c.suite == *s && !c.status.certifies())
                    .map(|c| match &c.status {
                        Status::Failed(d) => format!("{}: {d}", c.case),
                        Status::NotImplemented(d) => format!("{}: not implemented — {d}", c.case),
                        Status::Errored(d) => format!("{}: errored — {d}", c.case),
                        Status::NotDeclared => "not declared by the app".to_string(),
                        Status::Passed | Status::PassedByConstruction(_) => unreachable!(),
                    })
                    .next()
                    .unwrap_or_else(|| "no cases ran".to_string());
                (*s, why)
            })
            .collect()
    }

    /// Whether the process should exit non-zero.
    ///
    /// A failure or an error, yes. A suite the app did not declare, no: that is
    /// a smaller implementation reporting itself accurately, and failing the
    /// build over it would push implementers towards declaring suites they have
    /// not written.
    pub fn should_fail(&self) -> bool {
        self.cases.iter().any(|c| {
            matches!(
                c.status,
                Status::Failed(_) | Status::Errored(_) | Status::NotImplemented(_)
            )
        })
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "stepd conformance — protocol §12")?;
        if let Some(sdk) = &self.sdk {
            writeln!(f, "app under test: {sdk}")?;
        }
        writeln!(f, "declared suites: {}", self.declared.join(", "))?;
        writeln!(f)?;

        for suite in ALL_SUITES {
            let cases: Vec<&CaseResult> = self.cases.iter().filter(|c| c.suite == *suite).collect();
            if cases.is_empty() {
                writeln!(f, "  {suite}")?;
                writeln!(f, "      —  no cases ran")?;
                continue;
            }
            writeln!(f, "  {suite}")?;
            for c in cases {
                writeln!(f, "      {:<16} {}", c.status.marker(), c.case)?;
                match &c.status {
                    Status::Failed(d) | Status::Errored(d) => writeln!(f, "          {d}")?,
                    Status::NotImplemented(d) => writeln!(f, "          {d}")?,
                    Status::PassedByConstruction(d) => {
                        writeln!(f, "          {d} (asserted by the app, not verified here)")?
                    }
                    _ => {}
                }
            }
        }

        writeln!(f)?;
        let passed = self.cases.iter().filter(|c| c.status.certifies()).count();
        writeln!(f, "  {passed}/{} cases passed", self.cases.len())?;

        match self.level() {
            Some(2) => writeln!(f, "\n  CONFORMANT AT LEVEL 2")?,
            Some(1) => {
                writeln!(f, "\n  CONFORMANT AT LEVEL 1")?;
                writeln!(f, "  not level 2 — these suites are unaccounted for:")?;
                for (s, why) in self.unaccounted(2) {
                    writeln!(f, "    {s}: {why}")?;
                }
            }
            _ => {
                writeln!(f, "\n  NOT CONFORMANT")?;
                writeln!(
                    f,
                    "  level 1 requires these, and this run cannot vouch for them:"
                )?;
                for (s, why) in self.unaccounted(1) {
                    writeln!(f, "    {s}: {why}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report_with(entries: &[(&'static str, Status)]) -> Report {
        let mut r = Report::default();
        for (suite, status) in entries {
            r.push(suite, "a case", status.clone());
        }
        r
    }

    fn all_passing() -> Report {
        report_with(
            &ALL_SUITES
                .iter()
                .map(|s| (*s, Status::Passed))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn a_full_pass_certifies_level_two() {
        assert_eq!(all_passing().level(), Some(2));
    }

    #[test]
    fn level_one_alone_certifies_level_one() {
        let r = report_with(
            &LEVEL_1
                .iter()
                .map(|s| (*s, Status::Passed))
                .collect::<Vec<_>>(),
        );
        assert_eq!(r.level(), Some(1));
        // …and says what is missing rather than leaving the reader to diff two
        // lists in their head.
        assert!(!r.unaccounted(2).is_empty());
    }

    #[test]
    fn an_unimplemented_suite_bars_the_level_it_belongs_to() {
        // The property this whole module exists for. A runner that has not
        // written a suite must not certify the level containing it; the report
        // would otherwise be indistinguishable from one that checked.
        let mut r = all_passing();
        r.cases.retain(|c| c.suite != "cascade");
        r.push(
            "cascade",
            "a case",
            Status::NotImplemented("not written yet"),
        );

        assert_eq!(r.level(), Some(1), "level 2 must not be certified");
        assert!(
            r.unaccounted(2).iter().any(|(s, _)| *s == "cascade"),
            "and the report must name it"
        );
        assert!(r.should_fail(), "an unimplemented suite fails the build");
    }

    #[test]
    fn an_undeclared_suite_bars_certification_but_not_the_build() {
        // A smaller implementation reporting itself accurately. Failing the
        // build over it would push implementers towards declaring suites they
        // have not written, which is the opposite of what the manifest is for.
        let mut r = all_passing();
        r.cases.retain(|c| c.suite != "blobs");
        r.push("blobs", "a case", Status::NotDeclared);

        assert_eq!(r.level(), Some(1));
        assert!(!r.should_fail());
    }

    #[test]
    fn a_hazard_prevented_by_construction_certifies_but_prints_differently() {
        // An SDK whose type system makes the hazard unrepresentable must not
        // score worse than one that merely detects it at run time. But the
        // report has to show that the runner relayed the claim rather than
        // checking it, or the two look like the same evidence.
        let mut r = all_passing();
        r.cases.retain(|c| c.suite != "determinism");
        r.push(
            "determinism",
            "off-path claims cannot be written",
            Status::PassedByConstruction("Ctx is !Send"),
        );
        assert_eq!(r.level(), Some(2));
        assert!(!r.should_fail());
        assert!(r.to_string().contains("not verified here"));
    }

    #[test]
    fn a_suite_with_no_cases_is_not_a_pass() {
        // Vacuous truth is the quietest way for a conformance tool to lie: no
        // failures because nothing ran.
        let r = Report::default();
        assert!(!r.suite_passed("memoization"));
        assert_eq!(r.level(), None);
    }

    #[test]
    fn one_failing_case_fails_its_whole_suite() {
        let mut r = all_passing();
        r.push(
            "sleep",
            "another case",
            Status::Failed("timer was early".into()),
        );
        assert!(!r.suite_passed("sleep"));
        assert_eq!(r.level(), None);
        assert!(r.should_fail());
    }

    #[test]
    fn the_printed_report_names_what_it_could_not_vouch_for() {
        let mut r = all_passing();
        r.cases.retain(|c| c.suite != "fencing");
        r.push(
            "fencing",
            "stale fence ignored",
            Status::NotImplemented("no harness"),
        );
        let text = r.to_string();
        assert!(text.contains("NOT IMPLEMENTED"));
        assert!(text.contains("fencing"));
        assert!(
            !text.contains("CONFORMANT AT LEVEL 2"),
            "a report with an unimplemented suite must not read as a level-2 pass"
        );
    }
}
