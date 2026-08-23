//! Cron schedules (protocol §3.1).
//!
//! Pure: parsing, next-occurrence computation and the catch-up decision, with no
//! I/O and no clock of its own. Every function here takes the current time as an
//! argument, because scheduling must derive from **database time** — a replica
//! with a skewed clock that decided fire times from its own would fire early,
//! and would be the only thing in the system that thought so (gap C6).
//!
//! ## Why this is harder than "add one minute"
//!
//! Three of the four things that make cron subtle are about time zones:
//!
//! * **Spring forward.** `30 1 * * *` in `Europe/London` on the last Sunday in
//!   March names a local time that does not exist — the clocks jump 01:00 → 02:00.
//!   The occurrence is **skipped**. Firing it at 02:30 instead would run a job an
//!   hour late once a year, silently.
//! * **Fall back.** The same local time happens **twice**. It fires **once**, on
//!   the first (pre-transition) occurrence. Firing twice would double-execute a
//!   daily job once a year, which is exactly the class of bug a durable engine
//!   exists to prevent.
//! * **Day-of-month and day-of-week together.** Standard cron ORs them when both
//!   are restricted. `0 0 13 * 5` is "the 13th, *or* any Friday" — not "Friday
//!   the 13th". Getting this wrong is the classic cron footgun, and it fails in
//!   the direction of running far too often.
//!
//! The fourth is the server having been down: see [`CatchUp`].

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use std::collections::VecDeque;
use uuid::Uuid;

/// The largest span searched for a next occurrence.
///
/// Bounded because a parseable expression can still be unsatisfiable — `0 0 30 2 *`
/// is "the 30th of February", which is syntactically fine and never happens. An
/// unbounded search would hang the sweep on it; returning `None` lets the caller
/// disable the schedule and say why.
const MAX_SEARCH_YEARS: i32 = 5;

/// Ceiling on how many occurrences a single catch-up decision will walk.
///
/// Reached only by a minutely schedule with a misfire window of weeks — at which
/// point the operator's intent is not "replay every minute of the outage", and
/// the decision still fires the newest occurrences correctly. It exists so that
/// a bad `next_fire_at` in the database cannot spin a scheduler thread.
const MAX_BACKLOG_SCAN: usize = 100_000;

/// Extra occurrences retained beyond what the policy will fire, so that the
/// `skipped` list names some of what was dropped instead of only counting it.
const RETAIN_FOR_AUDIT: usize = 16;

/// What to do about occurrences missed while the server was down (protocol §3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUp {
    /// Fire once on recovery, however many were missed. The default, because it
    /// is what almost every job actually wants: a nightly report that missed
    /// three nights needs running, not running three times.
    One,
    /// Fire nothing. For jobs where a late run is worse than no run — a "send
    /// the 9am digest" that would arrive at 3pm and confuse everyone.
    Skip,
    /// Fire every missed occurrence, capped by `catchup_limit`. For jobs that
    /// process a window per fire and would leave a gap otherwise.
    All,
}

impl CatchUp {
    /// Parse the wire form, defaulting to `one` for anything unrecognised.
    ///
    /// Defaulting rather than erroring because this is read from a stored
    /// function config: a value that arrived through an older SDK should not stop
    /// the schedule firing at all.
    pub fn parse(s: &str) -> Self {
        match s {
            "skip" => Self::Skip,
            "all" => Self::All,
            _ => Self::One,
        }
    }

    /// The wire form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Skip => "skip",
            Self::All => "all",
        }
    }
}

/// One field of a cron expression, already expanded to the values it matches.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    /// Matching values, sorted and deduplicated.
    values: Vec<u32>,
    /// Whether the field was written as `*`.
    ///
    /// Kept because day-of-month and day-of-week combine differently depending
    /// on whether each was restricted, and "expanded to every value" is not the
    /// same fact as "was written as `*`".
    wildcard: bool,
}

impl Field {
    fn matches(&self, v: u32) -> bool {
        self.values.binary_search(&v).is_ok()
    }
}

/// Why an expression was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CronError {
    /// Not five whitespace-separated fields.
    #[error("expected 5 fields (minute hour day-of-month month day-of-week), found {0}")]
    FieldCount(usize),
    /// A field did not parse.
    #[error("field {field} ({name}): {reason}")]
    Field {
        /// 1-based field position.
        field: usize,
        /// What the field is called.
        name: &'static str,
        /// What was wrong with it.
        reason: String,
    },
    /// The time zone is not in the IANA database.
    #[error("unknown time zone '{0}'; use an IANA name such as 'Europe/London'")]
    UnknownTimeZone(String),
}

/// A parsed cron schedule bound to a time zone.
#[derive(Debug, Clone)]
pub struct Schedule {
    minute: Field,
    hour: Field,
    dom: Field,
    month: Field,
    dow: Field,
    tz: Tz,
    source: String,
}

const FIELD_NAMES: [&str; 5] = ["minute", "hour", "day-of-month", "month", "day-of-week"];
// Day-of-week is parsed as 0–7, not 0–6: `7` is Sunday in most dialects and is
// normalised to `0` after parsing. Rejecting it at the range check instead would
// mean a schedule copied from a crontab that works elsewhere fails to register
// here, for no semantic reason.
const FIELD_RANGES: [(u32, u32); 5] = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)];

impl Schedule {
    /// Parse a five-field expression in an IANA time zone.
    ///
    /// Accepts `*`, `n`, `a-b`, `*/n`, `a-b/n` and comma-separated lists of those,
    /// plus three-letter month and day names. Deliberately not the whole of the
    /// various cron dialects: `@daily`, `L`, `W`, `#` and seconds fields are all
    /// rejected rather than silently misinterpreted, because each means something
    /// different in a different implementation and a schedule that runs at the
    /// wrong time is worse than one that refuses to register.
    pub fn parse(expr: &str, tz: &str) -> Result<Self, CronError> {
        let tz: Tz = tz
            .parse()
            .map_err(|_| CronError::UnknownTimeZone(tz.to_string()))?;

        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(CronError::FieldCount(parts.len()));
        }

        let mut fields = Vec::with_capacity(5);
        for (i, raw) in parts.iter().enumerate() {
            let (lo, hi) = FIELD_RANGES[i];
            fields.push(
                parse_field(raw, lo, hi, i).map_err(|reason| CronError::Field {
                    field: i + 1,
                    name: FIELD_NAMES[i],
                    reason,
                })?,
            );
        }

        Ok(Self {
            minute: fields[0].clone(),
            hour: fields[1].clone(),
            dom: fields[2].clone(),
            month: fields[3].clone(),
            dow: fields[4].clone(),
            tz,
            source: expr.to_string(),
        })
    }

    /// The expression as written.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The zone this schedule is interpreted in.
    pub fn timezone(&self) -> Tz {
        self.tz
    }

    /// Whether a local date matches the day fields.
    ///
    /// Standard cron semantics: when **both** day-of-month and day-of-week are
    /// restricted, a day matches if **either** does. `0 0 13 * 5` is "the 13th,
    /// or any Friday" — not "Friday the 13th". This is the single most
    /// misread rule in cron, and it errs towards running too often, so it is
    /// worth stating in code rather than leaving to a reader's memory.
    fn day_matches(&self, date: NaiveDate) -> bool {
        let dom_ok = self.dom.matches(date.day());
        // chrono's Sunday is 0 in cron's numbering.
        let dow_ok = self.dow.matches(date.weekday().num_days_from_sunday());

        match (self.dom.wildcard, self.dow.wildcard) {
            (true, true) => true,
            (false, true) => dom_ok,
            (true, false) => dow_ok,
            (false, false) => dom_ok || dow_ok,
        }
    }

    /// The first occurrence strictly after `after`.
    ///
    /// `None` when the expression cannot be satisfied within
    /// [`MAX_SEARCH_YEARS`] — "the 30th of February" parses and never happens.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Work in local time and convert at the end: a cron expression names a
        // wall-clock time in its zone, and the whole point of the zone is that
        // the UTC instant moves twice a year.
        let local = after.with_timezone(&self.tz);
        let mut candidate = local
            .naive_local()
            .with_second(0)?
            .with_nanosecond(0)?
            .checked_add_signed(Duration::minutes(1))?;

        let limit = candidate.date().year() + MAX_SEARCH_YEARS;

        loop {
            if candidate.date().year() > limit {
                return None;
            }

            if !self.month.matches(candidate.month()) {
                // Jump to the first instant of the next month rather than
                // stepping a minute at a time: `0 0 1 1 *` is once a year, and a
                // minute-wise search would be half a million iterations.
                candidate = first_of_next_month(candidate.date())?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.day_matches(candidate.date()) {
                candidate = candidate.date().succ_opt()?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.hour.matches(candidate.hour()) {
                candidate = candidate
                    .with_minute(0)?
                    .checked_add_signed(Duration::hours(1))?;
                // Adding an hour can roll the date; the loop re-checks it.
                continue;
            }
            if !self.minute.matches(candidate.minute()) {
                candidate = candidate.checked_add_signed(Duration::minutes(1))?;
                continue;
            }

            // Every field matches. Now the zone gets a say.
            match self.tz.from_local_datetime(&candidate) {
                // The ordinary case.
                chrono::LocalResult::Single(dt) => return Some(dt.with_timezone(&Utc)),

                // Spring forward: this local time does not exist. Skip the
                // occurrence entirely (§3.1). Firing at the shifted time instead
                // would run the job an hour late, once a year, with nothing
                // saying so.
                chrono::LocalResult::None => {
                    candidate = candidate.checked_add_signed(Duration::minutes(1))?;
                    continue;
                }

                // Fall back: this local time happens twice. Fire once, on the
                // first — the pre-transition instant. Returning both would
                // double-execute a daily job once a year.
                chrono::LocalResult::Ambiguous(first, _second) => {
                    return Some(first.with_timezone(&Utc))
                }
            }
        }
    }

    /// Every occurrence in `(after, until]`, capped at `max`.
    ///
    /// Used to work out what was missed while the server was down.
    pub fn occurrences_between(
        &self,
        after: DateTime<Utc>,
        until: DateTime<Utc>,
        max: usize,
    ) -> Vec<DateTime<Utc>> {
        let mut out = Vec::new();
        let mut cursor = after;
        while out.len() < max {
            match self.next_after(cursor) {
                Some(next) if next <= until => {
                    out.push(next);
                    cursor = next;
                }
                _ => break,
            }
        }
        out
    }

    /// The most recent occurrences in `(after, until]`, keeping at most `cap`,
    /// plus a count of how many older ones were dropped to stay within it.
    ///
    /// The distinction from [`occurrences_between`](Self::occurrences_between)
    /// is which end gets truncated, and it is the whole point of this function.
    /// A minutely schedule whose scheduler was down for a month has forty
    /// thousand missed occurrences; enumerating them into a `Vec` is a memory
    /// hazard, and truncating the *newest* — which a plain forward scan with a
    /// limit does — is worse than a hazard, because the caller then fires the
    /// oldest backlog entries and computes, say, last month's totals while
    /// reporting them as this hour's. Walking forward but retaining a sliding
    /// window of the last `cap` keeps memory bounded and keeps the entries a
    /// catch-up policy actually wants.
    fn recent_occurrences(
        &self,
        after: DateTime<Utc>,
        until: DateTime<Utc>,
        cap: usize,
    ) -> (Vec<DateTime<Utc>>, usize) {
        let mut window: VecDeque<DateTime<Utc>> = VecDeque::with_capacity(cap.min(1024));
        let mut dropped = 0usize;
        let mut cursor = after;
        // A hard bound on *work*, separate from the bound on what is retained:
        // `until` is caller-supplied and a clock skew or a corrupt `next_fire_at`
        // must not turn this into an unbounded loop.
        let mut budget = MAX_BACKLOG_SCAN;
        while budget > 0 {
            budget -= 1;
            match self.next_after(cursor) {
                Some(next) if next <= until => {
                    if cap > 0 && window.len() == cap {
                        window.pop_front();
                        dropped += 1;
                    }
                    if cap > 0 {
                        window.push_back(next);
                    } else {
                        dropped += 1;
                    }
                    cursor = next;
                }
                _ => break,
            }
        }
        (window.into(), dropped)
    }
}

fn first_of_next_month(date: NaiveDate) -> Option<NaiveDate> {
    let (y, m) = if date.month() == 12 {
        (date.year() + 1, 1)
    } else {
        (date.year(), date.month() + 1)
    };
    NaiveDate::from_ymd_opt(y, m, 1)
}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

fn parse_value(s: &str, field_index: usize) -> Result<u32, String> {
    let lower = s.to_ascii_lowercase();
    if field_index == 3 {
        if let Some(i) = MONTHS.iter().position(|m| *m == lower) {
            return Ok(i as u32 + 1);
        }
    }
    if field_index == 4 {
        if let Some(i) = DAYS.iter().position(|d| *d == lower) {
            return Ok(i as u32);
        }
    }
    s.parse::<u32>()
        .map_err(|_| format!("'{s}' is not a number"))
}

fn parse_field(raw: &str, lo: u32, hi: u32, field_index: usize) -> Result<Field, String> {
    // Reject the dialect extensions explicitly. Each of `L`, `W` and `#` means
    // something different in different cron implementations, and a schedule that
    // fires at the wrong time is worse than one that refuses to register.
    for bad in ['L', 'W', '#', '?'] {
        if raw.to_ascii_uppercase().contains(bad) {
            return Err(format!(
                "'{bad}' is not supported; stepd implements standard five-field cron only, \
                 because that character means different things in different dialects"
            ));
        }
    }

    let mut values = Vec::new();
    let mut wildcard = false;

    for part in raw.split(',') {
        if part.is_empty() {
            return Err("empty element in a comma-separated list".into());
        }

        let (spec, step) = match part.split_once('/') {
            Some((s, st)) => {
                let n: u32 = st
                    .parse()
                    .map_err(|_| format!("step '{st}' is not a number"))?;
                if n == 0 {
                    return Err("step must be at least 1".into());
                }
                (s, n)
            }
            None => (part, 1),
        };

        let (start, end) = if spec == "*" {
            wildcard = true;
            (lo, hi)
        } else if let Some((a, b)) = spec.split_once('-') {
            (parse_value(a, field_index)?, parse_value(b, field_index)?)
        } else {
            let v = parse_value(spec, field_index)?;
            // `5/15` means "from 5, every 15" — a range to the top of the field,
            // not the single value 5.
            if step > 1 {
                (v, hi)
            } else {
                (v, v)
            }
        };

        if start < lo || end > hi || start > end {
            return Err(format!("'{spec}' is outside the valid range {lo}–{hi}"));
        }

        let mut v = start;
        while v <= end {
            values.push(v);
            v += step;
        }
    }

    // Day-of-week 7 is Sunday in most dialects; normalise so the caller never
    // has to know which.
    if field_index == 4 {
        for v in values.iter_mut() {
            if *v == 7 {
                *v = 0;
            }
        }
    }

    values.sort_unstable();
    values.dedup();
    if values.is_empty() {
        return Err("matches nothing".into());
    }

    // A field written as a full-range list is not a wildcard for the purposes of
    // the day-of-month/day-of-week rule. `0 0 * * 1-7` restricts day-of-week even
    // though it covers every day, and combining that with a restricted
    // day-of-month must still OR them.
    Ok(Field { values, wildcard })
}

/// What a sweep decided to do with one schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronDecision {
    /// Occurrences to fire now, oldest first.
    pub fire: Vec<DateTime<Utc>>,
    /// Occurrences deliberately not fired, with the reason, for the audit trail.
    pub skipped: Vec<(DateTime<Utc>, SkipReason)>,
    /// When this schedule should next be looked at.
    pub next_fire_at: Option<DateTime<Utc>>,
}

/// Why an occurrence was not fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Older than the misfire window.
    MisfireWindow,
    /// `catchup: skip`, or beyond what `catchup: one` fires.
    CatchUpPolicy,
    /// More missed occurrences than `catchup_limit`.
    CatchUpLimit,
}

impl SkipReason {
    /// A short stable form for the audit row.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MisfireWindow => "misfire_window",
            Self::CatchUpPolicy => "catchup_policy",
            Self::CatchUpLimit => "catchup_limit",
        }
    }
}

/// Settings that govern what happens after downtime.
#[derive(Debug, Clone, Copy)]
pub struct CatchUpPolicy {
    /// What to do with missed occurrences.
    pub catchup: CatchUp,
    /// Ceiling on how many are fired under `all`.
    pub limit: usize,
    /// Occurrences older than this are never caught up, whatever `catchup` says.
    pub misfire_window: Duration,
}

impl Default for CatchUpPolicy {
    fn default() -> Self {
        Self {
            catchup: CatchUp::One,
            limit: 10,
            misfire_window: Duration::hours(1),
        }
    }
}

/// Decide what to fire for a schedule whose next occurrence has come due.
///
/// `due_at` is the occurrence the schedule was waiting for; `now` is database
/// time. When the two are close together nothing interesting happens — that is
/// the normal path, one occurrence, fire it. The interesting case is a server
/// that was down for six hours, where `due_at` is six hours stale and the
/// question is what to do about the occurrences in between.
///
/// The misfire window is applied **before** the catch-up policy, and applies to
/// every occurrence including the due one. `catchup: all` on a job that was down
/// for a week must not fire a week of occurrences at 3am on Monday just because
/// the limit is 1 000 — the window is what makes "how stale is too stale" a
/// separate decision from "how many".
pub fn decide(
    schedule: &Schedule,
    due_at: DateTime<Utc>,
    now: DateTime<Utc>,
    policy: CatchUpPolicy,
) -> CronDecision {
    let cutoff = now - policy.misfire_window;

    // How many occurrences are worth retaining. `CatchUp::All` is bounded by the
    // policy's own limit; the other two policies keep exactly one, but retaining
    // a few gives the audit trail something to report beyond "a lot were
    // dropped". Either way the retained set is bounded before anything is
    // allocated per-occurrence, which is what keeps a month-long outage on a
    // minutely schedule from being a memory event.
    let retain = match policy.catchup {
        CatchUp::All => policy.limit.max(1),
        CatchUp::One | CatchUp::Skip => 1,
    }
    .saturating_add(RETAIN_FOR_AUDIT);

    // Every occurrence from the due one up to now, the due one included, newest
    // kept if the backlog is larger than we retain.
    let (later, mut dropped) = schedule.recent_occurrences(due_at, now, retain);
    let mut pending = Vec::with_capacity(later.len() + 1);
    if dropped == 0 {
        pending.push(due_at);
    } else {
        // `due_at` itself fell out of the retained window.
        dropped += 1;
    }
    pending.extend(later);

    let mut fire = Vec::new();
    let mut skipped = Vec::new();

    // Occurrences dropped to keep the scan bounded are reported as one entry, at
    // the oldest time we know was missed, rather than silently. An operator
    // reading the audit trail needs to know a backlog was truncated even when
    // listing every entry would be useless.
    if dropped > 0 {
        skipped.push((due_at, SkipReason::CatchUpLimit));
    }

    for occ in &pending {
        if *occ < cutoff {
            skipped.push((*occ, SkipReason::MisfireWindow));
        } else {
            fire.push(*occ);
        }
    }

    match policy.catchup {
        CatchUp::Skip => {
            // Nothing is caught up. The *current* occurrence still fires if it is
            // inside the window — "skip" means "do not catch up", not "do not
            // run"; a job that never runs after any restart would be a surprising
            // reading of the word.
            let keep = fire.pop();
            for occ in fire.drain(..) {
                skipped.push((occ, SkipReason::CatchUpPolicy));
            }
            if let Some(k) = keep {
                fire.push(k);
            }
        }
        CatchUp::One => {
            // One fire on recovery, however many were missed. The most recent, so
            // a job that computes "yesterday's totals" gets the right yesterday.
            if fire.len() > 1 {
                let keep = *fire.last().unwrap();
                for occ in fire.drain(..fire.len() - 1) {
                    skipped.push((occ, SkipReason::CatchUpPolicy));
                }
                fire.clear();
                fire.push(keep);
            }
        }
        CatchUp::All => {
            if fire.len() > policy.limit {
                // Drop the OLDEST beyond the limit, not the newest: a job that
                // processes a window per fire needs the recent windows more than
                // the ancient ones, and the ancient ones are the ones an operator
                // is most likely to want to re-run deliberately.
                let excess = fire.len() - policy.limit;
                for occ in fire.drain(..excess) {
                    skipped.push((occ, SkipReason::CatchUpLimit));
                }
            }
        }
    }

    skipped.sort_by_key(|(t, _)| *t);
    CronDecision {
        next_fire_at: schedule.next_after(*pending.last().unwrap_or(&now).max(&now)),
        fire,
        skipped,
    }
}

// ==================================================================== planner
//
// One sweep of the scheduler, as a pure function.
//
// The transactional dance around it — claim rows, apply, advance, commit —
// belongs to whatever is storing the schedules. The *decisions* do not. This
// project has already paid once for letting a storage crate grow its own copy
// of engine logic (README finding 2: two correctness centres, and the tests
// guarded the one that was not running), so the planner is here, where it can
// be tested against a table of times with no database in the room, and the
// store executes what it is handed.

/// A schedule that has come due, as the planner needs it.
///
/// Deliberately not the storage row: no `paused`, no `created_at`, no identity
/// beyond what is needed to name the result. A planner that could see whether a
/// schedule was paused would eventually be asked to decide about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueSchedule {
    /// Storage identity, echoed back on the plan.
    pub id: Uuid,
    /// The expression as registered.
    pub expr: String,
    /// The IANA zone the expression is read in.
    pub tz: String,
    /// `one`, `skip` or `all`.
    pub catchup: String,
    /// Cap on how many occurrences `all` will fire in one recovery.
    pub catchup_limit: i32,
    /// Occurrences older than this are never caught up.
    pub misfire_window: Duration,
    /// The occurrence this schedule was waiting for.
    pub next_fire_at: DateTime<Utc>,
}

/// What the scheduler should do about one schedule this sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulePlan {
    /// The schedule this plan is for.
    pub id: Uuid,
    /// Occurrences to fire, oldest first.
    pub fire: Vec<DateTime<Utc>>,
    /// Occurrences deliberately not fired, with the reason, for the ledger.
    pub skip: Vec<(DateTime<Utc>, SkipReason)>,
    /// Where to move `next_fire_at` to.
    pub advance_to: Option<DateTime<Utc>>,
    /// The most recent occurrence actually fired, for `last_fired_at`.
    pub last_fired: Option<DateTime<Utc>>,
    /// Set when the schedule cannot be planned at all, with the reason.
    ///
    /// A schedule in this state must be **paused**, not retried. Its expression
    /// will not parse or names no future occurrence, so re-claiming it next
    /// sweep produces the same failure — and because a due row is claimed every
    /// pass until its `next_fire_at` moves, "retry" here means a hot loop for as
    /// long as the row exists. The two ways to get here are a zone that
    /// disappeared from tzdata under a running server and a row edited by hand.
    pub unschedulable: Option<String>,
}

impl SchedulePlan {
    /// Whether this plan changes anything.
    pub fn is_empty(&self) -> bool {
        self.fire.is_empty() && self.skip.is_empty() && self.unschedulable.is_none()
    }
}

/// Plan one sweep.
///
/// `now` must be **database time**, not the calling process's clock (F-DL-8). A
/// replica that trusts its own clock and runs a minute fast fires every schedule
/// in the fleet a minute early, and nothing anywhere reports a problem — the
/// runs all succeed.
pub fn plan(due: &[DueSchedule], now: DateTime<Utc>) -> Vec<SchedulePlan> {
    due.iter().map(|d| plan_one(d, now)).collect()
}

fn plan_one(due: &DueSchedule, now: DateTime<Utc>) -> SchedulePlan {
    let unschedulable = |why: String| SchedulePlan {
        id: due.id,
        fire: Vec::new(),
        skip: Vec::new(),
        advance_to: None,
        last_fired: None,
        unschedulable: Some(why),
    };

    let schedule = match Schedule::parse(&due.expr, &due.tz) {
        Ok(s) => s,
        Err(e) => return unschedulable(e.to_string()),
    };

    let decision = decide(
        &schedule,
        due.next_fire_at,
        now,
        CatchUpPolicy {
            catchup: CatchUp::parse(&due.catchup),
            limit: due.catchup_limit.max(1) as usize,
            misfire_window: due.misfire_window,
        },
    );

    // No next occurrence within the search horizon. `0 0 30 2 *` is the honest
    // example — February has no thirtieth, so the expression parses and names
    // nothing, ever. Leaving `next_fire_at` where it is would re-claim this row
    // on every sweep forever.
    let Some(advance_to) = decision.next_fire_at else {
        return unschedulable(format!(
            "'{}' in {} has no occurrence within {MAX_SEARCH_YEARS} years of {now}",
            due.expr, due.tz
        ));
    };

    // The invariant that stops the sweep spinning: the row must leave the due
    // set. `decide` computes from `max(last occurrence, now)` so this holds by
    // construction — but it is the difference between a scheduler and a busy
    // loop, so it is checked rather than assumed.
    if advance_to <= now {
        return unschedulable(format!(
            "'{}' in {} computed a next fire at {advance_to}, which is not after {now}",
            due.expr, due.tz
        ));
    }

    SchedulePlan {
        id: due.id,
        last_fired: decision.fire.last().copied(),
        fire: decision.fire,
        skip: decision.skipped,
        advance_to: Some(advance_to),
        unschedulable: None,
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn due(expr: &str, next: &str) -> DueSchedule {
        DueSchedule {
            id: Uuid::nil(),
            expr: expr.into(),
            tz: "UTC".into(),
            catchup: "one".into(),
            catchup_limit: 10,
            misfire_window: Duration::hours(1),
            next_fire_at: utc(next),
        }
    }

    #[test]
    fn the_ordinary_sweep_fires_once_and_advances() {
        let p = plan(
            &[due("0 * * * *", "2026-03-01T05:00:00Z")],
            utc("2026-03-01T05:00:02Z"),
        );
        assert_eq!(p[0].fire, vec![utc("2026-03-01T05:00:00Z")]);
        assert_eq!(p[0].advance_to, Some(utc("2026-03-01T06:00:00Z")));
        assert_eq!(p[0].last_fired, Some(utc("2026-03-01T05:00:00Z")));
        assert!(p[0].unschedulable.is_none());
    }

    fn d_slice(d: &DueSchedule) -> Vec<DueSchedule> {
        vec![d.clone()]
    }

    #[test]
    fn a_sweep_that_fires_nothing_still_advances() {
        // The 00:00 occurrence is half an hour old and the misfire window is a
        // minute, so nothing fires. The plan must still advance: if it did not,
        // the row would stay due and be re-claimed on every pass forever — a
        // schedule that fires nothing *and* costs a claim per sweep to do it.
        let mut d = due("0 * * * *", "2026-03-05T00:00:00Z");
        d.misfire_window = Duration::minutes(1);
        let now = utc("2026-03-05T00:30:00Z");

        let p = plan(&d_slice(&d), now);
        assert!(p[0].fire.is_empty());
        assert_eq!(p[0].skip.len(), 1);
        assert_eq!(p[0].skip[0].1, SkipReason::MisfireWindow);
        assert_eq!(p[0].advance_to, Some(utc("2026-03-05T01:00:00Z")));
        assert!(
            p[0].last_fired.is_none(),
            "nothing fired, nothing to record"
        );
    }

    #[test]
    fn catchup_skip_is_not_the_same_as_firing_nothing() {
        // The distinction ADR-016 draws and the one most likely to be
        // misimplemented: `skip` declines to catch *up*, it does not decline to
        // run. A schedule that stopped running after any restart would be a very
        // quiet way to lose a job.
        let mut d = due("0 * * * *", "2026-03-05T00:00:00Z");
        d.catchup = "skip".into();
        // Wide enough that the misfire window is not what drops them — the
        // window is applied first and would otherwise take the credit, and the
        // reason recorded against each occurrence is what an operator reads.
        d.misfire_window = Duration::hours(24);

        let p = plan(&d_slice(&d), utc("2026-03-05T04:00:30Z"));
        assert_eq!(p[0].fire, vec![utc("2026-03-05T04:00:00Z")]);
        assert!(
            p[0].skip
                .iter()
                .any(|(_, r)| *r == SkipReason::CatchUpPolicy),
            "the occurrences it declined to catch up are on the record"
        );
    }

    #[test]
    fn an_unparseable_expression_is_unschedulable_not_retried() {
        // The row is claimed on every sweep while it is due, so "log it and move
        // on" is a hot loop rather than resilience.
        let p = plan(
            &[due("not a cron", "2026-03-01T05:00:00Z")],
            utc("2026-03-01T05:00:02Z"),
        );
        assert!(p[0].unschedulable.is_some());
        assert!(p[0].fire.is_empty());
        assert_eq!(p[0].advance_to, None);
    }

    #[test]
    fn an_expression_that_never_occurs_is_unschedulable() {
        // `0 0 30 2 *` parses cleanly and names a day February does not have.
        let p = plan(
            &[due("0 0 30 2 *", "2026-03-01T05:00:00Z")],
            utc("2026-03-01T05:00:02Z"),
        );
        assert!(
            p[0].unschedulable
                .as_deref()
                .unwrap()
                .contains("no occurrence"),
            "got {:?}",
            p[0].unschedulable
        );
    }

    #[test]
    fn a_vanished_time_zone_is_unschedulable_rather_than_silently_utc() {
        // tzdata can drop a zone under a running server. Falling back to UTC
        // would keep the schedule firing at a time nobody chose, which is worse
        // than stopping: "09:00 in Berlin" is a business requirement (ADR-016).
        let mut d = due("0 9 * * *", "2026-03-01T05:00:00Z");
        d.tz = "Mars/Olympus_Mons".into();
        let p = plan(&d_slice(&d), utc("2026-03-01T05:00:02Z"));
        assert!(p[0].unschedulable.is_some());
    }

    #[test]
    fn the_next_fire_is_always_strictly_after_now() {
        // The anti-spin invariant, across a spread of schedules and a sweep that
        // arrives late. A plan that advanced to `now` or earlier would be
        // re-claimed immediately and the scheduler would stop being a scheduler.
        let exprs = [
            "* * * * *",
            "0 * * * *",
            "0 0 * * *",
            "*/5 * * * *",
            "0 0 1 * *",
        ];
        let now = utc("2026-03-01T05:07:31Z");
        for e in exprs {
            for late in [0, 1, 60, 3600, 86_400 * 40] {
                let d = DueSchedule {
                    next_fire_at: now - Duration::seconds(late),
                    ..due(e, "2026-03-01T00:00:00Z")
                };
                let p = plan(&d_slice(&d), now);
                let next = p[0].advance_to.expect(e);
                assert!(
                    next > now,
                    "{e} late by {late}s advanced to {next}, not after {now}"
                );
            }
        }
    }

    #[test]
    fn every_planned_occurrence_is_accounted_for_exactly_once() {
        // Fire and skip must partition the occurrences considered. An occurrence
        // in neither list is one the ledger will never hear about; an occurrence
        // in both would fire and be reported as skipped.
        let mut d = due("* * * * *", "2026-03-01T05:00:00Z");
        d.catchup = "all".into();
        d.catchup_limit = 5;
        d.misfire_window = Duration::hours(2);
        let p = plan(&d_slice(&d), utc("2026-03-01T05:30:30Z"));

        let mut seen: Vec<DateTime<Utc>> = p[0].fire.clone();
        seen.extend(p[0].skip.iter().map(|(t, _)| *t));
        let n = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), n, "an occurrence appears in both fire and skip");
        assert_eq!(p[0].fire.len(), 5, "the limit is respected");
    }

    #[test]
    fn planning_is_per_schedule_and_one_bad_row_does_not_stop_the_sweep() {
        // A single hand-edited row must not take the fleet's scheduling with it.
        let plans = plan(
            &[
                due("0 * * * *", "2026-03-01T05:00:00Z"),
                due("garbage", "2026-03-01T05:00:00Z"),
                due("*/30 * * * *", "2026-03-01T05:00:00Z"),
            ],
            utc("2026-03-01T05:00:02Z"),
        );
        assert_eq!(plans.len(), 3);
        assert!(plans[0].unschedulable.is_none());
        assert!(plans[1].unschedulable.is_some());
        assert!(plans[2].unschedulable.is_none());
        assert_eq!(plans[2].fire, vec![utc("2026-03-01T05:00:00Z")]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn next(expr: &str, tz: &str, from: &str) -> String {
        Schedule::parse(expr, tz)
            .unwrap()
            .next_after(utc(from))
            .unwrap()
            .to_rfc3339()
    }

    // ------------------------------------------------------------ parsing

    #[test]
    fn the_protocols_own_examples_parse() {
        // §3 shows `0 3 * * *` in Europe/London. If the spec's own example does
        // not parse, the spec documents something the engine cannot do.
        assert!(Schedule::parse("0 3 * * *", "Europe/London").is_ok());
        assert!(Schedule::parse("*/5 * * * *", "UTC").is_ok());
        assert!(Schedule::parse("0 0 1 1 *", "America/New_York").is_ok());
    }

    #[test]
    fn field_forms_expand_correctly() {
        let s = Schedule::parse("0,15,30,45 9-17 * * MON-FRI", "UTC").unwrap();
        assert_eq!(s.minute.values, vec![0, 15, 30, 45]);
        assert_eq!(s.hour.values, (9..=17).collect::<Vec<_>>());
        assert_eq!(s.dow.values, vec![1, 2, 3, 4, 5]);

        let s = Schedule::parse("*/20 * * * *", "UTC").unwrap();
        assert_eq!(s.minute.values, vec![0, 20, 40]);

        // `5/15` is "from 5, every 15" — a range to the top of the field, not the
        // single value 5.
        let s = Schedule::parse("5/15 * * * *", "UTC").unwrap();
        assert_eq!(s.minute.values, vec![5, 20, 35, 50]);
    }

    #[test]
    fn day_of_week_seven_is_sunday() {
        // Dialects disagree about whether Sunday is 0 or 7. Normalising means the
        // caller never has to know, and `0 0 * * 0,7` is not two different days.
        let a = Schedule::parse("0 0 * * 7", "UTC").unwrap();
        let b = Schedule::parse("0 0 * * 0", "UTC").unwrap();
        assert_eq!(a.dow.values, b.dow.values);
    }

    #[test]
    fn dialect_extensions_are_rejected_by_name() {
        // `L`, `W` and `#` mean different things in different implementations. A
        // schedule that fires at the wrong time is worse than one that refuses
        // to register.
        for expr in ["0 0 L * *", "0 0 15W * *", "0 0 * * 5#3", "0 0 ? * *"] {
            let e = Schedule::parse(expr, "UTC").unwrap_err();
            assert!(
                e.to_string().contains("not supported"),
                "{expr} should be rejected by name, got {e}"
            );
        }
    }

    #[test]
    fn a_malformed_expression_names_the_field() {
        let e = Schedule::parse("0 99 * * *", "UTC").unwrap_err();
        assert!(e.to_string().contains("hour"), "{e}");
        assert!(e.to_string().contains("0–23"), "{e}");

        let e = Schedule::parse("0 0 * *", "UTC").unwrap_err();
        assert!(e.to_string().contains("5 fields"), "{e}");
    }

    #[test]
    fn an_unknown_time_zone_is_rejected() {
        let e = Schedule::parse("0 0 * * *", "Mars/Olympus").unwrap_err();
        assert!(e.to_string().contains("IANA"), "{e}");
    }

    // ------------------------------------------------------------ next

    #[test]
    fn simple_schedules_advance() {
        assert_eq!(
            next("*/15 * * * *", "UTC", "2026-03-01T10:02:00Z"),
            "2026-03-01T10:15:00+00:00"
        );
        assert_eq!(
            next("0 3 * * *", "UTC", "2026-03-01T10:00:00Z"),
            "2026-03-02T03:00:00+00:00"
        );
        assert_eq!(
            next("0 0 1 * *", "UTC", "2026-03-15T00:00:00Z"),
            "2026-04-01T00:00:00+00:00"
        );
    }

    #[test]
    fn the_boundary_is_strictly_after() {
        // A schedule already exactly at the current instant must not return that
        // instant, or a sweep that runs twice in one second fires it twice.
        assert_eq!(
            next("0 * * * *", "UTC", "2026-03-01T10:00:00Z"),
            "2026-03-01T11:00:00+00:00"
        );
    }

    #[test]
    fn a_zone_offset_is_applied() {
        // 03:00 in New York is 08:00 UTC in March (EDT, UTC-4).
        assert_eq!(
            next("0 3 * * *", "America/New_York", "2026-03-20T00:00:00Z"),
            "2026-03-20T07:00:00+00:00"
        );
    }

    #[test]
    fn an_unsatisfiable_expression_terminates() {
        // "The 30th of February" parses and never happens. An unbounded search
        // would hang the sweep; returning None lets the caller disable it.
        let s = Schedule::parse("0 0 30 2 *", "UTC").unwrap();
        assert_eq!(s.next_after(utc("2026-01-01T00:00:00Z")), None);
    }

    #[test]
    fn a_rare_but_real_date_is_found() {
        // 29 February. Bounded search must still reach a leap year.
        let s = Schedule::parse("0 0 29 2 *", "UTC").unwrap();
        let n = s.next_after(utc("2026-03-01T00:00:00Z")).unwrap();
        assert_eq!(n.to_rfc3339(), "2028-02-29T00:00:00+00:00");
    }

    // ------------------------------------------------------------ dom/dow

    #[test]
    // The capital OR is deliberate: this is the rule readers misremember as AND.
    #[allow(non_snake_case)]
    fn day_of_month_and_day_of_week_are_ORed_when_both_are_restricted() {
        // The classic cron footgun. `0 0 13 * 5` is "the 13th, OR any Friday" —
        // not "Friday the 13th". Reading it the other way makes a job run far
        // less often than intended, and the mistake is invisible until someone
        // notices the missing reports.
        let s = Schedule::parse("0 0 13 * 5", "UTC").unwrap();

        // 2026-03-13 is a Friday: matches both. 2026-04-13 is a Monday: matches
        // only the day-of-month. 2026-03-06 is a Friday: matches only the
        // day-of-week. All three must fire.
        assert!(s.day_matches(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap()));
        assert!(s.day_matches(NaiveDate::from_ymd_opt(2026, 4, 13).unwrap()));
        assert!(s.day_matches(NaiveDate::from_ymd_opt(2026, 3, 6).unwrap()));
        // A Tuesday that is not the 13th must not.
        assert!(!s.day_matches(NaiveDate::from_ymd_opt(2026, 3, 10).unwrap()));
    }

    #[test]
    fn one_restricted_day_field_is_used_alone() {
        let dom_only = Schedule::parse("0 0 13 * *", "UTC").unwrap();
        assert!(dom_only.day_matches(NaiveDate::from_ymd_opt(2026, 4, 13).unwrap()));
        assert!(!dom_only.day_matches(NaiveDate::from_ymd_opt(2026, 3, 6).unwrap()));

        let dow_only = Schedule::parse("0 0 * * 5", "UTC").unwrap();
        assert!(dow_only.day_matches(NaiveDate::from_ymd_opt(2026, 3, 6).unwrap()));
        assert!(!dow_only.day_matches(NaiveDate::from_ymd_opt(2026, 4, 13).unwrap()));
    }

    // ------------------------------------------------------------ DST

    #[test]
    fn a_nonexistent_local_time_is_skipped_not_shifted() {
        // Europe/London springs forward at 01:00 on 2026-03-29: 01:30 does not
        // exist that day. §3.1 says the occurrence is skipped. Shifting it to
        // 02:30 instead would run the job an hour late, once a year, silently.
        let s = Schedule::parse("30 1 * * *", "Europe/London").unwrap();

        // The 28th's occurrence is 01:30 GMT = 01:30 UTC, and is the last one
        // before the transition.
        let before = s.next_after(utc("2026-03-27T12:00:00Z")).unwrap();
        assert_eq!(before.to_rfc3339(), "2026-03-28T01:30:00+00:00");

        // The next one skips the 29th's nonexistent 01:30 entirely and lands on
        // the 30th at 01:30 BST = 00:30 UTC. Note what is NOT here: an
        // occurrence on the 29th at any time. A shift-forward implementation
        // would produce 2026-03-29T01:30:00+01:00 (00:30 UTC) and look plausible.
        let after = s.next_after(before).unwrap();
        assert_eq!(
            after.to_rfc3339(),
            "2026-03-30T00:30:00+00:00",
            "the nonexistent 01:30 on the 29th must be skipped, not shifted"
        );
        assert_ne!(
            after.date_naive(),
            NaiveDate::from_ymd_opt(2026, 3, 29).unwrap()
        );
    }

    #[test]
    fn an_ambiguous_local_time_fires_once_on_the_first_occurrence() {
        // Europe/London falls back at 02:00 on 2026-10-25: 01:30 happens twice,
        // at 00:30 UTC (BST) and 01:30 UTC (GMT). It must fire ONCE, on the
        // first. Firing both would double-execute a daily job once a year, which
        // is exactly what a durable engine exists to prevent.
        let s = Schedule::parse("30 1 * * *", "Europe/London").unwrap();
        let n = s.next_after(utc("2026-10-24T12:00:00Z")).unwrap();
        assert_eq!(
            n.to_rfc3339(),
            "2026-10-25T00:30:00+00:00",
            "the pre-transition instant"
        );

        // The next occurrence is the following day, not the second 01:30.
        let after = s.next_after(n).unwrap();
        assert_eq!(
            after.to_rfc3339(),
            "2026-10-26T01:30:00+00:00",
            "the repeated 01:30 must not fire a second time"
        );
    }

    #[test]
    fn an_hourly_schedule_across_a_fall_back_does_not_repeat_an_hour() {
        // An hourly job on the fall-back day: the 01:00 hour happens twice in
        // local time. Each *instant* must fire once.
        let s = Schedule::parse("0 * * * *", "Europe/London").unwrap();
        let mut cursor = utc("2026-10-24T22:00:00Z");
        let mut fired = Vec::new();
        for _ in 0..8 {
            cursor = s.next_after(cursor).unwrap();
            fired.push(cursor);
        }
        let mut sorted = fired.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            fired.len(),
            sorted.len(),
            "no instant fired twice: {fired:?}"
        );
        for w in fired.windows(2) {
            assert!(w[1] > w[0], "occurrences must strictly advance");
        }
    }

    #[test]
    fn a_utc_schedule_is_unaffected_by_any_transition() {
        // The control: UTC has no transitions, so the same expression is a clean
        // 24-hour cadence across the dates that break the London tests above.
        let s = Schedule::parse("30 1 * * *", "UTC").unwrap();
        let a = s.next_after(utc("2026-03-28T12:00:00Z")).unwrap();
        let b = s.next_after(a).unwrap();
        assert_eq!(b - a, Duration::hours(24));
    }

    // ------------------------------------------------------------ catch-up

    fn every_hour() -> Schedule {
        Schedule::parse("0 * * * *", "UTC").unwrap()
    }

    #[test]
    fn the_normal_path_fires_exactly_one_occurrence() {
        // Nothing was missed: the sweep runs a moment after the due time.
        let d = decide(
            &every_hour(),
            utc("2026-03-01T10:00:00Z"),
            utc("2026-03-01T10:00:05Z"),
            CatchUpPolicy::default(),
        );
        assert_eq!(d.fire, vec![utc("2026-03-01T10:00:00Z")]);
        assert!(d.skipped.is_empty());
        assert_eq!(d.next_fire_at, Some(utc("2026-03-01T11:00:00Z")));
    }

    #[test]
    fn catchup_one_fires_the_most_recent_missed_occurrence() {
        // Down for five hours. `one` is the default because it is what almost
        // every job wants: a nightly report that missed three nights needs
        // running, not running three times. The *most recent* so that a job
        // computing "yesterday's totals" gets the right yesterday.
        let d = decide(
            &every_hour(),
            utc("2026-03-01T05:00:00Z"),
            utc("2026-03-01T10:00:30Z"),
            CatchUpPolicy {
                misfire_window: Duration::hours(24),
                ..Default::default()
            },
        );
        assert_eq!(d.fire, vec![utc("2026-03-01T10:00:00Z")]);
        assert_eq!(d.skipped.len(), 5);
        assert!(d
            .skipped
            .iter()
            .all(|(_, r)| *r == SkipReason::CatchUpPolicy));
    }

    #[test]
    fn catchup_all_fires_every_missed_occurrence_up_to_the_limit() {
        let d = decide(
            &every_hour(),
            utc("2026-03-01T05:00:00Z"),
            utc("2026-03-01T10:00:30Z"),
            CatchUpPolicy {
                catchup: CatchUp::All,
                limit: 10,
                misfire_window: Duration::hours(24),
            },
        );
        assert_eq!(d.fire.len(), 6, "05:00 through 10:00 inclusive");
        assert_eq!(d.fire[0], utc("2026-03-01T05:00:00Z"));
        assert_eq!(*d.fire.last().unwrap(), utc("2026-03-01T10:00:00Z"));
    }

    #[test]
    fn a_long_outage_on_a_frequent_schedule_still_fires_the_newest() {
        // Thirty days of missed minutes: 43,200 occurrences. The bug this pins is
        // a forward scan with a limit, which returns the FIRST n and makes the
        // engine fire a month-old backlog while stamping it with today's date.
        let every_minute = Schedule::parse("* * * * *", "UTC").unwrap();
        let due = utc("2026-02-01T00:00:00Z");
        let now = utc("2026-03-03T00:00:30Z");

        let d = decide(
            &every_minute,
            due,
            now,
            CatchUpPolicy {
                catchup: CatchUp::All,
                limit: 3,
                misfire_window: Duration::days(60),
            },
        );

        assert_eq!(d.fire.len(), 3);
        assert_eq!(*d.fire.last().unwrap(), utc("2026-03-03T00:00:00Z"));
        assert_eq!(d.fire[0], utc("2026-03-02T23:58:00Z"));
        // …and the truncation is on the record rather than silent.
        assert!(d
            .skipped
            .iter()
            .any(|(_, r)| *r == SkipReason::CatchUpLimit));
    }

    #[test]
    fn a_long_outage_under_catchup_one_fires_the_most_recent_occurrence() {
        // The reason `One` is the default: whatever the outage, recovery costs
        // one run, and it is the run whose window is the one the operator cares
        // about.
        let every_minute = Schedule::parse("* * * * *", "UTC").unwrap();
        let d = decide(
            &every_minute,
            utc("2026-02-01T00:00:00Z"),
            utc("2026-03-03T00:00:30Z"),
            CatchUpPolicy {
                catchup: CatchUp::One,
                limit: 10,
                misfire_window: Duration::days(60),
            },
        );
        assert_eq!(d.fire, vec![utc("2026-03-03T00:00:00Z")]);
        assert_eq!(d.next_fire_at, Some(utc("2026-03-03T00:01:00Z")));
    }

    #[test]
    fn the_catchup_limit_drops_the_oldest_not_the_newest() {
        // A job that processes a window per fire needs the recent windows more
        // than the ancient ones — and the ancient ones are what an operator is
        // most likely to want to re-run deliberately.
        let d = decide(
            &every_hour(),
            utc("2026-03-01T00:00:00Z"),
            utc("2026-03-01T10:00:30Z"),
            CatchUpPolicy {
                catchup: CatchUp::All,
                limit: 3,
                misfire_window: Duration::hours(24),
            },
        );
        assert_eq!(d.fire.len(), 3);
        assert_eq!(
            *d.fire.last().unwrap(),
            utc("2026-03-01T10:00:00Z"),
            "newest kept"
        );
        assert!(d
            .skipped
            .iter()
            .any(|(_, r)| *r == SkipReason::CatchUpLimit));
    }

    #[test]
    fn catchup_skip_still_fires_the_current_occurrence() {
        // "Skip" means "do not catch up", not "do not run". A job that never ran
        // again after any restart would be a surprising reading of the word, and
        // a very quiet way to lose a schedule.
        let d = decide(
            &every_hour(),
            utc("2026-03-01T05:00:00Z"),
            utc("2026-03-01T10:00:30Z"),
            CatchUpPolicy {
                catchup: CatchUp::Skip,
                misfire_window: Duration::hours(24),
                ..Default::default()
            },
        );
        assert_eq!(d.fire, vec![utc("2026-03-01T10:00:00Z")]);
        assert_eq!(d.skipped.len(), 5);
    }

    #[test]
    fn the_misfire_window_beats_the_catchup_policy() {
        // `all` with a generous limit must not fire a week of occurrences at 3am
        // on Monday. The window is what makes "how stale is too stale" a separate
        // decision from "how many".
        let d = decide(
            &every_hour(),
            utc("2026-03-01T00:00:00Z"),
            utc("2026-03-01T10:00:30Z"),
            CatchUpPolicy {
                catchup: CatchUp::All,
                limit: 1000,
                misfire_window: Duration::hours(2),
            },
        );
        assert!(
            d.fire.iter().all(|t| *t >= utc("2026-03-01T08:00:00Z")),
            "nothing older than the window may fire: {:?}",
            d.fire
        );
        assert!(d
            .skipped
            .iter()
            .any(|(_, r)| *r == SkipReason::MisfireWindow));
    }

    #[test]
    fn every_skipped_occurrence_carries_a_reason() {
        // The audit trail is the only way an operator can answer "why did the
        // 03:00 run not happen?" after the fact.
        let d = decide(
            &every_hour(),
            utc("2026-03-01T00:00:00Z"),
            utc("2026-03-01T10:00:30Z"),
            CatchUpPolicy {
                catchup: CatchUp::One,
                limit: 10,
                misfire_window: Duration::hours(3),
            },
        );
        assert!(!d.skipped.is_empty());
        for (_, r) in &d.skipped {
            assert!(!r.as_str().is_empty());
        }
        // And they are ordered, so the console can render them as a timeline.
        let times: Vec<_> = d.skipped.iter().map(|(t, _)| *t).collect();
        let mut sorted = times.clone();
        sorted.sort();
        assert_eq!(times, sorted);
    }

    #[test]
    fn the_next_fire_time_always_advances_past_now() {
        // Otherwise the sweep re-reads the same schedule immediately and spins.
        let now = utc("2026-03-01T10:00:30Z");
        for catchup in [CatchUp::One, CatchUp::Skip, CatchUp::All] {
            let d = decide(
                &every_hour(),
                utc("2026-03-01T05:00:00Z"),
                now,
                CatchUpPolicy {
                    catchup,
                    limit: 10,
                    misfire_window: Duration::hours(24),
                },
            );
            assert!(
                d.next_fire_at.unwrap() > now,
                "{catchup:?} produced a stale next_fire_at"
            );
        }
    }

    #[test]
    fn catchup_parses_and_round_trips() {
        for s in ["one", "skip", "all"] {
            assert_eq!(CatchUp::parse(s).as_str(), s);
        }
        // An unrecognised value from an older SDK must not stop the schedule
        // firing at all.
        assert_eq!(CatchUp::parse("nonsense"), CatchUp::One);
    }
}
