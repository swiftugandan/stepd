//! Structured parallelism (protocol §6.1 rule 2).
//!
//! ## What this adds over `futures::join!`
//!
//! Because [`Ctx::step`](crate::Ctx::step) claims eagerly, `futures::join!` is
//! *already* safe: every hash was fixed before any member was polled. That is
//! the point of the design, and it means users are not one forgotten import away
//! from silent corruption.
//!
//! `ctx.join` exists for the two things eager claiming cannot do by itself:
//!
//! 1. **Reject a repeated step id inside one group.** Two members with the same
//!    id get occurrences 0 and 1 in declaration order, which is stable — but it
//!    is almost never what the developer meant. A loop that fans out must supply
//!    an explicit discriminator, and silently numbering them makes a real bug
//!    look like it works until the loop's length changes.
//! 2. **Emit every member in one envelope.** Awaiting members one at a time
//!    yields after the first, so a five-way fan-out costs five round trips
//!    instead of one.
//!
//! The uniqueness check runs before any member is polled. It can, because the
//! closures passed to `ctx.step` have been *called* — producing futures — but
//! their bodies have not run, so nothing has happened yet that a rejection would
//! have to undo.

use crate::{Ctx, Halt, StepError, StepFuture, StepResult};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

/// Why a group was rejected.
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    /// The same step id appears more than once in one group.
    #[error("ambiguous_step_id: '{0}' appears more than once in one parallel group")]
    DuplicateId(String),
}

/// A member of a parallel group.
///
/// Implemented only by [`StepFuture`], so a group cannot accidentally contain an
/// ordinary future whose hash was never claimed.
pub trait Member: Future + Unpin {
    /// The developer-supplied step id.
    fn member_id(&self) -> &str;
}

impl<'a, T> Member for StepFuture<'a, T>
where
    StepFuture<'a, T>: Future + Unpin,
{
    fn member_id(&self) -> &str {
        self.step_id()
    }
}

/// Poll every member to completion, then decide the group's outcome.
///
/// The outcome rules mirror the `all_settled` join policy the server applies to
/// the resulting batch (protocol §5.2.1): every member reaches a terminal state
/// before the group resolves, and a member's failure is surfaced rather than
/// cancelling its siblings. Returning early on the first failure would leave
/// siblings that had already executed unrecorded, which is the one thing a
/// durable engine must never do — the work happened and nothing remembers it.
fn group_outcome<T>(results: Vec<StepResult<T>>) -> StepResult<Vec<T>> {
    let mut values = Vec::with_capacity(results.len());
    let mut failure: Option<StepError> = None;
    let mut yielded = false;

    for r in results {
        match r {
            Ok(v) => values.push(v),
            Err(StepError::Halt(Halt::Yield(_))) => yielded = true,
            Err(e @ StepError::Halt(Halt::Fatal(_))) => return Err(e),
            Err(e) => failure = Some(failure.unwrap_or(e)),
        }
    }

    // A genuine failure outranks a yield: the handler should see the error, not
    // be replayed into the same failing step forever.
    if let Some(e) = failure {
        return Err(e);
    }
    if yielded {
        return Err(StepError::Halt(Halt::Yield(0)));
    }
    Ok(values)
}

/// Await a homogeneous group.
pub struct JoinAll<F: Future> {
    members: Vec<Option<F>>,
    results: Vec<Option<F::Output>>,
    rejected: Option<StepError>,
}

impl<F: Member> JoinAll<F>
where
    F::Output: Unpin,
{
    fn new(members: Vec<F>) -> Self {
        // Uniqueness first, before anything is polled.
        let mut seen: Vec<&str> = Vec::with_capacity(members.len());
        let mut rejected = None;
        for m in &members {
            if seen.contains(&m.member_id()) {
                rejected = Some(StepError::Halt(Halt::Fatal(
                    JoinError::DuplicateId(m.member_id().to_string()).to_string()
                        + ". Add a discriminator to the id, e.g. format!(\"charge-{invoice_id}\") \
                           (protocol §6.1 rule 3)",
                )));
                break;
            }
            seen.push(m.member_id());
        }
        let n = members.len();
        Self {
            members: members.into_iter().map(Some).collect(),
            results: (0..n).map(|_| None).collect(),
            rejected,
        }
    }
}

impl<F: Member> Future for JoinAll<F>
where
    F::Output: Unpin,
{
    type Output = Vec<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut all_done = true;
        for (i, slot) in this.members.iter_mut().enumerate() {
            if let Some(f) = slot {
                match Pin::new(f).poll(cx) {
                    Poll::Ready(v) => {
                        this.results[i] = Some(v);
                        *slot = None;
                    }
                    Poll::Pending => all_done = false,
                }
            }
        }
        if !all_done {
            return Poll::Pending;
        }
        Poll::Ready(this.results.iter_mut().map(|r| r.take().unwrap()).collect())
    }
}

impl Ctx {
    /// Run a homogeneous group of steps concurrently, emitting them in one
    /// envelope.
    ///
    /// Ids must be unique within the group; a repeat is a non-retryable
    /// `ambiguous_step_id` error raised before any member runs.
    pub fn join_all<'a, T>(
        &'a self,
        members: Vec<StepFuture<'a, T>>,
    ) -> impl Future<Output = StepResult<Vec<T>>> + 'a
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Unpin + 'a,
    {
        let mut j = JoinAll::new(members);
        let rejected = j.rejected.take();
        async move {
            if let Some(e) = rejected {
                return Err(e);
            }
            group_outcome(j.await)
        }
    }
}

/// Await a heterogeneous group.
///
/// Generated for tuples of two to six members. Six because beyond that a tuple
/// stops being readable and a `Vec` with a discriminator in each id is the
/// clearer expression of what the code is doing.
macro_rules! join_tuple {
    ($name:ident, $trait_fn:ident, $($t:ident $i:tt),+) => {
        impl Ctx {
            /// Run a heterogeneous group of steps concurrently, emitting them in
            /// one envelope. Ids must be unique within the group.
            pub fn $trait_fn<'a, $($t),+>(
                &'a self,
                members: ($(StepFuture<'a, $t>,)+),
            ) -> impl Future<Output = StepResult<($($t,)+)>> + 'a
            where
                $($t: serde::Serialize + serde::de::DeserializeOwned + Unpin + 'a,)+
            {
                // Uniqueness before any poll, exactly as in `join_all`.
                let ids: Vec<String> = vec![$(members.$i.step_id().to_string()),+];
                let mut dup = None;
                for (n, id) in ids.iter().enumerate() {
                    if ids[..n].contains(id) {
                        dup = Some(id.clone());
                        break;
                    }
                }
                async move {
                    if let Some(id) = dup {
                        return Err(StepError::Halt(Halt::Fatal(
                            JoinError::DuplicateId(id).to_string()
                                + ". Add a discriminator to the id (protocol §6.1 rule 3)",
                        )));
                    }
                    let mut members = members;
                    let mut done = ($(Option::<StepResult<$t>>::None,)+);
                    std::future::poll_fn(|cx| {
                        let mut all = true;
                        $(
                            if done.$i.is_none() {
                                match Pin::new(&mut members.$i).poll(cx) {
                                    Poll::Ready(v) => done.$i = Some(v),
                                    Poll::Pending => all = false,
                                }
                            }
                        )+
                        if all { Poll::Ready(()) } else { Poll::Pending }
                    })
                    .await;

                    // Same outcome rules as `group_outcome`, but the values have
                    // distinct types so they cannot go through a Vec.
                    let mut failure: Option<StepError> = None;
                    let mut yielded = false;
                    // Bound to the tuple position rather than the type
                    // parameter's name: reusing `A` as a value binding is legal
                    // but warns, and a warning nobody can fix is a warning
                    // everybody learns to ignore.
                    $(
                        #[allow(non_snake_case)]
                        let $t = match done.$i.take().unwrap() {
                            Ok(v) => Some(v),
                            Err(StepError::Halt(Halt::Yield(_))) => { yielded = true; None }
                            Err(e @ StepError::Halt(Halt::Fatal(_))) => return Err(e),
                            Err(e) => { failure = failure.or(Some(e)); None }
                        };
                    )+
                    if let Some(e) = failure { return Err(e); }
                    if yielded { return Err(StepError::Halt(Halt::Yield(0))); }
                    Ok(($($t.unwrap(),)+))
                }
            }
        }
    };
}

join_tuple!(Join2, join, A 0, B 1);
join_tuple!(Join3, join3, A 0, B 1, C 2);
join_tuple!(Join4, join4, A 0, B 1, C 2, D 3);
join_tuple!(Join5, join5, A 0, B 1, C 2, D 3, E 4);
join_tuple!(Join6, join6, A 0, B 1, C 2, D 3, E 4, F 5);
