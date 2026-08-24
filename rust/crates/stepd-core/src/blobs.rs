//! Finding `$blob` references in payload JSON (protocol §8.3).
//!
//! This is protocol logic, not storage logic: what a `$blob` looks like and
//! where one may appear is fixed by §8.3.1, and the answer is the same whatever
//! holds the bytes or the index. It lives here so the engine and the store share
//! one walk rather than keeping two in step.
//!
//! Postgres has a second copy, `blob_ids_in` in migration `0011_blob_refs.sql`,
//! and that one is deliberate: it runs inside the trigger that records
//! references in the same transaction as the step carrying them, which a Rust
//! function cannot do. Finding A13 in `docs/GAPS.md` is why that trigger exists
//! — the reference machinery was all present and nothing called any of it.

use stepd_proto::{Event, Op};
use uuid::Uuid;

/// Walk a JSON value and collect every `$blob` id it references.
///
/// Recursive over objects and arrays because a step result is arbitrary JSON
/// and a blob can be nested anywhere in it — a `$blob` inside an array inside
/// an object is the ordinary payload shape, not a corner case.
///
/// Ids are appended in document order and are not deduplicated; callers that
/// care sort and dedup.
pub fn blob_ids(value: &serde_json::Value, out: &mut Vec<Uuid>) {
    match value {
        serde_json::Value::Object(m) => {
            if let Some(b) = m.get("$blob") {
                if let Some(id) = b.get("id").and_then(|v| v.as_str()) {
                    if let Ok(u) = Uuid::parse_str(id) {
                        out.push(u);
                    }
                }
            }
            for v in m.values() {
                blob_ids(v, out);
            }
        }
        serde_json::Value::Array(a) => {
            for v in a {
                blob_ids(v, out);
            }
        }
        _ => {}
    }
}

/// Every `$blob` id carried by an op.
///
/// Every field destructured by name and no `..` anywhere, so that adding an op
/// variant or a JSON-bearing field to an existing one is a compile error here
/// rather than a reference that silently stops being found.
///
/// Wider than what the reference-recording triggers in `0011_blob_refs.sql`
/// cover — those walk `run_steps.result` and `runs.input`/`output`, so a blob
/// carried only in `meta`, `prompt` or a signalled event is verified by this
/// walk but not reference-counted, and is collected once its reservation window
/// or (after commit) its unreferenced window elapses.
pub fn op_blob_ids(op: &Op, out: &mut Vec<Uuid>) {
    fn walk(v: &Option<serde_json::Value>, out: &mut Vec<Uuid>) {
        if let Some(v) = v {
            blob_ids(v, out);
        }
    }
    match op {
        Op::Step {
            id: _,
            hash: _,
            data,
            meta,
            error: _,
        } => {
            walk(data, out);
            walk(meta, out);
        }
        Op::Sleep {
            id: _,
            hash: _,
            until: _,
        } => {}
        Op::WaitEvent {
            id: _,
            hash: _,
            event: _,
            since: _,
            timeout_at: _,
            prompt,
        } => walk(prompt, out),
        Op::Invoke {
            id: _,
            hash: _,
            function: _,
            input,
            detach: _,
        } => walk(input, out),
        Op::Signal {
            id: _,
            hash: _,
            target_run: _,
            event,
        } => event_blob_ids(event, out),
        Op::ContinueAsNew {
            id: _,
            hash: _,
            input,
        } => walk(input, out),
        Op::Done { data } => walk(data, out),
        // `ErrorBody` is `code`, `message`, `stack` and `attempts` — no JSON,
        // so nothing to walk.
        Op::Error {
            retryable: _,
            step: _,
            error: _,
        } => {}
    }
}

/// Every `$blob` id carried by an event's payload.
pub fn event_blob_ids(event: &Event, out: &mut Vec<Uuid>) {
    blob_ids(&event.data, out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_references_are_found_wherever_they_are_nested() {
        let id = Uuid::now_v7();
        let v = serde_json::json!({
            "receipts": [ { "file": { "$blob": { "id": id, "size": 1, "sha256": "ab" } } } ],
            "note": "no blob here"
        });
        let mut out = vec![];
        blob_ids(&v, &mut out);
        assert_eq!(
            out,
            vec![id],
            "a blob nested in an array inside an object must be found"
        );
    }

    #[test]
    fn a_step_result_and_a_done_output_both_yield_their_references() {
        let in_step = Uuid::now_v7();
        let in_done = Uuid::now_v7();
        let mut out = vec![];
        op_blob_ids(
            &Op::Step {
                id: "s".into(),
                hash: "h".into(),
                data: Some(serde_json::json!({ "$blob": { "id": in_step } })),
                meta: None,
                error: None,
            },
            &mut out,
        );
        op_blob_ids(
            &Op::Done {
                data: Some(serde_json::json!([{ "$blob": { "id": in_done } }])),
            },
            &mut out,
        );
        assert_eq!(out, vec![in_step, in_done]);
    }

    #[test]
    fn a_signalled_events_payload_is_walked_like_any_other_event() {
        let id = Uuid::now_v7();
        let event = Event::new(
            "order.shipped",
            "test",
            serde_json::json!({ "$blob": { "id": id } }),
        );
        let mut out = vec![];
        op_blob_ids(
            &Op::Signal {
                id: "sig".into(),
                hash: "h".into(),
                target_run: Uuid::now_v7(),
                event,
            },
            &mut out,
        );
        assert_eq!(out, vec![id], "a signal carries a payload like any event");
    }

    #[test]
    fn an_op_with_no_payload_yields_nothing() {
        let mut out = vec![];
        op_blob_ids(
            &Op::Sleep {
                id: "s".into(),
                hash: "h".into(),
                until: chrono::Utc::now(),
            },
            &mut out,
        );
        assert!(out.is_empty());
    }
}
