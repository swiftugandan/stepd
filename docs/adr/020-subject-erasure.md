# ADR-020: Subject erasure and redaction model

| | |
|---|---|
| Status | Proposed |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Gap D5 records that the design had no GDPR subject-erasure path at all, rated S2.
A durable workflow engine is an awkward place to acquire one, because personal
data does not sit in one column: it arrives in an event payload, is copied into a
run's input, copied again into step results as the handler transforms it, may be
written to a blob, and is echoed in the console. An erasure that misses any of
those has not erased anything — it has made the remaining copies harder to find.

Two properties of this system are in direct tension with erasure, and pretending
otherwise would be the more dangerous document.

**The journal is append-only by design.** `run_steps` is the memoisation
substrate: it is what makes a replayed attempt skip work it already did. Deleting
a step result does not merely remove data — it tells the next attempt the step
never ran, so the side effect executes again. Erasing a live run's journal and
at-least-once execution are not compatible, and it must be stated which gives.

**Backups are the point of backups.** A restore from before an erasure resurrects
the erased data, and continuous archiving (F-DL-4) puts it in WAL segments no
`DELETE` reaches. Any honest erasure claim is about live data plus a retention
horizon, never about every byte the operator holds.

## Decision

* **Subjects are declared, not inferred.** A function declares a `subject_expr`
  (CEL) so the engine knows what identifies a data subject in its payloads;
  heuristic scanning produces both misses and false positives, neither defensible
  to a regulator.
* **A `subject_index` maps a subject key to every entity referencing it** —
  `(ns, subject_key, entity_kind, entity_id)` — written at persist time, so
  erasure never needs a full scan of an event table partitioned across months.
* **Erasure is a durable, resumable job**, not a request-scoped operation. The
  `erasures` table records requester, state and a `progress_cursor`, so a crash
  mid-erasure resumes rather than restarts and "did this complete?" is answered
  by a row rather than by a log.
* **Terminal runs are erased; live runs are tombstoned.** Once a run is terminal
  its journal has no further use and the payloads are deleted. For a live run the
  step result becomes a tombstone preserving the step's *existence* and hash but
  not its content, because deleting the row would re-execute a side effect that
  has already happened. A handler reading a tombstone fails non-retryably — loud,
  which is correct here.
* **Blobs are dereferenced, not unconditionally deleted.** `blob_refs` holds the
  refcount and dedupe is per namespace, so the same bytes may serve another
  subject's run; bytes go when the refcount reaches zero, and the erasure is not
  complete until they do.
* **Redaction is the cheaper, better-placed control.** Per-namespace policy
  declares JSON paths redacted *before persistence* (F-SEC-6). Every field
  redacted at ingest is a field erasure never has to chase.
* **Every erasure is audited**, and the audit row records the subject key — itself
  personal data, retained deliberately as proof the erasure happened. **The backup
  horizon is documented, not hidden:** erased data persists in backups and WAL
  until they age out, and that window belongs in the retention policy rather than
  in an auditor's findings.

## Consequences

### What this makes easy
* An erasure request is one API call with a checkable completion state, not a
  bespoke script per incident.
* The index makes erasure cost proportional to the subject's footprint rather
  than to the size of the database.
* Redaction lets a namespace hold no personal data at all — a stronger position
  than being able to remove it.

### What this makes hard
* Tombstoning breaks replay for a live run. Intentional: an erasure request can
  fail an in-flight business process, and the alternative is re-charging a
  customer whose data was just erased.
* Detached partitions must be swept too, so erasure interacts with partition
  maintenance and cannot be a purely online operation.

### What we accept
* Erasure is not cryptographic. Without encryption at rest and per-subject keys
  (F-SEC-7), "erased" means "deleted from live storage", and the backup window
  above remains.
* The `subject_index` and the audit trail themselves hold subject keys. Erasure
  cannot erase its own evidence without becoming unprovable.

## Alternatives considered

| Option | Why not |
|---|---|
| Delete rows outright, live runs included | Deleting a step result tells the next attempt the step never ran, so an already-executed side effect executes again. Silent, and it is money. |
| Rely on retention expiry alone | A subject's request has a statutory deadline; a ninety-day retention window does not meet it, and long-running runs outlive the window anyway. |
| Crypto-shredding — per-subject keys, discard the key | The strongest answer, and it presupposes encryption at rest with per-subject key derivation (F-SEC-7), which does not exist. Recorded as the intended direction, not as this decision. |
| Scan payloads for personal-looking data | Both misses and false positives, neither defensible. A declared `subject_expr` makes the boundary explicit and reviewable. |
| Synchronous erasure inside the request | A subject with thousands of runs times out, and a timed-out erasure has no record of how far it got. |
| Erase backups too | Not achievable without destroying the recovery position. Documenting the horizon is the honest control. |

## Verification

**Not verified: there is no erasure implementation.** This ADR is `Proposed` and
records a design plus its open questions. What exists is schema only:

* `rust/migrations/0001_initial.sql` defines `subject_index`
  `(ns, subject_key, entity_kind, entity_id)` and `erasures` `(id, ns,
  subject_key, requested_by, requested_at, state, progress_cursor,
  completed_at)`, plus `events.subject_key` and `runs.subject_key` with the
  partial indexes `events_subject` and `runs_subject`. Nothing references any of
  them: `grep -rn subject_key --include=*.rs rust/` returns no matches at all.
* **No API.** `rust/crates/stepd-server/src/api.rs::router()` registers thirteen
  routes and none is the `DELETE /v1/namespaces/{ns}/subjects/{id}` of F-SEC-5.
  There is no erasure handler, no worker sweep in
  `rust/crates/stepd-core/src/housekeeper.rs`, and no `erase` command in the
  audit path.
* **No declaration either.** `subject_expr` appears in neither
  `spec/schemas/function-config.schema.json` nor
  `rust/crates/stepd-sdk/src/function.rs`, so a function currently has no way to
  say what identifies a subject in its payloads. The declaration side must be
  specified before the erasure side can be built.
* Redaction (F-SEC-6) is likewise schema-only: `namespaces.redaction_cfg` exists
  in 0001 and nothing reads it.

## Open questions

1. A live run whose journal is tombstoned — fail it, cancel it, or let it fail
   non-retryably at the next replay? Each is defensible; none is chosen.
2. Does erasure block on partition detachment, or sweep detached partitions
   asynchronously and report completion only afterwards?
3. Is the subject key in the audit log itself erasable? Erasing it destroys the
   proof, retaining it retains an identifier. A legal answer, not an engineering
   one.
4. Is crypto-shredding (F-SEC-7) a prerequisite for a defensible claim — making
   this ADR dependent on an unwritten one?
