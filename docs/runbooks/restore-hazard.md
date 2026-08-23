# Runbook: point-in-time restore

| | |
|---|---|
| Severity | S2 — this procedure re-executes side effects that already happened |
| Gap | C3 (F-DL-5) |
| Read this | **before** you need it |

---

## Read this paragraph first

**Restoring a stepd database to an earlier point in time causes committed steps
to un-commit and their side effects to run a second time.** Cards get charged
twice. Emails get sent twice. Shipments get created twice.

This is not a defect in stepd. It is what durable execution *is*. The engine's
guarantee is "a recorded step is never re-executed", and a point-in-time restore
un-records steps. The guarantee is intact; the record it depends on was rewound
underneath it.

**Cron schedules re-fire too.** The at-most-once guarantee for a cron occurrence
is a primary key on `cron_fires`; a restore deletes rows from that table and
rewinds `cron_schedules.next_fire_at`. Occurrences that already fired become
eligible to fire again, and a schedule with `catchup: all` will fire every one of
them at once when the first server starts. Section 3a below is not optional.

Nothing in this runbook makes that untrue. What it does is make the blast radius
knowable, bounded, and visible to the people who have to explain it.

---

## Why this happens

A run's journal is the record of what has already been done. On every attempt
the app replays its handler from the top, skips every step the journal contains,
and executes the first one it does not.

```
  before restore     journal: { charge ✓, ship ✓ }   →  handler skips both
  restore to T                                          (T is before `ship`)
  after  restore     journal: { charge ✓ }          →  handler skips charge,
                                                         EXECUTES ship again
```

The parcel from the first `ship` is already on a van. stepd has no way to know
that: the only evidence was the journal row the restore removed.

The same applies to anything a step did — a payment, a webhook, an email, a
row written in another system, a message on a queue.

---

## Decide first: is a restore actually the right tool?

A restore is a very large hammer. Before reaching for it, check whether a
smaller instrument does the job.

| Situation | Better tool |
|---|---|
| One run is stuck on a wait | `stepd`'s **resolve-wait** command in the console |
| One run is failing repeatedly | Cancel it, fix the app, retry from the DLQ |
| A batch of runs failed identically | The DLQ view groups by error signature; bulk retry |
| A bad deploy started wrong runs | Cancel the runs; the cascade takes their children |
| Data in *another* system is wrong | Fix it there. stepd's journal is not the source of truth for your domain |
| The database is corrupt, or was deleted | **Restore.** This runbook. |

The only case that genuinely needs a restore is one where the stepd database
itself is lost or damaged. If your incident is "a workflow did the wrong thing",
a restore usually makes it worse — it re-does the wrong thing and some of the
right things.

---

## Procedure

### 0. Before you restore — capture the current state

Do this even in a hurry. It takes two minutes and it is the only record of what
was true before the rewind. Without it you cannot answer "what ran twice?"
afterwards, and that is the question you will be asked.

```bash
# The journal as it stands, for every run that will be affected.
psql "$STEPD_DATABASE_URL" -c "\copy (
  SELECT r.id, r.ns, r.fn_id, r.key, r.status, r.started_at, r.ended_at,
         s.step_id, s.step_hash, s.op, s.status AS step_status, s.ended_at AS step_ended
    FROM runs r LEFT JOIN run_steps s ON s.run_id = r.id
   WHERE r.started_at < now()
) TO 'pre-restore-journal.csv' CSV HEADER"

# And the events, so you can tell replayed work from genuinely new work.
psql "$STEPD_DATABASE_URL" -c "\copy (
  SELECT id, ns, type, source, received_at, idem FROM events
) TO 'pre-restore-events.csv' CSV HEADER"
```

Keep both files with the incident. They are the difference between a reconciliation
and a guess.

### 1. Stop the servers

Every `stepd serve` process, in every replica. Do this **before** the restore, not
after. A server still running against the restored database will start driving
rewound runs the instant it comes up, and it will do so before anyone has looked
at them.

```bash
# Whatever your orchestrator's equivalent is:
kubectl scale deploy/stepd --replicas=0
```

Confirm nothing is claiming work:

```sql
SELECT count(*) FROM queue WHERE claimed_by IS NOT NULL AND claimed_until > now();
-- expect 0 after the lease duration has elapsed
```

### 2. Restore

Follow your database provider's point-in-time restore procedure. Restore to a
**new** database or instance rather than in place, if you have the option: it
keeps the damaged original available for comparison, and comparison is how you
work out what ran twice.

Record the exact restore target time. Everything below depends on it.

### 3. Mark the rewound runs — before any server starts

This is the step that makes the hazard visible instead of silent. Runs that were
in flight at the restore point are the ones whose steps may re-execute.

```sql
-- Substitute your actual restore target.
\set restore_point '2026-08-23 14:07:00+00'

UPDATE runs
   SET restored_at = now()
 WHERE status NOT IN ('completed', 'failed', 'cancelled')
    OR ended_at > :'restore_point';
```

`restored_at` is surfaced in the console's run list and by `stepd run <id>`, which
prints a warning banner. Anyone who opens one of these runs from now on is told
what happened to it, without having to have read this document.

Count them, and put the number in the incident channel:

```sql
SELECT ns, fn_id, count(*) FROM runs WHERE restored_at IS NOT NULL
 GROUP BY ns, fn_id ORDER BY count(*) DESC;
```

### 3a. Pause every cron schedule — before any server starts

Same reasoning as the step above, different mechanism, and it is easy to miss
because no run exists yet to be marked.

The `cron_fires` ledger is what makes an occurrence fire at most once. The
restore removed the rows written after the restore point and rewound
`next_fire_at` to wherever it was. Every occurrence between the restore point and
now therefore looks unfired — and it is not: it fired, and its side effects
happened, and they are outside the database.

The first housekeeping pass after start-up will fire them. A schedule with
`catchup: all` fires up to its limit at once; one with `catchup: one` fires a
single stale occurrence. Both are re-executions of work that already happened,
and neither is marked `restored_at`, because the runs did not exist at the
restore point to be marked.

```sql
-- Do this while the servers are still down.
UPDATE cron_schedules SET paused = true,
       last_error = 'paused for point-in-time restore ' || now()::text
 WHERE NOT paused;
```

Then look at what is about to happen, before deciding to unpause:

```sql
-- How stale is each schedule, and what would it do about it?
SELECT ns, fn_id, expr, tz, catchup, catchup_limit,
       next_fire_at,
       now() - next_fire_at AS behind,
       misfire_window,
       now() - next_fire_at > misfire_window AS beyond_the_window
  FROM cron_schedules
 ORDER BY behind DESC;
```

`beyond_the_window` is the good case: those occurrences will be skipped and
recorded as `skipped_misfire_window` rather than fired. For the rest, decide per
schedule, using the same three questions as section 4 — is the work idempotent,
is a second execution harmful, is the occurrence still meaningful?

To resume a schedule *without* firing anything it missed, move its next fire
forward before unpausing:

```sql
-- Resume, next occurrence only, nothing caught up.
-- (`POST /v1/schedules/{id}/resume` does exactly this, and re-parses the
-- expression first; prefer it if the API is up.)
UPDATE cron_schedules
   SET paused = false, last_error = NULL,
       next_fire_at = now() + interval '1 minute'
 WHERE id = '...';
```

That is deliberately a lie about `next_fire_at` — it is not an occurrence of the
expression — and the next sweep corrects it, firing that one instant and then
advancing to a real occurrence. If firing even once is unacceptable, set it to a
time past the schedule's own next occurrence instead and accept losing one.

Finally: **do not delete `cron_fires` rows to tidy up.** Those rows are the only
record of which occurrences already fired, and after a restore they are the
evidence you will want during reconciliation. `trim_cron_fires` will remove them
on its own schedule, and it will not touch anything inside a misfire window.

### 4. Decide, per function, what to do with them

This is a judgement call and it belongs to whoever owns the workflow, not to
whoever is running the restore. The question for each function is: **what does
its steps re-executing actually do?**

| The function's steps are… | Do this |
|---|---|
| Idempotent (they use `ctx.idempotency_key()` or a provider-side key) | **Let them run.** The provider deduplicates. This is what the idempotency key is for, and this is the moment it pays for itself. |
| Naturally safe to repeat (reads, recomputation, writes keyed by run id) | Let them run. |
| Not idempotent, and the effect is expensive or externally visible (charges, shipments, emails) | **Cancel them and reconcile by hand.** See below. |
| Unknown | Treat as not idempotent. Being wrong the other way is unrecoverable. |

Cancel the ones you are not letting run, before starting the servers:

```sql
SELECT cancel_run(ns, id) FROM runs
 WHERE restored_at IS NOT NULL AND fn_id = 'order-fulfilment';
```

`cancel_run` cascades to non-detached children, so a cancelled parent does not
leave a child running against the same rewound state.

### 5. Sanity-check the schema before starting

A restore can bring back an *older schema* along with the data — including a
version of `deliver_to_inbox` from before the explicit serialization lock, which
would silently reopen the lost-signal race.

```bash
stepd doctor
```

The `invariants` check is the one that matters here. If it reports a violation,
re-apply the migrations before starting any server:

```bash
stepd migrate
stepd doctor   # expect: invariants — serialization points intact
```

Also confirm the partition for the current month exists; a restore from before a
month boundary will not have it, and ingest fails outright without one.

The `cron` and `cron-lag` checks will report every schedule you paused in step
3a. That is expected at this point in the procedure, and it is the reminder that
they are still paused — do not clear them until you have worked through section
3a's decision.

### 6. Start one server, watch it, then scale up

One replica first. Watch the run list for the functions you decided to let run.
Confirm the steps that re-execute are the ones you expected, and that the
idempotency keys are doing their job at the provider.

```bash
kubectl scale deploy/stepd --replicas=1
stepd run <one-of-the-restored-run-ids>
```

Only then restore full capacity.

### 7. Reconcile

For every function you decided was not idempotent, someone has to compare the
pre-restore journal against the external system and find the duplicates. The CSV
from step 0 is the input. There is no automated path here, and pretending
otherwise is how duplicates reach customers.

`stepd`'s side of it:

```sql
-- Steps that were recorded before the restore and are gone now: these are the
-- effects that happened and are no longer remembered.
-- Load pre-restore-journal.csv into a scratch table first.
SELECT p.run_id, p.step_id, p.step_ended
  FROM pre_restore p
  LEFT JOIN run_steps s ON s.run_id = p.run_id AND s.step_hash = p.step_hash
 WHERE s.step_hash IS NULL
 ORDER BY p.step_ended;
```

Also reconcile the schedules. A cron fire creates a run with the intended
occurrence in its input, which is what lets you tell a replay from a new fire —
`started_at` is when recovery happened, not what the schedule meant:

```sql
SELECT r.fn_id,
       r.input -> 'cron' ->> 'occurrence_at' AS occurrence,
       r.started_at, r.status
  FROM runs r
 WHERE r.input -> 'cron' IS NOT NULL
   AND r.started_at > :'restore_point'
 ORDER BY r.fn_id, occurrence;
```

Any row whose `occurrence` is earlier than the restore point is a re-fire of work
that already happened. Those are the ones to check against the downstream system.

### 8. Clear the markers when reconciliation is done

Not before. The marker is what tells the next person to read the run carefully.

```sql
UPDATE runs SET restored_at = NULL WHERE restored_at IS NOT NULL AND ns = 'prod';
```

---

## What to say, and when

**In the incident channel, immediately after step 3:**

> We have restored the stepd database to <time>. <N> runs were in flight at that
> point and their already-completed steps may execute a second time. I am going
> through them by function now. Functions with idempotent steps will be allowed to
> resume; the rest are being cancelled for manual reconciliation. Nothing is
> starting until that decision is made.

**To anyone who asks whether this is a stepd bug:** no. Durable execution's
guarantee is that a *recorded* step never re-executes, and a point-in-time restore
un-records steps. Every durable execution engine has this property. The mitigation
is idempotent steps, and it has to be in place before the incident, not during it.

---

## Preventing the worst version of this

None of these are things you can do during an incident. They are the reason to
read this document early.

1. **Use `ctx.idempotency_key()` for every step with an external effect.** The key
   is derived from the run id and the step hash, both of which survive a crash,
   a retry *and* a restore. A step that uses one is safe to re-execute, and this
   entire runbook becomes a formality for it.
2. **Prefer reserve/confirm to a single irreversible call**, where the provider
   offers it. A repeated reserve is free; a repeated capture is not.
3. **Rehearse this.** A restore you have never practised takes hours and produces
   the decisions in step 4 under pressure, which is exactly when they are made
   badly. The rehearsal also tells you your real RTO, which is otherwise a number
   somebody wrote in a document.
4. **Keep the pre-restore capture in step 0 scripted**, not a thing to be typed
   during an incident.
5. **Know which of your functions are idempotent before you need to.** Step 4 is a
   table you can fill in today, on a calm afternoon, and paste into the incident
   channel in thirty seconds.

---

## Related

- `docs/adr/010-payload-tiering.md` — blob lifecycle after a restore
- `docs/runbooks/stuck-run.md` — the smaller instruments in the table above
- `spec/PROTOCOL.md` §7.1 — at-least-once execution, the contract this rests on
- `docs/GAPS.md` C3 — the gap register entry, and why it is documented rather
  than fixed
