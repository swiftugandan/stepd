# Runbook: a run is not making progress

| | |
|---|---|
| Severity | S2 — one workflow is late; if it is keyed, everything behind that key is late too |
| Requirement | A1 (protocol §7.6), F-DL-7; invariant P3 |
| Read this | when a run has sat in the same state longer than it should |

---

## Read this paragraph first

**`runs.status` tells you which of five things is wrong, and the remedy for each
is actively harmful for the other four.** A `waiting` run is not stuck in any
sense the engine can fix; a `pending` run may be healthy and simply backing off.
Read the status before you retry, cancel or restore anything.

There is no `stepd retry` or `stepd cancel` on the command line. `stepd run <id>`
reads. Everything that mutates goes through the API, which writes to
`commands_audit`; the same thing done in SQL leaves no record of who did it.

---

## 0. Establish the state

```bash
stepd run 018f2c31-....          # status, journal, pending waits, restore banner
```

```sql
SELECT r.status, r.fn_id, r.key, r.attempt_no, r.error->>'code' AS err,
       q.available_at, q.claimed_by, q.claimed_until,
       (SELECT count(*) FROM run_steps s
         WHERE s.run_id = r.id AND s.status = 'pending') AS blocking_steps
  FROM runs r LEFT JOIN queue q ON q.run_id = r.id
 WHERE r.id = '018f2c31-....';
```

`blocking_steps` is what the diagnosis turns on: `fire_due_timers`,
`deliver_to_inbox` and `resolve_child_result` each requeue a run only when it
reaches zero. A run with three parallel ops and one still `pending` is correctly
parked, and waking it by hand replays the handler against a journal it will not
recognise.

---

## 1. `waiting` — an event that never came

```sql
SELECT step_hash, event_type, expr, since, expires_at
  FROM waits WHERE run_id = $1 AND resolved_at IS NULL;

SELECT id, type, key, received_at FROM events
 WHERE ns = 'prod' AND type = 'payment.captured'
   AND received_at > now() - interval '2 hours' ORDER BY received_at DESC LIMIT 20;
```

`expires_at IS NULL` means the app registered the wait with no timeout, so
nothing will ever fire it — `commit_ops` schedules a `wait_timeout` timer only
when the op carried `timeout_at`. That is a workflow bug, not an engine fault.

No matching event: the producer is at fault, ingest returned 429 (see
`backlog.md`), or it deduplicated against an earlier event with the same
`stepdidempotency`. Event present and still parked: correlation did not match.
`correlate_event` needs `waits.event_type` equal and, when both are set,
`runs.key = events.key` — an approval for order 4711 must not resolve 4712's.

### 1a. The signal arrived and the run is still parked

The one case here that is an engine fault:

```sql
-- Invariant P3. This must always return zero rows.
SELECT w.run_id, w.event_type
  FROM waits w JOIN runs r ON r.id = w.run_id
 WHERE w.resolved_at IS NULL
   AND r.status NOT IN ('completed','failed','cancelled')
   AND EXISTS (SELECT 1 FROM run_inbox i
                WHERE i.run_id = w.run_id AND i.event_type = w.event_type
                  AND i.consumed_by_step_hash IS NULL);
```

A run holding an unconsumed inbox entry matching its own open wait is a lost
signal. `deliver_to_inbox` and `commit_ops` both take `FOR UPDATE` on the run row
as their **first** statement, and that explicit pairing is what makes this state
unreachable. Rows here mean the schema was altered: run `stepd doctor`, read the
`invariants` check, `stepd migrate`, and only then touch the run — unsticking one
run leaves the race open and it will eat the next signal too. This is what
`engine/rust/tests/sql/test_invariants.sql` and simulation property P3 exist to prevent.

Signals from other runs are not delivered inline; they go to `signal_outbox` and
are drained by the housekeeping loop, because inline delivery would take a second
run row lock while holding the sender's and deadlock two runs signalling each
other. A persistent `SELECT count(*) FROM signal_outbox WHERE delivered_at IS NULL`
means no server is running that loop (`stepd doctor` calls it `signals`).

### Remedy

Inject the event. This goes through `deliver_to_inbox`, the path a real event
takes, so FIFO ordering, the early-signal rules and the serialization lock all
still apply and you cannot create a state a real event could not:

```bash
curl -sS -X POST "$STEPD_URL/v1/runs/$RUN/resolve-wait" \
  -H "Authorization: Bearer $STEPD_TOKEN" -H 'Content-Type: application/json' \
  -d '{"event_type":"payment.captured","data":{"amount":1200}}'
```

`command` comes back as `resolve_wait:resolved` (wait satisfied, run requeued),
`:buffered` (nothing matched, held for later — you used the wrong `event_type`;
compare it to `waits.event_type` exactly) or `:duplicate`.

---

## 2. `sleeping` — a timer that has not fired

```sql
SELECT kind, fire_at, fired_at, now() - fire_at AS overdue
  FROM timers WHERE run_id = $1 ORDER BY fire_at;

SELECT count(*) FROM timers WHERE fired_at IS NULL AND fire_at < now();
```

`fire_at` in the future is normal; sleeps carry up to 60 s of jitter by default
so a million midnight timers do not arrive together. Past `fire_at` with
`fired_at IS NULL`, and a fleet-wide count that is large and growing, is the
housekeeping loop not running anywhere. Every `stepd serve` process runs both
loops, so an API that answers while timers do not fire means the loops died
inside a live process — restart the replicas. Do not call `fire_due_timers()` by
hand as the fix: it is the function the loop calls, so if it works when you run
it, the loop is not running, and that is the fault to repair.

---

## 3. `pending` and never claimed

`pending` is not an error state.

```sql
SELECT available_at, available_at > now() AS in_the_future,
       claimed_by, attempts, priority FROM queue WHERE run_id = $1;
```

| What you see | What it means |
|---|---|
| No `queue` row | Dequeued and not requeued — cancelled, quarantined or terminal. Re-read `runs.status` |
| `available_at` future, `attempts` climbing | **Retry backoff.** A failed attempt sets the run back to `pending` and pushes `available_at` out. Read `runs.error`: this run is failing, not stalled |
| `available_at` ≈ `now() + 5s`, repeatedly | The app's circuit is open and dispatch is deferring. See `backlog.md` |
| `available_at` past, nothing claiming | Real backlog, or no dispatcher. See `backlog.md` |

No registered app looks like nothing at all — the dispatcher cannot resolve a
target and the run simply sits:

```sql
SELECT f.fn_id, f.paused, f.archived_at, a.app_id, a.url, a.last_seen
  FROM functions f JOIN app_bindings a ON a.id = f.app_binding_id
 WHERE f.ns = 'prod' AND f.fn_id = 'order-fulfilment';
```

Zero rows: no app has ever registered that function and the run can never be
driven. `paused = true` stops new runs starting from triggers, not existing ones.
`archived_at` set is fine — archived functions are still resolved deliberately,
so removing one from a manifest does not strand its in-flight runs.

---

## 4. `running` with an expired lease

The worker died or was partitioned away. The fence was bumped at claim time, so a
late response from the dead worker cannot commit; you do not have to defend
against it.

```sql
SELECT lease_owner, lease_until, now() - lease_until AS expired_for
  FROM runs WHERE id = $1;
SELECT reclaim_expired_leases(100);   -- returns how many were reclaimed
```

The housekeeping loop does this every pass, so needing to run it by hand *is* the
finding. Expect a trickle regardless: the default lease is 60 s, the default
attempt timeout is also 60 s, and nothing heartbeats a lease while an attempt is
in flight. An app slower than the lease has its run reclaimed and re-dispatched
underneath it, and its eventual response discarded as a stale fence — the work
happened twice, the record was written once. Correct, and still a double charge
for a non-idempotent step. A persistent `leases` finding from `stepd doctor`
against a healthy fleet means a longer lease or faster steps, not more workers.

---

## 5. `quarantined`

Enough consecutive failed attempts that the dispatcher removed the run from
dispatch. It consumes no capacity and makes no progress. Go to `poison-pill.md`.
Do not retry first: `retry_run` clears `error_signature` and puts the run
straight back on the queue to fail identically.

---

## Which remedy, given what you found

| State | Do this | Do **not** |
|---|---|---|
| `waiting`, event never ingested | Fix the producer; inject with `resolve-wait` only if the business needs it now | Cancel — the run is healthy and resolves when the event arrives |
| `waiting`, ingested but key mismatch | Inject with `resolve-wait` | Re-ingest — it dedupes, or resolves someone else's wait |
| `waiting` **and** holding a matching inbox entry | `stepd doctor`, re-apply migrations, *then* unstick | Unstick and move on; the race stays open |
| `sleeping`, timer overdue | Restart the servers so the loop runs | Call `fire_due_timers()` and call it fixed |
| `pending`, `available_at` future | Read `runs.error`; go to `poison-pill.md` | Force `available_at = now()` — you strip the backoff protecting a failing app |
| `pending`, no function registered | Deploy and register the app | Cancel; the runs drive fine once it is back |
| `running`, lease expired | `reclaim_expired_leases`, then find out why the loop stopped | Nothing else — the fence already protects you |
| `quarantined` | `poison-pill.md` | Blind retry |

---

## What to say, and when

**In the incident channel, once you have the status:**

> `order-fulfilment` run 018f2c31 has been `waiting` on `payment.captured` since
> 09:14. The event was never ingested, so this is upstream of stepd. <N> runs of
> the same function are in the same state. I can inject the event per run with
> resolve-wait once payments confirm the captures actually happened — not before,
> because injecting resumes a workflow that ships goods.

**If the P3 query returns rows**, escalate rather than patch: that is a schema
integrity finding affecting every run in the namespace, not one stuck run.

---

## Preventing this

1. **Always give `wait_event` a timeout.** No `expires_at` means no timer and no
   upper bound. A timeout turns a silent stall into a `timed_out` step the
   handler can branch on.
2. **Alert on `stepd doctor`'s exit code.** Non-zero on any critical finding, so
   it works as a cron check and a deploy gate without anyone reading prose.
3. **Alert on the P3 query, on undelivered `signal_outbox` rows, and on overdue
   timers.** The first is the only continuous check for the race the inbox design
   exists to close; the other two both mean "the housekeeping loop stopped",
   which otherwise presents as unrelated workflows quietly going late.
4. **Make `timeouts.attempt` shorter than the lease**, not equal to it. Equal is
   the default and guarantees a race on every slow attempt.

---

## Related

- `docs/runbooks/backlog.md` — when many runs are `pending`, not one
- `docs/runbooks/poison-pill.md` — repeated identical failure and quarantine
- `docs/runbooks/restore-hazard.md` — the large hammer, and why not to reach for it
- `docs/adr/011-durable-run-inbox.md` — the early-signal guarantee
- `docs/adr/009-fencing-and-leases.md` — why an expired lease is safe
- `spec/PROTOCOL.md` §7.6 — early signals and the lost-signal race
