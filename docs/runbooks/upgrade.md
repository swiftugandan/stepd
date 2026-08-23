# Runbook: upgrading stepd with runs in flight

| | |
|---|---|
| Severity | S2 — a bad upgrade strands in-flight runs or duplicates their side effects |
| Gap | C4 (F-DL-6) |
| Read this | **before** you plan the release, not during it |

---

## Read this paragraph first

**The schema is shared and unversioned across the whole cluster.** The engine's
correctness lives in SQL functions — `commit_ops`, `deliver_to_inbox`,
`cascade_cancel` — and a migration replaces them with `CREATE OR REPLACE`. The
moment `stepd migrate` commits, *every* replica starts calling the new bodies,
including the ones still running the old binary. Expand/contract is therefore a
rule about functions as much as about columns: the new body must be correct for
the old servers too, for as long as any of them are alive.

The second thing: **draining on SIGTERM is not necessary for correctness, and you
should still do it.** Leases expire and fencing makes an abrupt death safe — a
dead worker's late response cannot commit. But an undrained attempt is retried,
and its step re-executes. At-least-once is the contract, so that is correct, and
it also means every rolling deploy that skips the drain charges some customers
twice. Correct and expensive is still expensive.

---

## 1. Before you plan the release

```bash
stepd doctor                      # must be clean before, or you cannot attribute after
stepd limits                      # the engine limits in force, for the record
psql "$STEPD_DATABASE_URL" -c \
  "SELECT version, description, success FROM _sqlx_migrations ORDER BY version"
```

Read every migration in the release and classify it. Anything in the right-hand
column below means the release is not rollback-safe and must be split.

| Expand — safe with N-1 running | Contract — only after every N-1 replica is gone |
|---|---|
| Add a nullable column | Drop a column, or rename one |
| Add a table, an index (`CONCURRENTLY`), a new function | `DROP FUNCTION`, or change an existing one's signature |
| Add a parameter **at the end, with a default** | Add or reorder a parameter without a default |
| Add an enum value (own migration — see below) | Remove an enum value |
| `DROP NOT NULL`, widen a type | `SET NOT NULL`, narrow a type |
| Backfill data the old code ignores | Delete data the old code still reads |

Two real examples from this repository, because they are the shapes that bite:

* **Migration 0005** adds `continue_as_new` to the `step_op` enum and is marked
  `-- no-transaction` and kept in its own file, because PostgreSQL will not let a
  new enum value be *used* in the transaction that adds it. Folding it into 0006
  would fail on a fresh database and pass on an already-migrated one — a bug that
  only appears on the deployment that matters.
* **Migration 0006** drops the four-argument `commit_ops` rather than leaving it
  as an overload. That is a contract change, and it is only safe because the
  structural invariant test asserts exactly one `commit_ops` exists. Signature
  changes to that function are a flag day; the only in-place-compatible change is
  a new trailing parameter with a default, so an old server's shorter call still
  binds.

---

## 2. Apply the migration as its own release step

```bash
stepd migrate                     # exits when done; starts no server
```

`stepd migrate` is deliberately a separate command, not a flag on `serve`
(twelve-factor XII): a release step has to be runnable without starting a server,
and against a database whose server will not start. `stepd serve --migrate`
exists for single-node and development use — do not use it for a rolling deploy,
because N replicas then race the same migration at start-up and the outcome
becomes N interleaved log lines instead of one ordered, visible event.

Run it once, from one place, and confirm it before rolling anything:

```bash
stepd doctor                      # expect: schema — migration <n> applied
```

Then, in the same breath, confirm the serialization points survived:

```
[ok  ] invariants   serialization points intact, no advisory locks
```

A `FAIL invariants` here means the migration edited `commit_ops` or
`deliver_to_inbox` in a way that moved or removed the run-row lock, which
silently reopens the lost-signal race. Stop the release. Nothing else in the
upgrade matters more than this line.

---

## 3. Roll the servers

```bash
kubectl set image deploy/stepd stepd=stepd:<new>   # or your orchestrator's rolling update
```

During the window, N-1 and N replicas share one queue and drive one another's
runs. That is supported and it is the point of the rule above. Keep it short, and
keep these true throughout:

* **Signing keys overlap.** Set `STEPD_SIGNING_KEY` to the new key and
  `STEPD_SIGNING_KEY_PREVIOUS` to the old one before the roll begins; both are
  accepted, the first signs. A key change is otherwise a flag day.
* **Do not register apps against the new protocol during the window.** `PUT
  /v1/apps` rejects any manifest whose `protocol` is not exactly the server's own
  (`1`). A protocol major bump is a hard cutover in this build, not the N/N-1
  overlap the spec describes — roll the servers completely, then the apps.
* **Watch reclaim churn.** Restarting replicas is the main source of it:

```sql
SELECT count(*) FROM queue WHERE claimed_by IS NOT NULL AND claimed_until < now();
```

---

## 4. What SIGTERM actually does, and how long to allow

`wait_for_shutdown` catches SIGTERM (and Ctrl-C) and sets one shared flag. The
dispatch and housekeeping loops check it at the top of each iteration, so the
**current tick finishes**. A tick claims a batch and then awaits each attempt in
that batch *sequentially*, so the drain is bounded by the batch size times the
attempt latency — not by one attempt. With the compiled defaults, that is up to
16 attempts of up to 60 s each. There is no configurable drain timeout.

Practically:

* `terminationGracePeriodSeconds` must exceed a worst-case tick, not a worst-case
  attempt. Set it well above your app's p99 attempt latency times the batch size.
* A pod SIGKILLed before the tick finishes abandons its attempts. Their leases
  expire within 60 s, `reclaim_expired_leases` requeues them, another worker
  re-dispatches, and the abandoned steps re-execute.
* The cost of skipping the drain is roughly `replicas × attempts in flight` step
  re-executions per deploy. For idempotent steps that is free. For a payment
  capture without an idempotency key it is a duplicate charge, every deploy.

Before the roll, know the number you are risking:

```sql
SELECT claimed_by, count(*) FROM queue
 WHERE claimed_by IS NOT NULL AND claimed_until > now() GROUP BY 1;
```

---

## 5. After the roll

```bash
stepd doctor                      # non-zero exit on any critical finding — use it as a gate
```

Then check the things a migration is most likely to have disturbed:

```sql
-- Runs stranded by the roll: claimed by a replica that no longer exists.
SELECT claimed_by, count(*) FROM queue
 WHERE claimed_by IS NOT NULL AND claimed_until < now() GROUP BY 1;

-- Failures that started at the deploy: usually memo_decode_failed.
SELECT error->>'code', count(*) FROM runs
 WHERE ns='prod' AND status IN ('failed','quarantined')
   AND COALESCE(quarantined_at, ended_at) > now() - interval '30 minutes'
 GROUP BY 1 ORDER BY 2 DESC;
```

A cluster of `memo_decode_failed` immediately after a deploy means a step's
return type changed underneath in-flight runs. That is `poison-pill.md`, and the
fix is a rollback of the *app*, not of stepd.

Leave the contract half of the migration — the drops, the renames, the
`SET NOT NULL` — for a later release, once no N-1 replica remains. That deferral
is the entire reason the rule exists: it is what keeps the binary rollback
available for the hour when you need it.

---

## Can I roll back?

| The release contained | Roll back the binary? |
|---|---|
| Expand-only migrations, or none | **Yes.** The old binary reads the new schema fine |
| A new function, or a new trailing defaulted parameter | **Yes.** The old binary never calls it |
| A changed `commit_ops` / `deliver_to_inbox` body | **The binary, yes; the migration, no.** Re-apply the previous migration set and re-run `stepd doctor` before assuming the invariants hold |
| Any contract change | **No.** This is why contract changes ship alone, one release later |
| An app-side step type change | Roll back the **app**, not stepd. See `poison-pill.md` |

---

## What to say, and when

**In the release channel, before starting:**

> Upgrading `prod` stepd to <version>. The release is expand-only (one nullable
> column, one new index), so a binary rollback stays available. `stepd migrate`
> runs first as its own step, then `stepd doctor` — I will not roll any replica
> until the `invariants` check reads clean, because a migration that moves the
> run-row lock reopens the lost-signal race silently. Roll is one replica at a
> time with a 300 s grace period; there are currently <N> attempts in flight, and
> any that do not drain will have their step re-executed.

---

## Preventing the worst version of this

1. **Make step effects idempotent.** It converts the whole drain question from a
   correctness risk into a latency one, and it is the only mitigation that works
   for a SIGKILL, a node failure and a restore alike.
2. **Never change an existing SQL engine function's signature in a release that
   also ships servers calling it.** Add a trailing defaulted parameter, or add a
   new function and switch callers in the following release.
3. **Keep enum additions in their own `-- no-transaction` migration.** The failure
   mode is invisible on any database that has already been migrated.
4. **Run `stepd doctor` as a deployment gate**, before and after. It exits
   non-zero on any critical finding, so no one has to read and interpret it.
5. **Rehearse a rolling deploy under load** and count the re-executed steps. That
   number is the recurring cost of every future deploy, and it is much easier to
   argue for idempotency keys with it in hand.

---

## Related

- `docs/runbooks/restore-hazard.md` — the other way steps re-execute, and worse
- `docs/runbooks/poison-pill.md` — `memo_decode_failed` after a deploy
- `docs/runbooks/backlog.md` — reclaim churn and capacity during the roll
- `docs/adr/009-fencing-and-leases.md` — why abrupt death is safe but not free
- `docs/adr/019-pooler-safe-locking.md` — why no session state survives a restart
- `spec/PROTOCOL.md` §11 — versioning and compatibility
- `docs/GAPS.md` C4 — the gap register entry
