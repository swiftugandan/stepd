# Runbook: a run, or a class of runs, fails identically and repeatedly

| | |
|---|---|
| Severity | S2 — a class of work is not completing, and blind retries turn it into an afternoon |
| Requirement | B4 (F-LP-6, F-LP-7) |
| Read this | when the DLQ grows, or `stepd doctor` reports quarantined runs |

---

## Read this paragraph first

**Retry resumes a run from its journal; it does not re-run it.** Every recorded
step is memoised and skipped, and the run picks up at the first step it never
completed. So retrying a poison pill without changing anything replays exactly
the same failure, at cost, and clears the `error_signature` that told you what
the failure was. Decide *why* it failed first. There are failures no retry can
ever fix, and the error code tells you which ones.

---

## 1. See the shape of it

```bash
curl -sS "$STEPD_URL/v1/dlq?limit=200" -H "Authorization: Bearer $STEPD_TOKEN" | jq .
```

Each item carries `id`, `fn_id`, `status`, `key`, `error`, `error_signature`,
`quarantined_at` and `ended_at`. The endpoint returns `failed` and `quarantined`
runs newest first, capped at 200, with no cursor — despite its name it does not
group anything for you. Group it yourself, and group by **code** as well as by
signature:

```sql
SELECT error->>'code' AS code, error_signature, count(*),
       min(ended_at) AS first_seen, max(ended_at) AS last_seen,
       (array_agg(id ORDER BY ended_at DESC))[1:3] AS examples
  FROM runs
 WHERE ns = 'prod' AND status IN ('failed','quarantined')
   AND COALESCE(quarantined_at, ended_at) > now() - interval '6 hours'
 GROUP BY 1, 2 ORDER BY count(*) DESC;
```

Group by code as well because **`error_signature` includes the error message.**
It is `sha256(code ‖ 0x1F ‖ message)`, truncated to sixteen hex characters. One
poison pill whose message embeds an order id, a URL or a timestamp produces a
different signature per run and looks like a thousand unrelated failures. The
code is the stable part.

There is a second signature format in the same column. Rule violations detected
inside `commit_ops` are failed by `fail_run`, which sets the signature to
`sha256(code)` in full — sixty-four hex characters, over the code alone. So a
64-character signature is an engine-side rule violation and a 16-character one is
an app or transport failure. They never group together even when they should.

---

## 2. Understand what quarantine actually did

```sql
SELECT status, quarantined_at, attempt_no, error_signature, error
  FROM runs WHERE id = $1;
```

A quarantined run has been removed from `queue` entirely: it consumes no dispatch
capacity, holds no lease, and will never be attempted again until someone acts.
`stepd doctor` counts them under `quarantine`.

The threshold is `quarantine_after` in `DispatchConfig`, **20**, and it is a
compile-time default — `Server::build` does not override it and no environment
variable reads it. Two things about it are worth knowing at 3am:

* It counts **attempts, not identical failures.** The signature is recorded on
  every failure but never compared, so a run that fails twenty times for twenty
  different reasons is quarantined the same as a true poison pill. Read the
  errors before concluding you have found a class.
* A quarantined run is not terminal for the purposes of cancellation. Both
  `retry` and `cancel` accept it.

---

## 3. The error codes an SDK can produce

These three come from the SDK itself, not from your handler, and all three are
non-retryable. Retrying them without a code change is guaranteed to fail again.

**`swallowed_halt`** — the handler returned `Ok` after a step had yielded. In the
Rust SDK a step that is not yet recorded halts the pass by returning an error
through `?`; user code that absorbs it (`let _ = ctx.step(..)`, `.ok()`,
`.unwrap_or_default()`, a catch-all `match` arm) then carries on executing
against state that does not exist, and returns success for a run whose middle
never happened. The SDK detects this at the end of the pass and fails the run
instead of committing a `done`. **Fix**: propagate step results with `?`. The
journal is intact; once the code is fixed the run resumes correctly from it.

**`memo_decode_failed`** — a step's *recorded* result no longer deserialises into
the type the handler now expects. Someone changed a step's return type in a
deploy, and every in-flight run holding an old-shaped result now fails on replay.
The SDK deliberately does **not** re-execute the step: its side effect already
happened, and re-running it to obtain a value of the new type would repeat that
effect. **Fix**: revert the type, and the runs resume; or give the step a new id,
which orphans the old result and re-executes it, which is a deliberate decision
about a side effect and not a formality. This is the one code where retrying
without a deploy is not merely useless but infinite.

**`protocol_violation`** — the handler broke a step-identity rule, so the SDK
refused to guess a hash. A guessed hash silently re-executes work that was
already recorded, which is worse than failing. Two causes, distinguishable by the
message: a step claimed its occurrence outside the sequential replay pass (a step
created on another task — use `ctx.join`, which assigns every hash up front), or
`ambiguous_step_id`, the same step id used twice inside one parallel group (add a
discriminator, `format!("charge-{id}")`). **Fix**: a code change, always.

Other codes you will see in the same view: `transport` and `invalid_envelope`
from the dispatcher; `timed_out` and `cancelled` from step outcomes;
`output_not_serialisable` from the handler's return value; and the engine's own
rule violations — `step_limit_exceeded`, `invoke_depth_exceeded`,
`invoke_fanout_exceeded`, `invoke_cycle`, `continue_as_new_with_live_children`,
`chain_limit_exceeded`, `unknown_op`. The limits behind those are rows you can
read, and raise, without a deploy:

```bash
stepd limits
```

```sql
UPDATE engine_limits SET value = 2000 WHERE name = 'invoke_fanout';
```

---

## 4. Fix and retry, or cancel

| The failure is | Do this |
|---|---|
| `memo_decode_failed`, `swallowed_halt`, `protocol_violation` | **Deploy the fix first.** Retrying before the deploy burns the attempt and clears the signature |
| A transient dependency that has recovered (`transport`, 5xx) | **Retry.** The run resumes at the failed step; completed steps are not re-executed |
| An engine limit (`invoke_fanout_exceeded`, `step_limit_exceeded`) | Raise the limit in `engine_limits` **or** fix the workflow, then retry. Raising a limit that the workflow is genuinely abusing just moves the failure later |
| Bad input that will never be valid (a malformed order, a deleted customer) | **Cancel.** No amount of retrying makes the input valid, and each attempt costs an app call |
| The work has already been done by hand or by another system | **Cancel**, and record why. A retry would repeat the side effects of every step after the failure point |
| Unknown, and the run is keyed | Cancel or retry promptly either way — a keyed run blocks every subsequent run on that key while it is not terminal |

Both commands are audited, both cascade correctly, and both are the same code
paths the console uses:

```bash
curl -sS -X POST "$STEPD_URL/v1/runs/$RUN/retry"  -H "Authorization: Bearer $STEPD_TOKEN"
curl -sS -X POST "$STEPD_URL/v1/runs/$RUN/cancel" -H "Authorization: Bearer $STEPD_TOKEN"
```

`retry` sets the run back to `pending`, clears `error`, `error_signature` and
`quarantined_at`, and requeues it. `cancel` marks it cancelled and cascades to
every non-detached descendant, so a cancelled parent does not leave a child
running against state nobody is going to look at.

---

## 5. Bulk operations

There is no bulk endpoint. Pull the ids for one signature and loop:

```bash
psql "$STEPD_DATABASE_URL" -tAc "
  SELECT id FROM runs
   WHERE ns='prod' AND status='quarantined'
     AND error->>'code' = 'transport'
     AND quarantined_at > now() - interval '3 hours'" \
| xargs -P4 -I{} curl -sS -o /dev/null -w '%{http_code} {}\n' \
    -X POST "$STEPD_URL/v1/runs/{}/retry" -H "Authorization: Bearer $STEPD_TOKEN"
```

Do it through the API rather than in SQL, for two reasons. The audit log:
`commands_audit` is how "who retried four thousand runs at 03:40?" gets an answer
other than "someone". And correctness: the obvious SQL —

```sql
-- WRONG. Does nothing.
UPDATE runs SET status = 'pending' WHERE status = 'quarantined';
```

— sets a status nothing reads. Dispatch claims from `queue`, and quarantine
deleted those rows. The runs sit in `pending` forever, now with their error
cleared, so you have lost the diagnosis as well. There is no `retry_run` SQL
function to fall back on — retry is implemented in the server, not in the
schema — so working in SQL means doing both halves yourself, in one transaction:

```sql
BEGIN;
WITH revived AS (
  UPDATE runs SET status='pending', ended_at=NULL, error=NULL,
                  quarantined_at=NULL, error_signature=NULL
   WHERE ns='prod' AND status='quarantined' AND error->>'code'='transport'
  RETURNING id, ns, fn_id, key)
INSERT INTO queue (ns, fn_id, key, run_id)
SELECT ns, fn_id, key, id FROM revived
ON CONFLICT (run_id) DO UPDATE
   SET claimed_by=NULL, claimed_until=NULL, available_at=now();
COMMIT;
```

That is what `POST /v1/runs/{id}/retry` does per run, minus the audit row.

**Retry a sample of ten before the other four thousand.** If the fix is wrong you
have burned ten app calls instead of four thousand, and quarantine will not save
you the second time: the attempt counter is already near its ceiling, so the
whole batch quarantines again almost immediately.

---

## What to say, and when

**In the incident channel, once you have the grouping:**

> 4 120 `order-fulfilment` runs are quarantined since 02:10, all with code
> `memo_decode_failed` on step `reserve-stock`. This is the 02:05 deploy: it
> changed that step's return type, and every in-flight run holds a result in the
> old shape. The steps themselves already ran — stock **is** reserved — so the SDK
> is correctly refusing to re-execute them. I am rolling the deploy back rather
> than retrying; retrying before the rollback fails identically and costs us the
> error signatures. No customer-visible duplication so far, and there will be
> none if we roll back rather than re-id the step.

---

## Preventing this

1. **Give every failure a stable `code` and keep variable detail out of the
   message.** The signature hashes the message, so an order id in it defeats
   grouping exactly when grouping matters most.
2. **Never change a step's return type in place.** It fails every in-flight run
   with `memo_decode_failed`, and the only two exits are a rollback or a
   deliberate re-execution. Add a new step id instead, or widen the type first.
3. **Propagate step results with `?`.** Every `swallowed_halt` in the DLQ is a
   `let _ =` or an `.unwrap_or_default()` somewhere.
4. **Use `ctx.join` for parallelism, never `join!` on ad-hoc tasks.** It assigns
   hashes up front and rejects duplicate ids at the call site rather than
   producing a `protocol_violation` in production.
5. **Alert on quarantined-run count, not on DLQ size.** Failures come and go;
   quarantine means twenty attempts have already been spent and something has
   stopped moving.

---

## Related

- `docs/runbooks/stuck-run.md` — when the run is parked rather than failing
- `docs/runbooks/backlog.md` — when the queue depth is retry backoff, not arrivals
- `docs/runbooks/upgrade.md` — deploys that create this class of failure
- `docs/adr/012-eager-occurrence-claiming.md` — why a wrong hash is worse than a failure
- `docs/adr/001-execution-model.md` — memoisation, and what retry actually replays
- `docs/GAPS.md` B4 — quarantine and the DLQ as specified
