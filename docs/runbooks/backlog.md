# Runbook: the queue is deep and runs are late

| | |
|---|---|
| Severity | S2 — every workflow in the namespace is late, including the ones nobody is watching |
| Requirement | B3 (F-LP-5), B5 (F-LP-8) |
| Read this | when `oldest_seconds` is climbing, or ingest starts returning 429 |

---

## Read this paragraph first

**Work out whether nothing is claiming or everything is claimed, before you scale
anything.** Those are opposite faults with opposite fixes, and the wrong fix
makes each worse: adding servers to a saturated app drives its circuit breaker
open and collapses throughput to a probe every fifteen seconds; adding app
replicas when the servers are the bottleneck adds nothing but database
connections. `/v1/queue/stats` distinguishes them in one call.

Second thing to know: **almost nothing in stepd is tunable at runtime.** Batch
size, lease duration, attempt timeout, poll interval and the backpressure
threshold are compile-time constants — `Config::from_env` reads only the database
URL, bind address, worker name, pool size, signing keys and egress policy. Your
levers during an incident are the number of `stepd serve` replicas, pausing
functions, `queue.priority`, and the rows in `engine_limits`. Plan accordingly.

---

## 1. Read the queue

```bash
curl -sS "$STEPD_URL/v1/queue/stats" -H "Authorization: Bearer $STEPD_TOKEN" | jq .
```

```jsonc
{"functions":[
  {"function_id":"order-fulfilment","backlog":41200,"in_flight":48,"oldest_seconds":900}
]}
```

* **`backlog`** — `queue` rows with `claimed_by IS NULL`. Nobody has picked them up.
* **`in_flight`** — rows with `claimed_by IS NOT NULL`. A server holds a lease and
  is calling, or about to call, the app.
* **`oldest_seconds`** — `now()` minus the earliest `available_at` among unclaimed
  rows: how long the most overdue piece of work has been ready and untouched.
  This is the number that corresponds to what a customer experiences. `backlog`
  alone does not — a hundred thousand rows that turn over in ten seconds is not
  an incident.

The same view straight from the database, with the split that matters most —
work that is genuinely due versus work merely scheduled for later:

```sql
SELECT ns, fn_id,
       count(*) FILTER (WHERE claimed_by IS NULL AND available_at <= now()) AS due_now,
       count(*) FILTER (WHERE claimed_by IS NULL AND available_at >  now()) AS backing_off,
       count(*) FILTER (WHERE claimed_by IS NOT NULL)                       AS in_flight,
       count(*) FILTER (WHERE claimed_by IS NOT NULL AND claimed_until < now()) AS lease_expired
  FROM queue GROUP BY ns, fn_id ORDER BY due_now DESC LIMIT 20;
```

`backing_off` is retry backoff, not backlog. A queue that is 90% `backing_off` is
a *failure* incident, not a capacity one — go to `poison-pill.md`. `lease_expired`
counts work claimed by a server that has not come back; a non-zero steady state
means the housekeeping loop is not reclaiming, and the count is dead capacity.

---

## 2. Backlog, or in-flight?

One server drives its claimed leases **sequentially** — `tick_namespace` claims a
batch, then awaits each attempt in turn. Attempt concurrency across a deployment
is therefore roughly the number of `stepd serve` replicas, not the batch size.
That single fact decides most of this runbook.

| Signature | Diagnosis |
|---|---|
| `backlog` large, `in_flight` ≈ 0 | **Nothing is claiming.** No server is running the dispatch loop, or every function's app is unresolvable, or all work is in a namespace no replica reaches |
| `backlog` large, `in_flight` steady and small, app idle | **Servers are the bottleneck.** Add replicas |
| `backlog` large, `in_flight` steady, app at capacity | **The app is the bottleneck.** Adding servers makes it worse |
| `backlog` large, `in_flight` large, `lease_expired` large | Attempts are outrunning the 60 s lease. Runs are being reclaimed and re-executed mid-flight; throughput is being spent twice |
| `backlog` ≈ 0, `oldest_seconds` large | One slow function is holding a batch. Look at the per-function rows, not the total |

Confirm "nothing is claiming" directly — this is not a subtle diagnosis and it
has a different fix from everything else:

```sql
SELECT claimed_by, count(*), max(claimed_until) FROM queue
 WHERE claimed_by IS NOT NULL GROUP BY 1;   -- one row per live worker
SELECT * FROM active_namespaces();          -- namespaces with claimable work now
```

---

## 3. Is a circuit breaker open?

```bash
curl -sS "$STEPD_URL/v1/functions" -H "Authorization: Bearer $STEPD_TOKEN" | jq .
```

Each entry carries `paused`, `archived_at`, `url`, `last_seen`, `circuit` and
`consecutive_failures`.

**Read `last_seen`, and treat `circuit` with suspicion.** `circuit` is read from
the `app_health` table, and in this build nothing writes to it: the breaker lives
in each dispatcher process's memory, one per `(namespace, function)`, and is not
shared between replicas or persisted. The endpoint will report `closed` for an app
that every replica has stopped calling. The reliable evidence that a breaker is
open is in the queue instead:

```sql
-- A deferring circuit releases the run with available_at = now() + 5s, over and over.
SELECT fn_id, count(*) FROM queue
 WHERE claimed_by IS NULL AND available_at BETWEEN now() AND now() + interval '6 seconds'
 GROUP BY fn_id ORDER BY 2 DESC;
```

A function whose whole backlog sits permanently five seconds in the future is
being held back by an open breaker. It opens after five consecutive transport
failures, stays open for fifteen seconds, then admits a deterministic budget of
probes; a single failure while probing sends it straight back to open. It closes
on five consecutive successes **or** sixty seconds without a failure — the second
clause exists because a breaker that closes only on observed successes stays
half-open forever once traffic stops.

`stepd doctor`'s `apps` check is the blunt version: it names every app not seen
within the hour.

---

## 4. Backpressure at ingest

Above **100 000** unclaimed queue rows in a namespace, `POST /v1/events` stops
accepting and returns `429` with `Retry-After: 5` and a `backpressure` problem
code. The threshold is `BACKPRESSURE_THRESHOLD` in
`engine/rust/crates/stepd-server/src/ingest.rs` — a compile-time constant, not a row in
`engine_limits`, so it cannot be raised during an incident.

This is deliberate and you should not try to defeat it. Accepting work the system
cannot drain converts a visible rejection into an invisible backlog, and the first
anyone hears of that is a workflow four hours late. When you see 429s, the queue
is already deep enough that the events would have been late anyway; the producer
retrying after five seconds is the correct behaviour.

```sql
-- How close to the wall each namespace is.
SELECT ns, count(*) AS unclaimed FROM queue WHERE claimed_by IS NULL
 GROUP BY ns ORDER BY 2 DESC;
```

The way out is to drain the queue, not to raise the wall: add servers if they are
the bottleneck, or pause the function producing the flood.

```sql
UPDATE functions SET paused = true WHERE ns = 'prod' AND fn_id = 'bulk-reindex';
```

`paused` stops *new* runs starting from triggers. In-flight runs keep being
driven, which is what you want — pausing is not a stop button for work already
accepted.

---

## 5. Namespace fairness

The dispatcher calls `active_namespaces()` (namespaces with work claimable *now*,
so one whose whole backlog is scheduled for the future does not consume a turn),
sorts them, rotates a cursor by one each tick, and then claims a batch from each
in that order via `claim_runs_ns`. Every namespace gets a batch every tick; the
rotation decides only who goes first.

That ordering still matters, because attempts within a tick are driven
sequentially. A namespace whose app takes ten seconds per attempt delays every
namespace after it in that tick, and the rotation is what stops the same tenant
being the delayed one every time. It bounds unfairness; it does not remove it.

If one tenant must not be able to slow another at all, this build cannot give you
that. `stepd serve` runs `tick()`, which visits every active namespace; there is
no flag or environment variable that pins a replica to one namespace, so a
"dedicated pool" would still claim everyone's work. `Dispatcher::tick_namespace`
is the seam a sharded deployment would be built on, but nothing calls it. Today
the only real isolation is a separate database. Weighted fair queuing (F-LP-5) is
specified and not implemented.

Priority is the in-incident lever, and it is not wired to anything else —
`priority_expr` in the function config is not implemented, so the column is
whatever an operator sets:

```sql
UPDATE queue SET priority = 10
 WHERE ns = 'prod' AND fn_id = 'payment-capture' AND claimed_by IS NULL;
```

Claiming is `ORDER BY priority DESC, available_at`, so this jumps that function's
work to the front of its namespace's queue without touching anyone else's.

---

## 6. Scale the right side

| Evidence | Act on |
|---|---|
| `in_flight` pinned near `replicas × 16` (the batch), app p99 flat and low, app CPU idle | **Servers.** Add `stepd serve` replicas |
| App p99 climbing with `in_flight`, 5xx or timeouts in the app | **The app.** Adding servers deepens the overload and trips the breaker |
| Breaker signature in §3 | **Neither.** Fix the app's health first; the breaker is already protecting it |
| `lease_expired` climbing | **Neither.** Attempts are exceeding the lease; make steps faster or split them. More servers multiply the double-execution |
| One namespace starving others | **Neither.** Round-robin already bounds it; real isolation needs a separate database (see §5) |
| `backing_off` dominates | **Neither.** This is `poison-pill.md` |

Two constraints on adding servers. Each replica opens up to
`STEPD_MAX_CONNECTIONS` (default 16) Postgres connections, so the fleet has a
ceiling set by the database or the pooler, not by stepd. And every replica runs
the housekeeping loop as well as dispatch; the sweeps use `FOR UPDATE SKIP
LOCKED` and are bounded per pass, so they do not conflict, but they are not free
either.

Scale in steps and re-read `/v1/queue/stats` between them. `oldest_seconds`
turning downwards is the signal that you have added enough; `backlog` falling is
not, because the arrival rate may simply have dropped.

---

## What to say, and when

**In the incident channel, after the first `/v1/queue/stats`:**

> `prod` has 41 200 runs queued on `order-fulfilment`, oldest 15 minutes. 48 are
> in flight and the app's p99 has gone from 200 ms to 9 s, so the app is the
> bottleneck, not stepd. I am **not** adding dispatch capacity — that would push
> a struggling app harder and trip its circuit breaker, which would take
> throughput to near zero. Ingest will start returning 429 at 100 000 queued;
> producers should honour `Retry-After`. Next update in 15 minutes with
> `oldest_seconds`.

---

## Preventing this

1. **Alert on `oldest_seconds`, not on `backlog`.** Depth is a throughput
   statistic; age is the customer's experience.
2. **Alert on 429s from `/v1/events` at all.** By the time backpressure engages
   the queue is six figures deep and the incident started some time ago.
3. **Keep `timeouts.attempt` well under the 60 s lease.** Attempts that outlive
   their lease are counted as capacity twice and executed twice.
4. **Put tenants that must not interfere on separate databases.** Round-robin
   bounds the damage one namespace can do to another; it does not eliminate it,
   and no configuration available in this build changes that.
5. **Load-test with the sequential drive in mind.** Attempt concurrency is
   replicas, not batch size; a benchmark that assumes otherwise will size the
   fleet wrongly by an order of magnitude.

---

## Related

- `docs/runbooks/stuck-run.md` — when it is one run, not the queue
- `docs/runbooks/poison-pill.md` — when the depth is retry backoff, not arrivals
- `docs/runbooks/upgrade.md` — deploying more replicas without re-charging customers
- `docs/adr/009-fencing-and-leases.md` — leases, and why an expired one is safe
- `docs/adr/019-pooler-safe-locking.md` — why claiming scales behind pgbouncer
- `docs/GAPS.md` B3, B5 — namespace fairness and ingest backpressure
