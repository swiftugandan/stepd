# stepd engine — Postgres schema and correctness tests

Applied and tested against a live PostgreSQL 16. Every assertion below has been
executed, not merely written down.

## Files

| File | Contains |
|---|---|
| `001_initial.sql` | Full schema: tenancy, events (partitioned), runs, steps, inbox, timers, waits, queue, outbox, blobs, health, audit, erasure |
| `002_engine.sql` | `claim_runs`, `commit_ops`, `deliver_to_inbox` — the commit transaction is the correctness centre |
| `003_fix_signal_race.sql` | Makes the signal/wait serialization explicit (see finding below) |
| `test_engine.sql` | 25 behavioural assertions |
| `test_invariants.sql` | 6 structural assertions that catch a future edit reopening an R1 race |
| `stress_concurrency.py` | 12 concurrent workers, 60 runs — dispatch exclusivity and fencing |
| `race_signal.py` | 120 randomised signal-vs-wait races |
| `force_race.sh` | Deliberately forces the dangerous interleaving |
| `prove_fix.sh` | Drops the FK to prove the explicit lock alone closes the race |
| `dispatcher.py` | Reference dispatch loop: claim → deliver → commit, with circuit breaker, recovery ramp, fairness, quarantine, timer jitter |
| `test_dispatcher.py` | 19 assertions incl. a complete workflow driven end to end |
| `bench.py` | Throughput of the commit path against the NFR target |
| `simulation.py` | Deterministic simulation harness: 11 fault types, 9 properties, positive controls |
| `coverage_check.py` | Reports which engine paths the simulation never exercised |
| `004_fix_idempotency_and_wait_upsert.sql` | Two defects found by API testing (below) |
| `api.py` | Management API: read model, commands, ingest, OpenAPI 3.1 |
| `test_api.py` | 30 assertions, incl. adversarial namespace-isolation tests |
| `openapi.json` | Generated spec: 11 endpoints, 8 schemas |
| `console.html` | Operations console, served at `/` |
| `test_console.py` | 17 assertions: endpoint coverage, content safety, a11y floor, operator flow |

## Running

```bash
initdb -D /tmp/pgdata -A trust
pg_ctl -D /tmp/pgdata -o '-p 5433' start
createdb -p 5433 stepd
psql -p 5433 -d stepd -f 001_initial.sql -f 002_engine.sql -f 003_fix_signal_race.sql
psql -p 5433 -d stepd -f test_engine.sql -f test_invariants.sql
python3 stress_concurrency.py && python3 race_signal.py
python3 test_dispatcher.py && python3 bench.py
python3 simulation.py 1500 && python3 coverage_check.py 400
python3 test_api.py && python3 test_console.py
python3 -c "import uvicorn, api; uvicorn.run(api.app, port=8099)"   # console at /
```

## Simulation status

**120,000 seeds, zero property violations** (251s, ~480 seeds/sec). The first 1,500-seed
run found a real specification gap; 120k has found nothing further, which is the signal
described in §10.3 for moving the seed budget to a less-explored zone rather than growing
it. The next increment of value comes from widening the workflow shapes the simulator
explores, not from more seeds against the same shape.

## Measured throughput

Single connection, unoptimised container, no pipelining:

| Path | Rate |
|---|---|
| `commit_ops`, sequential | ~2 650 commits/s |
| `commit_ops`, batched in one transaction | ~7 500 commits/s |
| `claim_runs`, batches of 100 | ~27 000 claims/s |
| `deliver_to_inbox` (with the explicit lock) | ~4 650 deliveries/s |

The PRD's ≥1 000 step commits/s target (NFR §5) is met with roughly 2.5× headroom before
any pooling or batching. The explicit run-row lock added in `003` costs nothing measurable.

## Design rules enforced

* **No advisory locks anywhere.** All claiming is row-level `FOR UPDATE SKIP LOCKED`,
  so the engine works behind a transaction-mode pooler (PRD F-DL-1). Asserted structurally.
* **The op commit is one transaction**: step results, inbox consumption, emitted events
  and the next schedule commit together or not at all.
* **Keyed exclusivity is a database invariant**, not application logic — a partial unique
  index on `(ns, fn_id, key)` over active statuses.
* **Blob dedupe is namespace-scoped**, so a digest cannot probe another tenant's data.

## Finding: the lost-signal race was closed by accident

Forced-interleaving testing showed the race was already closed — but only because the
foreign key `run_inbox.run_id -> runs.id` makes an inbox INSERT take `FOR KEY SHARE` on
the runs row, which happens to conflict with the `FOR UPDATE` held by `commit_ops`.

Correct behaviour, wrong reason. Dropping the FK, deferring it, partitioning `run_inbox`,
or weakening the lock in `commit_ops` would silently reopen an R1 race with no test failing.

`003` makes the mutual exclusion explicit: `deliver_to_inbox` now takes the run row lock as
its first statement. Proven by dropping the foreign key entirely and re-running the forced
interleaving — five trials, all resolved correctly with only the explicit lock protecting
them. `test_invariants.sql` now fails the build if that lock is removed or moved after the
insert.

This is the class of defect the risk-based QA regime exists to find: a correct system whose
correctness rests on something nobody wrote down.

## Finding: a circuit breaker that only closes on success never closes

The first breaker implementation moved `open → half_open → closed` by counting successes.
It stayed half-open indefinitely once traffic stopped, because closing required observing
successes that would never arrive — so the next burst of work was throttled for no reason,
long after the app had recovered.

Closing now happens on consecutive successes **or** a quiet period with no failures,
whichever comes first, and half-open always admits at least a probe. Covered by a test that
asserts a breaker self-closes with no traffic at all.


## Finding: `continue_as_new` orphaned in-flight children

Found by the simulator on its first run, via property P8 (cascade completeness).

The cascade rules in protocol §7.5 covered parent cancellation, timeout and failure — but
not continuation. `continue_as_new` marks the predecessor completed, so a run with a live
non-detached child orphaned it. Worse, the child's result would be delivered into the
predecessor's journal, which `continue_as_new` discards: the result had nowhere to go.

The op now fails with `continue_as_new_with_live_children` when a non-detached child is
live; detached children are unaffected. This was a specification gap, not a coding error —
no amount of testing the implementation against the spec would have found it, because the
spec was silent.

## Three lessons about the harness itself

1. **A mis-stated property is worse than no property.** P3 initially asserted "an event is
   consumed or the run is terminal", which fires on any run still in flight. It generated
   false failures until restated as the real condition: *a run must never sit parked on a
   wait while a matching unconsumed inbox entry exists* — checkable continuously, and true
   only when something is genuinely broken.

2. **Faults belong in the infrastructure, never in the handler.** A `slow_children` fault
   that flipped a coin inside the workflow made user code non-deterministic, and P4 duly
   caught it. Correct behaviour from the property, wrong experiment: injecting
   non-determinism into a handler tests the workflow, not the engine.

3. **Passing is not the same as exercising.** `coverage_check.py` showed cascade
   cancellation was hit zero times, so 500 green seeds had never tested the fix that had
   just been made. Adding cancellation injection and detached children fixed that. It also
   showed `continue_as_new_with_live_children` is *unreachable by construction* under
   blocking invoke — so that rule is covered by a targeted test, and documented as a
   defensive invariant rather than pretended to be fuzz-covered.

Positive controls are included for the same reason: P4 is deliberately fired by a
non-deterministic handler on every run, so a property that silently stopped working would
be caught.


## Finding: idempotent ingest never deduplicated

The unique index was `(ns, idem, received_at)`, and `received_at` differs on every insert,
so it could never collide. The root cause is structural, not a typo: `events` is partitioned
by `received_at`, and PostgreSQL requires a unique index on a partitioned table to include
the partition key — so a global `UNIQUE (ns, idem)` **cannot be expressed on that table**.

Dedupe moved to its own non-partitioned `event_idempotency` table, claimed in the same
transaction as the insert via `ingest_event()`. That also gives an explicit, prunable
dedupe window instead of an unbounded one.

A feature can be specified, implemented and shipped while being structurally impossible in
the schema it was built on. Only an end-to-end test that sends the same event twice catches
this — a unit test of the insert would have passed.

## Finding: `deliver_to_inbox` could lose a signal it had already consumed

It marked the wait resolved, consumed the inbox entry, then ran
`UPDATE run_steps SET status='completed'`. If no pending step row existed the UPDATE matched
nothing: the event was consumed, the wait closed, and no result recorded — so the next
attempt re-registered the wait with the event gone.

It only ever worked because `commit_ops` happens to create that row first. Same class as the
foreign-key finding in `003`: correct for an undocumented reason. Now an UPSERT, so the
result is recorded unconditionally.

## Finding: a probabilistic circuit-breaker ramp is untestable

Half-open recovery admitted requests with `random() < ramp`. The test failed roughly one run
in five. The flakiness was the symptom; the design was the defect — probabilistic admission
makes recovery time non-deterministic, so operators cannot reason about it and any test of
it is inherently unstable.

Replaced with a deterministic token budget that doubles on each success (1, 2, 4, 8…). Same
gradual ramp, no randomness, and the dispatcher hot path is now RNG-free. Five consecutive
clean runs.


## The console

Design register: technical drawing. A cool blueprint ground with a hairline grid, IBM Plex
Mono for every identifier (they are hashes, keys and timestamps — genuinely data), Inter
Tight for interface text.

The signature element is the **execution strip**. The truth of durable execution is that
most of a run's wall-clock is spent doing nothing, so rather than hide that, sleeps and
waits render as explicit elided spans labelled with their real duration — `⋯ 24h idle ⋯` —
and a run currently parked shows a live marker naming the event it needs. An operator sees
where the time actually went, which is the first question in any "why is this stuck" call.

Amber is reserved exclusively for *needs a human*. Nothing else in the palette uses it, so
the one thing Priya must find is the one thing that stands out.

### Finding: run ids were interpolated into URLs unencoded

An exhaustive audit of every template interpolation in the console — flagging any that does
not route through the HTML escaper, a numeric field, or pure control flow — caught run ids
being placed directly into request paths. Low severity, since ids come from our own API,
but it is exactly the assumption that stops being true the moment an id is ever taken from
a URL fragment or a paste. Now encoded via `encodeURIComponent`. HTML context and URL
context need different escaping, and the audit is now a permanent test.
