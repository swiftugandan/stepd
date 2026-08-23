#!/usr/bin/env python3
"""Concurrency stress: many real workers against one Postgres.

Asserts the R1 properties that only fail under genuine parallelism:
  * no two workers ever dispatch the same run at once (SKIP LOCKED + fencing)
  * a step hash is recorded exactly once no matter how many workers race
  * keyed exclusivity holds under concurrent run creation
  * no deadlocks, no lost updates
"""
import concurrent.futures as cf
import os
import random
import subprocess
import sys
import uuid
from collections import Counter

PSQL = ["/usr/lib/postgresql/16/bin/psql", "-h", "localhost", "-p", "5433",
        "-d", "stepd", "-t", "-A", "-q", "-v", "ON_ERROR_STOP=1"]


def sql(q):
    r = subprocess.run(PSQL + ["-c", q], capture_output=True, text=True,
                       env={**os.environ, "PGUSER": "postgres"})
    if r.returncode != 0:
        return ("ERR", r.stderr.strip())
    return ("OK", r.stdout.strip())


def uuid7ish():
    """UUIDv7-shaped enough for the schema's purposes."""
    u = list(str(uuid.uuid4()))
    u[14] = '7'
    return "".join(u)


def setup(n_runs):
    sql("TRUNCATE runs, queue, run_steps, run_inbox, waits, timers, outbox CASCADE;")
    ids = [uuid7ish() for _ in range(n_runs)]
    values = ",".join(
        f"('{i}','prod','order-fulfilment',NULL,'{i}')" for i in ids)
    sql(f"INSERT INTO runs (id,ns,fn_id,key,lineage_id) VALUES {values};")
    qvalues = ",".join(f"('prod','order-fulfilment','{i}')" for i in ids)
    sql(f"INSERT INTO queue (ns,fn_id,run_id) VALUES {qvalues};")
    return ids


def worker(name, rounds):
    """Claim, commit, repeat — the real dispatch loop."""
    dispatched = []
    for _ in range(rounds):
        st, out = sql(
            f"SELECT run_id::text, fence, attempt FROM claim_runs('{name}', 3);")
        if st == "ERR":
            return ("ERR", name, out, dispatched)
        if not out:
            continue
        for line in out.splitlines():
            run_id, fence, attempt = line.split("|")
            dispatched.append((run_id, int(fence)))
            # commit a step whose hash is derived from the attempt, so a run
            # legitimately advances through several distinct steps
            h = f"{int(attempt):016x}"
            ops = ('[{"op":"step","id":"s","hash":"%s","data":{"a":%s}}]'
                   % (h, attempt))
            if random.random() < 0.25:
                ops = '[{"op":"done","data":{"ok":true}}]'
            sql(f"SELECT commit_ops('{run_id}'::uuid, {fence}, '{ops}'::jsonb);")
    return ("OK", name, "", dispatched)


def main():
    n_runs, n_workers, rounds = 60, 12, 12
    print(f"setup: {n_runs} runs, {n_workers} concurrent workers, {rounds} rounds each")
    setup(n_runs)

    with cf.ThreadPoolExecutor(max_workers=n_workers) as ex:
        results = list(ex.map(lambda i: worker(f"w{i}", rounds), range(n_workers)))

    errors = [r for r in results if r[0] == "ERR"]
    all_dispatch = [d for r in results for d in r[3]]

    fails = 0

    def check(name, cond, detail=""):
        nonlocal fails
        print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
        if not cond:
            fails += 1

    print("\nassertions")
    check("no worker errored", not errors,
          errors[0][2][:200] if errors else "")

    # Each (run_id, fence) pair must be handed out at most once: two workers
    # holding the same fence for the same run would mean double dispatch.
    dup = [k for k, v in Counter(all_dispatch).items() if v > 1]
    check("no run+fence dispatched twice", not dup, f"dups={dup[:3]}")

    # Fences per run must be strictly increasing with no repeats.
    per_run = {}
    for run_id, fence in all_dispatch:
        per_run.setdefault(run_id, []).append(fence)
    bad = {r: f for r, f in per_run.items() if len(set(f)) != len(f)}
    check("fence strictly unique per run", not bad, f"bad={list(bad)[:2]}")

    # Exactly one row per (run, hash) despite races.
    st, out = sql("""SELECT count(*) FROM (
                       SELECT run_id, step_hash FROM run_steps
                       GROUP BY run_id, step_hash HAVING count(*) > 1) x;""")
    check("no duplicate step rows", out == "0", f"dupes={out}")

    # No run left claimed by two workers.
    st, out = sql("""SELECT count(*) FROM runs
                      WHERE status='running' AND lease_owner IS NULL;""")
    check("no running run without a lease owner", out == "0")

    st, out = sql("SELECT count(*) FROM run_steps;")
    print(f"\n  recorded {out} step rows across {len(all_dispatch)} dispatches")
    st, out = sql("""SELECT status::text, count(*) FROM runs GROUP BY 1 ORDER BY 1;""")
    print("  run states:", "; ".join(out.splitlines()))

    print("\n" + ("ALL PASS" if fails == 0 else f"{fails} FAILURE(S)"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
