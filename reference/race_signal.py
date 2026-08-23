#!/usr/bin/env python3
"""The race the inbox exists to close.

A signal is delivered at the same moment the handler registers its wait. Whichever
order the two transactions land in, the run must end up resolved — never parked
forever holding an event that already arrived.

Runs the collision many times with randomised timing to hit both interleavings.
"""
import concurrent.futures as cf
import os
import random
import subprocess
import sys
import time
import uuid
from collections import Counter

PSQL = ["/usr/lib/postgresql/16/bin/psql", "-h", "localhost", "-p", "5433",
        "-d", "stepd", "-t", "-A", "-q"]


def sql(q):
    r = subprocess.run(PSQL + ["-c", q], capture_output=True, text=True,
                       env={**os.environ, "PGUSER": "postgres"})
    return r.stdout.strip() if r.returncode == 0 else f"ERR:{r.stderr.strip()}"


def uuid7ish():
    u = list(str(uuid.uuid4()))
    u[14] = '7'
    return "".join(u)


def trial(i):
    run_id = uuid7ish()
    h = f"{i:016x}"
    sql(f"""INSERT INTO runs (id,ns,fn_id,lineage_id)
            VALUES ('{run_id}','prod','order-fulfilment','{run_id}');
            INSERT INTO queue (ns,fn_id,run_id)
            VALUES ('prod','order-fulfilment','{run_id}');""")
    sql(f"SELECT claim_runs('racer', 1);")
    # ensure this run is the one claimed
    fence = sql(f"SELECT fence_token FROM runs WHERE id='{run_id}';")
    if not fence.isdigit():
        return ("skip", run_id)

    jitter_a = random.random() / 800
    jitter_b = random.random() / 800

    def register_wait():
        time.sleep(jitter_a)
        return sql(f"""SELECT commit_ops('{run_id}'::uuid, {fence},
            '[{{"op":"wait_event","id":"approval","hash":"{h}","event":"order.approved"}}]'::jsonb);""")

    def deliver():
        time.sleep(jitter_b)
        return sql(f"""SELECT deliver_to_inbox('{run_id}'::uuid, 'order.approved',
            '{{"order_id":{i}}}'::jsonb);""")

    with cf.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(register_wait)
        f2 = ex.submit(deliver)
        commit_res, deliver_res = f1.result(), f2.result()

    step_status = sql(f"""SELECT status::text FROM run_steps
                          WHERE run_id='{run_id}' AND step_hash='{h}';""")
    parked = sql(f"""SELECT count(*) FROM waits
                     WHERE run_id='{run_id}' AND resolved_at IS NULL;""")
    consumed = sql(f"""SELECT count(*) FROM run_inbox
                       WHERE run_id='{run_id}' AND consumed_by_step_hash IS NOT NULL;""")
    unconsumed = sql(f"""SELECT count(*) FROM run_inbox
                         WHERE run_id='{run_id}' AND consumed_by_step_hash IS NULL;""")
    return (deliver_res, step_status, parked, consumed, unconsumed, run_id)


def main():
    sql("TRUNCATE runs, queue, run_steps, run_inbox, waits, timers, outbox CASCADE;")
    n = 120
    print(f"racing signal delivery against wait registration, {n} trials\n")

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(trial, range(1, n + 1)))

    results = [r for r in results if r[0] != "skip"]
    outcomes = Counter(r[0] for r in results)
    print("  delivery outcomes:", dict(outcomes))
    print("  (buffered = delivery won the race; resolved = wait was already parked)")

    fails = 0

    def check(name, cond, detail=""):
        nonlocal fails
        print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
        if not cond:
            fails += 1

    print("\nassertions")
    # The point: whichever way the race lands, the step must end up completed.
    stuck = [r for r in results if r[1] != "completed"]
    check("every run resolved regardless of interleaving", not stuck,
          f"stuck={[(s[5][:8], s[1], s[0]) for s in stuck[:3]]}")

    still_parked = [r for r in results if r[2] != "0"]
    check("no wait left parked holding an already-delivered event", not still_parked,
          f"parked={[s[5][:8] for s in still_parked[:3]]}")

    bad_consume = [r for r in results if r[3] != "1"]
    check("exactly one inbox entry consumed per run", not bad_consume,
          f"bad={[(s[5][:8], s[3]) for s in bad_consume[:3]]}")

    leftover = [r for r in results if r[4] != "0"]
    check("no unconsumed leftovers", not leftover,
          f"leftover={[(s[5][:8], s[4]) for s in leftover[:3]]}")

    check("both interleavings actually occurred",
          len(outcomes) > 1 or n < 10,
          f"only saw {list(outcomes)}")

    print("\n" + ("ALL PASS" if fails == 0 else f"{fails} FAILURE(S)"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
