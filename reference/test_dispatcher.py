#!/usr/bin/env python3
"""End-to-end: a real multi-step workflow driven to completion through the
dispatcher and the real database. Also exercises the load-protection behaviours.

The handler is written the way an SDK would generate it — replay from the top,
return memoized values, yield one new op — so this validates the whole loop:
  claim_runs -> transport -> handler replay -> commit_ops -> repeat
"""
import json
import sys
import time
from collections import Counter

import psycopg2
import psycopg2.extras
from dispatcher import Dispatcher, LocalTransport, Attempt, step_hash, uuid7ish

DSN = "host=localhost port=5433 dbname=stepd user=postgres"
FAILS = 0


def check(name, cond, detail=""):
    global FAILS
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + str(detail) if detail else ''}")
    if not cond:
        FAILS += 1


def db():
    c = psycopg2.connect(DSN)
    c.autocommit = True
    return c


def q(conn, sql, args=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, args or ())
        return cur.fetchall() if cur.description else None


# ------------------------------------------------------------------ the handler
# Mirrors the SDK's replay model exactly: walk from the top, return memoized
# results, execute the first unmemoized step, yield.

EXECUTIONS = Counter()


def order_fulfilment(att: Attempt) -> dict:
    FN = "order-fulfilment"
    memo = att.steps
    counters = Counter()

    def claim(step_id):
        occ = counters[step_id]
        counters[step_id] += 1
        return step_hash(FN, step_id, occ)

    def run_step(step_id, fn):
        h = claim(step_id)
        if h in memo:
            return ("memo", memo[h].get("data") if memo[h] else None, h)
        EXECUTIONS[step_id] += 1
        return ("new", fn(), h)

    # --- step 1
    kind, charge, h = run_step("charge", lambda: {"tx": "ch_1"})
    if kind == "new":
        return {"ops": [{"op": "step", "id": "charge", "hash": h, "data": charge}]}

    # --- step 2: three parallel fetches, hashes claimed in declaration order
    ph = [claim(i) for i in ("fetch-invoice", "fetch-customer", "fetch-risk")]
    if not all(x in memo for x in ph):
        ops = []
        for sid, hh, val in zip(("fetch-invoice", "fetch-customer", "fetch-risk"),
                                ph, ({"inv": 1}, {"cust": 2}, {"risk": 3})):
            if hh not in memo:
                EXECUTIONS[sid] += 1
                ops.append({"op": "step", "id": sid, "hash": hh, "data": val})
        return {"ops": ops, "join": "all_settled"}

    # --- step 3: wait for approval
    h = claim("approval")
    if h not in memo:
        return {"ops": [{"op": "wait_event", "id": "approval", "hash": h,
                         "event": "order.approved"}]}
    approval = memo[h].get("data")

    # --- step 4: ship, emitting an event with the same commit
    kind, ship, h = run_step("ship", lambda: {"carrier": "dhl"})
    if kind == "new":
        return {"ops": [{"op": "step", "id": "ship", "hash": h, "data": ship}],
                "emit": [{"specversion": "1.0", "id": uuid7ish(),
                          "source": "/fn/order-fulfilment", "type": "order.shipped"}]}

    return {"ops": [{"op": "done", "data": {"shipped": True,
                                            "approved_by": (approval or {}).get("by")}}]}


def always_fails(att: Attempt) -> dict:
    raise RuntimeError("unreachable")


# ------------------------------------------------------------------ fixtures

def reset(conn):
    q(conn, "TRUNCATE runs, queue, run_steps, run_inbox, waits, timers, outbox CASCADE;")
    EXECUTIONS.clear()


def new_run(conn, fn="order-fulfilment", key=None):
    rid = uuid7ish()
    q(conn, """INSERT INTO runs (id,ns,fn_id,key,lineage_id) VALUES (%s,'prod',%s,%s,%s)""",
      (rid, fn, key, rid))
    q(conn, "INSERT INTO queue (ns,fn_id,run_id) VALUES ('prod',%s,%s)", (fn, rid))
    return rid


# ------------------------------------------------------------------ tests

def test_end_to_end(conn):
    print("\n--- end-to-end: multi-step workflow to completion ---")
    reset(conn)
    rid = new_run(conn)
    t = LocalTransport({"order-fulfilment": order_fulfilment})
    d = Dispatcher(DSN, t)

    d.run_until_idle(50)   # runs until it parks on the wait
    status = q(conn, "SELECT status::text FROM runs WHERE id=%s", (rid,))[0]["status"]
    check("run suspends at wait_event", status == "sleeping", status)
    check("steps recorded before the wait",
          q(conn, "SELECT count(*) n FROM run_steps WHERE run_id=%s", (rid,))[0]["n"] == 5)

    # deliver the approval, then drive to completion
    q(conn, "SELECT deliver_to_inbox(%s::uuid,'order.approved','{\"by\":\"priya\"}'::jsonb)", (rid,))
    d.run_until_idle(50)

    r = q(conn, "SELECT status::text, output FROM runs WHERE id=%s", (rid,))[0]
    check("run completed", r["status"] == "completed", r["status"])
    check("output carries the approval", (r["output"] or {}).get("approved_by") == "priya", r["output"])
    check("every step executed exactly once",
          all(v == 1 for v in EXECUTIONS.values()), dict(EXECUTIONS))
    check("emitted event landed in the outbox",
          q(conn, "SELECT count(*) n FROM outbox WHERE run_id=%s", (rid,))[0]["n"] == 1)
    check("parallel group ran as one batch", EXECUTIONS["fetch-invoice"] == 1
          and EXECUTIONS["fetch-risk"] == 1)


def test_early_signal_e2e(conn):
    print("\n--- end-to-end: signal arrives BEFORE the run reaches its wait ---")
    reset(conn)
    rid = new_run(conn)
    # deliver before the dispatcher has run at all
    res = q(conn, "SELECT deliver_to_inbox(%s::uuid,'order.approved','{\"by\":\"early\"}'::jsonb) r", (rid,))[0]["r"]
    check("delivery buffered (no wait yet)", res == "buffered", res)

    t = LocalTransport({"order-fulfilment": order_fulfilment})
    d = Dispatcher(DSN, t)
    d.run_until_idle(50)

    r = q(conn, "SELECT status::text, output FROM runs WHERE id=%s", (rid,))[0]
    check("run completed without ever suspending", r["status"] == "completed", r["status"])
    check("resolved from the buffered event",
          (r["output"] or {}).get("approved_by") == "early", r["output"])


def test_circuit_breaker(conn):
    print("\n--- circuit breaker and recovery ramp ---")
    reset(conn)
    for _ in range(6):
        new_run(conn)
    t = LocalTransport({"order-fulfilment": order_fulfilment})
    t.fail_next = 12                      # app is down
    d = Dispatcher(DSN, t)
    d.tick(); d.tick(); d.tick(); d.tick()

    cb = d.breakers["order-fulfilment"]
    check("circuit opens after repeated failures", cb.state == "open", cb.state)
    before = t.calls
    d.tick()
    check("open circuit stops hammering the app", t.calls == before, f"calls {before}->{t.calls}")
    check("skipped dispatches counted", d.stats["skipped_open_circuit"] > 0,
          d.stats["skipped_open_circuit"])

    time.sleep(0.55)                      # cooldown elapses
    t.fail_next = 0                       # app recovers
    d.tick()
    check("circuit probes after cooldown", cb.state in ("half_open", "closed"), cb.state)
    # keep real work flowing so the ramp has successes to observe
    for _ in range(40):
        if q(conn, "SELECT count(*) n FROM queue")[0]["n"] < 3:
            new_run(conn)
        d.tick()
    check("circuit closes after sustained success", cb.state == "closed", cb.state)

    # A breaker must not remain half-open forever once traffic stops.
    cb2 = __import__("dispatcher").CircuitBreaker(cooldown=0.05, quiet_period=0.1)
    cb2.record_failure(); cb2.record_failure(); cb2.record_failure()
    time.sleep(0.06); cb2.allow()          # -> half_open
    time.sleep(0.15); cb2.allow()          # quiet period elapses with no traffic
    check("circuit self-closes after a quiet period (no traffic needed)",
          cb2.state == "closed", cb2.state)


def test_quarantine(conn):
    print("\n--- poison-pill quarantine ---")
    reset(conn)
    rid = new_run(conn)
    t = LocalTransport({"order-fulfilment": order_fulfilment})
    t.fail_next = 999
    d = Dispatcher(DSN, t, quarantine_after=3)
    from dispatcher import CircuitBreaker
    d.breakers["order-fulfilment"] = CircuitBreaker(threshold=10**6)  # isolate quarantine from CB
    for _ in range(40):
        d.tick()
        time.sleep(0.02)

    st = q(conn, "SELECT status::text FROM runs WHERE id=%s", (rid,))[0]["status"]
    check("repeatedly failing run is quarantined", st == "quarantined", st)
    check("quarantined run leaves the queue",
          q(conn, "SELECT count(*) n FROM queue WHERE run_id=%s", (rid,))[0]["n"] == 0)


def test_fairness(conn):
    print("\n--- namespace fairness ---")
    reset(conn)
    q(conn, "INSERT INTO namespaces (id) VALUES ('tenant-b') ON CONFLICT DO NOTHING")
    q(conn, """INSERT INTO app_bindings (id,ns,app_id,url,key_hash_current)
               VALUES (gen_random_uuid(),'tenant-b','billing','https://x','\\x00')
               ON CONFLICT DO NOTHING""")
    # noisy tenant floods, quiet tenant has one run
    for _ in range(40):
        new_run(conn)
    rid = uuid7ish()
    q(conn, "INSERT INTO runs (id,ns,fn_id,lineage_id) VALUES (%s,'tenant-b','order-fulfilment',%s)", (rid, rid))
    q(conn, "INSERT INTO queue (ns,fn_id,run_id) VALUES ('tenant-b','order-fulfilment',%s)", (rid,))

    t = LocalTransport({"order-fulfilment": order_fulfilment})
    d = Dispatcher(DSN, t)
    for _ in range(3):
        d.tick(batch=5)

    attempts = q(conn, "SELECT attempt_no FROM runs WHERE id=%s", (rid,))[0]["attempt_no"]
    check("quiet tenant dispatched despite a noisy neighbour", attempts >= 1,
          f"attempts={attempts}")


def main():
    conn = db()
    print("stepd dispatcher — end-to-end and load protection")
    test_end_to_end(conn)
    test_early_signal_e2e(conn)
    test_circuit_breaker(conn)
    test_quarantine(conn)
    test_fairness(conn)
    print("\n" + ("ALL PASS" if FAILS == 0 else f"{FAILS} FAILURE(S)"))
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
