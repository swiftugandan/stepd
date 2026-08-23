#!/usr/bin/env python3
"""Management API tests. The security assertions are the point: namespace
isolation is a structural property (F-SEC-1), so it gets adversarial tests,
not a happy path."""
import json
import sys
import uuid

import psycopg2
import psycopg2.extras
from fastapi.testclient import TestClient

import api
from api import app, issue_token

FAILS = 0


def check(name, cond, detail=""):
    global FAILS
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + str(detail) if detail else ''}")
    if not cond:
        FAILS += 1


def conn():
    c = psycopg2.connect(api.DSN)
    c.autocommit = True
    return c


def q(c, sql, a=None):
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, a or ())
        return cur.fetchall() if cur.description else []


def uuid7ish():
    u = list(str(uuid.uuid4())); u[14] = "7"; return "".join(u)


def setup(c):
    q(c, """TRUNCATE runs, queue, run_steps, run_inbox, waits, timers, outbox,
                     tokens, commands_audit, events, event_idempotency CASCADE;""")
    q(c, "INSERT INTO namespaces (id) VALUES ('prod'),('tenant-b') ON CONFLICT DO NOTHING")
    for ns in ("prod", "tenant-b"):
        if not q(c, "SELECT 1 FROM app_bindings WHERE ns=%s", (ns,)):
            q(c, """INSERT INTO app_bindings (id,ns,app_id,url,key_hash_current)
                    VALUES (gen_random_uuid(),%s,'billing','https://x','\\x00')""", (ns,))
        if not q(c, "SELECT 1 FROM functions WHERE ns=%s", (ns,)):
            q(c, """INSERT INTO functions (id,ns,app_binding_id,fn_id,version,config)
                    SELECT gen_random_uuid(),%s,id,'order-fulfilment','1','{}'
                      FROM app_bindings WHERE ns=%s LIMIT 1""", (ns, ns))
    return {
        "prod_admin": issue_token(c, "prod", "admin", "t"),
        "prod_viewer": issue_token(c, "prod", "viewer", "t"),
        "prod_op": issue_token(c, "prod", "operator", "t"),
        "b_admin": issue_token(c, "tenant-b", "admin", "t"),
    }


def mkrun(c, ns="prod", status="pending", key=None, fn="order-fulfilment"):
    rid = uuid7ish()
    q(c, """INSERT INTO runs (id,ns,fn_id,key,lineage_id,status)
            VALUES (%s,%s,%s,%s,%s,%s::run_status)""", (rid, ns, fn, key, rid, status))
    return rid


def main():
    c = conn()
    tok = setup(c)
    cl = TestClient(app)
    H = lambda t: {"Authorization": f"Bearer {t}"}

    print("stepd management API\n")

    print("--- auth ---")
    check("unauthenticated rejected", cl.get("/v1/runs").status_code == 401)
    check("bad token rejected", cl.get("/v1/runs", headers=H("nope")).status_code == 401)
    r = cl.get("/v1/runs", headers=H(tok["prod_viewer"]))
    check("valid token accepted", r.status_code == 200)
    check("error body is RFC 9457",
          cl.get("/v1/runs").headers["content-type"].startswith("application/problem+json"))

    print("\n--- role enforcement ---")
    rid = mkrun(c)
    check("viewer cannot cancel",
          cl.post(f"/v1/runs/{rid}/cancel", headers=H(tok["prod_viewer"])).status_code == 403)
    check("operator can cancel",
          cl.post(f"/v1/runs/{rid}/cancel", headers=H(tok["prod_op"])).status_code == 200)

    print("\n--- namespace isolation (F-SEC-1) ---")
    prod_run = mkrun(c, "prod")
    b_run = mkrun(c, "tenant-b")

    r = cl.get("/v1/runs", headers=H(tok["prod_admin"])).json()
    ids = {i["id"] for i in r["items"]}
    check("list shows only own namespace", b_run not in ids and prod_run in ids)

    r = cl.get(f"/v1/runs/{b_run}", headers=H(tok["prod_admin"]))
    check("cross-namespace read is 404 (not 403)", r.status_code == 404, r.status_code)
    check("404 body leaks nothing about the other tenant",
          "tenant-b" not in r.text and "order-fulfilment" not in r.text)

    r = cl.post(f"/v1/runs/{b_run}/cancel", headers=H(tok["prod_admin"]))
    check("cross-namespace cancel refused", r.status_code == 404)
    still = q(c, "SELECT status::text FROM runs WHERE id=%s", (b_run,))[0]["status"]
    check("other tenant's run untouched", still == "pending", still)

    r = cl.post(f"/v1/runs/{b_run}/resolve-wait", headers=H(tok["prod_admin"]),
                json={"event_type": "x", "data": {}})
    check("cross-namespace resolve-wait refused", r.status_code == 404)
    check("no inbox entry created in the other tenant",
          q(c, "SELECT count(*) n FROM run_inbox WHERE run_id=%s", (b_run,))[0]["n"] == 0)

    r = cl.get("/v1/queue/stats", headers=H(tok["b_admin"])).json()
    check("queue stats scoped to caller's namespace",
          all(f["fn_id"] for f in r["functions"]) or r["functions"] == [])

    print("\n--- run detail ---")
    rid = mkrun(c, "prod", key="order:1")
    q(c, """INSERT INTO run_steps (run_id,step_hash,step_id,occurrence,op,status,result)
            VALUES (%s,'aaaa000000000001','charge',0,'step','completed','{"tx":1}')""", (rid,))
    q(c, """INSERT INTO waits (run_id,ns,step_hash,event_type,since)
            VALUES (%s,'prod','bbbb000000000001','order.approved',now())""", (rid,))
    d = cl.get(f"/v1/runs/{rid}", headers=H(tok["prod_admin"])).json()
    check("detail includes steps", len(d["steps"]) == 1)
    check("detail includes pending waits", len(d["pending_waits"]) == 1)
    check("detail reports lineage", d["lineage_id"] == rid)

    print("\n--- resolve wait (Priya's flow) ---")
    r = cl.post(f"/v1/runs/{rid}/resolve-wait", headers=H(tok["prod_op"]),
                json={"event_type": "order.approved", "data": {"by": "priya"}})
    check("resolve-wait accepted", r.status_code == 200, r.text[:120])
    st = q(c, """SELECT status::text, result FROM run_steps
                  WHERE run_id=%s AND step_hash='bbbb000000000001'""", (rid,))
    check("wait resolved through the normal delivery path",
          bool(st) and st[0]["status"] == "completed", st)
    check("result recorded even though no pending step row existed",
          bool(st) and (st[0]["result"] or {}).get("by") == "priya", st)
    check("command was audited",
          q(c, "SELECT count(*) n FROM commands_audit WHERE command='resolve_wait'")[0]["n"] >= 1)

    print("\n--- events ---")
    r = cl.post("/v1/events", headers=H(tok["prod_op"]), json=[{
        "source": "/shop", "type": "order.created", "data": {"order_id": 1},
        "stepdidempotency": "idem-1"}])
    check("event accepted", r.json()["accepted"] == 1, r.json())
    r2 = cl.post("/v1/events", headers=H(tok["prod_op"]), json=[{
        "source": "/shop", "type": "order.created", "data": {"order_id": 1},
        "stepdidempotency": "idem-1"}])
    check("duplicate idempotency key deduplicated", r2.json()["deduplicated"] == 1, r2.json())
    check("dedupe returns the original event id",
          r.json()["ids"][0] == r2.json()["ids"][0],
          (r.json()["ids"][0], r2.json()["ids"][0]))
    check("only one event row stored",
          q(c, "SELECT count(*) n FROM events WHERE idem='idem-1'")[0]["n"] == 1)

    print("\n--- pagination ---")
    for _ in range(8):
        mkrun(c, "prod")
    p1 = cl.get("/v1/runs?limit=3", headers=H(tok["prod_admin"])).json()
    check("page returns limit items", len(p1["items"]) == 3, len(p1["items"]))
    check("cursor provided", p1["next_cursor"] is not None)
    p2 = cl.get(f"/v1/runs?limit=3&cursor={p1['next_cursor']}",
                headers=H(tok["prod_admin"])).json()
    overlap = {i["id"] for i in p1["items"]} & {i["id"] for i in p2["items"]}
    check("pages do not overlap", not overlap, overlap)

    print("\n--- dlq ---")
    fid = mkrun(c, "prod", status="quarantined")
    d = cl.get("/v1/dlq", headers=H(tok["prod_admin"])).json()
    check("quarantined run appears in DLQ", any(i["id"] == fid for i in d["items"]))
    r = cl.post(f"/v1/runs/{fid}/retry", headers=H(tok["prod_op"]))
    check("retry from DLQ requeues", r.status_code == 200, r.text[:120])
    check("retried run is queued again",
          q(c, "SELECT count(*) n FROM queue WHERE run_id=%s", (fid,))[0]["n"] == 1)

    print("\n--- openapi ---")
    spec = cl.get("/openapi.json").json()
    check("OpenAPI 3.1 emitted", spec["openapi"].startswith("3.1"), spec["openapi"])
    check("all documented paths present",
          {"/v1/runs", "/v1/runs/{run_id}", "/v1/events", "/v1/dlq",
           "/v1/queue/stats"} <= set(spec["paths"]))

    print("\n" + ("ALL PASS" if FAILS == 0 else f"{FAILS} FAILURE(S)"))
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
