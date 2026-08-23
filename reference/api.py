#!/usr/bin/env python3
"""stepd management API — reference implementation.

Serves the read model and command surface the console consumes, plus the event
ingest endpoint. Generates OpenAPI 3.1 (PRD F-API-1).

Design points that matter and are asserted by tests:
  * Namespace authorisation is enforced in the QUERY, not by filtering results
    (F-SEC-1). A token scoped to one namespace cannot observe another's existence.
  * Every mutating command goes through the same audited path the console uses
    (F-UI-4) — the UI has no privileged route.
  * Errors are RFC 9457 Problem Details (F-API-3).
  * Cursor pagination, never OFFSET, so deep pages stay cheap.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

DSN = os.environ.get("STEPD_DATABASE_URL",
                     "host=localhost port=5433 dbname=stepd user=postgres")

POOL = psycopg2.pool.ThreadedConnectionPool(1, 8, DSN)


def db():
    conn = POOL.getconn()
    conn.autocommit = True
    try:
        yield conn
    finally:
        POOL.putconn(conn)


def q(conn, sql, args=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        c.execute(sql, args or ())
        return c.fetchall() if c.description else []


# ------------------------------------------------------------------ problems

class Problem(HTTPException):
    def __init__(self, status: int, title: str, detail: str = "", code: str = ""):
        super().__init__(status_code=status, detail=detail)
        self.title, self.code = title, code


def problem_handler(request: Request, exc: HTTPException):
    """RFC 9457 Problem Details (F-API-3)."""
    body = {
        "type": "about:blank",
        "title": getattr(exc, "title", exc.detail or "Error"),
        "status": exc.status_code,
        "detail": exc.detail if isinstance(exc.detail, str) else "",
        "instance": str(request.url.path),
    }
    if getattr(exc, "code", ""):
        body["code"] = exc.code
    return JSONResponse(status_code=exc.status_code, content=body,
                        media_type="application/problem+json")


# ------------------------------------------------------------------ auth

class Principal(BaseModel):
    ns: str
    role: str


ROLE_RANK = {"viewer": 0, "operator": 1, "admin": 2}


def token_hash(raw: str) -> bytes:
    return hashlib.sha256(raw.encode()).digest()


def authenticate(authorization: str = Header(default=""),
                 conn=Depends(db)) -> Principal:
    if not authorization.startswith("Bearer "):
        raise Problem(401, "Unauthenticated", "Bearer token required", "no_token")
    raw = authorization[7:]
    rows = q(conn, """SELECT ns, role FROM tokens
                       WHERE token_hash=%s AND revoked_at IS NULL
                         AND (expires_at IS NULL OR expires_at > now())""",
             (token_hash(raw),))
    if not rows:
        raise Problem(401, "Unauthenticated", "Unknown or revoked token", "bad_token")
    return Principal(ns=rows[0]["ns"], role=rows[0]["role"])


def require(role: str):
    def dep(p: Principal = Depends(authenticate)) -> Principal:
        if ROLE_RANK[p.role] < ROLE_RANK[role]:
            raise Problem(403, "Forbidden", f"Requires {role}", "insufficient_role")
        return p
    return dep


# ------------------------------------------------------------------ models

class RunSummary(BaseModel):
    id: str
    fn_id: str
    status: str
    key: Optional[str] = None
    started_at: datetime
    ended_at: Optional[datetime] = None
    attempt_no: int
    chain_position: int
    restored: bool = Field(False, description="Rewound by a point-in-time restore")


class StepView(BaseModel):
    step_hash: str
    step_id: str
    op: str
    status: str
    result: Optional[Any] = None
    meta: Optional[Any] = None
    attempts: int
    ended_at: Optional[datetime] = None
    error: Optional[Any] = None


class RunDetail(RunSummary):
    output: Optional[Any] = None
    error: Optional[Any] = None
    parent_run_id: Optional[str] = None
    lineage_id: str
    steps: list[StepView] = []
    pending_waits: list[dict] = []
    inbox_pending: int = 0
    children: list[str] = []


class Page(BaseModel):
    items: list
    next_cursor: Optional[str] = None


class EventIn(BaseModel):
    specversion: str = "1.0"
    id: Optional[str] = None
    source: str
    type: str
    data: dict = {}
    stepdkey: Optional[str] = None
    stepdidempotency: Optional[str] = None


class CommandResult(BaseModel):
    ok: bool
    command: str
    target: str


# ------------------------------------------------------------------ app

app = FastAPI(
    title="stepd management API",
    version="1.0.0",
    description="Read model and command surface for the stepd durable workflow engine.",
    openapi_version="3.1.0",
)
app.add_exception_handler(HTTPException, problem_handler)


def audit(conn, p: Principal, command: str, target: str, request_id: str = ""):
    q(conn, """INSERT INTO commands_audit (ns, actor, command, target, request_id)
               VALUES (%s,%s,%s,%s,%s)""",
      (p.ns, p.role, command, target, request_id or str(uuid.uuid4())))


# ---------------- runs

@app.get("/v1/runs", response_model=Page, tags=["runs"])
def list_runs(p: Principal = Depends(authenticate), conn=Depends(db),
              status: Optional[str] = None, fn_id: Optional[str] = None,
              key: Optional[str] = None, cursor: Optional[str] = None,
              limit: int = Query(50, le=200)):
    """Runs in the caller's namespace. Namespace is a WHERE clause, not a filter
    applied after the fact — a token for one namespace cannot see another exists."""
    sql = ["SELECT id::text, fn_id, status::text, key, started_at, ended_at,",
           "       attempt_no, chain_position, (restored_at IS NOT NULL) AS restored",
           "  FROM runs WHERE ns = %s"]
    args: list = [p.ns]
    if status:
        sql.append("AND status = %s::run_status"); args.append(status)
    if fn_id:
        sql.append("AND fn_id = %s"); args.append(fn_id)
    if key:
        sql.append("AND key = %s"); args.append(key)
    if cursor:
        sql.append("AND (started_at, id) < (SELECT started_at, id FROM runs WHERE id=%s)")
        args.append(cursor)
    sql.append("ORDER BY started_at DESC, id DESC LIMIT %s")
    args.append(limit + 1)
    rows = q(conn, " ".join(sql), args)
    nxt = rows[limit]["id"] if len(rows) > limit else None
    return Page(items=[RunSummary(**r) for r in rows[:limit]], next_cursor=nxt)


@app.get("/v1/runs/{run_id}", response_model=RunDetail, tags=["runs"])
def get_run(run_id: str, p: Principal = Depends(authenticate), conn=Depends(db)):
    rows = q(conn, """SELECT id::text, fn_id, status::text, key, started_at, ended_at,
                             attempt_no, chain_position, output, error,
                             parent_run_id::text, lineage_id::text,
                             (restored_at IS NOT NULL) AS restored
                        FROM runs WHERE id=%s::uuid AND ns=%s""", (run_id, p.ns))
    if not rows:
        # 404 not 403: revealing "exists but forbidden" leaks across namespaces
        raise Problem(404, "Not found", "No such run", "run_not_found")
    r = rows[0]
    steps = q(conn, """SELECT step_hash, step_id, op::text, status::text, result, meta,
                              attempts, ended_at, error
                         FROM run_steps WHERE run_id=%s::uuid
                        ORDER BY ended_at NULLS LAST, step_hash""", (run_id,))
    waits = q(conn, """SELECT step_hash, event_type, expires_at, prompt
                         FROM waits WHERE run_id=%s::uuid AND resolved_at IS NULL""",
              (run_id,))
    inbox = q(conn, """SELECT count(*) AS n FROM run_inbox
                        WHERE run_id=%s::uuid AND consumed_by_step_hash IS NULL""",
              (run_id,))[0]["n"]
    kids = q(conn, "SELECT id::text FROM runs WHERE parent_run_id=%s::uuid", (run_id,))
    return RunDetail(**r, steps=[StepView(**s) for s in steps],
                     pending_waits=[dict(w) for w in waits],
                     inbox_pending=inbox, children=[k["id"] for k in kids])


# ---------------- commands (the console uses exactly these)

@app.post("/v1/runs/{run_id}/cancel", response_model=CommandResult, tags=["commands"])
def cancel_run(run_id: str, p: Principal = Depends(require("operator")), conn=Depends(db)):
    n = q(conn, """UPDATE runs SET status='cancelled', ended_at=now()
                    WHERE id=%s::uuid AND ns=%s AND status NOT IN
                          ('completed','failed','cancelled')
                RETURNING id""", (run_id, p.ns))
    if not n:
        raise Problem(404, "Not found", "No such active run", "run_not_found")
    q(conn, "DELETE FROM queue WHERE run_id=%s::uuid", (run_id,))
    # cascade to non-detached children (protocol §7.5)
    q(conn, """UPDATE runs SET status='cancelled', ended_at=now()
                WHERE parent_run_id=%s::uuid AND NOT detached
                  AND status NOT IN ('completed','failed','cancelled')""", (run_id,))
    audit(conn, p, "cancel_run", run_id)
    return CommandResult(ok=True, command="cancel_run", target=run_id)


@app.post("/v1/runs/{run_id}/retry", response_model=CommandResult, tags=["commands"])
def retry_run(run_id: str, p: Principal = Depends(require("operator")), conn=Depends(db)):
    rows = q(conn, """SELECT fn_id FROM runs WHERE id=%s::uuid AND ns=%s
                        AND status IN ('failed','quarantined','cancelled')""",
             (run_id, p.ns))
    if not rows:
        raise Problem(404, "Not found", "No such retryable run", "run_not_retryable")
    q(conn, """UPDATE runs SET status='pending', ended_at=NULL, error=NULL,
                      quarantined_at=NULL, error_signature=NULL WHERE id=%s::uuid""",
      (run_id,))
    q(conn, """INSERT INTO queue (ns, fn_id, run_id) VALUES (%s,%s,%s::uuid)
               ON CONFLICT (run_id) DO UPDATE SET claimed_by=NULL, available_at=now()""",
      (p.ns, rows[0]["fn_id"], run_id))
    audit(conn, p, "retry_run", run_id)
    return CommandResult(ok=True, command="retry_run", target=run_id)


class ResolveWaitIn(BaseModel):
    event_type: str
    data: dict = {}


@app.post("/v1/runs/{run_id}/resolve-wait", response_model=CommandResult, tags=["commands"])
def resolve_wait(run_id: str, body: ResolveWaitIn,
                 p: Principal = Depends(require("operator")), conn=Depends(db)):
    """Priya's flow: unstick a run parked on a wait by injecting a synthetic event.
    Goes through deliver_to_inbox, the same path a real event takes."""
    if not q(conn, "SELECT 1 FROM runs WHERE id=%s::uuid AND ns=%s", (run_id, p.ns)):
        raise Problem(404, "Not found", "No such run", "run_not_found")
    res = q(conn, "SELECT deliver_to_inbox(%s::uuid,%s,%s::jsonb) AS r",
            (run_id, body.event_type, json.dumps(body.data)))[0]["r"]
    audit(conn, p, "resolve_wait", run_id)
    if res == "no_such_run":
        raise Problem(409, "Conflict", "Run is not active", "run_inactive")
    return CommandResult(ok=True, command=f"resolve_wait:{res}", target=run_id)


# ---------------- events

@app.post("/v1/events", tags=["events"])
def ingest(events: list[EventIn], p: Principal = Depends(require("operator")),
           conn=Depends(db)):
    """CloudEvents ingest, idempotent on stepdidempotency (F-EV-3)."""
    accepted, deduped, ids = 0, 0, []
    for e in events:
        row = q(conn, """SELECT event_id::text, deduplicated
                           FROM ingest_event(%s,%s,%s,%s::jsonb,%s,%s)""",
                (p.ns, e.type, e.source, json.dumps(e.data),
                 e.stepdkey, e.stepdidempotency))[0]
        ids.append(row["event_id"])
        if row["deduplicated"]:
            deduped += 1
        else:
            accepted += 1
    return {"accepted": accepted, "deduplicated": deduped, "ids": ids}


@app.get("/v1/events", response_model=Page, tags=["events"])
def list_events(p: Principal = Depends(authenticate), conn=Depends(db),
                type: Optional[str] = None, limit: int = Query(50, le=200)):
    sql = ["SELECT id::text, type, source, time, key, data, received_at",
           "  FROM events WHERE ns=%s"]
    args: list = [p.ns]
    if type:
        sql.append("AND type=%s"); args.append(type)
    sql.append("ORDER BY received_at DESC LIMIT %s"); args.append(limit)
    return Page(items=[dict(r) for r in q(conn, " ".join(sql), args)])


# ---------------- operational views

@app.get("/v1/queue/stats", tags=["ops"])
def queue_stats(p: Principal = Depends(authenticate), conn=Depends(db)):
    """Backlog per function, with queue age — Omar's first screen in an incident."""
    return {"functions": [dict(r) for r in q(conn, """
        SELECT fn_id,
               count(*) FILTER (WHERE claimed_by IS NULL) AS backlog,
               count(*) FILTER (WHERE claimed_by IS NOT NULL) AS in_flight,
               COALESCE(EXTRACT(epoch FROM now() -
                        min(available_at) FILTER (WHERE claimed_by IS NULL)), 0)::int
                        AS oldest_seconds
          FROM queue WHERE ns=%s GROUP BY fn_id ORDER BY backlog DESC""", (p.ns,))]}


@app.get("/v1/dlq", response_model=Page, tags=["ops"])
def dead_letter(p: Principal = Depends(authenticate), conn=Depends(db),
                limit: int = Query(50, le=200)):
    """Failed and quarantined runs, grouped for bulk action (F-LP-7)."""
    return Page(items=[dict(r) for r in q(conn, """
        SELECT id::text, fn_id, status::text, key, error, error_signature,
               quarantined_at, ended_at
          FROM runs WHERE ns=%s AND status IN ('failed','quarantined')
         ORDER BY COALESCE(quarantined_at, ended_at) DESC NULLS LAST LIMIT %s""",
        (p.ns, limit))])


@app.get("/v1/functions", tags=["functions"])
def list_functions(p: Principal = Depends(authenticate), conn=Depends(db)):
    return {"functions": [dict(r) for r in q(conn, """
        SELECT f.fn_id, f.version, f.paused, f.archived_at,
               a.url, a.last_seen, h.circuit::text AS circuit,
               h.consecutive_failures
          FROM functions f
          JOIN app_bindings a ON a.id = f.app_binding_id
     LEFT JOIN app_health h ON h.app_binding_id = a.id
         WHERE f.ns=%s ORDER BY f.fn_id""", (p.ns,))]}


@app.get("/", include_in_schema=False)
def console():
    """Serve the operations console. Assets are embedded in the binary in the
    real build (rust-embed); here the file sits beside the module."""
    from fastapi.responses import HTMLResponse
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "console.html")
    with open(path) as f:
        return HTMLResponse(f.read())


@app.get("/v1/health", tags=["ops"])
def health(conn=Depends(db)):
    q(conn, "SELECT 1")
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


# ------------------------------------------------------------------ bootstrap

def issue_token(conn, ns: str, role: str, name: str = "") -> str:
    raw = secrets.token_urlsafe(24)
    q(conn, """INSERT INTO tokens (id, ns, role, token_hash, name)
               VALUES (gen_random_uuid(), %s,%s,%s,%s)""",
      (ns, role, token_hash(raw), name))
    return raw


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8099, log_level="warning")
