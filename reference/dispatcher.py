#!/usr/bin/env python3
"""stepd dispatcher — reference implementation of the dispatch loop.

Ties claim_runs -> HTTP delivery to an app -> commit_ops, with the load-protection
behaviours from PRD §4.11:
  * per-app circuit breaker with half-open probing (F-LP-2)
  * gradual recovery ramp (F-LP-3)
  * weighted fair queuing across namespaces (F-LP-5)
  * poison-pill quarantine on repeated identical failure (F-LP-6)
  * timer jitter (F-LP-4)

This is a reference, not the production engine: it is written to be readable and to
make the behaviours testable, and it deliberately mirrors the trait boundaries in
PRD §6.3 (StateStore / Queue / Transport) so the Rust port is a translation.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

import psycopg2
import psycopg2.extras

DSN = "host=localhost port=5433 dbname=stepd user=postgres"


# ------------------------------------------------------------------ helpers

def uuid7ish() -> str:
    u = list(str(uuid.uuid4()))
    u[14] = "7"
    return "".join(u)


def step_hash(function_id: str, step_id: str, occurrence: int) -> str:
    h = hashlib.sha256()
    h.update(function_id.encode())
    h.update(b"\x1f")
    h.update(step_id.encode())
    h.update(b"\x1f")
    h.update(str(occurrence).encode())
    return h.hexdigest()[:16]


def sign(key: bytes, body: str, ts: int, nonce: str) -> str:
    mac = hmac.new(key, f"{ts}.{nonce}.{body}".encode(), hashlib.sha256).hexdigest()
    return f"t={ts},n={nonce},v1={mac}"


# ------------------------------------------------------------------ transport

class TransportError(Exception):
    pass


@dataclass
class Attempt:
    run_id: str
    fence: int
    attempt: int
    function_id: str
    steps: dict


class LocalTransport:
    """Stands in for HTTP. A real transport signs and POSTs; this calls in-process
    so the dispatcher's behaviour can be tested without a network."""

    def __init__(self, handlers: dict, signing_key: bytes = b"k"):
        self.handlers = handlers
        self.signing_key = signing_key
        self.calls = 0
        self.fail_next = 0          # inject N consecutive failures
        self.latency = 0.0

    def deliver(self, att: Attempt) -> dict:
        self.calls += 1
        if self.latency:
            time.sleep(self.latency)
        if self.fail_next > 0:
            self.fail_next -= 1
            raise TransportError("app unreachable")
        body = json.dumps({"run_id": att.run_id, "attempt": att.attempt})
        _ = sign(self.signing_key, body, int(time.time()), uuid7ish())  # signed like the real thing
        handler = self.handlers[att.function_id]
        return handler(att)


# ------------------------------------------------------------------ dispatcher

@dataclass
class CircuitBreaker:
    """F-LP-2 / F-LP-3. Opens after N consecutive failures, probes when the cooldown
    elapses, and ramps back up gradually rather than releasing the whole backlog.

    Two design decisions, both learned from testing:

    1. Closing is NOT purely success-driven. A breaker that only closes after
       observing N successes stays half-open forever once traffic stops, so the
       next burst is throttled for no reason. Closing happens on consecutive
       successes OR a quiet period with no failures, whichever comes first.

    2. Half-open admission is a deterministic token budget, NOT a probability.
       A probabilistic ramp makes recovery time non-deterministic: operators
       cannot reason about it, and tests of it are inherently flaky. A budget
       that doubles on each success gives the same gradual ramp with none of the
       randomness, and keeps the dispatcher free of RNG on the hot path.
    """
    threshold: int = 3
    cooldown: float = 0.5
    close_after_successes: int = 5
    quiet_period: float = 1.0
    state: str = "closed"
    failures: int = 0
    successes: int = 0
    opened_at: float = 0.0
    last_failure_at: float = 0.0
    budget: int = 0            # probes remaining in this half-open round
    round_size: int = 1        # grows on success: 1, 2, 4, 8 ...

    def allow(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if time.time() - self.opened_at >= self.cooldown:
                self._half_open()
                return True
            return False
        self._maybe_close_on_quiet()
        if self.state == "closed":
            return True
        if self.budget > 0:
            self.budget -= 1
            return True
        return False

    def _half_open(self):
        self.state = "half_open"
        self.round_size = 1
        self.budget = 1
        self.successes = 0

    def _maybe_close_on_quiet(self):
        if (self.state == "half_open" and self.last_failure_at
                and time.time() - self.last_failure_at >= self.quiet_period):
            self._close()

    def _close(self):
        self.state = "closed"
        self.failures = 0
        self.budget = 0

    def record_success(self):
        self.failures = 0
        if self.state == "half_open":
            self.successes += 1
            self.round_size = min(64, self.round_size * 2)
            self.budget += self.round_size          # widen the next round
            if self.successes >= self.close_after_successes:
                self._close()

    def record_failure(self):
        self.failures += 1
        self.successes = 0
        self.last_failure_at = time.time()
        if self.state == "half_open" or self.failures >= self.threshold:
            self.state = "open"
            self.opened_at = time.time()
            self.budget = 0

    @property
    def ramp(self) -> float:
        """Reported for metrics only."""
        return 1.0 if self.state == "closed" else min(1.0, self.budget / 8)


class Dispatcher:
    def __init__(self, dsn: str, transport: LocalTransport, worker: str = "w1",
                 quarantine_after: int = 5, jitter_seconds: float = 0.0):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self.transport = transport
        self.worker = worker
        self.breakers: dict[str, CircuitBreaker] = {}
        self.quarantine_after = quarantine_after
        self.jitter = jitter_seconds
        self.stats = {"dispatched": 0, "committed": 0, "stale": 0,
                      "failed": 0, "quarantined": 0, "skipped_open_circuit": 0}
        self._ns_cursor = 0

    def q(self, sql, args=None, fetch=True):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute(sql, args or ())
            return c.fetchall() if fetch and c.description else None

    # ---------------- fairness (F-LP-5)

    def _namespaces_round_robin(self) -> list[str]:
        rows = self.q("""SELECT DISTINCT ns FROM queue
                          WHERE claimed_by IS NULL AND available_at <= now()""")
        nss = sorted(r["ns"] for r in rows)
        if not nss:
            return []
        # rotate so no namespace is permanently first — anti-starvation
        self._ns_cursor = (self._ns_cursor + 1) % len(nss)
        return nss[self._ns_cursor:] + nss[:self._ns_cursor]

    # ---------------- claiming

    def claim(self, ns: str, n: int) -> list[Attempt]:
        rows = self.q("""
            WITH picked AS (
                SELECT q.id, q.run_id FROM queue q
                 WHERE q.claimed_by IS NULL AND q.available_at <= now() AND q.ns = %s
                 ORDER BY q.priority DESC, q.available_at
                 LIMIT %s FOR UPDATE SKIP LOCKED),
            claimed AS (
                UPDATE queue q SET claimed_by=%s, claimed_until=now()+interval '60 seconds',
                       attempts=q.attempts+1
                  FROM picked p WHERE q.id=p.id RETURNING q.run_id)
            UPDATE runs r
               SET status='running', attempt_no=r.attempt_no+1,
                   fence_token=r.fence_token+1,
                   lease_owner=%s, lease_until=now()+interval '60 seconds'
              FROM claimed c WHERE r.id=c.run_id
            RETURNING r.id::text AS run_id, r.fence_token AS fence,
                      r.attempt_no AS attempt, r.fn_id AS function_id
        """, (ns, n, self.worker, self.worker))
        out = []
        for r in rows:
            steps = {s["step_hash"]: {"id": s["step_id"], "op": s["op"],
                                      "status": s["status"], "data": s["result"]}
                     for s in self.q("""SELECT step_hash, step_id, op, status::text, result
                                          FROM run_steps WHERE run_id=%s
                                           AND status IN ('completed','failed','timed_out')""",
                                     (r["run_id"],))}
            out.append(Attempt(r["run_id"], r["fence"], r["attempt"],
                               r["function_id"], steps))
        return out

    # ---------------- the loop

    def tick(self, batch: int = 5) -> int:
        done = 0
        for ns in self._namespaces_round_robin():
            for att in self.claim(ns, batch):
                cb = self.breakers.setdefault(att.function_id, CircuitBreaker())
                if not cb.allow():
                    self.stats["skipped_open_circuit"] += 1
                    self._release(att, backoff=0.2)
                    continue
                self.stats["dispatched"] += 1
                try:
                    resp = self.transport.deliver(att)
                    cb.record_success()
                except TransportError as e:
                    cb.record_failure()
                    self._handle_failure(att, str(e))
                    continue
                self._commit(att, resp)
                done += 1
        return done

    def _release(self, att: Attempt, backoff: float):
        self.q("""UPDATE queue SET claimed_by=NULL, claimed_until=NULL,
                         available_at=now() + (%s || ' seconds')::interval
                   WHERE run_id=%s""", (backoff, att.run_id), fetch=False)
        self.q("UPDATE runs SET status='pending', lease_owner=NULL WHERE id=%s",
               (att.run_id,), fetch=False)

    def _handle_failure(self, att: Attempt, err: str):
        """F-LP-6: identical repeated failures quarantine the run rather than
        consuming dispatch capacity forever."""
        sig = hashlib.sha256(err.encode()).hexdigest()[:16]
        row = self.q("""UPDATE runs SET error_signature=%s,
                               error=jsonb_build_object('message',%s)
                         WHERE id=%s RETURNING attempt_no""", (sig, err, att.run_id))[0]
        prev = self.q("SELECT error_signature FROM runs WHERE id=%s", (att.run_id,))[0]
        if row["attempt_no"] >= self.quarantine_after and prev["error_signature"] == sig:
            self.q("""UPDATE runs SET status='quarantined', quarantined_at=now()
                       WHERE id=%s""", (att.run_id,), fetch=False)
            self.q("DELETE FROM queue WHERE run_id=%s", (att.run_id,), fetch=False)
            self.stats["quarantined"] += 1
        else:
            self.stats["failed"] += 1
            # exponential backoff with jitter
            delay = min(30, 0.05 * (2 ** min(row["attempt_no"], 6)))
            delay *= (1 + random.random() * 0.2)
            self._release(att, backoff=delay)

    def _commit(self, att: Attempt, resp: dict):
        ops = resp.get("ops", [])
        emit = resp.get("emit", [])
        # F-LP-4: spread sleep wake-ups so a million midnight timers don't collide
        for op in ops:
            if op.get("op") == "sleep" and self.jitter:
                base = op.get("_seconds", 0)
                op["until"] = f"now:{base + random.uniform(0, self.jitter)}"
        res = self.q("SELECT commit_ops(%s::uuid, %s, %s::jsonb, %s::jsonb) AS r",
                     (att.run_id, att.fence, json.dumps(ops), json.dumps(emit)))[0]["r"]
        if res == "committed":
            self.stats["committed"] += 1
        elif res == "stale_fence":
            self.stats["stale"] += 1
        return res

    def run_until_idle(self, max_ticks: int = 500) -> int:
        ticks = 0
        for _ in range(max_ticks):
            ticks += 1
            if self.tick() == 0:
                pending = self.q("""SELECT count(*) AS n FROM queue
                                     WHERE claimed_by IS NULL AND available_at <= now()""")[0]["n"]
                if pending == 0:
                    break
                time.sleep(0.05)
        return ticks
