#!/usr/bin/env python3
"""stepd — deterministic simulation testing harness.

A seeded simulator of the engine: virtual clock, in-memory store, and an adversarial
scheduler that injects crashes, duplicate delivery, lease expiry, reordering and
clock skew at every point where the real system could fail.

The point is NOT to test cases someone thought of. It is to explore interleavings
nobody thought of, and assert invariants that must hold in all of them. A failing
seed reproduces the exact interleaving.

Properties asserted (PRD §10.3):
  P1 no lost effect          every accepted op is reflected in run state
  P2 no duplicate record     a step hash is recorded at most once
  P3 no lost signal          a delivered event is consumed or explicitly discarded
  P4 hash stability          the hash sequence is identical across replays
  P5 keyed exclusivity       at most one active run per (function, key)
  P6 fence monotonicity      a stale fence never mutates state
  P7 chain continuity        no foreign run interleaves on a key across continue_as_new
  P8 cascade completeness    no non-detached descendant outlives a terminal parent
  P9 termination             every run reaches a terminal state or is legitimately blocked
"""
from __future__ import annotations

import hashlib
import random
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ------------------------------------------------------------------ model

class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SLEEPING = "sleeping"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL = {Status.COMPLETED, Status.FAILED, Status.CANCELLED}


def h(fn: str, sid: str, occ: int) -> str:
    return hashlib.sha256(f"{fn}\x1f{sid}\x1f{occ}".encode()).hexdigest()[:16]


@dataclass
class Step:
    hash: str
    step_id: str
    op: str
    status: str = "completed"
    data: object = None


@dataclass
class InboxEntry:
    seq: int
    event_type: str
    payload: object
    consumed_by: Optional[str] = None


@dataclass
class Wait:
    step_hash: str
    event_type: str
    resolved: bool = False


@dataclass
class Run:
    id: int
    fn: str
    key: Optional[str]
    status: Status = Status.PENDING
    fence: int = 0
    attempt: int = 0
    steps: dict = field(default_factory=dict)
    inbox: list = field(default_factory=list)
    waits: list = field(default_factory=list)
    lease_until: Optional[int] = None
    lease_owner: Optional[str] = None
    parent: Optional[int] = None
    detached: bool = False
    lineage: int = 0
    chain_pos: int = 0
    wake_at: Optional[int] = None
    queued: bool = True
    output: object = None


# ------------------------------------------------------------------ workflow

# A workflow shape rich enough to exercise every op. The simulator replays it
# exactly as an SDK would: walk from the top, return memoized values, yield the
# first unmemoized op.
def visible(run: Run) -> dict:
    """Protocol §4: the attempt request carries ONLY completed or terminally-failed
    steps. Pending rows exist in the store but are never sent to the handler."""
    return {h: st for h, st in run.steps.items()
            if st.status in ("completed", "failed", "timed_out")}


def handler(run: Run, world: "World"):
    fn = run.fn
    counters = {}
    seen = visible(run)

    def claim(sid):
        occ = counters.get(sid, 0)
        counters[sid] = occ + 1
        return h(fn, sid, occ)

    seq = []           # the hash sequence, for P4

    # step 1
    hh = claim("charge"); seq.append(hh)
    if hh not in seen:
        return seq, [{"op": "step", "id": "charge", "hash": hh, "data": {"tx": 1}}]

    # step 2: a loop of three, same id, distinct occurrences
    for i in range(3):
        hh = claim("item"); seq.append(hh)
        if hh not in seen:
            return seq, [{"op": "step", "id": "item", "hash": hh, "data": {"i": i}}]

    # step 3: parallel pair, hashes claimed in declaration order
    pa = claim("fetch-a"); seq.append(pa)
    pb = claim("fetch-b"); seq.append(pb)
    missing = [x for x in (pa, pb) if x not in seen]
    if missing:
        return seq, [{"op": "step", "id": "p", "hash": m, "data": {"m": m[:4]}} for m in missing]

    # step 4: sleep
    hh = claim("nap"); seq.append(hh)
    if hh not in seen:
        return seq, [{"op": "sleep", "id": "nap", "hash": hh, "for": 5}]

    # step 5: wait for an external event
    hh = claim("approval"); seq.append(hh)
    if hh not in seen:
        return seq, [{"op": "wait_event", "id": "approval", "hash": hh,
                      "event": "approved"}]

    # step 6: invoke a child (only at chain position 0, to bound the tree)
    if run.chain_pos == 0:
        hh = claim("child"); seq.append(hh)
        if hh not in seen:
            return seq, [{"op": "invoke", "id": "child", "hash": hh, "fn": "child-fn"}]

    # step 6b: a DETACHED child, which must survive the parent's termination
    # and must not block continue_as_new
    if run.chain_pos == 0:
        hh = claim("bg"); seq.append(hh)
        if hh not in seen:
            return seq, [{"op": "invoke", "id": "bg", "hash": hh, "fn": "child-fn",
                          "detach": True}]

    # step 7: continue_as_new once, then finish
    if run.chain_pos == 0:
        hh = claim("cycle"); seq.append(hh)
        return seq, [{"op": "continue_as_new", "id": "cycle", "hash": hh}]

    return seq, [{"op": "done", "data": {"ok": True}}]


def child_handler(run: Run, world: "World"):
    """Must accumulate the FULL hash sequence walked, exactly as the main handler
    does — P4 compares sequences as prefixes, so returning only the current hash
    makes every attempt look like a divergence."""
    seen = visible(run)
    seq = []
    # Deterministic per-run property, NOT a per-attempt coin flip: a handler that
    # branches randomly produces different step ids on each replay, which is a
    # workflow defect (P4 catches it — see test_p4_catches_nondeterminism).
    if "slow_children" in world.features and run.id % 2 == 0:
        hh2 = h(run.fn, "slow", 0)
        seq.append(hh2)
        if hh2 not in seen:
            return seq, [{"op": "sleep", "id": "slow", "hash": hh2, "for": 25}]
    hh = h(run.fn, "work", 0)
    seq.append(hh)
    if hh not in seen:
        return seq, [{"op": "step", "id": "work", "hash": hh, "data": {"c": 1}}]
    return seq, [{"op": "done", "data": {"child": True}}]


HANDLERS = {"main-fn": handler, "child-fn": child_handler}


# ------------------------------------------------------------------ world

class Violation(Exception):
    pass


class World:
    def __init__(self, seed: int, features: set):
        self.rng = random.Random(seed)
        self.seed = seed
        self.features = features
        self.now = 0
        self.runs: dict[int, Run] = {}
        self.next_id = 1
        self.inbox_seq = 0
        # bookkeeping for properties
        self.accepted_ops: list[tuple] = []      # (run_id, hash) the server accepted
        self.delivered: list[tuple] = []         # (run_id, event_type, seq)
        self.hash_sequences: dict[int, list] = {}
        self.key_active_log: list[tuple] = []    # (time, key, run_id)
        self.log: list[str] = []
        self.counters = {"cascade_cancel": 0, "can_rejected": 0, "stale_fence": 0,
                         "inbox_resolved_at_wait": 0, "inbox_buffered": 0,
                         "detached_created": 0, "external_cancel": 0}

    def note(self, s):
        if len(self.log) < 400:
            self.log.append(f"t={self.now} {s}")

    # ---------------- run lifecycle

    def create_run(self, fn, key=None, parent=None, detached=False,
                   lineage=None, chain_pos=0):
        # P5: keyed exclusivity is enforced at creation, as the DB index does
        if key is not None:
            for r in self.runs.values():
                if r.key == key and r.fn == fn and r.status not in TERMINAL:
                    return None
        rid = self.next_id
        self.next_id += 1
        r = Run(id=rid, fn=fn, key=key, parent=parent, detached=detached,
                lineage=lineage if lineage is not None else rid, chain_pos=chain_pos)
        self.runs[rid] = r
        self.note(f"create run={rid} fn={fn} key={key} chain={chain_pos}")
        return r

    def claimable(self):
        return [r for r in self.runs.values()
                if r.queued and r.status in (Status.PENDING, Status.RUNNING)
                and (r.lease_until is None or r.lease_until <= self.now)
                and (r.wake_at is None or r.wake_at <= self.now)]

    def claim(self, worker):
        c = self.claimable()
        if not c:
            return None
        r = self.rng.choice(c)
        r.fence += 1                      # fencing: any older attempt is now stale
        r.attempt += 1
        r.status = Status.RUNNING
        r.lease_owner = worker
        r.lease_until = self.now + 10
        self.note(f"claim run={r.id} fence={r.fence} by={worker}")
        return (r.id, r.fence)

    # ---------------- the commit, mirroring commit_ops

    def commit(self, run_id, fence, ops):
        r = self.runs[run_id]
        if fence != r.fence:
            self.counters["stale_fence"] += 1
            self.note(f"REJECT stale fence run={run_id} got={fence} want={r.fence}")
            return "stale_fence"           # P6
        if r.status in TERMINAL:
            return "terminal"

        suspend = False
        for op in ops:
            kind = op["op"]
            hh = op.get("hash")

            if kind == "step":
                if hh not in r.steps:      # P2: idempotent, first write wins
                    r.steps[hh] = Step(hh, op["id"], "step", data=op.get("data"))
                self.accepted_ops.append((run_id, hh))

            elif kind == "sleep":
                if hh not in r.steps:
                    r.steps[hh] = Step(hh, op["id"], "sleep", status="pending")
                r.wake_at = self.now + op["for"]
                suspend = True
                self.accepted_ops.append((run_id, hh))

            elif kind == "wait_event":
                # P3: check the inbox in the same transaction (protocol §7.6)
                match = next((e for e in r.inbox
                              if e.event_type == op["event"] and e.consumed_by is None),
                             None)
                if match:
                    match.consumed_by = hh
                    r.steps[hh] = Step(hh, op["id"], "wait_event", data=match.payload)
                    self.counters["inbox_resolved_at_wait"] += 1
                    self.note(f"wait resolved from inbox run={run_id}")
                else:
                    if hh not in r.steps:
                        r.steps[hh] = Step(hh, op["id"], "wait_event", status="pending")
                    r.waits.append(Wait(hh, op["event"]))
                    suspend = True
                self.accepted_ops.append((run_id, hh))

            elif kind == "invoke":
                if hh not in r.steps:
                    detach = op.get("detach", False)
                    child = self.create_run("child-fn", parent=run_id, detached=detach)
                    if child is None:
                        r.steps[hh] = Step(hh, op["id"], "invoke", data={"skipped": True})
                    elif detach:
                        self.counters["detached_created"] += 1
                        # fire and forget: result is the child id, recorded at once
                        r.steps[hh] = Step(hh, op["id"], "invoke",
                                           data={"detached": child.id})
                    else:
                        r.steps[hh] = Step(hh, op["id"], "invoke", status="pending",
                                           data={"child": child.id})
                        suspend = True
                self.accepted_ops.append((run_id, hh))

            elif kind == "continue_as_new":
                # A child's result would be delivered into a journal that
                # continue_as_new discards, so live non-detached children make
                # the op an error rather than an orphan-maker (protocol §5.1).
                live = [c for c in self.runs.values()
                        if c.parent == r.id and not c.detached
                        and c.status not in TERMINAL]
                if live:
                    r.status = Status.FAILED
                    r.output = {"error": "continue_as_new_with_live_children"}
                    r.queued = False
                    self._on_terminal(r)
                    self.counters["can_rejected"] += 1
                    self.note(f"continue_as_new REJECTED run={r.id} live children={[c.id for c in live]}")
                    return "committed"
                r.status = Status.COMPLETED
                r.queued = False
                # P7: successor inherits the key; nothing may slip in between
                succ = self.create_run(r.fn, key=r.key, lineage=r.lineage,
                                       chain_pos=r.chain_pos + 1)
                if succ is None:
                    raise Violation(f"P7 chain broken: successor blocked on key {r.key}")
                self.note(f"continue_as_new {r.id} -> {succ.id}")
                return "committed"

            elif kind == "done":
                r.status = Status.COMPLETED
                r.output = op.get("data")
                r.queued = False
                self._on_terminal(r)
                return "committed"

        if suspend:
            r.status = Status.SLEEPING
            r.lease_owner = None
            r.lease_until = None
        else:
            r.status = Status.PENDING
            r.lease_owner = None
            r.lease_until = None
        return "committed"

    def _on_terminal(self, r: Run):
        # P8: cascade to non-detached descendants
        for c in self.runs.values():
            if c.parent == r.id and not c.detached and c.status not in TERMINAL:
                c.status = Status.CANCELLED
                c.queued = False
                self.counters["cascade_cancel"] += 1
                self.note(f"cascade cancel child={c.id} of={r.id}")
        # resolve the parent's invoke step, if this run was a child
        if r.parent is not None and r.parent in self.runs:
            p = self.runs[r.parent]
            for st in p.steps.values():
                if st.op == "invoke" and st.status == "pending" \
                        and isinstance(st.data, dict) and st.data.get("child") == r.id:
                    st.status = "completed"
                    st.data = {"child": r.id, "out": r.output}
                    if p.status not in TERMINAL:
                        p.status = Status.PENDING
                        p.wake_at = None

    # ---------------- signal delivery

    def deliver(self, run_id, event_type, payload):
        r = self.runs.get(run_id)
        if r is None or r.status in TERMINAL:
            return "no_run"
        self.inbox_seq += 1
        e = InboxEntry(self.inbox_seq, event_type, payload)
        r.inbox.append(e)
        self.delivered.append((run_id, event_type, e.seq))
        w = next((w for w in r.waits if w.event_type == event_type and not w.resolved), None)
        if w is None:
            self.counters["inbox_buffered"] += 1
            return "buffered"
        w.resolved = True
        e.consumed_by = w.step_hash
        r.steps[w.step_hash] = Step(w.step_hash, "approval", "wait_event", data=payload)
        r.status = Status.PENDING
        r.wake_at = None
        self.note(f"deliver resolved wait run={run_id}")
        return "resolved"

    # ---------------- properties

    def check_invariants(self):
        # P5: keyed exclusivity, continuously
        seen = {}
        for r in self.runs.values():
            if r.key is not None and r.status not in TERMINAL:
                k = (r.fn, r.key)
                if k in seen:
                    raise Violation(f"P5 two active runs on key {k}: {seen[k]} and {r.id}")
                seen[k] = r.id

        # P2: no duplicate records
        for r in self.runs.values():
            hs = list(r.steps.keys())
            if len(hs) != len(set(hs)):
                raise Violation(f"P2 duplicate step hash in run {r.id}")

        # P3 (continuous): a parked wait must never coexist with a matching
        # unconsumed inbox entry. THIS is the lost-signal condition. The weaker
        # "unconsumed and not terminal" fires on any run still in flight.
        for r in self.runs.values():
            if r.status in TERMINAL:
                continue
            for wt in r.waits:
                if wt.resolved:
                    continue
                m = next((e for e in r.inbox
                          if e.event_type == wt.event_type and e.consumed_by is None), None)
                if m is not None:
                    raise Violation(
                        f"P3 LOST SIGNAL run={r.id} parked on wait '{wt.event_type}' "
                        f"while inbox entry seq={m.seq} sits unconsumed")

        # P8: no non-detached descendant outlives a terminal parent
        for r in self.runs.values():
            if r.parent is not None and not r.detached:
                p = self.runs.get(r.parent)
                if p and p.status in TERMINAL and r.status not in TERMINAL:
                    raise Violation(f"P8 child {r.id} alive after parent {p.id} terminal")

    def check_final(self):
        # P3 (at quiescence): no run may be blocked on a wait it could satisfy.
        for (rid, et, seq) in self.delivered:
            r = self.runs[rid]
            entry = next((e for e in r.inbox if e.seq == seq), None)
            if entry is None:
                raise Violation(f"P3 delivered event vanished run={rid} seq={seq}")
        for r in self.runs.values():
            if r.status in TERMINAL:
                continue
            for wt in r.waits:
                if not wt.resolved and any(
                        e.event_type == wt.event_type and e.consumed_by is None
                        for e in r.inbox):
                    raise Violation(
                        f"P3 run={r.id} blocked at quiescence holding a matching event")

        # P1: every accepted op is present in run state
        for (rid, hh) in self.accepted_ops:
            r = self.runs[rid]
            if hh not in r.steps:
                raise Violation(f"P1 accepted op missing run={rid} hash={hh}")

        # P9: no run left running or holding an expired lease
        for r in self.runs.values():
            if r.status == Status.RUNNING and (r.lease_until or 0) > self.now:
                continue
            if r.status == Status.RUNNING:
                raise Violation(f"P9 run {r.id} stuck RUNNING with expired lease")

        # P7: chain continuity — positions unique per lineage
        by_lineage = {}
        for r in self.runs.values():
            by_lineage.setdefault(r.lineage, []).append(r.chain_pos)
        for lin, positions in by_lineage.items():
            if len(positions) != len(set(positions)):
                raise Violation(f"P7 duplicate chain position in lineage {lin}")


# ------------------------------------------------------------------ scheduler

FEATURES = ["crash_before_commit", "crash_after_commit", "duplicate_commit",
            "lease_expiry", "early_signal", "late_signal", "clock_jump",
            "reorder_delivery", "concurrent_workers", "cancel_runs",
            "slow_children"]


LAST_WORLD = {}


def simulate(seed: int, steps: int = 400) -> Optional[str]:
    rng = random.Random(seed)
    # swarm testing: each seed enables a random SUBSET of fault types, which
    # reaches unusual combinations far faster than enabling everything always
    k = rng.randint(2, len(FEATURES))
    features = set(rng.sample(FEATURES, k))
    w = World(seed, features)
    LAST_WORLD["w"] = w

    root = w.create_run("main-fn", key=f"order:{seed % 5}")
    if root is None:
        return None
    pending_commits = []          # attempts in flight, to be applied or dropped

    try:
        for _ in range(steps):
            w.now += rng.randint(0, 2) if "clock_jump" not in features else rng.randint(0, 9)

            action = rng.random()

            # 1. deliver a signal, possibly before the run has registered its wait
            if action < 0.18 and ("early_signal" in features or "late_signal" in features):
                targets = [r for r in w.runs.values() if r.status not in TERMINAL]
                if targets:
                    t = rng.choice(targets)
                    w.deliver(t.id, "approved", {"by": f"s{seed}"})

            # 2. a worker claims and runs an attempt
            elif action < 0.75:
                claimed = w.claim(f"w{rng.randint(1,3)}")
                if claimed:
                    rid, fence = claimed
                    r = w.runs[rid]
                    hnd = HANDLERS[r.fn]
                    seq, ops = hnd(r, w)
                    # P4: the hash sequence must never differ between attempts
                    prev = w.hash_sequences.get(rid)
                    if prev is not None:
                        n = min(len(prev), len(seq))
                        if prev[:n] != seq[:n]:
                            raise Violation(
                                f"P4 hash sequence diverged run={rid}\n"
                                f"  prev={prev[:n]}\n  now ={seq[:n]}")
                    if prev is None or len(seq) > len(prev):
                        w.hash_sequences[rid] = seq

                    if "crash_before_commit" in features and rng.random() < 0.15:
                        w.note(f"CRASH before commit run={rid}")
                        continue                       # attempt lost entirely
                    pending_commits.append((rid, fence, ops))

            # 3. apply a pending commit, possibly out of order or twice
            elif pending_commits:
                idx = rng.randrange(len(pending_commits)) if "reorder_delivery" in features else 0
                rid, fence, ops = pending_commits.pop(idx)
                w.commit(rid, fence, ops)
                if "duplicate_commit" in features and rng.random() < 0.2:
                    w.commit(rid, fence, ops)          # at-least-once delivery
                if "crash_after_commit" in features and rng.random() < 0.1:
                    w.note(f"CRASH after commit run={rid}")

            # 3b. external cancellation, which must cascade to non-detached children
            elif "cancel_runs" in features and rng.random() < 0.06:
                live = [r for r in w.runs.values()
                        if r.status not in TERMINAL and r.parent is None]
                if live:
                    victim = rng.choice(live)
                    victim.status = Status.CANCELLED
                    victim.queued = False
                    w.counters["external_cancel"] += 1
                    w.note(f"external cancel run={victim.id}")
                    w._on_terminal(victim)

            # 4. lease expiry: another worker steals the run mid-attempt
            elif "lease_expiry" in features and rng.random() < 0.5:
                for r in w.runs.values():
                    if r.status == Status.RUNNING and rng.random() < 0.3:
                        r.lease_until = w.now           # expire it now
                        w.note(f"lease expired run={r.id}")

            # wake sleepers
            for r in w.runs.values():
                if r.status == Status.SLEEPING and r.wake_at is not None \
                        and r.wake_at <= w.now and not r.waits:
                    for st in r.steps.values():
                        if st.op == "sleep" and st.status == "pending":
                            st.status = "completed"
                    r.status = Status.PENDING
                    r.wake_at = None

            w.check_invariants()

        # Drain to quiescence: apply everything in flight, then run the engine
        # with NO fault injection until nothing more can progress. Final
        # properties are only meaningful once the system has settled.
        for (rid, fence, ops) in pending_commits:
            w.commit(rid, fence, ops)
        for _ in range(600):
            w.now += 1
            for r in w.runs.values():
                if r.status == Status.SLEEPING and r.wake_at is not None \
                        and r.wake_at <= w.now and not [x for x in r.waits if not x.resolved]:
                    for st in r.steps.values():
                        if st.op == "sleep" and st.status == "pending":
                            st.status = "completed"
                    r.status = Status.PENDING
                    r.wake_at = None
            c = w.claim("drain")
            if c is None:
                if not any(r.status in (Status.PENDING, Status.RUNNING)
                           for r in w.runs.values()):
                    break
                continue
            rid, fence = c
            r = w.runs[rid]
            _seq, ops = HANDLERS[r.fn](r, w)
            w.commit(rid, fence, ops)
            w.check_invariants()
        w.check_invariants()
        w.check_final()
        return None

    except Violation as v:
        return f"seed={seed} features={sorted(features)}\n  {v}\n  trace tail:\n    " + \
               "\n    ".join(w.log[-12:])


def test_continue_as_new_rejects_live_children() -> bool:
    """Targeted test for a state the fuzzer CANNOT reach.

    Coverage showed `can_rejected` at 0 across every seed. That is not a gap in
    the fuzzer: with blocking `invoke`, a run suspends until its child is terminal,
    so a live non-detached child at continue_as_new is unreachable by construction.

    The rule is therefore a defensive invariant, not a live code path. It is kept
    because a future non-blocking invoke variant would reintroduce the hazard
    silently, and it is tested directly here rather than left to a fuzzer that
    provably cannot produce the state."""
    w = World(4242, set())
    parent = w.create_run("main-fn", key="k")
    child = w.create_run("child-fn", parent=parent.id, detached=False)
    parent.fence = 1
    res = w.commit(parent.id, 1, [{"op": "continue_as_new", "id": "c",
                                   "hash": h("main-fn", "c", 0)}])
    rejected = (w.counters["can_rejected"] == 1
                and parent.status == Status.FAILED
                and parent.output == {"error": "continue_as_new_with_live_children"})

    # and the detached case must be ALLOWED
    w2 = World(4243, set())
    p2 = w2.create_run("main-fn", key="k2")
    w2.create_run("child-fn", parent=p2.id, detached=True)
    p2.fence = 1
    w2.commit(p2.id, 1, [{"op": "continue_as_new", "id": "c",
                          "hash": h("main-fn", "c", 0)}])
    allowed = (w2.counters["can_rejected"] == 0
               and p2.status == Status.COMPLETED)
    return rejected and allowed


def test_p4_catches_nondeterminism() -> bool:
    """Positive control. A property that never fires proves nothing, so we
    deliberately introduce a non-deterministic handler and assert P4 fires."""
    w = World(999, {"concurrent_workers"})
    r = w.create_run("main-fn")
    flip = [0]

    def bad_handler(run, world):
        flip[0] += 1
        sid = "a" if flip[0] % 2 else "b"      # id depends on attempt count
        hh = h(run.fn, sid, 0)
        return [hh], [{"op": "step", "id": sid, "hash": hh, "data": {}}]

    seqs = {}
    for _ in range(2):
        rid, fence = w.claim("w1")
        seq, ops = bad_handler(w.runs[rid], w)
        prev = seqs.get(rid)
        if prev is not None and prev[:len(seq)] != seq[:len(prev)]:
            return True                         # P4 fired, as it must
        seqs[rid] = seq
        w.commit(rid, fence, ops)
    return False


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    if not test_p4_catches_nondeterminism():
        print("  FAIL  P4 is vacuous: it did not fire on a non-deterministic handler")
        return 1
    print("  positive control: P4 fires on a non-deterministic handler ✓")
    if not test_continue_as_new_rejects_live_children():
        print("  FAIL  continue_as_new does not reject live non-detached children")
        return 1
    print("  targeted: continue_as_new rejects live children, allows detached ✓")
    print(f"stepd deterministic simulation — {n} seeds")
    failures = []
    for seed in range(n):
        r = simulate(seed)
        if r:
            failures.append(r)
            if len(failures) >= 3:
                break
    if failures:
        print(f"\n{len(failures)} PROPERTY VIOLATION(S)\n")
        for f in failures:
            print(f)
            print()
        return 1
    print(f"  all {n} seeds passed — no property violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
