-- stepd — schema 010: make the cancellation compensation attempt actually happen.
--
-- WHY THIS MIGRATION EXISTS
--
-- Protocol §7.4: "On cancel the server stops scheduling attempts. If the function
-- declares `on_cancel`, one final attempt is made with `run.cancelling: true`;
-- the SDK runs only the compensation path. Compensation steps memoize normally."
--
-- Every part of that existed except the part that makes it run. The wire type
-- carries `cancelling` (stepd-proto). The SDK reads it (`ctx.run().cancelling`).
-- The store computes it (`load_attempt`). And `cancel_run` **deleted the queue
-- row**, so the run was never dispatched again and the flag could never be true.
--
-- Nothing errored. A workflow cancelled mid-flight simply never ran its
-- compensation: the refund was not issued, the reservation was not released, the
-- partner was not told. The run showed `cancelled`, which is what the operator
-- asked for and exactly what it looks like when it worked.
--
-- Found by the conformance suite (protocol §12, `cancel`) — the first thing that
-- ever asked whether the compensation path executed. It is the same shape as the
-- cron scheduler: specified, plumbed end to end, and unreachable by one line.
--
-- THE DESIGN, AND WHAT WAS REJECTED
--
-- A durable `runs.compensating` flag rather than a new `cancelling` run status.
--
-- The status looked right at first and is wrong in practice: compensation needs
-- *several* attempts, because one new step per attempt means a three-step
-- compensation path takes three. So the run cycles pending → running → pending
-- like any other, and a status that had to survive that churn would have to be
-- re-asserted on every transition — by a trigger, or by threading it through
-- every UPDATE in `commit_ops`. A boolean that nothing else writes is a smaller
-- thing to be right about.
--
-- The flag is durable rather than derived because it must survive a crash: a
-- server that dies between the cancel and the compensation attempt must still
-- know, on restart, that the run owes an undo.

ALTER TABLE runs ADD COLUMN IF NOT EXISTS compensating boolean NOT NULL DEFAULT false;

-- Keyed exclusivity already covers this: a compensating run is `pending` or
-- `running`, both of which are in the active set, so it keeps its key until the
-- compensation finishes. That is load-bearing and worth saying out loud —
-- releasing the key the moment cancel was requested would let the next run for
-- that key start while the previous one is still issuing refunds, which is two
-- runs interleaved on one key, appearing only when a cancel races an event.

-- ---------------------------------------------------------------- cancel

-- Cancel a run, and schedule its compensation attempt if the function wants one.
--
-- §7.4 conditions the attempt on the function declaring `on_cancel`, so this
-- reads the registered config rather than compensating unconditionally.
-- Dispatching a compensation attempt to a handler that has no compensation path
-- would hand it a normal-looking attempt with `cancelling: true` and rely on it
-- noticing — which is the sort of thing that works in the SDK that was tested
-- and not in the one that was not.
CREATE OR REPLACE FUNCTION cancel_run(p_ns text, p_run_id uuid) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    v_run       record;
    v_wants     boolean;
BEGIN
    SELECT id, ns, fn_id, key INTO v_run FROM runs
     WHERE id = p_run_id AND ns = p_ns
       AND status NOT IN ('completed', 'failed', 'cancelled')
       FOR UPDATE;

    IF v_run.id IS NULL THEN RETURN false; END IF;

    SELECT COALESCE((f.config->>'on_cancel')::boolean, false) INTO v_wants
      FROM functions f
     WHERE f.ns = v_run.ns AND f.fn_id = v_run.fn_id
     ORDER BY f.archived_at NULLS FIRST, f.created_at DESC
     LIMIT 1;

    IF COALESCE(v_wants, false) THEN
        -- Not terminal yet. The handler still owes an undo, and §7.4 requires
        -- its steps to memoize normally, which a terminal run cannot do.
        UPDATE runs SET compensating = true, status = 'pending',
                        lease_owner = NULL, lease_until = NULL
         WHERE id = p_run_id;

        -- The queue's unique index on run_id makes a double-enqueue impossible
        -- rather than merely unlikely, so a cancel delivered twice cannot
        -- compensate twice.
        INSERT INTO queue (ns, fn_id, key, run_id, available_at)
        VALUES (v_run.ns, v_run.fn_id, v_run.key, p_run_id, now())
        ON CONFLICT (run_id) DO UPDATE
            SET available_at = now(), claimed_by = NULL, claimed_until = NULL;
    ELSE
        UPDATE runs SET status = 'cancelled', ended_at = now() WHERE id = p_run_id;
        DELETE FROM queue WHERE run_id = p_run_id;
    END IF;

    -- The cascade runs either way. A descendant's own `on_cancel` is consulted
    -- by `cascade_cancel`, so a tree of compensating children each get their
    -- attempt without this function having to know the shape of the tree.
    PERFORM cascade_cancel(p_run_id, 'parent_cancelled');
    RETURN true;
END $$;

-- The cascade, extended to schedule compensation for descendants that want it.
--
-- Still one recursive statement — structural invariant 13 fails the build if it
-- becomes a per-level loop, because a cascade that commits as it goes can
-- half-finish and orphan every level below where it stopped.
CREATE OR REPLACE FUNCTION cascade_cancel(p_run_id uuid, p_reason text DEFAULT 'parent_cancelled')
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE v_n integer;
BEGIN
    WITH RECURSIVE tree AS (
        SELECT id FROM runs WHERE parent_run_id = p_run_id AND NOT detached
        UNION ALL
        SELECT r.id FROM runs r JOIN tree t ON r.parent_run_id = t.id
         WHERE NOT r.detached
    ),
    live AS (
        SELECT r.id, r.ns, r.fn_id, r.key,
               COALESCE((f.config->>'on_cancel')::boolean, false) AS wants
          FROM runs r
          JOIN tree t ON t.id = r.id
          LEFT JOIN LATERAL (
              SELECT config FROM functions f2
               WHERE f2.ns = r.ns AND f2.fn_id = r.fn_id
               ORDER BY f2.archived_at NULLS FIRST, f2.created_at DESC LIMIT 1
          ) f ON true
         WHERE r.status NOT IN ('completed', 'failed', 'cancelled')
    ),
    finished AS (
        UPDATE runs r SET status = 'cancelled', ended_at = now(),
                          error = jsonb_build_object('code', p_reason)
          FROM live l WHERE r.id = l.id AND NOT l.wants
        RETURNING r.id
    ),
    compensating AS (
        UPDATE runs r SET compensating = true, status = 'pending',
                          lease_owner = NULL, lease_until = NULL
          FROM live l WHERE r.id = l.id AND l.wants
        RETURNING r.id, l.ns, l.fn_id, l.key
    ),
    dequeued AS (
        DELETE FROM queue q USING finished f WHERE q.run_id = f.id RETURNING q.run_id
    ),
    requeued AS (
        INSERT INTO queue (ns, fn_id, key, run_id, available_at)
        SELECT c.ns, c.fn_id, c.key, c.id, now() FROM compensating c
        ON CONFLICT (run_id) DO UPDATE
            SET available_at = now(), claimed_by = NULL, claimed_until = NULL
        RETURNING run_id
    )
    SELECT (SELECT count(*) FROM finished) + (SELECT count(*) FROM compensating)
      INTO v_n;
    RETURN v_n;
END $$;

-- ---------------------------------------------------------------- commit

-- Finish a compensating run.
--
-- The compensation's own outcome does not change the verdict: the run was
-- cancelled, and a compensation path that itself failed is a fact about the
-- compensation — recorded in `error` — not a reason to call the run something
-- else. An operator counting cancelled runs must not have to know which of them
-- had a compensation path that threw.
CREATE OR REPLACE FUNCTION finish_compensation(p_run_id uuid, p_error jsonb DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE runs
       SET status = 'cancelled', ended_at = now(), compensating = false,
           error = COALESCE(p_error, error)
     WHERE id = p_run_id AND compensating;
    DELETE FROM queue WHERE run_id = p_run_id;
END $$;

-- Rewrite the terminal state of a compensating run.
--
-- A TRIGGER, which is invisible control flow and therefore owes an explanation.
--
-- `commit_ops` ends a run by writing `completed` or `failed` in two places. The
-- alternative to this trigger is re-issuing all eight hundred lines of it in
-- this migration with those two branches changed — which is precisely the
-- duplication migration 006 was written to remove, and which would leave the
-- next reader diffing two copies to find out which one runs.
--
-- The rule is narrow enough to state in one sentence and is asserted by
-- `test_engine_ops.sql` and by structural invariant 21: a run that owes an undo
-- ends as `cancelled`, whatever its compensation path returned.
CREATE OR REPLACE FUNCTION compensation_ends_cancelled() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.compensating AND NEW.status IN ('completed', 'failed') THEN
        NEW.status := 'cancelled';
        NEW.compensating := false;
        NEW.ended_at := COALESCE(NEW.ended_at, now());
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS runs_compensation_ends_cancelled ON runs;
CREATE TRIGGER runs_compensation_ends_cancelled
    BEFORE UPDATE OF status ON runs
    FOR EACH ROW
    EXECUTE FUNCTION compensation_ends_cancelled();
