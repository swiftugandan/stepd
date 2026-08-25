-- stepd — schema 006: complete the engine functions.
--
-- WHY THIS MIGRATION EXISTS
--
-- Migrations 001–004 left `commit_ops` handling only step / sleep / wait_event /
-- done / error. The Rust store had grown its own application-level SQL for
-- `invoke`, `signal`, `continue_as_new` and cascade cancellation. That is the
-- exact hazard recorded as finding (1) in the README: the structural invariant
-- tests in `test_invariants.sql` assert properties of the *SQL functions*, so
-- any path that bypasses them is unguarded. Two correctness centres is one too
-- many.
--
-- This migration moves every op into `commit_ops`, so:
--   * atomicity lives in one reviewable place (PRD §6.3, 002 header);
--   * the structural invariant tests protect the Rust path as well as Python;
--   * an alternative server implementing the published protocol can reuse it.
--
-- Three behavioural corrections come with it, each of which was a live defect in
-- the application-level version:
--
--   (a) `signal` inserted straight into `run_inbox`, bypassing `deliver_to_inbox`.
--       A signal sent to a run already parked on a matching wait was buffered and
--       never woke it: the run slept until its timeout. Signals now go through a
--       durable relay drained by `drain_signals`, which calls `deliver_to_inbox`
--       and therefore takes the documented serialization lock.
--
--   (b) Delivering inline would have introduced a lock-ordering deadlock: the
--       committing transaction already holds `FOR UPDATE` on its own run row and
--       would take the target's. Two runs signalling each other concurrently
--       deadlock. The relay removes the class of bug rather than relying on
--       PostgreSQL's deadlock detector to clean up after it.
--
--   (c) `wait_event` registered no timer, so `timeout` was accepted and silently
--       ignored — a run could wait forever on an event that never came.

-- ---------------------------------------------------------------- supersede

-- Drop the 4-argument commit_ops from 002 rather than leaving it as an overload.
-- Two functions of the same name reachable from a 3-argument call is an ambiguity
-- error at best and a silent dispatch to the older, less complete body at worst —
-- and the older body drops `invoke`, `signal` and `continue_as_new` on the floor
-- without erroring, which is precisely the silent-corruption class this project
-- is built to avoid.
DROP FUNCTION IF EXISTS commit_ops(uuid, bigint, jsonb, jsonb);

COMMENT ON FUNCTION claim_runs(text, integer, interval) IS
'Namespace-blind claim, retained only for the SQL test-suite fixtures written
against 002. Production dispatch uses claim_runs_ns: a namespace-blind claim lets
one tenant''s backlog starve every other however the caller sequences its calls
(F-LP-5).';

-- ---------------------------------------------------------------- limits

-- Engine limits (protocol §5.1, §7.5, §8.2). Held in a table, not as literals,
-- so an operator can raise one during an incident without a deploy, and so the
-- value that was in force is visible when reading a failed run.
CREATE TABLE IF NOT EXISTS engine_limits (
    name  text PRIMARY KEY,
    value bigint NOT NULL,
    note  text
);

INSERT INTO engine_limits (name, value, note) VALUES
    ('steps_per_run',     10000,  'protocol §8.2 — steps recorded for one run'),
    ('invoke_depth',      10,     'protocol §7.5 — depth of the invoke tree'),
    ('invoke_fanout',     1000,   'protocol §7.5 — live non-detached children per run'),
    ('chain_length',      100000, 'protocol §5.1 — continue_as_new chain positions'),
    ('inbox_depth',       1000,   'protocol §7.6 — retained inbox entries per run')
ON CONFLICT (name) DO NOTHING;

CREATE OR REPLACE FUNCTION engine_limit(p_name text) RETURNS bigint
LANGUAGE sql STABLE AS $$
    SELECT value FROM engine_limits WHERE name = p_name
$$;

-- ---------------------------------------------------------------- metrics

-- Counters an operator needs but that no row count can reconstruct after the
-- fact: an inbox overflow drops the evidence of itself.
CREATE TABLE IF NOT EXISTS engine_counters (
    ns    text NOT NULL,
    name  text NOT NULL,
    value bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (ns, name)
);

CREATE OR REPLACE FUNCTION bump_counter(p_ns text, p_name text, p_by bigint DEFAULT 1)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO engine_counters (ns, name, value) VALUES (p_ns, p_name, p_by)
    ON CONFLICT (ns, name) DO UPDATE SET value = engine_counters.value + p_by
$$;

-- ---------------------------------------------------------------- signal relay

-- Signals are recorded transactionally with the ops that produced them and
-- delivered by `drain_signals`. Deferring delivery is what removes the
-- lock-ordering deadlock described in the header; the protocol only promises
-- at-least-once event delivery (§7.1), and `run_inbox`'s sender-dedupe index
-- makes redelivery harmless.
CREATE TABLE IF NOT EXISTS signal_outbox (
    id             bigserial PRIMARY KEY,
    sender_run_id  uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    sender_hash    text NOT NULL,
    target_run_id  uuid NOT NULL,
    event_type     text NOT NULL,
    event          jsonb NOT NULL,
    delivered_at   timestamptz,
    outcome        text,
    UNIQUE (sender_run_id, sender_hash)
);
CREATE INDEX IF NOT EXISTS signal_outbox_pending ON signal_outbox (id) WHERE delivered_at IS NULL;

CREATE OR REPLACE FUNCTION drain_signals(p_max integer DEFAULT 100)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_row record;
    v_n   integer := 0;
BEGIN
    FOR v_row IN
        SELECT id, sender_run_id, sender_hash, target_run_id, event_type, event
          FROM signal_outbox
         WHERE delivered_at IS NULL
         ORDER BY id
         LIMIT p_max
         FOR UPDATE SKIP LOCKED
    LOOP
        UPDATE signal_outbox
           SET delivered_at = now(),
               outcome = deliver_to_inbox(v_row.target_run_id, v_row.event_type,
                                          v_row.event, v_row.sender_run_id,
                                          v_row.sender_hash)
         WHERE id = v_row.id;
        v_n := v_n + 1;
    END LOOP;
    RETURN v_n;
END $$;

-- ---------------------------------------------------------------- inbox bound

-- Trim the inbox to its configured depth, oldest first, counting what was lost.
-- Overflow means a run is being signalled faster than it consumes; the counter
-- is the only surviving evidence, so it is not optional (protocol §7.6).
CREATE OR REPLACE FUNCTION trim_inbox(p_run_id uuid) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    v_limit bigint := engine_limit('inbox_depth');
    v_ns    text;
    v_drop  integer;
BEGIN
    SELECT ns INTO v_ns FROM runs WHERE id = p_run_id;

    WITH doomed AS (
        SELECT id FROM run_inbox
         WHERE run_id = p_run_id AND consumed_by_step_hash IS NULL
         ORDER BY id DESC
         OFFSET v_limit
    )
    DELETE FROM run_inbox WHERE id IN (SELECT id FROM doomed);
    GET DIAGNOSTICS v_drop = ROW_COUNT;

    IF v_drop > 0 THEN
        PERFORM bump_counter(v_ns, 'inbox_overflow', v_drop);
    END IF;
    RETURN v_drop;
END $$;

-- ---------------------------------------------------------------- cascade

-- Cancel every non-detached descendant of a run, depth first (protocol §7.5).
--
-- Written as one recursive statement rather than a recursive plpgsql call so it
-- cannot half-finish: a server that dies partway through leaves the transaction
-- rolled back, and the cascade is retried whole. "Cascade is itself durable and
-- resumable" is a protocol requirement, and a loop that commits per level is not.
CREATE OR REPLACE FUNCTION cascade_cancel(p_run_id uuid, p_reason text DEFAULT 'parent_cancelled')
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_n integer;
BEGIN
    WITH RECURSIVE tree AS (
        SELECT id FROM runs WHERE parent_run_id = p_run_id AND NOT detached
        UNION ALL
        SELECT r.id FROM runs r JOIN tree t ON r.parent_run_id = t.id WHERE NOT r.detached
    ),
    cancelled AS (
        UPDATE runs r
           SET status = 'cancelled',
               ended_at = now(),
               error = jsonb_build_object('code', p_reason,
                                          'message', 'cancelled by cascade from ancestor')
          FROM tree t
         WHERE r.id = t.id
           AND r.status NOT IN ('completed', 'failed', 'cancelled')
        RETURNING r.id
    ),
    dequeued AS (
        DELETE FROM queue WHERE run_id IN (SELECT id FROM cancelled) RETURNING run_id
    )
    SELECT count(*) INTO v_n FROM cancelled;
    RETURN v_n;
END $$;

COMMENT ON FUNCTION cascade_cancel(uuid, text) IS
'Cancels all non-detached descendants in a single recursive statement. Detached
children are excluded at every level, including grandchildren reached through a
detached parent — a detached run''s subtree is independent (protocol §7.5).';

-- Cancel a run and its tree. The console and API both call exactly this, so the
-- UI has no privileged path (PRD F-UI-4).
CREATE OR REPLACE FUNCTION cancel_run(p_ns text, p_run_id uuid) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE v_hit boolean;
BEGIN
    UPDATE runs SET status = 'cancelled', ended_at = now()
     WHERE id = p_run_id AND ns = p_ns
       AND status NOT IN ('completed', 'failed', 'cancelled')
    RETURNING true INTO v_hit;

    IF v_hit IS NULL THEN RETURN false; END IF;

    DELETE FROM queue WHERE run_id = p_run_id;
    PERFORM cascade_cancel(p_run_id, 'parent_cancelled');
    RETURN true;
END $$;

-- ---------------------------------------------------------------- dispatch

-- Namespace-scoped claim. The namespace parameter is what makes fair dispatch
-- possible: the dispatcher rotates across namespaces and claims from each in
-- turn. A namespace-blind claim lets one tenant's backlog starve every other
-- however the caller sequences its calls (F-LP-5).
--
-- Row-level SKIP LOCKED only — no advisory locks, so this is pooler-safe (F-DL-1).
CREATE OR REPLACE FUNCTION claim_runs_ns(
    p_ns     text,
    p_worker text,
    p_max    integer,
    p_lease  interval DEFAULT interval '60 seconds'
) RETURNS TABLE (run_id uuid, fence bigint, attempt integer, lease_until timestamptz)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH picked AS (
        SELECT q.id, q.run_id
          FROM queue q
         WHERE q.claimed_by IS NULL
           AND q.available_at <= now()
           AND q.ns = p_ns
         ORDER BY q.priority DESC, q.available_at
         LIMIT p_max
         FOR UPDATE SKIP LOCKED
    ),
    claimed AS (
        UPDATE queue q
           SET claimed_by = p_worker,
               claimed_until = now() + p_lease,
               attempts = q.attempts + 1
          FROM picked p
         WHERE q.id = p.id
        RETURNING q.run_id
    )
    UPDATE runs r
       SET status = 'running',
           attempt_no = r.attempt_no + 1,
           fence_token = r.fence_token + 1,
           lease_owner = p_worker,
           lease_until = now() + p_lease
      FROM claimed c
     WHERE r.id = c.run_id
    RETURNING r.id, r.fence_token, r.attempt_no, r.lease_until;
END $$;

-- Namespaces with work that is claimable *right now*. Used to drive the
-- round-robin cursor; a namespace whose whole backlog is scheduled for the
-- future must not consume a dispatch turn.
CREATE OR REPLACE FUNCTION active_namespaces() RETURNS TABLE (ns text)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT q.ns FROM queue q
     WHERE q.claimed_by IS NULL AND q.available_at <= now()
$$;

-- Reclaim runs whose lease expired: the worker died, or was partitioned away.
-- The fence was already bumped at claim time, so a late response from the dead
-- worker cannot commit (protocol §7.3).
CREATE OR REPLACE FUNCTION reclaim_expired_leases(p_max integer DEFAULT 100)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE v_n integer;
BEGIN
    WITH expired AS (
        SELECT q.id, q.run_id FROM queue q
         WHERE q.claimed_by IS NOT NULL AND q.claimed_until < now()
         ORDER BY q.claimed_until
         LIMIT p_max
         FOR UPDATE SKIP LOCKED
    ),
    released AS (
        UPDATE queue q SET claimed_by = NULL, claimed_until = NULL, available_at = now()
          FROM expired e WHERE q.id = e.id
        RETURNING q.run_id
    )
    UPDATE runs r SET status = 'pending', lease_owner = NULL, lease_until = NULL
      FROM released x WHERE r.id = x.run_id AND r.status = 'running';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END $$;

-- ---------------------------------------------------------------- timers

-- Fire due timers. A sleep resolves its step; a wait timeout resolves the wait
-- with a null result and `timed_out`; a run deadline cancels the tree.
--
-- A run is only requeued when nothing else still blocks it, so a run that slept
-- and waited in the same parallel batch does not wake early with one op pending.
CREATE OR REPLACE FUNCTION fire_due_timers(p_now timestamptz DEFAULT now(), p_max integer DEFAULT 200)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_t     record;
    v_woken integer := 0;
    v_block bigint;
BEGIN
    FOR v_t IN
        UPDATE timers SET fired_at = now()
         WHERE id IN (SELECT id FROM timers
                       WHERE fired_at IS NULL AND fire_at <= p_now
                       ORDER BY fire_at LIMIT p_max FOR UPDATE SKIP LOCKED)
        RETURNING run_id, step_hash, kind
    LOOP
        IF v_t.kind = 'run_deadline' THEN
            UPDATE runs SET status = 'failed', ended_at = now(),
                   error = jsonb_build_object('code', 'run_timeout',
                                              'message', 'run deadline elapsed')
             WHERE id = v_t.run_id AND status NOT IN ('completed','failed','cancelled');
            DELETE FROM queue WHERE run_id = v_t.run_id;
            PERFORM cascade_cancel(v_t.run_id, 'parent_timeout');
            CONTINUE;
        END IF;

        IF v_t.kind = 'wait_timeout' THEN
            -- Resolve the wait, not just the step: leaving the wait row open
            -- would let a later event resolve a step that already timed out.
            UPDATE waits SET resolved_at = now()
             WHERE run_id = v_t.run_id AND step_hash = v_t.step_hash AND resolved_at IS NULL;
            UPDATE run_steps SET status = 'timed_out', result = NULL, ended_at = now()
             WHERE run_id = v_t.run_id AND step_hash = v_t.step_hash AND status = 'pending';
        ELSE
            UPDATE run_steps SET status = 'completed', ended_at = now()
             WHERE run_id = v_t.run_id AND step_hash = v_t.step_hash AND status = 'pending';
        END IF;

        SELECT count(*) INTO v_block FROM run_steps
         WHERE run_id = v_t.run_id AND status = 'pending';
        IF v_block > 0 THEN CONTINUE; END IF;

        UPDATE runs SET status = 'pending'
         WHERE id = v_t.run_id AND status IN ('sleeping', 'waiting');
        IF NOT FOUND THEN CONTINUE; END IF;

        INSERT INTO queue (ns, fn_id, key, run_id)
        SELECT ns, fn_id, key, id FROM runs WHERE id = v_t.run_id
        ON CONFLICT (run_id) DO UPDATE
            SET claimed_by = NULL, claimed_until = NULL, available_at = now();
        v_woken := v_woken + 1;
    END LOOP;
    RETURN v_woken;
END $$;

-- ---------------------------------------------------------------- child results

-- Resolve the parent's `invoke` step when a child reaches a terminal state, and
-- requeue the parent if nothing else blocks it (protocol §7.5).
CREATE OR REPLACE FUNCTION resolve_child_result(p_child uuid) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    v_child  record;
    v_status step_status;
    v_block  bigint;
BEGIN
    SELECT id, parent_run_id, parent_step_hash, detached, status, output, error
      INTO v_child FROM runs WHERE id = p_child;

    IF v_child.parent_run_id IS NULL OR v_child.detached THEN RETURN false; END IF;
    IF v_child.status NOT IN ('completed', 'failed', 'cancelled') THEN RETURN false; END IF;

    -- SERIALIZATION POINT: the parent row, same lock commit_ops and
    -- deliver_to_inbox take. A child finishing while the parent is mid-commit
    -- must not interleave with the parent registering further ops.
    PERFORM 1 FROM runs WHERE id = v_child.parent_run_id FOR UPDATE;

    v_status := CASE v_child.status
                    WHEN 'completed' THEN 'completed'::step_status
                    WHEN 'cancelled' THEN 'cancelled'::step_status
                    ELSE 'failed'::step_status END;

    UPDATE run_steps
       SET status = v_status,
           result = v_child.output,
           error  = v_child.error,
           ended_at = now()
     WHERE run_id = v_child.parent_run_id
       AND step_hash = v_child.parent_step_hash
       AND status = 'pending';

    IF NOT FOUND THEN RETURN false; END IF;

    SELECT count(*) INTO v_block FROM run_steps
     WHERE run_id = v_child.parent_run_id AND status = 'pending';
    IF v_block > 0 THEN RETURN false; END IF;

    UPDATE runs SET status = 'pending'
     WHERE id = v_child.parent_run_id AND status IN ('sleeping', 'waiting');

    INSERT INTO queue (ns, fn_id, key, run_id)
    SELECT ns, fn_id, key, id FROM runs WHERE id = v_child.parent_run_id
    ON CONFLICT (run_id) DO UPDATE
        SET claimed_by = NULL, claimed_until = NULL, available_at = now();
    RETURN true;
END $$;

-- Sweep children that finished but whose parent step is still pending. Belt and
-- braces for the inline call in commit_ops: if a server dies between committing
-- a child's `done` and resolving the parent, this recovers it.
CREATE OR REPLACE FUNCTION resolve_finished_children(p_max integer DEFAULT 200)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE v_c record; v_n integer := 0;
BEGIN
    FOR v_c IN
        SELECT c.id FROM runs c
          JOIN run_steps s ON s.run_id = c.parent_run_id AND s.step_hash = c.parent_step_hash
         WHERE c.parent_run_id IS NOT NULL AND NOT c.detached
           AND c.status IN ('completed', 'failed', 'cancelled')
           AND s.status = 'pending'
         LIMIT p_max
    LOOP
        IF resolve_child_result(v_c.id) THEN v_n := v_n + 1; END IF;
    END LOOP;
    RETURN v_n;
END $$;

-- ---------------------------------------------------------------- failure

-- Fail a run non-retryably, from inside the transaction that detected the
-- violation. Separate function so every rule failure produces the same shape.
CREATE OR REPLACE FUNCTION fail_run(p_run_id uuid, p_code text, p_message text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE runs
       SET status = 'failed', ended_at = now(),
           error = jsonb_build_object('code', p_code, 'message', p_message),
           error_signature = encode(sha256(convert_to(p_code, 'UTF8')), 'hex')
     WHERE id = p_run_id;
    DELETE FROM queue WHERE run_id = p_run_id;
    PERFORM cascade_cancel(p_run_id, 'parent_failed');
END $$;

-- ---------------------------------------------------------------- commit

-- The correctness centre. Atomically: verify the fence, record every op, consume
-- inbox entries for waits, enqueue emitted events, schedule timers, enforce the
-- limits, and decide the run's next state.
--
-- Returns one of:
--   'committed' | 'stale_fence' | 'no_such_run' | 'terminal' | 'failed:<code>'
--
-- A 'failed:<code>' return means the envelope violated a protocol rule that is
-- the app's fault (a limit, a live child under continue_as_new). The run is
-- failed non-retryably inside this same transaction, so the caller never has to
-- perform a second write to keep the state consistent.
CREATE OR REPLACE FUNCTION commit_ops(
    p_run_id uuid,
    p_fence  bigint,
    p_ops    jsonb,
    p_emit   jsonb DEFAULT '[]'::jsonb,
    p_join   text  DEFAULT 'all_settled'
) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_run       record;
    v_op        jsonb;
    v_hash      text;
    v_kind      text;
    v_inbox_id  bigint;
    v_event     jsonb;
    v_suspend   boolean := false;
    v_waited    boolean := false;
    v_terminal  boolean := false;
    v_steps     bigint;
    v_depth     bigint;
    v_live      bigint;
    v_child     uuid;
    v_succ      uuid;
    v_cycle     bigint;
    v_expires   timestamptz;
BEGIN
    -- SERIALIZATION POINT. Must be the first statement, and must be FOR UPDATE:
    -- deliver_to_inbox takes the same lock, and that explicit pairing is what
    -- closes the lost-signal race (see 003). test_invariants.sql fails the build
    -- if this moves.
    SELECT id, ns, fn_id, key, status, fence_token, lineage_id, chain_position, started_at
      INTO v_run FROM runs WHERE id = p_run_id FOR UPDATE;

    IF v_run.id IS NULL THEN RETURN 'no_such_run'; END IF;
    IF v_run.fence_token <> p_fence THEN RETURN 'stale_fence'; END IF;
    IF v_run.status IN ('completed', 'failed', 'cancelled') THEN RETURN 'terminal'; END IF;

    -- Runaway containment (F-LP-9). Checked before recording anything, so the
    -- limit is a ceiling on what is stored rather than a report on what was.
    SELECT count(*) INTO v_steps FROM run_steps WHERE run_id = p_run_id;
    IF v_steps + jsonb_array_length(p_ops) > engine_limit('steps_per_run') THEN
        PERFORM fail_run(p_run_id, 'step_limit_exceeded',
                         format('run would exceed %s recorded steps',
                                engine_limit('steps_per_run')));
        RETURN 'failed:step_limit_exceeded';
    END IF;

    FOR v_op IN SELECT * FROM jsonb_array_elements(p_ops) LOOP
        v_kind := v_op->>'op';
        v_hash := v_op->>'hash';

        IF v_kind = 'step' THEN
            INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status,
                                   result, meta, ended_at)
            VALUES (p_run_id, v_hash, v_op->>'id',
                    COALESCE((v_op->>'occurrence')::int, 0),
                    'step', 'completed', v_op->'data', v_op->'meta', now())
            ON CONFLICT (run_id, step_hash) DO NOTHING;   -- idempotent re-commit

        ELSIF v_kind = 'sleep' THEN
            INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status)
            VALUES (p_run_id, v_hash, v_op->>'id', 0, 'sleep', 'pending')
            ON CONFLICT (run_id, step_hash) DO NOTHING;
            INSERT INTO timers (run_id, step_hash, kind, fire_at)
            VALUES (p_run_id, v_hash, 'sleep', (v_op->>'until')::timestamptz);
            v_suspend := true;

        ELSIF v_kind = 'wait_event' THEN
            -- Protocol §7.6: check the inbox FIRST, in this same transaction.
            -- An event that arrived before the wait was registered still matches.
            SELECT id, event INTO v_inbox_id, v_event
              FROM run_inbox
             WHERE run_id = p_run_id
               AND event_type = v_op->>'event'
               AND consumed_by_step_hash IS NULL
             ORDER BY id                     -- FIFO
             LIMIT 1;

            IF v_inbox_id IS NOT NULL THEN
                -- Resolve immediately; the run never suspends.
                UPDATE run_inbox SET consumed_by_step_hash = v_hash WHERE id = v_inbox_id;
                INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op,
                                       status, result, ended_at)
                VALUES (p_run_id, v_hash, v_op->>'id', 0, 'wait_event',
                        'completed', v_event, now())
                ON CONFLICT (run_id, step_hash) DO UPDATE
                    SET status = 'completed', result = EXCLUDED.result, ended_at = now();
            ELSE
                INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status)
                VALUES (p_run_id, v_hash, v_op->>'id', 0, 'wait_event', 'pending')
                ON CONFLICT (run_id, step_hash) DO NOTHING;

                v_expires := (v_op->>'timeout_at')::timestamptz;
                INSERT INTO waits (run_id, ns, step_hash, event_type, expr, since,
                                   expires_at, prompt)
                VALUES (p_run_id, v_run.ns, v_hash, v_op->>'event', v_op->>'expr',
                        COALESCE((v_op->>'since_ts')::timestamptz, v_run.started_at),
                        v_expires, v_op->'prompt')
                ON CONFLICT (run_id, step_hash) DO NOTHING;

                -- Without this the `timeout` field was accepted and ignored, and
                -- a run could wait for an event that never arrived, forever.
                IF v_expires IS NOT NULL THEN
                    INSERT INTO timers (run_id, step_hash, kind, fire_at)
                    VALUES (p_run_id, v_hash, 'wait_timeout', v_expires);
                END IF;
                v_suspend := true;
                v_waited  := true;
            END IF;

        ELSIF v_kind = 'invoke' THEN
            SELECT count(*) INTO v_live FROM runs
             WHERE parent_run_id = p_run_id AND NOT detached
               AND status NOT IN ('completed', 'failed', 'cancelled');
            IF v_live >= engine_limit('invoke_fanout') THEN
                PERFORM fail_run(p_run_id, 'invoke_fanout_exceeded',
                                 format('more than %s live children',
                                        engine_limit('invoke_fanout')));
                RETURN 'failed:invoke_fanout_exceeded';
            END IF;

            WITH RECURSIVE anc AS (
                SELECT id, parent_run_id, fn_id, key, 0 AS depth FROM runs WHERE id = p_run_id
                UNION ALL
                SELECT r.id, r.parent_run_id, r.fn_id, r.key, a.depth + 1
                  FROM runs r JOIN anc a ON r.id = a.parent_run_id
            )
            SELECT max(depth) + 1,
                   count(*) FILTER (WHERE fn_id = v_op->>'function'
                                      AND key IS NOT NULL AND key = v_run.key)
              INTO v_depth, v_cycle FROM anc;

            IF v_depth > engine_limit('invoke_depth') THEN
                PERFORM fail_run(p_run_id, 'invoke_depth_exceeded',
                                 format('invoke tree deeper than %s',
                                        engine_limit('invoke_depth')));
                RETURN 'failed:invoke_depth_exceeded';
            END IF;
            -- Invoking an ancestor on the same key would deadlock on keyed
            -- ordering: the child can never start while the parent holds the key,
            -- and the parent is waiting on the child. Reject rather than hang.
            IF v_cycle > 0 THEN
                PERFORM fail_run(p_run_id, 'invoke_cycle',
                                 'invoked an ancestor holding the same key');
                RETURN 'failed:invoke_cycle';
            END IF;

            v_child := gen_random_uuid();
            INSERT INTO runs (id, ns, fn_id, input, parent_run_id, parent_step_hash,
                              detached, lineage_id, status)
            VALUES (v_child, v_run.ns, v_op->>'function', v_op->'input',
                    p_run_id, v_hash, COALESCE((v_op->>'detach')::boolean, false),
                    v_child, 'pending');
            INSERT INTO queue (ns, fn_id, run_id)
            VALUES (v_run.ns, v_op->>'function', v_child);

            IF COALESCE((v_op->>'detach')::boolean, false) THEN
                -- Fire and forget: the parent gets the child id now and never waits.
                INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op,
                                       status, result, ended_at)
                VALUES (p_run_id, v_hash, v_op->>'id', 0, 'invoke', 'completed',
                        jsonb_build_object('run_id', v_child, 'detached', true), now())
                ON CONFLICT (run_id, step_hash) DO NOTHING;
            ELSE
                INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op,
                                       status, result)
                VALUES (p_run_id, v_hash, v_op->>'id', 0, 'invoke', 'pending',
                        jsonb_build_object('run_id', v_child))
                ON CONFLICT (run_id, step_hash) DO NOTHING;
                v_suspend := true;
            END IF;

            IF (v_op->>'timeout_at') IS NOT NULL THEN
                INSERT INTO timers (run_id, step_hash, kind, fire_at)
                VALUES (v_child, NULL, 'run_deadline', (v_op->>'timeout_at')::timestamptz);
            END IF;

        ELSIF v_kind = 'signal' THEN
            -- Recorded here, delivered by drain_signals. See the header: inline
            -- delivery would take a second run row lock and deadlock two runs
            -- that signal each other.
            INSERT INTO signal_outbox (sender_run_id, sender_hash, target_run_id,
                                       event_type, event)
            VALUES (p_run_id, v_hash, (v_op->>'target_run')::uuid,
                    COALESCE(v_op#>>'{event,type}', v_op->>'event'),
                    COALESCE(v_op#>'{event,data}', v_op->'event', '{}'::jsonb))
            ON CONFLICT (sender_run_id, sender_hash) DO NOTHING;

            INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op,
                                   status, ended_at)
            VALUES (p_run_id, v_hash, v_op->>'id', 0, 'signal', 'completed', now())
            ON CONFLICT (run_id, step_hash) DO NOTHING;

        ELSIF v_kind = 'continue_as_new' THEN
            -- A non-detached child in flight would have its result delivered into
            -- a journal the successor discards, and would then be orphaned by the
            -- cascade rules, which cover cancellation and failure but not
            -- continuation. Found by simulation property P8, not by review.
            SELECT count(*) INTO v_live FROM runs
             WHERE parent_run_id = p_run_id AND NOT detached
               AND status NOT IN ('completed', 'failed', 'cancelled');
            IF v_live > 0 THEN
                PERFORM fail_run(p_run_id, 'continue_as_new_with_live_children',
                                 format('%s non-detached child run(s) still in flight', v_live));
                RETURN 'failed:continue_as_new_with_live_children';
            END IF;

            IF v_run.chain_position + 1 > engine_limit('chain_length') THEN
                PERFORM fail_run(p_run_id, 'chain_limit_exceeded',
                                 format('continue_as_new chain longer than %s',
                                        engine_limit('chain_length')));
                RETURN 'failed:chain_limit_exceeded';
            END IF;

            INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op,
                                   status, result, ended_at)
            VALUES (p_run_id, v_hash, v_op->>'id', 0, 'continue_as_new', 'completed',
                    v_op->'input', now())
            ON CONFLICT (run_id, step_hash) DO NOTHING;

            -- The predecessor must reach a terminal state before the successor is
            -- inserted, or the partial unique index on (ns, fn_id, key) rejects
            -- the successor for colliding with its own predecessor.
            UPDATE runs SET status = 'completed', ended_at = now() WHERE id = p_run_id;
            DELETE FROM queue WHERE run_id = p_run_id;

            v_succ := gen_random_uuid();
            INSERT INTO runs (id, ns, fn_id, fn_version, key, input,
                              lineage_id, chain_position, status)
            SELECT v_succ, ns, fn_id, fn_version, key, v_op->'input',
                   lineage_id, chain_position + 1, 'pending'
              FROM runs WHERE id = p_run_id;
            INSERT INTO queue (ns, fn_id, key, run_id)
            VALUES (v_run.ns, v_run.fn_id, v_run.key, v_succ);
            v_terminal := true;

        ELSIF v_kind = 'done' THEN
            UPDATE runs SET status = 'completed', output = v_op->'data', ended_at = now()
             WHERE id = p_run_id;
            v_terminal := true;

        ELSIF v_kind = 'error' THEN
            UPDATE runs SET status = 'failed', error = v_op->'error', ended_at = now()
             WHERE id = p_run_id;
            v_terminal := true;

        ELSE
            -- Protocol §11: an unknown op type is the app emitting above the
            -- negotiated version. Fail loudly rather than silently dropping work.
            PERFORM fail_run(p_run_id, 'unknown_op',
                             format('unknown op type %L', v_kind));
            RETURN 'failed:unknown_op';
        END IF;
    END LOOP;

    -- Emitted events go to the outbox in the SAME transaction (protocol §5):
    -- an event is published if and only if the step that produced it was recorded.
    IF jsonb_array_length(p_emit) > 0 THEN
        INSERT INTO outbox (run_id, ns, event)
        SELECT p_run_id, v_run.ns, e FROM jsonb_array_elements(p_emit) e;
    END IF;

    IF v_terminal THEN
        DELETE FROM queue WHERE run_id = p_run_id;
        -- continue_as_new already handled its own succession; for done/error the
        -- tree must come down with the parent (protocol §7.5).
        IF v_succ IS NULL THEN
            PERFORM cascade_cancel(p_run_id, 'parent_terminal');
        END IF;
        PERFORM resolve_child_result(p_run_id);
    ELSIF v_suspend THEN
        -- `waiting` and `sleeping` are distinguished so an operator can tell a run
        -- parked on an event from one parked on a timer without opening it.
        UPDATE runs SET status = CASE WHEN v_waited THEN 'waiting'::run_status ELSE 'sleeping'::run_status END,
                        lease_owner = NULL, lease_until = NULL
         WHERE id = p_run_id;
        DELETE FROM queue WHERE run_id = p_run_id;
    ELSE
        -- More work to do: make the run immediately claimable again.
        UPDATE runs SET status = 'pending', lease_owner = NULL, lease_until = NULL
         WHERE id = p_run_id;
        INSERT INTO queue (ns, fn_id, key, run_id)
        VALUES (v_run.ns, v_run.fn_id, v_run.key, p_run_id)
        ON CONFLICT (run_id) DO UPDATE
            SET claimed_by = NULL, claimed_until = NULL, available_at = now();
    END IF;

    RETURN 'committed';
END $$;

COMMENT ON FUNCTION commit_ops(uuid, bigint, jsonb, jsonb, text) IS
'The atomic op commit (protocol §5, §7.1). Takes FOR UPDATE on the runs row as its
first statement — the explicit serialization point paired with deliver_to_inbox.
Every op type is handled here so that atomicity lives in one reviewable place;
application code that records ops directly bypasses the structural invariant tests.';


-- ---------------------------------------------------------------- correlation

-- Route an ingested event to the runs waiting for it. Correlation is by
-- (ns, event_type) and, when the wait declared one, the run key: an approval for
-- order 4711 must not resolve the wait belonging to order 4712.
CREATE OR REPLACE FUNCTION correlate_event(
    p_ns   text,
    p_type text,
    p_key  text,
    p_data jsonb
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE v_r record; v_n integer := 0;
BEGIN
    FOR v_r IN
        SELECT DISTINCT w.run_id FROM waits w
          JOIN runs r ON r.id = w.run_id
         WHERE w.ns = p_ns AND w.event_type = p_type AND w.resolved_at IS NULL
           AND r.status NOT IN ('completed', 'failed', 'cancelled')
           AND (p_key IS NULL OR r.key IS NULL OR r.key = p_key)
    LOOP
        IF deliver_to_inbox(v_r.run_id, p_type, p_data) = 'resolved' THEN
            v_n := v_n + 1;
        END IF;
    END LOOP;
    RETURN v_n;
END $$;

-- ---------------------------------------------------------------- inbox delivery

-- Unchanged from 004 except for the bound: the inbox is trimmed to its configured
-- depth after every delivery. Without it a run signalled faster than it consumes
-- grows unboundedly, and protocol §7.6 requires the bound *and* the metric —
-- dropping an entry silently would destroy the only evidence it happened.
--
-- The FOR UPDATE on runs remains the first statement. That is asserted
-- structurally by test_invariants.sql; if this function is ever edited again,
-- the lock stays first or the build fails.
CREATE OR REPLACE FUNCTION deliver_to_inbox(
    p_run_id      uuid,
    p_event_type  text,
    p_event       jsonb,
    p_sender_run  uuid DEFAULT NULL,
    p_sender_hash text DEFAULT NULL
) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_inbox_id bigint;
    v_wait     record;
    v_exists   boolean;
BEGIN
    -- SERIALIZATION POINT (see 003). Must be first.
    SELECT true INTO v_exists FROM runs
     WHERE id = p_run_id AND status NOT IN ('completed', 'failed', 'cancelled')
     FOR UPDATE;
    IF v_exists IS NULL THEN RETURN 'no_such_run'; END IF;

    INSERT INTO run_inbox (run_id, event_type, event, sender_run_id, sender_step_hash)
    VALUES (p_run_id, p_event_type, p_event, p_sender_run, p_sender_hash)
    ON CONFLICT DO NOTHING
    RETURNING id INTO v_inbox_id;

    IF v_inbox_id IS NULL THEN RETURN 'duplicate'; END IF;

    SELECT * INTO v_wait FROM waits
     WHERE run_id = p_run_id AND event_type = p_event_type AND resolved_at IS NULL
     ORDER BY id LIMIT 1;

    IF v_wait IS NULL THEN
        PERFORM trim_inbox(p_run_id);
        RETURN 'buffered';
    END IF;

    UPDATE run_inbox SET consumed_by_step_hash = v_wait.step_hash WHERE id = v_inbox_id;
    UPDATE waits SET resolved_at = now() WHERE id = v_wait.id;

    -- UPSERT, not UPDATE (see 004): consuming an event without recording its
    -- result would lose the signal outright.
    INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status,
                           result, ended_at)
    VALUES (p_run_id, v_wait.step_hash, 'wait', 0, 'wait_event', 'completed',
            p_event, now())
    ON CONFLICT (run_id, step_hash) DO UPDATE
        SET status = 'completed', result = EXCLUDED.result, ended_at = now();

    -- A resolved wait cannot time out afterwards.
    UPDATE timers SET fired_at = now()
     WHERE run_id = p_run_id AND step_hash = v_wait.step_hash
       AND kind = 'wait_timeout' AND fired_at IS NULL;

    -- Only requeue when nothing else still blocks the run: a run that waited and
    -- slept in one parallel batch must not wake with the sleep still pending.
    IF EXISTS (SELECT 1 FROM run_steps WHERE run_id = p_run_id AND status = 'pending') THEN
        RETURN 'resolved';
    END IF;

    UPDATE runs SET status = 'pending' WHERE id = p_run_id;
    INSERT INTO queue (ns, fn_id, key, run_id)
    SELECT ns, fn_id, key, id FROM runs WHERE id = p_run_id
    ON CONFLICT (run_id) DO UPDATE
        SET claimed_by = NULL, claimed_until = NULL, available_at = now();

    RETURN 'resolved';
END $$;
