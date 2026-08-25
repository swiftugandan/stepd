-- stepd — schema 002: partitions, commit transaction, dispatch claiming
--
-- The commit function is the correctness centre of the system (PRD §6.3 StateStore::commit).
-- It is written as a single SQL function so the atomicity guarantee lives in one
-- reviewable place rather than being spread across application code.
-- ---------------------------------------------------------------- partitions

CREATE OR REPLACE FUNCTION ensure_event_partition(p_month date)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'events_' || to_char(p_month, 'YYYY_MM');
    from_ts   timestamptz := date_trunc('month', p_month);
    to_ts     timestamptz := date_trunc('month', p_month) + interval '1 month';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            part_name, from_ts, to_ts);
    END IF;
END $$;

SELECT ensure_event_partition(date_trunc('month', now())::date);
SELECT ensure_event_partition((date_trunc('month', now()) + interval '1 month')::date);

-- ---------------------------------------------------------------- dispatch

-- Claim work. Row-level SKIP LOCKED only: no advisory locks, so this is safe
-- behind a transaction-mode pooler (F-DL-1). Bumps the fence so any in-flight
-- attempt for the same run can no longer commit (protocol §7.3).
CREATE OR REPLACE FUNCTION claim_runs(
    p_worker text,
    p_max    integer,
    p_lease  interval DEFAULT interval '60 seconds'
) RETURNS TABLE (run_id uuid, fence bigint, attempt integer)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH picked AS (
        SELECT q.id, q.run_id
        FROM queue q
        WHERE q.claimed_by IS NULL
          AND q.available_at <= now()
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
    RETURNING r.id, r.fence_token, r.attempt_no;
END $$;

-- ---------------------------------------------------------------- commit

-- Atomically: verify fence, record step results, consume inbox entries for waits,
-- enqueue emitted events, schedule timers, and decide the run's next state.
-- Returns 'committed' | 'stale_fence'.
CREATE OR REPLACE FUNCTION commit_ops(
    p_run_id uuid,
    p_fence  bigint,
    p_ops    jsonb,          -- array of op objects
    p_emit   jsonb DEFAULT '[]'::jsonb
) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_fence     bigint;
    v_ns        text;
    v_op        jsonb;
    v_hash      text;
    v_kind      text;
    v_inbox_id  bigint;
    v_event     jsonb;
    v_suspend   boolean := false;
    v_terminal  boolean := false;
    v_status    run_status;
BEGIN
    -- Fence check under row lock: a stale attempt must not mutate anything.
    SELECT fence_token, ns INTO v_fence, v_ns
      FROM runs WHERE id = p_run_id FOR UPDATE;

    IF v_fence IS NULL THEN RETURN 'no_such_run'; END IF;
    IF v_fence <> p_fence THEN RETURN 'stale_fence'; END IF;

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
                ON CONFLICT (run_id, step_hash) DO NOTHING;
            ELSE
                INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status)
                VALUES (p_run_id, v_hash, v_op->>'id', 0, 'wait_event', 'pending')
                ON CONFLICT (run_id, step_hash) DO NOTHING;
                INSERT INTO waits (run_id, ns, step_hash, event_type, expr, since, expires_at, prompt)
                VALUES (p_run_id, v_ns, v_hash, v_op->>'event', v_op->>'expr',
                        COALESCE((v_op->>'since_ts')::timestamptz,
                                 (SELECT started_at FROM runs WHERE id = p_run_id)),
                        (v_op->>'expires_at')::timestamptz, v_op->'prompt')
                ON CONFLICT (run_id, step_hash) DO NOTHING;
                v_suspend := true;
            END IF;

        ELSIF v_kind = 'done' THEN
            UPDATE runs SET status = 'completed', output = v_op->'data', ended_at = now()
             WHERE id = p_run_id;
            v_terminal := true;

        ELSIF v_kind = 'error' THEN
            UPDATE runs SET status = 'failed', error = v_op->'error', ended_at = now()
             WHERE id = p_run_id;
            v_terminal := true;
        END IF;
    END LOOP;

    -- Emitted events go to the outbox in the SAME transaction (protocol §5).
    IF jsonb_array_length(p_emit) > 0 THEN
        INSERT INTO outbox (run_id, ns, event)
        SELECT p_run_id, v_ns, e FROM jsonb_array_elements(p_emit) e;
    END IF;

    -- Decide what happens next.
    IF v_terminal THEN
        DELETE FROM queue WHERE run_id = p_run_id;
    ELSIF v_suspend THEN
        UPDATE runs SET status = 'sleeping', lease_owner = NULL, lease_until = NULL
         WHERE id = p_run_id;
        DELETE FROM queue WHERE run_id = p_run_id;
    ELSE
        -- More work to do: make the run immediately claimable again.
        UPDATE runs SET status = 'pending', lease_owner = NULL, lease_until = NULL
         WHERE id = p_run_id;
        UPDATE queue SET claimed_by = NULL, claimed_until = NULL, available_at = now()
         WHERE run_id = p_run_id;
    END IF;

    RETURN 'committed';
END $$;

-- ---------------------------------------------------------------- inbox delivery

-- Deliver an event to a run's inbox and, if a matching wait is already parked,
-- resolve it and requeue the run. Sender dedupe via the unique index.
CREATE OR REPLACE FUNCTION deliver_to_inbox(
    p_run_id     uuid,
    p_event_type text,
    p_event      jsonb,
    p_sender_run uuid DEFAULT NULL,
    p_sender_hash text DEFAULT NULL
) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_inbox_id bigint;
    v_wait     record;
BEGIN
    INSERT INTO run_inbox (run_id, event_type, event, sender_run_id, sender_step_hash)
    VALUES (p_run_id, p_event_type, p_event, p_sender_run, p_sender_hash)
    ON CONFLICT DO NOTHING
    RETURNING id INTO v_inbox_id;

    IF v_inbox_id IS NULL THEN RETURN 'duplicate'; END IF;

    SELECT * INTO v_wait FROM waits
     WHERE run_id = p_run_id AND event_type = p_event_type AND resolved_at IS NULL
     ORDER BY id LIMIT 1 FOR UPDATE;

    IF v_wait IS NULL THEN
        RETURN 'buffered';       -- the early-signal case
    END IF;

    UPDATE run_inbox SET consumed_by_step_hash = v_wait.step_hash WHERE id = v_inbox_id;
    UPDATE waits SET resolved_at = now() WHERE id = v_wait.id;
    UPDATE run_steps SET status = 'completed', result = p_event, ended_at = now()
     WHERE run_id = p_run_id AND step_hash = v_wait.step_hash;
    UPDATE runs SET status = 'pending' WHERE id = p_run_id;
    INSERT INTO queue (ns, fn_id, run_id)
    SELECT ns, fn_id, id FROM runs WHERE id = p_run_id
    ON CONFLICT (run_id) DO UPDATE SET claimed_by = NULL, available_at = now();

    RETURN 'resolved';
END $$;
