-- stepd - schema 013: a failed step body is recorded as a failed step.
--
-- WHY THIS MIGRATION EXISTS
--
-- `run_steps.status` has had a `failed` value since 001, `RecordedStep` carries
-- `status` and `error` on the wire, and the SDK's memo path already turns a
-- recorded `failed` step back into the error the body raised. Nothing wrote one.
-- The only producer of `step_status = 'failed'` was `resolve_child_result`, for
-- a failed *child run* - a step whose own body raised left no row at all.
--
-- On its own that is a hole in the journal. Inside a parallel group it is worse,
-- and that is how it was found: the group's successful members had their results
-- discarded along with the failure, because an envelope could not carry both an
-- `error` op and the ops recorded before it. The work ran and nothing remembered
-- it, which is the one thing this engine exists to prevent. ADR-023.
--
-- WHAT CHANGES
--
-- The `step` branch reads an optional `error` on the op. Present means the body
-- reached a terminal failure: record `failed` and keep the error. Absent is
-- unchanged. Nothing else in the function moves.
--
-- HOW THIS FILE WAS PRODUCED
--
-- Generated from 012's body with that one branch substituted; the generator
-- asserted the pattern matched exactly once and that the rest is byte-identical.
-- **012's definition is now historical - edit this one.**

CREATE OR REPLACE FUNCTION commit_ops(
    p_run_id uuid,
    p_fence  bigint,
    p_ops    jsonb,
    p_emit   jsonb DEFAULT '[]'::jsonb
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
            -- An `error` on a step op is what distinguishes "this finished" from
            -- "this is over". Both are outcomes and both belong in the journal;
            -- only the first has a result. A retryable failure is neither, and
            -- the protocol forbids sending one here (§5.2.2) because recording
            -- it would memoise the error and the retry would never re-execute.
            INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status,
                                   result, error, meta, ended_at)
            VALUES (p_run_id, v_hash, v_op->>'id',
                    COALESCE((v_op->>'occurrence')::int, 0),
                    'step',
                    CASE WHEN v_op ? 'error' AND v_op->'error' <> 'null'::jsonb
                         THEN 'failed'::step_status ELSE 'completed'::step_status END,
                    v_op->'data', v_op->'error', v_op->'meta', now())
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

COMMENT ON FUNCTION commit_ops(uuid, bigint, jsonb, jsonb) IS
'The atomic op commit (protocol §5, §7.1). Takes FOR UPDATE on the runs row as its
first statement — the explicit serialization point paired with deliver_to_inbox.
Every op type is handled here so that atomicity lives in one reviewable place;
application code that records ops directly bypasses the structural invariant tests.
A batch resolves when every op has reached a terminal state; no op cancels a
sibling, because by the time a batch arrives the app has already executed them.';
