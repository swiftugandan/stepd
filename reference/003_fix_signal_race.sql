-- stepd — schema 003: make the signal/wait mutual exclusion EXPLICIT.
--
-- FINDING (from forced-interleaving testing): the lost-signal race was already
-- closed, but only incidentally — the foreign key run_inbox.run_id -> runs.id
-- makes an inbox INSERT take FOR KEY SHARE on the runs row, which happens to
-- conflict with the FOR UPDATE that commit_ops holds. Correct behaviour, wrong
-- reason: dropping the FK, deferring it, partitioning run_inbox, or weakening
-- the lock in commit_ops would silently reopen an R1 race.
--
-- Fix: deliver_to_inbox takes the run row lock explicitly and first. The runs row
-- is now the documented serialization point for everything that resolves a wait.

BEGIN;

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
    -- SERIALIZATION POINT. Must be the first statement. commit_ops takes the same
    -- lock, so delivery and wait-registration can never interleave. Do not rely on
    -- the run_inbox foreign key for this: it works today, but it is incidental.
    SELECT true INTO v_exists FROM runs WHERE id = p_run_id FOR UPDATE;
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
        RETURN 'buffered';           -- early signal: consumed later by commit_ops
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

COMMENT ON FUNCTION deliver_to_inbox IS
'Delivers an event to a run inbox. Takes FOR UPDATE on the runs row as its first
statement: this is the explicit serialization point against commit_ops, and is what
closes the lost-signal race (protocol §7.6). Do not remove or reorder it.';

COMMENT ON TABLE run_inbox IS
'Durable per-run event buffer. Mutual exclusion with wait registration is provided by
the FOR UPDATE lock on runs taken by both deliver_to_inbox and commit_ops — NOT by this
table''s foreign key, which merely happens to take a conflicting lock.';

COMMIT;
