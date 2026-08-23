-- stepd — schema 004: two defects found by API testing.
--
-- (1) deliver_to_inbox could consume an event without recording a result.
--     It marked the wait resolved and UPDATEd run_steps; if the pending row was
--     absent the UPDATE matched nothing, so the event was consumed and the result
--     lost, and the next attempt re-registered the wait. It only ever worked
--     because commit_ops creates the pending row first — the same "correct for an
--     undocumented reason" pattern as the FK finding in 003. Now an UPSERT.
--
-- (2) Idempotent event ingest never deduplicated. The unique index included
--     received_at, which differs on every insert. The root cause is structural:
--     `events` is partitioned by received_at, and PostgreSQL requires a unique
--     index on a partitioned table to include the partition key, so a global
--     UNIQUE (ns, idem) cannot exist there. Dedupe moves to its own table.

BEGIN;

-- ---------------- (1) upsert the step row

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
        RETURN 'buffered';
    END IF;

    UPDATE run_inbox SET consumed_by_step_hash = v_wait.step_hash WHERE id = v_inbox_id;
    UPDATE waits SET resolved_at = now() WHERE id = v_wait.id;

    -- UPSERT, not UPDATE: consuming an event without recording its result would
    -- lose the signal outright.
    INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status,
                           result, ended_at)
    VALUES (p_run_id, v_wait.step_hash, 'wait', 0, 'wait_event', 'completed',
            p_event, now())
    ON CONFLICT (run_id, step_hash) DO UPDATE
        SET status = 'completed', result = EXCLUDED.result, ended_at = now();

    UPDATE runs SET status = 'pending' WHERE id = p_run_id;
    INSERT INTO queue (ns, fn_id, run_id)
    SELECT ns, fn_id, id FROM runs WHERE id = p_run_id
    ON CONFLICT (run_id) DO UPDATE SET claimed_by = NULL, available_at = now();

    RETURN 'resolved';
END $$;

-- ---------------- (2) real idempotent ingest

DROP INDEX IF EXISTS events_idem;

CREATE TABLE event_idempotency (
    ns          text NOT NULL,
    idem        text NOT NULL,
    event_id    uuid NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ns, idem)
);
CREATE INDEX event_idempotency_prune ON event_idempotency (received_at);

COMMENT ON TABLE event_idempotency IS
'Ingest dedupe within a configurable window. Separate from `events` because that
table is partitioned by received_at, and PostgreSQL requires a unique index on a
partitioned table to include the partition key — so a global UNIQUE (ns, idem)
cannot be expressed there.';

-- Ingest as a function so the dedupe claim and the event insert are one transaction.
CREATE OR REPLACE FUNCTION ingest_event(
    p_ns     text,
    p_type   text,
    p_source text,
    p_data   jsonb,
    p_key    text DEFAULT NULL,
    p_idem   text DEFAULT NULL
) RETURNS TABLE (event_id uuid, deduplicated boolean)
LANGUAGE plpgsql AS $$
DECLARE
    v_id       uuid := gen_random_uuid();
    v_existing uuid;
BEGIN
    IF p_idem IS NOT NULL THEN
        INSERT INTO event_idempotency (ns, idem, event_id)
        VALUES (p_ns, p_idem, v_id)
        ON CONFLICT (ns, idem) DO NOTHING;
        IF NOT FOUND THEN
            SELECT e.event_id INTO v_existing FROM event_idempotency e
             WHERE e.ns = p_ns AND e.idem = p_idem;
            RETURN QUERY SELECT v_existing, true;
            RETURN;
        END IF;
    END IF;

    INSERT INTO events (id, ns, type, source, time, key, idem, data)
    VALUES (v_id, p_ns, p_type, p_source, now(), p_key, p_idem, p_data);
    RETURN QUERY SELECT v_id, false;
END $$;

COMMIT;
