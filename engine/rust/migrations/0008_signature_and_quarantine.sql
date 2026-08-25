-- stepd — schema 008: make error signatures group, and make quarantine mean
-- what it says.
--
-- TWO DEFECTS, ONE CAUSE: nobody had reason to look at `error_signature` after
-- writing it.
--
-- (1) The column held two incompatible formats. `ErrorBody::signature()` hashed
--     `code ‖ 0x1F ‖ message` and took 16 hex characters; `fail_run` hashed the
--     code alone and took all 64. A run failed by a protocol rule and a run
--     failed by its app could never group together, and the DLQ view exists
--     precisely to group them.
--
-- (2) Quarantine documented itself as triggering on "repeated identical failure
--     signature" (F-LP-6) and actually triggered on an attempt count. A run
--     failing a different way each time — which is a run worth a human's
--     attention — was quarantined identically to a genuine poison pill, and a
--     poison pill that resolved and re-broke had its counter carried over.
--
-- Both are fixed by giving the signature one definition and having
-- `record_failure` report whether it *changed*, so the dispatcher can count
-- consecutive identical failures rather than attempts.

-- Match `ErrorBody::signature()` exactly: the code when there is one, the
-- message otherwise, 16 hex characters. Including the message alongside a code
-- defeats grouping, because messages embed run ids and order numbers.
CREATE OR REPLACE FUNCTION error_signature(p_code text, p_message text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT substring(
        encode(sha256(convert_to(
            CASE WHEN COALESCE(p_code, '') <> '' THEN p_code ELSE COALESCE(p_message, '') END,
            'UTF8')), 'hex')
        for 16)
$$;

COMMENT ON FUNCTION error_signature(text, text) IS
'The one definition of an error fingerprint. Must stay byte-identical to
ErrorBody::signature() in stepd-proto, or a run failed by a protocol rule and a
run failed by its app will never group together in the DLQ.';

CREATE OR REPLACE FUNCTION fail_run(p_run_id uuid, p_code text, p_message text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE runs
       SET status = 'failed', ended_at = now(),
           error = jsonb_build_object('code', p_code, 'message', p_message),
           error_signature = error_signature(p_code, p_message)
     WHERE id = p_run_id;
    DELETE FROM queue WHERE run_id = p_run_id;
    PERFORM cascade_cancel(p_run_id, 'parent_failed');
END $$;

-- Consecutive identical failures, which is what F-LP-6 actually specifies.
-- Reset whenever the signature changes: a run failing a different way each time
-- is a different problem from a poison pill, and quarantining it hides it.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS consecutive_failures integer NOT NULL DEFAULT 0;

COMMENT ON COLUMN runs.consecutive_failures IS
'Failures in a row with the SAME error_signature. Reset when the signature
changes. Quarantine triggers on this, not on attempt_no — a run failing a
different way each time deserves a human, not a quarantine.';

-- Record a failed attempt and report whether it is the same failure as last
-- time. One statement, so the count and the signature cannot disagree.
CREATE OR REPLACE FUNCTION record_failure(
    p_run_id  uuid,
    p_code    text,
    p_message text,
    p_retry_at timestamptz
) RETURNS TABLE (attempts integer, consecutive integer, signature text)
LANGUAGE plpgsql AS $$
DECLARE
    v_sig text := error_signature(p_code, p_message);
BEGIN
    RETURN QUERY
    WITH updated AS (
        UPDATE runs
           SET status = 'pending',
               error = jsonb_build_object('code', p_code, 'message', p_message),
               consecutive_failures =
                   CASE WHEN error_signature IS NOT DISTINCT FROM v_sig
                        THEN consecutive_failures + 1 ELSE 1 END,
               error_signature = v_sig,
               lease_owner = NULL, lease_until = NULL
         WHERE id = p_run_id
        RETURNING attempt_no, consecutive_failures
    ),
    requeued AS (
        UPDATE queue SET claimed_by = NULL, claimed_until = NULL, available_at = p_retry_at
         WHERE run_id = p_run_id
        RETURNING 1
    )
    SELECT u.attempt_no, u.consecutive_failures, v_sig FROM updated u;
END $$;
