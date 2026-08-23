-- Rust migration-set copy of reference/test_engine.sql. Sole divergence: migration
-- 006 distinguishes `waiting` from `sleeping`, so the wait_event assertion below
-- expects `waiting`. Everything else must stay byte-identical to the reference,
-- so that a behaviour the Python engine has and the Rust one lacks fails here.
-- stepd engine — correctness tests against real Postgres.
-- Each test prints PASS/FAIL. Any FAIL is an R1 or R2 defect.

\set ON_ERROR_STOP on
\pset pager off

-- ---------------------------------------------------------------- fixture

TRUNCATE namespaces CASCADE;
INSERT INTO namespaces (id) VALUES ('prod');
INSERT INTO app_bindings (id, ns, app_id, url, key_hash_current)
VALUES ('00000000-0000-7000-8000-000000000001', 'prod', 'billing',
        'https://app/stepd', '\x00');
INSERT INTO functions (id, ns, app_binding_id, fn_id, version, config)
VALUES ('00000000-0000-7000-8000-000000000002', 'prod',
        '00000000-0000-7000-8000-000000000001', 'order-fulfilment', '1', '{}');

CREATE OR REPLACE FUNCTION mkrun(p_id uuid, p_key text DEFAULT NULL)
RETURNS uuid LANGUAGE sql AS $$
    INSERT INTO runs (id, ns, fn_id, key, lineage_id)
    VALUES (p_id, 'prod', 'order-fulfilment', p_key, p_id)
    RETURNING id;
$$;

CREATE OR REPLACE FUNCTION assert_that(p_name text, p_cond boolean)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_cond THEN RAISE NOTICE 'PASS  %', p_name;
    ELSE RAISE EXCEPTION 'FAIL  %', p_name;
    END IF;
END $$;

-- ================================================================ R1: fencing

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a001';
        v_res text;
BEGIN
    PERFORM mkrun(v_run);
    INSERT INTO queue (ns, fn_id, run_id) VALUES ('prod','order-fulfilment',v_run);
    PERFORM claim_runs('worker-1', 10);           -- fence -> 1

    -- correct fence commits
    v_res := commit_ops(v_run, 1,
        '[{"op":"step","id":"charge","hash":"3f2a91c4b70e1d55","data":{"tx":"ch_1"}}]'::jsonb);
    PERFORM assert_that('fence: correct fence commits', v_res = 'committed');

    -- a stale attempt (fence 0) must not mutate anything
    v_res := commit_ops(v_run, 0,
        '[{"op":"step","id":"ghost","hash":"deadbeefdeadbeef","data":{"bad":true}}]'::jsonb);
    PERFORM assert_that('fence: stale fence rejected', v_res = 'stale_fence');
    PERFORM assert_that('fence: stale attempt wrote nothing',
        NOT EXISTS (SELECT 1 FROM run_steps WHERE run_id=v_run AND step_id='ghost'));
END $$;

-- ================================================================ R1: no duplicate record

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a002';
        v_n int;
BEGIN
    PERFORM mkrun(v_run);
    INSERT INTO queue (ns, fn_id, run_id) VALUES ('prod','order-fulfilment',v_run);
    PERFORM claim_runs('worker-1', 10);

    -- same op committed twice (at-least-once delivery of the response)
    PERFORM commit_ops(v_run, 1, '[{"op":"step","id":"charge","hash":"aaaa000000000001","data":{"tx":1}}]'::jsonb);
    PERFORM claim_runs('worker-1', 10);
    PERFORM commit_ops(v_run, 2, '[{"op":"step","id":"charge","hash":"aaaa000000000001","data":{"tx":2}}]'::jsonb);

    SELECT count(*) INTO v_n FROM run_steps WHERE run_id=v_run AND step_hash='aaaa000000000001';
    PERFORM assert_that('no duplicate record: one row per hash', v_n = 1);
    PERFORM assert_that('no duplicate record: first write wins',
        (SELECT result->>'tx' FROM run_steps WHERE run_id=v_run AND step_hash='aaaa000000000001') = '1');
END $$;

-- ================================================================ R1: keyed exclusivity

DO $$
DECLARE v_err text;
BEGIN
    PERFORM mkrun('00000000-0000-7000-8000-00000000a003', 'order:4711');
    BEGIN
        PERFORM mkrun('00000000-0000-7000-8000-00000000a004', 'order:4711');
        PERFORM assert_that('keyed exclusivity: second active run rejected', false);
    EXCEPTION WHEN unique_violation THEN
        PERFORM assert_that('keyed exclusivity: second active run rejected', true);
    END;

    -- once the first completes, the key frees up
    UPDATE runs SET status='completed', ended_at=now()
     WHERE id='00000000-0000-7000-8000-00000000a003';
    PERFORM mkrun('00000000-0000-7000-8000-00000000a005', 'order:4711');
    PERFORM assert_that('keyed exclusivity: key reusable after terminal state', true);
END $$;

-- ================================================================ R1: early signal (lost-signal race)

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a006';
        v_res text; v_status step_status; v_result jsonb;
BEGIN
    PERFORM mkrun(v_run);
    INSERT INTO queue (ns, fn_id, run_id) VALUES ('prod','order-fulfilment',v_run);
    PERFORM claim_runs('worker-1', 10);

    -- The event arrives BEFORE the run ever registers its wait.
    v_res := deliver_to_inbox(v_run, 'order.approved', '{"order_id":4711,"by":"priya"}'::jsonb);
    PERFORM assert_that('early signal: buffered when no wait registered', v_res = 'buffered');

    -- Now the handler reaches wait_event. It must resolve immediately from the inbox.
    v_res := commit_ops(v_run, 1,
        '[{"op":"wait_event","id":"approval","hash":"bbbb000000000001","event":"order.approved"}]'::jsonb);
    PERFORM assert_that('early signal: commit accepted', v_res = 'committed');

    SELECT status, result INTO v_status, v_result
      FROM run_steps WHERE run_id=v_run AND step_hash='bbbb000000000001';
    PERFORM assert_that('early signal: wait resolved immediately, not suspended',
                  v_status = 'completed');
    PERFORM assert_that('early signal: resolved with the buffered event',
                  v_result->>'by' = 'priya');
    PERFORM assert_that('early signal: run did NOT suspend',
                  (SELECT status FROM runs WHERE id=v_run) = 'pending');
    PERFORM assert_that('early signal: no wait row was parked',
                  NOT EXISTS (SELECT 1 FROM waits WHERE run_id=v_run AND resolved_at IS NULL));
    PERFORM assert_that('early signal: inbox entry consumed exactly once',
                  (SELECT count(*) FROM run_inbox
                    WHERE run_id=v_run AND consumed_by_step_hash='bbbb000000000001') = 1);
END $$;

-- ================================================================ R1: late signal still works

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a007';
        v_res text;
BEGIN
    PERFORM mkrun(v_run);
    INSERT INTO queue (ns, fn_id, run_id) VALUES ('prod','order-fulfilment',v_run);
    PERFORM claim_runs('worker-1', 10);

    -- wait registered first, event arrives later (the ordinary case)
    PERFORM commit_ops(v_run, 1,
        '[{"op":"wait_event","id":"approval","hash":"cccc000000000001","event":"order.approved"}]'::jsonb);
    PERFORM assert_that('late signal: run suspended awaiting event',
                  -- 006 separates `waiting` (parked on an event) from `sleeping`
                  -- (parked on a timer). The reference schema conflates them; an
                  -- operator scanning the run list cannot tell a stuck approval
                  -- from a month-long sleep if both read `sleeping`.
                  (SELECT status FROM runs WHERE id=v_run) = 'waiting');

    v_res := deliver_to_inbox(v_run, 'order.approved', '{"order_id":99}'::jsonb);
    PERFORM assert_that('late signal: delivery resolves the parked wait', v_res = 'resolved');
    PERFORM assert_that('late signal: step completed',
                  (SELECT status FROM run_steps
                    WHERE run_id=v_run AND step_hash='cccc000000000001') = 'completed');
    PERFORM assert_that('late signal: run requeued',
                  (SELECT status FROM runs WHERE id=v_run) = 'pending');
END $$;

-- ================================================================ R1: FIFO + one entry per wait

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a008';
BEGIN
    PERFORM mkrun(v_run);
    INSERT INTO queue (ns, fn_id, run_id) VALUES ('prod','order-fulfilment',v_run);
    PERFORM claim_runs('worker-1', 10);

    PERFORM deliver_to_inbox(v_run, 'tick', '{"n":1}'::jsonb);
    PERFORM deliver_to_inbox(v_run, 'tick', '{"n":2}'::jsonb);

    PERFORM commit_ops(v_run, 1, '[{"op":"wait_event","id":"t1","hash":"dddd000000000001","event":"tick"}]'::jsonb);
    PERFORM claim_runs('worker-1', 10);
    PERFORM commit_ops(v_run, 2, '[{"op":"wait_event","id":"t2","hash":"dddd000000000002","event":"tick"}]'::jsonb);

    PERFORM assert_that('inbox FIFO: first wait got first event',
        (SELECT result->>'n' FROM run_steps WHERE run_id=v_run AND step_hash='dddd000000000001') = '1');
    PERFORM assert_that('inbox FIFO: second wait got second event',
        (SELECT result->>'n' FROM run_steps WHERE run_id=v_run AND step_hash='dddd000000000002') = '2');
END $$;

-- ================================================================ R1: sender dedupe

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a009';
        v_sender uuid := '00000000-0000-7000-8000-00000000b001';
        v_res text;
BEGIN
    PERFORM mkrun(v_run);
    PERFORM mkrun(v_sender);
    PERFORM deliver_to_inbox(v_run, 'ping', '{"x":1}'::jsonb, v_sender, 'eeee000000000001');
    v_res := deliver_to_inbox(v_run, 'ping', '{"x":1}'::jsonb, v_sender, 'eeee000000000001');
    PERFORM assert_that('sender dedupe: retried signal does not double-deliver', v_res = 'duplicate');
    PERFORM assert_that('sender dedupe: exactly one inbox entry',
        (SELECT count(*) FROM run_inbox WHERE run_id=v_run) = 1);
END $$;

-- ================================================================ R2: outbox atomicity

DO $$
DECLARE v_run uuid := '00000000-0000-7000-8000-00000000a010';
BEGIN
    PERFORM mkrun(v_run);
    INSERT INTO queue (ns, fn_id, run_id) VALUES ('prod','order-fulfilment',v_run);
    PERFORM claim_runs('worker-1', 10);
    PERFORM commit_ops(v_run, 1,
        '[{"op":"step","id":"ship","hash":"ffff000000000001","data":{"c":"dhl"}}]'::jsonb,
        '[{"specversion":"1.0","id":"e1","source":"/fn","type":"order.shipped"}]'::jsonb);
    PERFORM assert_that('outbox: event written in the same transaction as the step',
        (SELECT count(*) FROM outbox WHERE run_id=v_run AND NOT published) = 1);

    -- stale fence must not emit either
    PERFORM commit_ops(v_run, 0, '[{"op":"step","id":"x","hash":"ffff000000000002"}]'::jsonb,
        '[{"specversion":"1.0","id":"e2","source":"/fn","type":"bad"}]'::jsonb);
    PERFORM assert_that('outbox: stale fence emits nothing',
        (SELECT count(*) FROM outbox WHERE run_id=v_run) = 1);
END $$;

-- ================================================================ R2: no advisory locks anywhere

DO $$
DECLARE v_n int;
BEGIN
    SELECT count(*) INTO v_n FROM pg_proc
     WHERE prosrc ILIKE '%pg_advisory%'
       AND pronamespace = 'public'::regnamespace;
    PERFORM assert_that('pooler-safe: no advisory locks in any engine function', v_n = 0);
END $$;

DO $$ BEGIN RAISE NOTICE '--- all engine assertions passed ---'; END $$;
