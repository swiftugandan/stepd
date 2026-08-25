-- stepd — behavioural tests for the ops added in migration 006.
--
-- These are the ops that had lived in application-level Rust and were therefore
-- untested by anything: invoke, signal, continue_as_new, cascade, wait timeout
-- and the runaway limits. Each assertion states the behaviour, not the mechanism,
-- so the test survives a reimplementation of the function it exercises.
--
--   psql -d stepd -f 0001..0006 -f test_engine_ops.sql
--
-- Every assertion raises NOTICE on pass and EXCEPTION on failure, so a non-zero
-- psql exit is the only signal a CI lane needs.
--
-- HELPER NAMING. Every helper here is prefixed `ops_`. `test_engine.sql` defines
-- a `mkrun` too, with a different signature — so the two were *overloads* rather
-- than replacements, and a call with a text literal could resolve to the other
-- file's helper, which defaults to a different namespace. Running the two files
-- in sequence twice produced a foreign-key violation that looked exactly like an
-- engine regression. It is the same hazard structural invariant 7 guards against
-- for `commit_ops`, arriving through the test suite instead.

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION ops_assert(cond boolean, what text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF cond THEN RAISE NOTICE 'PASS  %', what;
    ELSE RAISE EXCEPTION 'FAIL  %', what;
    END IF;
END $$;

-- A run created straight into the queue, bypassing triggers, so each test starts
-- from an unambiguous state.
CREATE OR REPLACE FUNCTION ops_mkrun(p_fn text, p_key text DEFAULT NULL,
                                 p_ns text DEFAULT 'test')
RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_id uuid := gen_random_uuid();
BEGIN
    INSERT INTO runs (id, ns, fn_id, key, lineage_id, status)
    VALUES (v_id, p_ns, p_fn, p_key, v_id, 'pending');
    INSERT INTO queue (ns, fn_id, key, run_id) VALUES (p_ns, p_fn, p_key, v_id);
    RETURN v_id;
END $$;

-- Claim a specific run and return its new fence, so a test never depends on
-- which of several queued runs the dispatcher happened to pick.
CREATE OR REPLACE FUNCTION ops_claim_one(p_run uuid) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE v_f bigint;
BEGIN
    UPDATE queue SET claimed_by = 't', claimed_until = now() + interval '5 min'
     WHERE run_id = p_run;
    UPDATE runs SET status = 'running', attempt_no = attempt_no + 1,
                    fence_token = fence_token + 1
     WHERE id = p_run RETURNING fence_token INTO v_f;
    RETURN v_f;
END $$;

INSERT INTO namespaces (id) VALUES ('test'), ('other') ON CONFLICT DO NOTHING;

-- A per-invocation discriminator for the tests that need a *fixed* business key.
--
-- Keyed exclusivity is a database invariant over active runs, so a test that
-- hard-codes `order:1` leaves a run holding that key and collides with itself the
-- next time the file is run. Discovered by running the suite twice: the second
-- run failed on `runs_singleton_key` at the continue_as_new block, which looked
-- exactly like an engine regression and was a test-hygiene defect.
--
-- Truncating instead would work and would make this file hostile to running
-- beside anything else.
CREATE OR REPLACE FUNCTION ops_test_key(p_prefix text) RETURNS text
LANGUAGE sql VOLATILE AS $$
    SELECT p_prefix || ':' || substring(gen_random_uuid()::text for 8)
$$;

-- ================================================================ invoke

DO $$
DECLARE
    v_parent uuid; v_child uuid; v_f bigint; v_res text; v_status text;
BEGIN
    v_parent := ops_mkrun('parent');
    v_f := ops_claim_one(v_parent);
    v_res := commit_ops(v_parent, v_f,
        '[{"op":"invoke","id":"refund","hash":"1000000000000001","function":"child"}]'::jsonb);
    PERFORM ops_assert(v_res = 'committed', 'invoke: committed');

    SELECT id INTO v_child FROM runs WHERE parent_run_id = v_parent;
    PERFORM ops_assert(v_child IS NOT NULL, 'invoke: child run created');
    PERFORM ops_assert((SELECT fn_id FROM runs WHERE id = v_child) = 'child',
                   'invoke: child runs the named function');
    PERFORM ops_assert(EXISTS (SELECT 1 FROM queue WHERE run_id = v_child),
                   'invoke: child is queued');

    SELECT status::text INTO v_status FROM runs WHERE id = v_parent;
    PERFORM ops_assert(v_status = 'sleeping', 'invoke: parent suspends awaiting the child');
    PERFORM ops_assert(NOT EXISTS (SELECT 1 FROM queue WHERE run_id = v_parent),
                   'invoke: suspended parent is not dispatchable');
    PERFORM ops_assert((SELECT status::text FROM run_steps
                     WHERE run_id = v_parent AND step_hash = '1000000000000001') = 'pending',
                   'invoke: parent step is pending, not completed');

    -- Child finishes. The parent's step must resolve with the child's output and
    -- the parent must become dispatchable again.
    v_f := ops_claim_one(v_child);
    PERFORM commit_ops(v_child, v_f, '[{"op":"done","data":{"refunded":42}}]'::jsonb);

    PERFORM ops_assert((SELECT status::text FROM run_steps
                     WHERE run_id = v_parent AND step_hash = '1000000000000001') = 'completed',
                   'invoke: child completion resolves the parent step');
    PERFORM ops_assert((SELECT result->>'refunded' FROM run_steps
                     WHERE run_id = v_parent AND step_hash = '1000000000000001') = '42',
                   'invoke: parent step carries the child output');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_parent) = 'pending',
                   'invoke: parent requeued once the child resolved');
    PERFORM ops_assert(EXISTS (SELECT 1 FROM queue WHERE run_id = v_parent),
                   'invoke: parent back on the queue');
    RAISE NOTICE '--- invoke ok ---';
END $$;

DO $$
DECLARE v_parent uuid; v_child uuid; v_f bigint;
BEGIN
    -- A failing child resolves the parent step as failed but does NOT fail the
    -- parent: the handler decides (protocol §7.5).
    v_parent := ops_mkrun('parent-f');
    v_f := ops_claim_one(v_parent);
    PERFORM commit_ops(v_parent, v_f,
        '[{"op":"invoke","id":"c","hash":"1000000000000002","function":"child"}]'::jsonb);
    SELECT id INTO v_child FROM runs WHERE parent_run_id = v_parent;
    v_f := ops_claim_one(v_child);
    PERFORM commit_ops(v_child, v_f,
        '[{"op":"error","retryable":false,"error":{"code":"boom","message":"x"}}]'::jsonb);

    PERFORM ops_assert((SELECT status::text FROM run_steps
                     WHERE run_id = v_parent AND step_hash = '1000000000000002') = 'failed',
                   'invoke: failed child resolves the step as failed');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_parent) = 'pending',
                   'invoke: a failed child does not by itself fail the parent');
    RAISE NOTICE '--- invoke failure ok ---';
END $$;

DO $$
DECLARE v_parent uuid; v_child uuid; v_f bigint;
BEGIN
    -- Detached: parent never suspends, and the child survives the parent.
    v_parent := ops_mkrun('parent-d');
    v_f := ops_claim_one(v_parent);
    PERFORM commit_ops(v_parent, v_f,
        '[{"op":"invoke","id":"bg","hash":"1000000000000003","function":"child","detach":true}]'::jsonb);
    SELECT id INTO v_child FROM runs WHERE parent_run_id = v_parent;

    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_parent) = 'pending',
                   'detach: parent does not suspend');
    PERFORM ops_assert((SELECT status::text FROM run_steps
                     WHERE run_id = v_parent AND step_hash = '1000000000000003') = 'completed',
                   'detach: step resolves immediately with the child id');

    v_f := ops_claim_one(v_parent);
    PERFORM commit_ops(v_parent, v_f, '[{"op":"done"}]'::jsonb);
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_child) = 'pending',
                   'detach: child unaffected by the parent reaching a terminal state');
    RAISE NOTICE '--- detach ok ---';
END $$;

-- ================================================================ cascade

DO $$
DECLARE v_a uuid; v_b uuid; v_c uuid; v_d uuid; v_f bigint;
BEGIN
    -- a -> b -> c (tracked), a -> d (detached). Cancelling a must take b and c
    -- and leave d alone, including the grandchild reached through b.
    v_a := ops_mkrun('casc-a');
    v_f := ops_claim_one(v_a);
    PERFORM commit_ops(v_a, v_f,
        '[{"op":"invoke","id":"b","hash":"2000000000000001","function":"casc-b"}]'::jsonb);
    SELECT id INTO v_b FROM runs WHERE parent_run_id = v_a;

    v_f := ops_claim_one(v_b);
    PERFORM commit_ops(v_b, v_f,
        '[{"op":"invoke","id":"c","hash":"2000000000000002","function":"casc-c"}]'::jsonb);
    SELECT id INTO v_c FROM runs WHERE parent_run_id = v_b;

    INSERT INTO runs (id, ns, fn_id, parent_run_id, parent_step_hash, detached,
                      lineage_id, status)
    VALUES (gen_random_uuid(), 'test', 'casc-d', v_a, '2000000000000003', true,
            gen_random_uuid(), 'pending')
    RETURNING id INTO v_d;

    PERFORM ops_assert(cancel_run('test', v_a), 'cascade: cancel accepted');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_a) = 'cancelled',
                   'cascade: parent cancelled');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_b) = 'cancelled',
                   'cascade: child cancelled');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_c) = 'cancelled',
                   'cascade: grandchild cancelled');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_d) = 'pending',
                   'cascade: detached child left running');
    PERFORM ops_assert(NOT EXISTS (SELECT 1 FROM queue WHERE run_id IN (v_a, v_b, v_c)),
                   'cascade: cancelled runs removed from dispatch');
    RAISE NOTICE '--- cascade ok ---';
END $$;

DO $$
DECLARE v_a uuid; v_b uuid; v_c uuid; v_f bigint;
BEGIN
    -- A detached child's own subtree is independent: cancelling the root must not
    -- reach a grandchild whose path passes through a detached run.
    v_a := ops_mkrun('sub-a');
    INSERT INTO runs (id, ns, fn_id, parent_run_id, parent_step_hash, detached,
                      lineage_id, status)
    VALUES (gen_random_uuid(), 'test', 'sub-b', v_a, 'h1', true, gen_random_uuid(), 'pending')
    RETURNING id INTO v_b;
    INSERT INTO runs (id, ns, fn_id, parent_run_id, parent_step_hash, detached,
                      lineage_id, status)
    VALUES (gen_random_uuid(), 'test', 'sub-c', v_b, 'h2', false, gen_random_uuid(), 'pending')
    RETURNING id INTO v_c;

    PERFORM cancel_run('test', v_a);
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_b) = 'pending',
                   'cascade: detached child survives');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_c) = 'pending',
                   'cascade: subtree below a detached child survives with it');
    RAISE NOTICE '--- detached subtree ok ---';
END $$;

-- ================================================================ continue_as_new

DO $$
DECLARE
    v_run uuid; v_succ uuid; v_f bigint; v_res text;
    v_key text := ops_test_key('sub');
BEGIN
    v_run := ops_mkrun('loop', v_key);
    v_f := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_f,
        '[{"op":"step","id":"work","hash":"3000000000000001","data":{"n":1}}]'::jsonb);
    v_f := ops_claim_one(v_run);
    v_res := commit_ops(v_run, v_f,
        '[{"op":"continue_as_new","id":"next","hash":"3000000000000002","input":{"cursor":41200}}]'::jsonb);
    PERFORM ops_assert(v_res = 'committed', 'continue_as_new: committed');

    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_run) = 'completed',
                   'continue_as_new: predecessor completes');

    SELECT id INTO v_succ FROM runs
     WHERE lineage_id = (SELECT lineage_id FROM runs WHERE id = v_run) AND id <> v_run;
    PERFORM ops_assert(v_succ IS NOT NULL, 'continue_as_new: successor created');
    PERFORM ops_assert((SELECT key FROM runs WHERE id = v_succ) = v_key,
                   'continue_as_new: successor keeps the key');
    PERFORM ops_assert((SELECT chain_position FROM runs WHERE id = v_succ) = 1,
                   'continue_as_new: chain position advances');
    PERFORM ops_assert((SELECT input->>'cursor' FROM runs WHERE id = v_succ) = '41200',
                   'continue_as_new: input carried to the successor');
    PERFORM ops_assert((SELECT count(*) FROM run_steps WHERE run_id = v_succ) = 0,
                   'continue_as_new: successor starts with an empty journal');
    PERFORM ops_assert(EXISTS (SELECT 1 FROM queue WHERE run_id = v_succ),
                   'continue_as_new: successor is dispatchable');
    -- Keyed exclusivity must still hold across the transition: exactly one
    -- active run on the key, never two, never zero.
    PERFORM ops_assert((SELECT count(*) FROM runs
                     WHERE ns='test' AND fn_id='loop' AND key=v_key
                       AND status IN ('pending','running','sleeping','waiting')) = 1,
                   'continue_as_new: exactly one active run on the key throughout');
    RAISE NOTICE '--- continue_as_new ok ---';
END $$;

DO $$
DECLARE v_run uuid; v_f bigint; v_res text;
BEGIN
    -- Property P8, found by simulation: a live non-detached child would be
    -- orphaned, and its result delivered into a journal the successor discards.
    v_run := ops_mkrun('loop-live');
    v_f := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_f,
        '[{"op":"invoke","id":"c","hash":"3000000000000010","function":"kid"}]'::jsonb);
    v_f := ops_claim_one(v_run);
    v_res := commit_ops(v_run, v_f,
        '[{"op":"continue_as_new","id":"n","hash":"3000000000000011"}]'::jsonb);

    PERFORM ops_assert(v_res = 'failed:continue_as_new_with_live_children',
                   'continue_as_new: rejected while a tracked child is live');
    PERFORM ops_assert((SELECT error->>'code' FROM runs WHERE id = v_run)
                     = 'continue_as_new_with_live_children',
                   'continue_as_new: the reason is recorded on the run');
    PERFORM ops_assert(NOT EXISTS (SELECT 1 FROM runs
                     WHERE lineage_id = (SELECT lineage_id FROM runs WHERE id = v_run)
                       AND id <> v_run),
                   'continue_as_new: no successor created on rejection');
    RAISE NOTICE '--- continue_as_new live children ok ---';
END $$;

-- ================================================================ signal

DO $$
DECLARE v_sender uuid; v_target uuid; v_f bigint; v_n integer;
BEGIN
    -- The defect this replaces: a signal inserted straight into run_inbox left a
    -- run already parked on a matching wait asleep until its timeout.
    v_target := ops_mkrun('sig-target');
    v_f := ops_claim_one(v_target);
    PERFORM commit_ops(v_target, v_f,
        '[{"op":"wait_event","id":"approval","hash":"4000000000000001","event":"approved"}]'::jsonb);
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_target) = 'waiting',
                   'signal: target parked on the wait');

    v_sender := ops_mkrun('sig-sender');
    v_f := ops_claim_one(v_sender);
    PERFORM commit_ops(v_sender, v_f,
        format('[{"op":"signal","id":"notify","hash":"4000000000000002","target_run":"%s",
                  "event":{"type":"approved","data":{"by":"priya"}}}]', v_target)::jsonb);

    PERFORM ops_assert(EXISTS (SELECT 1 FROM signal_outbox WHERE sender_run_id = v_sender
                            AND delivered_at IS NULL),
                   'signal: recorded transactionally, pending delivery');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_target) = 'waiting',
                   'signal: not yet delivered before the drain');

    v_n := drain_signals(10);
    PERFORM ops_assert(v_n = 1, 'signal: one signal drained');
    PERFORM ops_assert((SELECT outcome FROM signal_outbox WHERE sender_run_id = v_sender) = 'resolved',
                   'signal: delivery resolved the parked wait');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_target) = 'pending',
                   'signal: target woken');
    PERFORM ops_assert((SELECT result->>'by' FROM run_steps
                     WHERE run_id = v_target AND step_hash = '4000000000000001') = 'priya',
                   'signal: wait resolved with the signalled payload');

    -- A retried sender must not double-deliver (protocol §7.6).
    PERFORM drain_signals(10);
    PERFORM ops_assert((SELECT count(*) FROM run_inbox WHERE run_id = v_target) = 1,
                   'signal: redelivery deduplicated at the inbox');
    RAISE NOTICE '--- signal ok ---';
END $$;

-- ================================================================ wait timeout

DO $$
DECLARE v_run uuid; v_f bigint;
BEGIN
    -- Before 006 the `timeout` field was accepted and silently ignored, so a run
    -- could wait for an event that never came, forever.
    v_run := ops_mkrun('wait-to');
    v_f := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_f,
        format('[{"op":"wait_event","id":"a","hash":"5000000000000001","event":"never",
                  "timeout_at":"%s"}]',
               to_char(now() - interval '1 second', 'YYYY-MM-DD"T"HH24:MI:SSOF'))::jsonb);

    PERFORM ops_assert(EXISTS (SELECT 1 FROM timers WHERE run_id = v_run AND kind = 'wait_timeout'),
                   'wait timeout: a timer was actually scheduled');

    PERFORM fire_due_timers(now(), 100);
    PERFORM ops_assert((SELECT status::text FROM run_steps
                     WHERE run_id = v_run AND step_hash = '5000000000000001') = 'timed_out',
                   'wait timeout: step resolves as timed_out');
    PERFORM ops_assert((SELECT resolved_at IS NOT NULL FROM waits
                     WHERE run_id = v_run AND step_hash = '5000000000000001'),
                   'wait timeout: the wait is closed, so a late event cannot resolve it');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_run) = 'pending',
                   'wait timeout: run resumes so the handler can see the timeout');
    RAISE NOTICE '--- wait timeout ok ---';
END $$;

DO $$
DECLARE v_run uuid; v_f bigint;
BEGIN
    -- A resolved wait must not fire its timeout afterwards and clobber the result.
    v_run := ops_mkrun('wait-race');
    v_f := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_f,
        format('[{"op":"wait_event","id":"a","hash":"5000000000000002","event":"soon",
                  "timeout_at":"%s"}]',
               to_char(now() + interval '1 hour', 'YYYY-MM-DD"T"HH24:MI:SSOF'))::jsonb);
    PERFORM deliver_to_inbox(v_run, 'soon', '{"ok":true}'::jsonb);
    PERFORM ops_assert((SELECT fired_at IS NOT NULL FROM timers
                     WHERE run_id = v_run AND kind = 'wait_timeout'),
                   'wait timeout: timer defused when the wait resolves');
    PERFORM fire_due_timers(now() + interval '2 hours', 100);
    PERFORM ops_assert((SELECT status::text FROM run_steps
                     WHERE run_id = v_run AND step_hash = '5000000000000002') = 'completed',
                   'wait timeout: a resolved step is not overwritten by its timer');
    RAISE NOTICE '--- wait timeout race ok ---';
END $$;

-- ================================================================ parallel batch

DO $$
DECLARE v_run uuid; v_f bigint;
BEGIN
    -- A batch mixing a sleep and a wait must not wake on the first to resolve.
    v_run := ops_mkrun('batch');
    v_f := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_f,
        format('[{"op":"sleep","id":"s","hash":"6000000000000001","until":"%s"},
                 {"op":"wait_event","id":"w","hash":"6000000000000002","event":"go"}]',
               to_char(now() + interval '1 hour', 'YYYY-MM-DD"T"HH24:MI:SSOF'))::jsonb);

    PERFORM deliver_to_inbox(v_run, 'go', '{"x":1}'::jsonb);
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_run) <> 'pending',
                   'batch: run stays parked while a sibling op is still pending');

    PERFORM fire_due_timers(now() + interval '2 hours', 100);
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_run) = 'pending',
                   'batch: run resumes only once every op in the batch settled');
    RAISE NOTICE '--- parallel batch ok ---';
END $$;

-- ================================================================ limits

DO $$
DECLARE v_run uuid; v_f bigint; v_res text;
BEGIN
    v_run := ops_mkrun('lim');
    UPDATE engine_limits SET value = 2 WHERE name = 'steps_per_run';
    v_f := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_f,
        '[{"op":"step","id":"a","hash":"7000000000000001"},
          {"op":"step","id":"b","hash":"7000000000000002"}]'::jsonb);
    v_f := ops_claim_one(v_run);
    v_res := commit_ops(v_run, v_f, '[{"op":"step","id":"c","hash":"7000000000000003"}]'::jsonb);

    PERFORM ops_assert(v_res = 'failed:step_limit_exceeded', 'limits: step ceiling enforced');
    PERFORM ops_assert((SELECT status::text FROM runs WHERE id = v_run) = 'failed',
                   'limits: run failed non-retryably');
    PERFORM ops_assert(NOT EXISTS (SELECT 1 FROM run_steps
                     WHERE run_id = v_run AND step_hash = '7000000000000003'),
                   'limits: the offending op was not recorded');
    UPDATE engine_limits SET value = 10000 WHERE name = 'steps_per_run';
    RAISE NOTICE '--- step limit ok ---';
END $$;

DO $$
DECLARE v_run uuid; v_kid uuid; v_f bigint; v_res text; i integer;
BEGIN
    UPDATE engine_limits SET value = 3 WHERE name = 'invoke_depth';
    v_run := ops_mkrun('deep');
    FOR i IN 1..4 LOOP
        v_f := ops_claim_one(v_run);
        v_res := commit_ops(v_run, v_f,
            format('[{"op":"invoke","id":"d","hash":"800000000000000%s","function":"deep"}]', i)::jsonb);
        EXIT WHEN v_res <> 'committed';
        SELECT id INTO v_kid FROM runs WHERE parent_run_id = v_run;
        v_run := v_kid;
    END LOOP;
    PERFORM ops_assert(v_res = 'failed:invoke_depth_exceeded', 'limits: invoke depth enforced');
    UPDATE engine_limits SET value = 10 WHERE name = 'invoke_depth';
    RAISE NOTICE '--- invoke depth ok ---';
END $$;

DO $$
DECLARE v_a uuid; v_b uuid; v_f bigint; v_res text;
BEGIN
    -- Invoking an ancestor on the same key deadlocks keyed ordering: the child
    -- cannot start while the parent holds the key, and the parent waits on it.
    v_a := ops_mkrun('cyc', ops_test_key('k'));
    v_f := ops_claim_one(v_a);
    v_res := commit_ops(v_a, v_f,
        '[{"op":"invoke","id":"self","hash":"9000000000000001","function":"cyc"}]'::jsonb);
    PERFORM ops_assert(v_res = 'failed:invoke_cycle', 'limits: same-key ancestor invoke rejected');
    RAISE NOTICE '--- invoke cycle ok ---';
END $$;

DO $$
DECLARE v_run uuid; v_before bigint; i integer;
BEGIN
    UPDATE engine_limits SET value = 5 WHERE name = 'inbox_depth';
    v_run := ops_mkrun('inbox');
    SELECT COALESCE(value, 0) INTO v_before FROM engine_counters
     WHERE ns = 'test' AND name = 'inbox_overflow';

    FOR i IN 1..9 LOOP
        PERFORM deliver_to_inbox(v_run, 'flood', jsonb_build_object('n', i),
                                 gen_random_uuid(), 'h' || i);
    END LOOP;

    PERFORM ops_assert((SELECT count(*) FROM run_inbox
                     WHERE run_id = v_run AND consumed_by_step_hash IS NULL) = 5,
                   'inbox bound: depth held at the configured limit');
    PERFORM ops_assert((SELECT min((event->>'n')::int) FROM run_inbox WHERE run_id = v_run) = 5,
                   'inbox bound: the OLDEST entries were dropped, not the newest');
    PERFORM ops_assert((SELECT value FROM engine_counters
                     WHERE ns = 'test' AND name = 'inbox_overflow') > COALESCE(v_before, 0),
                   'inbox bound: overflow counted — the drop leaves evidence');
    UPDATE engine_limits SET value = 1000 WHERE name = 'inbox_depth';
    RAISE NOTICE '--- inbox bound ok ---';
END $$;

-- ================================================================ fencing

DO $$
DECLARE v_run uuid; v_f bigint; v_res text;
BEGIN
    -- Everything above assumes fencing still holds for the new ops too: a
    -- superseded attempt must not create a child run or a successor.
    v_run := ops_mkrun('fence-ops');
    v_f := ops_claim_one(v_run);
    PERFORM ops_claim_one(v_run);                        -- supersede
    v_res := commit_ops(v_run, v_f,
        '[{"op":"invoke","id":"x","hash":"a000000000000001","function":"kid"}]'::jsonb);
    PERFORM ops_assert(v_res = 'stale_fence', 'fencing: superseded invoke rejected');
    PERFORM ops_assert(NOT EXISTS (SELECT 1 FROM runs WHERE parent_run_id = v_run),
                   'fencing: no child run created by a stale attempt');

    v_res := commit_ops(v_run, v_f,
        '[{"op":"continue_as_new","id":"n","hash":"a000000000000002"}]'::jsonb);
    PERFORM ops_assert(v_res = 'stale_fence', 'fencing: superseded continue_as_new rejected');
    PERFORM ops_assert((SELECT count(*) FROM runs
                     WHERE lineage_id = (SELECT lineage_id FROM runs WHERE id = v_run)) = 1,
                   'fencing: no successor created by a stale attempt');
    RAISE NOTICE '--- fencing ok ---';
END $$;

-- ================================================================ unknown op

DO $$
DECLARE v_run uuid; v_f bigint; v_res text;
BEGIN
    -- Protocol §11: an op above the negotiated version must fail loudly rather
    -- than be dropped, which would look like a run that silently skipped work.
    v_run := ops_mkrun('unknown');
    v_f := ops_claim_one(v_run);
    v_res := commit_ops(v_run, v_f, '[{"op":"teleport","id":"t","hash":"b000000000000001"}]'::jsonb);
    PERFORM ops_assert(v_res = 'failed:unknown_op', 'protocol: unknown op fails the run');
    RAISE NOTICE '--- unknown op ok ---';
END $$;

DO $$ BEGIN RAISE NOTICE '=== all op assertions passed ==='; END $$;

-- ================================================================ compensation
--
-- Protocol §7.4. Every part of this existed and none of it ran: `cancel_run`
-- deleted the queue row, so the run was never dispatched again and
-- `run.cancelling` could never be true. Found by the conformance suite, which
-- was the first thing to ask whether the compensation path executed.

CREATE OR REPLACE FUNCTION ops_register(p_fn text, p_config jsonb) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE v_binding uuid;
BEGIN
    INSERT INTO app_bindings (id, ns, app_id, url)
    VALUES (gen_random_uuid(), 'test', 'ops-app-' || p_fn, 'http://app.invalid/')
    ON CONFLICT (ns, app_id) DO UPDATE SET url = EXCLUDED.url
    RETURNING id INTO v_binding;

    INSERT INTO functions (id, ns, app_binding_id, fn_id, version, config)
    VALUES (gen_random_uuid(), 'test', v_binding, p_fn, '1', p_config)
    ON CONFLICT (ns, fn_id, version) DO UPDATE SET config = EXCLUDED.config;
END $$;

DO $$
DECLARE v_run uuid;
BEGIN
    -- A function that declares no compensation path finishes immediately, as it
    -- always did. Dispatching an attempt to a handler that has none would hand
    -- it a normal-looking attempt and rely on it noticing `cancelling`.
    PERFORM ops_register('ops-nocomp', '{"id":"ops-nocomp"}'::jsonb);
    v_run := ops_mkrun('ops-nocomp');
    PERFORM cancel_run('test', v_run);

    PERFORM ops_assert(
        (SELECT status FROM runs WHERE id = v_run) = 'cancelled',
        'cancelling a function with no on_cancel finishes it immediately');
    PERFORM ops_assert(
        NOT EXISTS (SELECT 1 FROM queue WHERE run_id = v_run),
        'and dequeues it, because nothing more will be dispatched');
    PERFORM ops_assert(
        NOT (SELECT compensating FROM runs WHERE id = v_run),
        'and it owes no undo');
END $$;

DO $$
DECLARE
    v_run   uuid;
    v_fence bigint;
BEGIN
    PERFORM ops_register('ops-comp', '{"id":"ops-comp","on_cancel":true}'::jsonb);
    v_run := ops_mkrun('ops-comp');
    PERFORM cancel_run('test', v_run);

    -- The line that was missing. Without a queue row the run is never dispatched
    -- again, and the compensation path — the refund, the release, the notice —
    -- silently never runs.
    PERFORM ops_assert(
        (SELECT compensating FROM runs WHERE id = v_run),
        'a function declaring on_cancel enters the compensation phase');
    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM queue WHERE run_id = v_run AND claimed_by IS NULL),
        'and is queued for dispatch, which is what makes the phase happen at all');
    PERFORM ops_assert(
        (SELECT status FROM runs WHERE id = v_run) <> 'cancelled',
        'and is not yet cancelled, or its steps could not memoize (§7.4)');

    -- Compensation steps memoize normally: several passes, one new step each.
    v_fence := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','step','id','undo-1','hash','c0mp0000000000a1','data','"undone"')));

    PERFORM ops_assert(
        (SELECT status FROM run_steps WHERE run_id = v_run AND step_id = 'undo-1') = 'completed',
        'a compensation step commits and memoizes like any other');
    PERFORM ops_assert(
        (SELECT compensating FROM runs WHERE id = v_run),
        'and the run is still owed an undo until the path returns');
    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM queue WHERE run_id = v_run AND claimed_by IS NULL),
        'so it is queued again — one new step per attempt applies here too');

    -- …and the path returning is what ends it.
    v_fence := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','done','data','"compensated"')));

    PERFORM ops_assert(
        (SELECT status FROM runs WHERE id = v_run) = 'cancelled',
        'a compensation path returning done ends the run as cancelled, not completed');
    PERFORM ops_assert(
        NOT (SELECT compensating FROM runs WHERE id = v_run),
        'and the run no longer owes an undo');
    PERFORM ops_assert(
        NOT EXISTS (SELECT 1 FROM queue WHERE run_id = v_run),
        'and is dequeued');
END $$;

DO $$
DECLARE
    v_run   uuid;
    v_fence bigint;
BEGIN
    -- A compensation path that itself fails is a fact about the compensation,
    -- not a reason to call the run something else. An operator counting
    -- cancelled runs must not have to know which of them had an undo that threw.
    PERFORM ops_register('ops-comp-fail', '{"id":"ops-comp-fail","on_cancel":true}'::jsonb);
    v_run := ops_mkrun('ops-comp-fail');
    PERFORM cancel_run('test', v_run);

    v_fence := ops_claim_one(v_run);
    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','error','error',
            jsonb_build_object('code','refund_declined','message','the gateway said no'))));

    PERFORM ops_assert(
        (SELECT status FROM runs WHERE id = v_run) = 'cancelled',
        'a compensation path that fails still leaves the run cancelled, not failed');
    PERFORM ops_assert(
        (SELECT error->>'code' FROM runs WHERE id = v_run) = 'refund_declined',
        'and the compensation failure is recorded where an operator will find it');
END $$;

DO $$
DECLARE v_key text := ops_test_key('comp'); v_run uuid; v_second uuid;
BEGIN
    -- Keyed exclusivity must hold through compensation. Releasing the key when
    -- cancel was requested lets the next run start while the previous one is
    -- still releasing what it reserved — two runs interleaved on one key, which
    -- would show up only when a cancel raced an event.
    PERFORM ops_register('ops-comp-key', '{"id":"ops-comp-key","on_cancel":true}'::jsonb);
    v_run := ops_mkrun('ops-comp-key', v_key);
    PERFORM cancel_run('test', v_run);

    BEGIN
        v_second := ops_mkrun('ops-comp-key', v_key);
    EXCEPTION WHEN unique_violation THEN v_second := NULL;
    END;

    PERFORM ops_assert(v_second IS NULL,
        'no run may start on a key whose previous run is still compensating');
END $$;

DO $$
DECLARE v_parent uuid; v_child uuid; v_fence bigint;
BEGIN
    -- The cascade consults each descendant's own declaration, so a tree of
    -- compensating children each get their phase without the parent having to
    -- know the shape of the tree.
    PERFORM ops_register('ops-casc-parent', '{"id":"ops-casc-parent"}'::jsonb);
    PERFORM ops_register('ops-casc-child', '{"id":"ops-casc-child","on_cancel":true}'::jsonb);

    v_parent := ops_mkrun('ops-casc-parent');
    v_child := gen_random_uuid();
    INSERT INTO runs (id, ns, fn_id, parent_run_id, lineage_id, status)
    VALUES (v_child, 'test', 'ops-casc-child', v_parent, v_child, 'pending');

    PERFORM cancel_run('test', v_parent);

    PERFORM ops_assert(
        (SELECT compensating FROM runs WHERE id = v_child),
        'a cascaded child that declares on_cancel gets its compensation phase');
    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM queue WHERE run_id = v_child),
        'and is queued for it');
END $$;

-- ================================================================ blob refs
--
-- Protocol §8.3.4. `blob_refs`, `add_ref` and `blob_ids` all existed and nothing
-- called any of them, so the collector — which deletes committed blobs with no
-- references — was eligible to delete the bytes behind every `$blob` in a live
-- run's journal from the moment they were committed.

CREATE OR REPLACE FUNCTION ops_mkblob() RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE v_id uuid := gen_random_uuid();
BEGIN
    INSERT INTO blobs (id, ns, size, sha256, state, committed_at)
    VALUES (v_id, 'test', 3, sha256(v_id::text::bytea), 'committed', now());
    RETURN v_id;
END $$;

CREATE OR REPLACE FUNCTION ops_blobref(p_id uuid) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT jsonb_build_object('$blob',
        jsonb_build_object('id', p_id, 'size', 3, 'sha256', repeat('0', 64)))
$$;

DO $$
DECLARE v_run uuid; v_blob uuid; v_fence bigint;
BEGIN
    v_blob := ops_mkblob();
    v_run := ops_mkrun('ops-blobref');
    v_fence := ops_claim_one(v_run);

    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','step','id','make','hash','b10b000000000001',
                           'data', ops_blobref(v_blob))));

    -- The reference must exist the moment the step does. Recording it in a
    -- second statement after the commit would leave a window in which a crash
    -- produces a referenced blob with no reference — indistinguishable from
    -- garbage to a collector.
    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM blob_refs WHERE blob_id = v_blob AND run_id = v_run),
        'a blob in a step result is referenced as soon as the step is recorded');
END $$;

DO $$
DECLARE v_run uuid; v_blob uuid; v_fence bigint;
BEGIN
    -- Nested anywhere. A walk that only checked the top level would miss every
    -- ordinary payload shape and the miss would be silent until collection.
    v_blob := ops_mkblob();
    v_run := ops_mkrun('ops-blobnest');
    v_fence := ops_claim_one(v_run);

    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','step','id','nested','hash','b10b000000000002',
            'data', jsonb_build_object(
                'receipts', jsonb_build_array(
                    jsonb_build_object('doc', ops_blobref(v_blob)))))));

    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM blob_refs WHERE blob_id = v_blob AND run_id = v_run),
        'a blob nested inside arrays and objects is still referenced');
END $$;

DO $$
DECLARE v_run uuid; v_blob uuid;
BEGIN
    -- A run's input outlives every step and is read by the console and by a
    -- parent resolving a child's result, so a blob referenced only there is
    -- exactly as live as one in a journal.
    v_blob := ops_mkblob();
    v_run := gen_random_uuid();
    INSERT INTO runs (id, ns, fn_id, lineage_id, status, input)
    VALUES (v_run, 'test', 'ops-blobinput', v_run, 'pending', ops_blobref(v_blob));

    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM blob_refs WHERE blob_id = v_blob AND run_id = v_run),
        'a blob in a run input is referenced');
END $$;

DO $$
DECLARE v_run uuid; v_blob uuid; v_fence bigint;
BEGIN
    v_blob := ops_mkblob();
    v_run := ops_mkrun('ops-bloboutput');
    v_fence := ops_claim_one(v_run);

    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','done','data', ops_blobref(v_blob))));

    PERFORM ops_assert(
        EXISTS (SELECT 1 FROM blob_refs WHERE blob_id = v_blob AND run_id = v_run),
        'a blob in a run output is referenced');
END $$;

DO $$
DECLARE v_run uuid; v_ghost uuid := gen_random_uuid(); v_fence bigint;
BEGIN
    -- A reference to an id that was never reserved is an app inventing one.
    -- Inserting the row anyway would violate the foreign key and abort the whole
    -- commit, failing a run over a payload the engine is not supposed to read.
    v_run := ops_mkrun('ops-blobghost');
    v_fence := ops_claim_one(v_run);

    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','step','id','ghost','hash','b10b000000000003',
                           'data', ops_blobref(v_ghost))));

    PERFORM ops_assert(
        (SELECT status FROM run_steps WHERE run_id = v_run AND step_id = 'ghost')
        = 'completed',
        'a step naming an unknown blob still commits; the engine does not read payloads');
    PERFORM ops_assert(
        NOT EXISTS (SELECT 1 FROM blob_refs WHERE blob_id = v_ghost),
        'and no dangling reference is recorded for it');
END $$;


-- ---------------------------------------------------------------------------
-- A parallel group whose members did not all succeed (013, ADR-023).
--
-- The join policies were removed because every batch already behaved as
-- `all_settled`. What made that removal safe to write down was fixing what the
-- batch *did* on the failing path: it committed nothing, so the members that
-- had run and returned were forgotten along with the one that raised.
DO $$
DECLARE v_run uuid; v_fence bigint; v_rows int;
BEGIN
    v_run := ops_mkrun('fn-partial');
    v_fence := ops_claim_one(v_run);

    PERFORM commit_ops(v_run, v_fence, jsonb_build_array(
        jsonb_build_object('op','step','id','ok-1','hash','h-ok-1','data', to_jsonb(1)),
        jsonb_build_object('op','step','id','bad','hash','h-bad',
                           'error', jsonb_build_object('code','member_failed',
                                                       'message','on purpose')),
        jsonb_build_object('op','step','id','ok-2','hash','h-ok-2','data', to_jsonb(3)),
        jsonb_build_object('op','error','retryable', false,
                           'error', jsonb_build_object('code','member_failed',
                                                       'message','on purpose'))
    ));

    SELECT count(*) INTO v_rows FROM run_steps
     WHERE run_id = v_run AND status = 'completed' AND step_id IN ('ok-1','ok-2');
    PERFORM ops_assert(v_rows = 2,
        'a member that succeeded is recorded even though a sibling failed');

    PERFORM ops_assert(
        (SELECT status FROM run_steps WHERE run_id = v_run AND step_id = 'bad')
            = 'failed',
        'a member whose body raised is recorded as failed, not completed');

    PERFORM ops_assert(
        (SELECT error->>'code' FROM run_steps WHERE run_id = v_run AND step_id = 'bad')
            = 'member_failed',
        'the failing member keeps the error its body raised');

    PERFORM ops_assert(
        (SELECT result FROM run_steps WHERE run_id = v_run AND step_id = 'bad') IS NULL,
        'a failed member carries no result to memoise');

    PERFORM ops_assert(
        (SELECT status FROM runs WHERE id = v_run) = 'failed',
        'the error op still fails the run after the outcomes are recorded');

    -- The reason `error` must be last. If the engine processed it first the run
    -- would already be terminal, and the loop would go on inserting steps into
    -- a finished run — a journal that grows after the run ends.
    PERFORM ops_assert(
        (SELECT count(*) FROM run_steps WHERE run_id = v_run) = 3,
        'every member of the group reached the journal exactly once');
END $$;
