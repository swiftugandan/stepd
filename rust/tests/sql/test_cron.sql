-- stepd — behavioural tests for the cron schema (migration 009).
--
-- These cover the half of ADR-016 that lives in the database: the at-most-once
-- ledger, singleton overlap, claiming, registration and trimming. The other
-- half — expression parsing, DST, the catch-up decision — is pure arithmetic
-- and is tested in `stepd-core::cron` against a table of known transitions,
-- where a failure names the transition rather than a row count.
--
--   psql -d stepd -f 0001..0009 -f test_cron.sql
--
-- Every assertion raises NOTICE on pass and EXCEPTION on failure, so a non-zero
-- psql exit is the only signal a CI lane needs.
--
-- HELPER NAMING. Prefixed `cron_t_`, for the reason recorded at the top of
-- test_engine_ops.sql: helpers in different files with the same name become
-- overloads, and a call can silently resolve to the wrong one.

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION cron_t_assert(cond boolean, what text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF cond THEN RAISE NOTICE 'PASS  %', what;
    ELSE RAISE EXCEPTION 'FAIL  %', what;
    END IF;
END $$;

INSERT INTO namespaces (id) VALUES ('test') ON CONFLICT DO NOTHING;

-- Reset this suite's own fixtures before anything else.
--
-- Not tidiness: `cron_schedules` is UNIQUE (ns, fn_id, trigger_idx) and
-- `runs_singleton_key` is exclusive over active runs, so a second run of the
-- file collides with its own leftovers and fails in a way that reads exactly
-- like an engine regression. test_engine_ops.sql learned this the expensive
-- way. The scope is deliberately narrow — the `test` namespace, function ids
-- this file creates — so running the suite cannot disturb anything else in the
-- database it is pointed at.
DELETE FROM runs           WHERE ns = 'test' AND fn_id LIKE 'cron-%';
DELETE FROM cron_schedules WHERE ns = 'test' AND fn_id LIKE 'cron-%';

-- A schedule with everything defaulted but the bits a test is about. Each test
-- gets its own function id so the suite is order-independent and re-runnable —
-- the property test_engine_ops.sql lost once and spent an afternoon recovering.
CREATE OR REPLACE FUNCTION cron_t_sched(
    p_fn        text,
    p_next      timestamptz DEFAULT now(),
    p_singleton boolean DEFAULT false,
    p_key       text DEFAULT NULL,
    p_expr      text DEFAULT '0 * * * *'
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_id uuid := gen_random_uuid();
BEGIN
    INSERT INTO cron_schedules (id, ns, fn_id, trigger_idx, expr, tz,
                                singleton, run_key, next_fire_at)
    VALUES (v_id, 'test', p_fn, 0, p_expr, 'UTC', p_singleton, p_key, p_next);
    RETURN v_id;
END $$;

-- A discriminator, so a test needing a *fixed* run key does not collide with
-- its own leftovers through `runs_singleton_key`.
CREATE OR REPLACE FUNCTION cron_t_key(p_prefix text) RETURNS text
LANGUAGE sql AS $$ SELECT p_prefix || ':' || gen_random_uuid()::text $$;

-- ================================================================ at most once

DO $$
DECLARE
    v_s   uuid;
    v_occ timestamptz := '2026-03-01T05:00:00Z';
    v_a   record;
    v_b   record;
BEGIN
    v_s := cron_t_sched('cron-once');

    SELECT * INTO v_a FROM fire_cron_occurrence(v_s, v_occ);
    -- The whole point of the table. A second caller for the same occurrence —
    -- a replica that lost a race, a retry after a lost commit ack, a scheduler
    -- restarted mid-transaction — must create nothing.
    SELECT * INTO v_b FROM fire_cron_occurrence(v_s, v_occ);

    PERFORM cron_t_assert(v_a.outcome = 'fired', 'first fire of an occurrence creates a run');
    PERFORM cron_t_assert(v_a.run_id IS NOT NULL, 'the fired occurrence names its run');
    PERFORM cron_t_assert(v_b.outcome = 'duplicate', 'the second fire of the same occurrence is a duplicate');
    PERFORM cron_t_assert(v_b.run_id IS NULL, 'a duplicate creates no run');
    PERFORM cron_t_assert(
        (SELECT count(*) FROM runs WHERE ns = 'test' AND fn_id = 'cron-once') = 1,
        'exactly one run exists for the occurrence');
    PERFORM cron_t_assert(
        (SELECT count(*) FROM cron_fires WHERE schedule_id = v_s) = 1,
        'exactly one ledger row exists for the occurrence');
END $$;

DO $$
DECLARE
    v_s uuid;
    v_r record;
BEGIN
    v_s := cron_t_sched('cron-queued');
    SELECT * INTO v_r FROM fire_cron_occurrence(v_s, '2026-03-01T06:00:00Z');
    -- A run that exists but is not queued is a run that never starts, and it
    -- looks like a successful fire from the ledger.
    PERFORM cron_t_assert(
        (SELECT count(*) FROM queue WHERE run_id = v_r.run_id) = 1,
        'a cron fire enqueues its run for dispatch');
END $$;

DO $$
DECLARE
    v_s uuid;
    v_r record;
    v_in jsonb;
BEGIN
    v_s := cron_t_sched('cron-input', now(), false, NULL, '30 4 * * *');
    SELECT * INTO v_r FROM fire_cron_occurrence(v_s, '2026-03-01T04:30:00Z');
    SELECT input INTO v_in FROM runs WHERE id = v_r.run_id;

    -- ADR-016 accepts that `started_at` carries the recovery time rather than
    -- the intended occurrence. That is only tolerable because the occurrence is
    -- recorded somewhere the handler can read it; if it were not, a catch-up
    -- fire computing "the last hour" would compute the wrong hour with nothing
    -- to notice it by.
    -- Compared as an instant, not as text: the exact rendering is the
    -- database's business, and pinning it here would make a precision fix look
    -- like a regression. What matters is that the value round-trips to the
    -- occurrence, at full precision — truncating it makes two occurrences inside
    -- one second indistinguishable to the handler.
    PERFORM cron_t_assert(
        (v_in -> 'cron' ->> 'occurrence_at')::timestamptz = '2026-03-01T04:30:00Z'::timestamptz,
        'the run input carries the intended occurrence, not the fire time');
    PERFORM cron_t_assert(
        (v_in -> 'cron' ->> 'occurrence_at')::timestamptz
            = '2026-03-01T04:30:00.000123Z'::timestamptz IS NOT TRUE,
        'and does so at full precision, not truncated to the second');
    PERFORM cron_t_assert(v_in -> 'cron' ->> 'expr' = '30 4 * * *',
        'the run input carries the expression that produced it');
    PERFORM cron_t_assert(v_in -> 'cron' ->> 'tz' = 'UTC',
        'the run input carries the zone the expression was read in');
END $$;

-- ================================================================ skips

DO $$
DECLARE
    v_s uuid;
    v_r record;
    v_before bigint;
    v_after  bigint;
BEGIN
    v_s := cron_t_sched('cron-skip');
    SELECT COALESCE(value, 0) INTO v_before FROM engine_counters
     WHERE ns = 'test' AND name = 'cron_skipped';
    v_before := COALESCE(v_before, 0);

    SELECT * INTO v_r FROM fire_cron_occurrence(
        v_s, '2026-03-01T07:00:00Z', 'skipped_misfire_window');

    SELECT value INTO v_after FROM engine_counters
     WHERE ns = 'test' AND name = 'cron_skipped';

    PERFORM cron_t_assert(v_r.outcome = 'skipped_misfire_window',
        'a skipped occurrence reports its reason');
    PERFORM cron_t_assert(v_r.run_id IS NULL, 'a skipped occurrence creates no run');
    PERFORM cron_t_assert(v_after = v_before + 1,
        'a skipped occurrence bumps cron_skipped');
    -- The skip is in the ledger, so an occurrence that was deliberately not
    -- fired is distinguishable from one that was never considered.
    PERFORM cron_t_assert(
        (SELECT outcome FROM cron_fires
          WHERE schedule_id = v_s AND occurrence_at = '2026-03-01T07:00:00Z')
        = 'skipped_misfire_window',
        'the skip and its reason are on the record');
END $$;

DO $$
DECLARE
    v_s   uuid;
    v_key text := cron_t_key('nightly');
    v_a   record;
    v_b   record;
BEGIN
    v_s := cron_t_sched('cron-singleton', now(), true, v_key);

    SELECT * INTO v_a FROM fire_cron_occurrence(v_s, '2026-03-01T01:00:00Z');
    -- The previous fire's run is still active, so this one is skipped rather
    -- than queued behind it. Queueing would make a schedule that cannot keep up
    -- grow a backlog of runs that each start progressively later, and nothing
    -- would report that it had happened.
    SELECT * INTO v_b FROM fire_cron_occurrence(v_s, '2026-03-01T02:00:00Z');

    PERFORM cron_t_assert(v_a.outcome = 'fired', 'the first singleton fire runs');
    PERFORM cron_t_assert(v_b.outcome = 'skipped_singleton',
        'a singleton fire whose key is still active is skipped');
    PERFORM cron_t_assert(
        (SELECT count(*) FROM runs WHERE ns = 'test' AND key = v_key) = 1,
        'the singleton key has exactly one run');
    PERFORM cron_t_assert(
        (SELECT outcome FROM cron_fires
          WHERE schedule_id = v_s AND occurrence_at = '2026-03-01T02:00:00Z')
        = 'skipped_singleton',
        'the singleton skip is recorded against the occurrence it skipped');

    -- …and once the run finishes, the schedule resumes. A singleton that stays
    -- blocked after its run ends is a schedule that stops forever.
    UPDATE runs SET status = 'completed', ended_at = now()
     WHERE ns = 'test' AND key = v_key;

    SELECT * INTO v_b FROM fire_cron_occurrence(v_s, '2026-03-01T03:00:00Z');
    PERFORM cron_t_assert(v_b.outcome = 'fired',
        'a singleton schedule resumes once its run completes');
END $$;

DO $$
DECLARE v_ok boolean := false;
BEGIN
    -- Accepting `singleton: true` with no key would degrade silently to "no
    -- overlap control at all", which is precisely the property the operator
    -- asked for.
    BEGIN
        INSERT INTO cron_schedules (id, ns, fn_id, trigger_idx, expr, tz,
                                    singleton, run_key, next_fire_at)
        VALUES (gen_random_uuid(), 'test', 'cron-nokey', 0, '0 * * * *', 'UTC',
                true, NULL, now());
    EXCEPTION WHEN check_violation THEN v_ok := true;
    END;
    PERFORM cron_t_assert(v_ok, 'a singleton schedule without a key is rejected');
END $$;

-- ================================================================ claiming

DO $$
DECLARE
    v_due   uuid;
    v_later uuid;
    v_paused uuid;
    v_ids   uuid[];
BEGIN
    v_due    := cron_t_sched('cron-claim-due',    now() - interval '1 min');
    v_later  := cron_t_sched('cron-claim-later',  now() + interval '1 hour');
    v_paused := cron_t_sched('cron-claim-paused', now() - interval '1 min');
    UPDATE cron_schedules SET paused = true WHERE id = v_paused;

    SELECT array_agg(c.id) INTO v_ids FROM claim_due_cron('test', 100) c
     WHERE c.fn_id LIKE 'cron-claim-%';

    PERFORM cron_t_assert(v_due = ANY (v_ids), 'a due schedule is claimed');
    PERFORM cron_t_assert(NOT (v_later = ANY (v_ids)),
        'a schedule whose time has not come is not claimed');
    PERFORM cron_t_assert(NOT (v_paused = ANY (v_ids)),
        'a paused schedule is never claimed');
END $$;

DO $$
DECLARE v_s uuid; v_now timestamptz;
BEGIN
    v_s := cron_t_sched('cron-dbtime', now() - interval '1 min');
    SELECT c.db_now INTO v_now FROM claim_due_cron('test', 100) c WHERE c.id = v_s;
    -- F-DL-8: fire times must derive from database time. Handing the caller the
    -- database's own clock alongside the row removes its reason to consult its
    -- own, which on a skewed replica would fire the whole fleet early.
    PERFORM cron_t_assert(v_now IS NOT NULL, 'the claim hands back database time');
    PERFORM cron_t_assert(abs(extract(epoch FROM (v_now - now()))) < 5,
        'the database time handed back is the current transaction time');
END $$;

DO $$
DECLARE v_s uuid; v_next timestamptz := '2026-03-01T09:00:00Z';
BEGIN
    v_s := cron_t_sched('cron-advance', now() - interval '1 min');
    PERFORM advance_cron(v_s, v_next, '2026-03-01T08:00:00Z');
    PERFORM cron_t_assert(
        (SELECT next_fire_at FROM cron_schedules WHERE id = v_s) = v_next,
        'advancing moves the next fire time');
    PERFORM cron_t_assert(
        (SELECT last_fired_at FROM cron_schedules WHERE id = v_s)
        = '2026-03-01T08:00:00Z',
        'advancing records what was last fired');

    -- A sweep that fires nothing (every occurrence skipped) still advances, and
    -- must not erase the record of the last real fire.
    PERFORM advance_cron(v_s, '2026-03-01T10:00:00Z', NULL);
    PERFORM cron_t_assert(
        (SELECT last_fired_at FROM cron_schedules WHERE id = v_s)
        = '2026-03-01T08:00:00Z',
        'advancing without a fire leaves the last fire time alone');
END $$;

-- ================================================================ registration

DO $$
DECLARE
    v_a record;
    v_b record;
    v_c record;
    v_t timestamptz := now() + interval '1 day';
BEGIN
    SELECT * INTO v_a FROM upsert_cron_schedule(
        'test', 'cron-reg', 0, '0 3 * * *', 'UTC', 'one', 10,
        interval '1 hour', false, NULL, v_t);
    PERFORM cron_t_assert(v_a.definition_changed, 'a new schedule counts as changed');

    -- ADR-016: re-registering with the SAME expression must not move the next
    -- fire. A deploy loop that reset it on every restart would postpone a
    -- schedule indefinitely, one deploy at a time, and each deploy would look
    -- successful.
    SELECT * INTO v_b FROM upsert_cron_schedule(
        'test', 'cron-reg', 0, '0 3 * * *', 'UTC', 'one', 10,
        interval '1 hour', false, NULL, now() + interval '99 days');
    PERFORM cron_t_assert(v_b.id = v_a.id, 're-registration keeps the schedule identity');
    PERFORM cron_t_assert(NOT v_b.definition_changed,
        'an unchanged expression is not a definition change');
    PERFORM cron_t_assert(
        (SELECT next_fire_at FROM cron_schedules WHERE id = v_a.id) = v_t,
        're-registering an unchanged schedule does not postpone it');

    -- …and a changed one does take effect from the next occurrence.
    SELECT * INTO v_c FROM upsert_cron_schedule(
        'test', 'cron-reg', 0, '0 4 * * *', 'UTC', 'one', 10,
        interval '1 hour', false, NULL, '2026-04-01T04:00:00Z');
    PERFORM cron_t_assert(v_c.definition_changed, 'a changed expression is a definition change');
    PERFORM cron_t_assert(
        (SELECT next_fire_at FROM cron_schedules WHERE id = v_a.id)
        = '2026-04-01T04:00:00Z',
        'a changed expression takes effect from the next occurrence');
END $$;

DO $$
DECLARE v_zero record; v_one record; v_n integer;
BEGIN
    SELECT * INTO v_zero FROM upsert_cron_schedule(
        'test', 'cron-prune', 0, '0 1 * * *', 'UTC', 'one', 10,
        interval '1 hour', false, NULL, now());
    SELECT * INTO v_one FROM upsert_cron_schedule(
        'test', 'cron-prune', 1, '0 2 * * *', 'UTC', 'one', 10,
        interval '1 hour', false, NULL, now());

    -- The function now declares only its first trigger. Leaving the second
    -- firing would be indistinguishable from the deploy not having happened.
    v_n := prune_cron_schedules('test', 'cron-prune', ARRAY[0]);

    PERFORM cron_t_assert(v_n = 1, 'pruning removes the undeclared trigger');
    PERFORM cron_t_assert(
        EXISTS (SELECT 1 FROM cron_schedules WHERE id = v_zero.id),
        'pruning keeps the declared trigger');
    PERFORM cron_t_assert(
        NOT EXISTS (SELECT 1 FROM cron_schedules WHERE id = v_one.id),
        'pruning removes the withdrawn trigger');

    -- A function that declares no cron triggers at all loses all of them.
    PERFORM prune_cron_schedules('test', 'cron-prune', ARRAY[]::integer[]);
    PERFORM cron_t_assert(
        NOT EXISTS (SELECT 1 FROM cron_schedules WHERE ns = 'test' AND fn_id = 'cron-prune'),
        'a function with no cron triggers keeps no schedules');
END $$;

-- ================================================================ trimming

DO $$
DECLARE
    v_s      uuid;
    v_recent timestamptz := now() - interval '1 day';
    v_old    timestamptz := now() - interval '400 days';
BEGIN
    v_s := cron_t_sched('cron-trim');
    INSERT INTO cron_fires (schedule_id, occurrence_at, outcome)
    VALUES (v_s, v_recent, 'fired'), (v_s, v_old, 'fired');

    PERFORM trim_cron_fires(1000);

    PERFORM cron_t_assert(
        EXISTS (SELECT 1 FROM cron_fires WHERE schedule_id = v_s AND occurrence_at = v_recent),
        'trimming keeps a recent fire');
    PERFORM cron_t_assert(
        NOT EXISTS (SELECT 1 FROM cron_fires WHERE schedule_id = v_s AND occurrence_at = v_old),
        'trimming removes an ancient fire');
END $$;

DO $$
DECLARE
    v_s uuid;
    v_occ timestamptz := now() - interval '45 days';
BEGIN
    -- Retention is 30 days, but this schedule's misfire window is 60. Deleting
    -- the ledger row would make a 45-day-old occurrence eligible to fire again
    -- on the next restart — and the evidence of why would have gone with it.
    v_s := cron_t_sched('cron-trim-window');
    UPDATE cron_schedules SET misfire_window = interval '60 days' WHERE id = v_s;
    INSERT INTO cron_fires (schedule_id, occurrence_at, outcome)
    VALUES (v_s, v_occ, 'fired');

    PERFORM trim_cron_fires(1000);

    PERFORM cron_t_assert(
        EXISTS (SELECT 1 FROM cron_fires WHERE schedule_id = v_s AND occurrence_at = v_occ),
        'trimming never deletes inside the misfire window, whatever retention says');
END $$;

-- ================================================================ lifecycle

DO $$
DECLARE v_s uuid; v_r record;
BEGIN
    v_s := cron_t_sched('cron-cascade');
    SELECT * INTO v_r FROM fire_cron_occurrence(v_s, '2026-05-01T00:00:00Z');
    DELETE FROM cron_schedules WHERE id = v_s;

    -- Deleting a schedule takes its ledger with it but leaves the runs it
    -- created. A completed run is history; removing a trigger must not rewrite
    -- what already happened.
    PERFORM cron_t_assert(
        NOT EXISTS (SELECT 1 FROM cron_fires WHERE schedule_id = v_s),
        'deleting a schedule removes its ledger');
    PERFORM cron_t_assert(
        EXISTS (SELECT 1 FROM runs WHERE id = v_r.run_id),
        'deleting a schedule leaves the runs it already created');
END $$;

DO $$ BEGIN RAISE NOTICE 'cron schema suite complete'; END $$;

-- ================================================================ fairness

DO $$
DECLARE
    v_mine  uuid;
    v_other uuid;
    v_ids   uuid[];
BEGIN
    -- Claiming is namespace-scoped, for the same reason dispatch claiming is:
    -- a blind claim ordered by next_fire_at is won by whoever is furthest
    -- behind, so one tenant's backlog silently stops every other tenant's
    -- schedules. Nothing fails and no queue grows anywhere an operator looks.
    INSERT INTO namespaces (id) VALUES ('other') ON CONFLICT DO NOTHING;
    DELETE FROM cron_schedules WHERE ns = 'other' AND fn_id LIKE 'cron-%';

    v_mine := cron_t_sched('cron-fair-mine', now() - interval '1 min');
    v_other := gen_random_uuid();
    INSERT INTO cron_schedules (id, ns, fn_id, trigger_idx, expr, tz, next_fire_at)
    VALUES (v_other, 'other', 'cron-fair-theirs', 0, '0 * * * *', 'UTC',
            now() - interval '10 years');

    SELECT array_agg(c.id) INTO v_ids FROM claim_due_cron('test', 100) c;

    PERFORM cron_t_assert(v_mine = ANY (v_ids),
        'a claim returns the requested namespace''s schedules');
    PERFORM cron_t_assert(NOT (v_other = ANY (v_ids)),
        'and never another namespace''s, however far behind it is');

    PERFORM cron_t_assert(
        EXISTS (SELECT 1 FROM cron_active_namespaces() WHERE ns = 'other'),
        'the other namespace is still reported as having work');
END $$;
