-- stepd — schema 009: the cron scheduler.
--
-- WHY THIS MIGRATION EXISTS
--
-- ADR-016 settled cron's semantics in full and closed with the sentence "the
-- honest statement is that stepd accepts cron triggers and does not run them".
-- `on_cron` registered a trigger, `trigger_matches` returned false for anything
-- that was not an event, and nothing ever inserted a timer. A cron function
-- registered cleanly and never fired — the worst failure shape there is,
-- because nothing anywhere reports it.
--
-- WHAT IS LOAD-BEARING HERE
--
-- (1) `cron_fires` has PRIMARY KEY (schedule_id, occurrence_at). "Each
--     occurrence fires at most once" is therefore an invariant the database
--     enforces, not a property of the scheduler being careful. Two replicas
--     that both decide an occurrence is due — after a lease expiry, a clock
--     wobble, a restart mid-transaction — produce one run, and the loser learns
--     it lost. Every other design for this ends in "and then we make sure the
--     scheduler is a singleton", which is a much larger promise.
--
-- (2) Claiming is `FOR UPDATE SKIP LOCKED` on `cron_schedules` rows and nothing
--     else. No advisory lock, no `SET`, no session state: F-DL-1 / ADR-019 says
--     the whole engine must work behind a transaction-mode pooler, and the
--     scheduler is the component most tempted to elect a leader with one.
--
-- (3) `next_fire_at` is computed by the caller from `now()` read *in this
--     database*, never from a replica's wall clock (F-DL-8). The claim function
--     therefore returns the database's own `now()` alongside each row, so the
--     caller cannot accidentally use its own.
--
-- WHAT IS DELIBERATELY NOT HERE
--
-- Cron expression parsing. Postgres has no cron parser and writing one in
-- PL/pgSQL would put the DST rules — the subtlest part of ADR-016 — somewhere
-- they cannot be unit-tested against a table of known transitions. Parsing and
-- next-occurrence computation live in `stepd-core::cron`, which is pure and has
-- no database. This migration owns the invariants; that module owns the
-- arithmetic. The split is the same one migration 006 was written to restore
-- elsewhere: one correctness centre per concern.

-- ---------------------------------------------------------------- schedules

CREATE TABLE IF NOT EXISTS cron_schedules (
    id              uuid PRIMARY KEY,
    ns              text NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    fn_id           text NOT NULL,
    -- A function may carry several cron triggers. The index into its trigger
    -- array is what makes each one addressable across re-registrations, so a
    -- function that changes one of three schedules does not lose the other two's
    -- fire history.
    trigger_idx     integer NOT NULL,

    expr            text NOT NULL,
    tz              text NOT NULL DEFAULT 'UTC',
    catchup         text NOT NULL DEFAULT 'one'
                    CHECK (catchup IN ('one', 'skip', 'all')),
    catchup_limit   integer NOT NULL DEFAULT 10
                    CHECK (catchup_limit BETWEEN 1 AND 1000),
    misfire_window  interval NOT NULL DEFAULT '1 hour',
    -- With `singleton`, a fire whose key already has an active run is skipped
    -- and counted, rather than queued behind it (ADR-016, protocol §3.1). The
    -- key is a literal, not a CEL expression: a cron fire has no event to
    -- evaluate an expression against, and pretending otherwise would give
    -- `key_expr` two meanings depending on what triggered the run.
    singleton       boolean NOT NULL DEFAULT false,
    run_key         text,

    next_fire_at    timestamptz NOT NULL,
    last_fired_at   timestamptz,
    paused          boolean NOT NULL DEFAULT false,
    -- Why a schedule was paused, in the row rather than only in a log line. A
    -- job that has silently stopped is the failure this whole migration exists
    -- to prevent, and "check the logs from whenever it happened" is not an
    -- answer an operator can act on at 3am.
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    UNIQUE (ns, fn_id, trigger_idx),

    -- Singleton overlap control needs something to be exclusive *on*. Accepting
    -- `singleton: true` with no key would silently degrade to "no overlap
    -- control", which is the failure mode this table exists to make loud.
    CONSTRAINT cron_singleton_needs_a_key
        CHECK (NOT singleton OR run_key IS NOT NULL)
);

-- The sweep's only index. Partial on `paused` because a paused schedule should
-- not cost anything to skip over — an operator pausing a noisy schedule during
-- an incident is a common move, and it should not leave the sweep scanning it
-- every second for the rest of the outage.
CREATE INDEX IF NOT EXISTS cron_due
    ON cron_schedules (next_fire_at)
    WHERE NOT paused;

CREATE INDEX IF NOT EXISTS cron_by_function
    ON cron_schedules (ns, fn_id);

-- ---------------------------------------------------------------- fire ledger

CREATE TABLE IF NOT EXISTS cron_fires (
    schedule_id   uuid NOT NULL REFERENCES cron_schedules(id) ON DELETE CASCADE,
    occurrence_at timestamptz NOT NULL,
    run_id        uuid REFERENCES runs(id) ON DELETE SET NULL,
    outcome       text NOT NULL CHECK (outcome IN (
                      'fired',
                      'skipped_singleton',
                      'skipped_misfire_window',
                      'skipped_catchup_policy',
                      'skipped_catchup_limit'
                  )),
    decided_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schedule_id, occurrence_at)
);

-- Skipped occurrences are recorded, not just dropped. A schedule that is
-- silently keeping up and one that is silently skipping every fire look
-- identical from the runs table alone, and the second is the one an operator
-- needs to find out about (ADR-016: "skipping silently would make a schedule
-- that never keeps up look identical to one that is working").
CREATE INDEX IF NOT EXISTS cron_fires_recent
    ON cron_fires (schedule_id, occurrence_at DESC);

-- ---------------------------------------------------------------- retention

INSERT INTO engine_limits (name, value, note) VALUES
    ('cron_fire_retention_days', 30,
     'ADR-016 — how long the cron fire ledger is kept beyond the misfire window')
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------------------------------------- claiming

-- Drop the single-argument form. It only exists in a database migrated from an
-- in-development copy of this file, and leaving it would make `claim_due_cron`
-- an overload set — the hazard structural invariant 7 exists for, and the one
-- that cost this project an afternoon through `mkrun` in the SQL test suites.
DROP FUNCTION IF EXISTS claim_due_cron(integer);

-- Claim schedules whose next fire time has arrived.
--
-- Returns the database's own `now()` with each row. That is not a convenience:
-- it is the mechanism that stops a replica with a skewed clock from computing
-- fire times from its own idea of the time and firing the whole fleet early
-- (F-DL-8). A caller that wants the current time has it here and has no reason
-- to look anywhere else.
--
-- The rows stay locked for the remainder of the caller's transaction, which is
-- what makes the claim mean anything. The caller is expected to compute a
-- decision — pure arithmetic, no I/O — and commit promptly; anything that
-- blocks on the network while holding these locks turns a scheduler into an
-- outage.
CREATE OR REPLACE FUNCTION claim_due_cron(p_ns text, p_max integer DEFAULT 100)
RETURNS TABLE (
    id             uuid,
    ns             text,
    fn_id          text,
    expr           text,
    tz             text,
    catchup        text,
    catchup_limit  integer,
    misfire_window_secs double precision,
    singleton      boolean,
    run_key        text,
    next_fire_at   timestamptz,
    db_now         timestamptz
) LANGUAGE sql AS $$
    -- Seconds rather than the interval itself: an interval carries months, and
    -- a month is not a fixed number of seconds, so handing one to a caller that
    -- must do arithmetic with it invites a quiet off-by-a-few-days. Every value
    -- stored here comes from a fixed duration in the first place.
    SELECT s.id, s.ns, s.fn_id, s.expr, s.tz, s.catchup, s.catchup_limit,
           EXTRACT(EPOCH FROM s.misfire_window)::double precision,
           s.singleton, s.run_key, s.next_fire_at, now()
      FROM cron_schedules s
     WHERE s.ns = p_ns
       AND NOT s.paused
       AND s.next_fire_at <= now()
     ORDER BY s.next_fire_at
     LIMIT p_max
       FOR UPDATE SKIP LOCKED
$$;

-- Namespaces with at least one schedule due.
--
-- The sweep is namespace-scoped for the same reason dispatch claiming is
-- (structural invariant 15): a namespace-blind `LIMIT n ORDER BY next_fire_at`
-- is claimed by whoever is furthest behind. One tenant with a thousand overdue
-- per-minute schedules would fill every sweep, and every other tenant's
-- schedules would stop firing — silently, since nothing fails and no backlog
-- accumulates anywhere an operator would look. Rotating over this list is what
-- makes the guarantee "your schedule fires" rather than "your schedule fires if
-- nobody noisier shares the database".
CREATE OR REPLACE FUNCTION cron_active_namespaces() RETURNS TABLE (ns text)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT s.ns FROM cron_schedules s
     WHERE NOT s.paused AND s.next_fire_at <= now()
$$;

-- ---------------------------------------------------------------- firing

-- Record one occurrence's outcome, creating a run if it is to fire.
--
-- Idempotent by construction: the ledger insert is the first statement and its
-- primary key is the at-most-once guarantee. A second caller for the same
-- occurrence — a replica that lost a race, a retry after a lost commit ack —
-- takes the `duplicate` branch and creates nothing.
--
-- `p_outcome` lets the caller record a decision it already made (a misfire
-- window skip, a catch-up policy skip) through the same ledger as a fire, so
-- there is exactly one place that answers "what happened to the 03:00
-- occurrence" and it never says "nothing is recorded, so probably nothing
-- happened".
CREATE OR REPLACE FUNCTION fire_cron_occurrence(
    p_schedule_id uuid,
    p_occurrence  timestamptz,
    p_outcome     text DEFAULT 'fired'
) RETURNS TABLE (run_id uuid, outcome text)
LANGUAGE plpgsql AS $$
DECLARE
    v_sched   cron_schedules%ROWTYPE;
    v_run     uuid;
    v_outcome text := p_outcome;
BEGIN
    SELECT * INTO v_sched FROM cron_schedules WHERE id = p_schedule_id;
    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::uuid, 'no_such_schedule'::text;
        RETURN;
    END IF;

    -- FIRST STATEMENT, and the whole reason this function exists. Everything
    -- after it is conditional on having won the occurrence.
    INSERT INTO cron_fires (schedule_id, occurrence_at, outcome)
    VALUES (p_schedule_id, p_occurrence, v_outcome)
    ON CONFLICT (schedule_id, occurrence_at) DO NOTHING;

    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::uuid, 'duplicate'::text;
        RETURN;
    END IF;

    IF v_outcome <> 'fired' THEN
        PERFORM bump_counter(v_sched.ns, 'cron_skipped');
        RETURN QUERY SELECT NULL::uuid, v_outcome;
        RETURN;
    END IF;

    v_run := gen_random_uuid();

    -- The keyed-exclusivity index (`runs_singleton_key`) decides the singleton
    -- case. Asking "is a run already active?" and then inserting would be a
    -- check-then-act race between two replicas; letting the unique index refuse
    -- the insert is the same question answered atomically.
    BEGIN
        INSERT INTO runs (id, ns, fn_id, key, status, lineage_id, input)
        VALUES (
            v_run, v_sched.ns, v_sched.fn_id,
            CASE WHEN v_sched.singleton THEN v_sched.run_key ELSE NULL END,
            'pending', v_run,
            jsonb_build_object(
                'cron', jsonb_build_object(
                    -- `to_jsonb`, not `to_char` with a seconds-precision format.
                    -- The occurrence is the handler's only view of which fire it
                    -- is, and truncating it makes two occurrences inside one
                    -- second indistinguishable — which is not hypothetical: a
                    -- point-in-time restore rewinds `next_fire_at` to an
                    -- arbitrary instant, and so does an operator fixing a
                    -- schedule by hand. The simulation harness found this by
                    -- reporting two runs for one occurrence that were in fact
                    -- two occurrences it could no longer tell apart.
                    'occurrence_at', to_jsonb(p_occurrence),
                    'expr',          v_sched.expr,
                    'tz',            v_sched.tz,
                    'schedule_id',   v_sched.id
                )
            )
        );
    EXCEPTION WHEN unique_violation THEN
        -- A run on this key is still active. ADR-016: the fire is skipped and
        -- counted, not queued — a schedule that cannot keep up should show up as
        -- a number, not as an ever-growing backlog of runs that each start late.
        UPDATE cron_fires SET outcome = 'skipped_singleton'
         WHERE schedule_id = p_schedule_id AND occurrence_at = p_occurrence;
        PERFORM bump_counter(v_sched.ns, 'cron_skipped');
        RETURN QUERY SELECT NULL::uuid, 'skipped_singleton'::text;
        RETURN;
    END;

    INSERT INTO queue (ns, fn_id, key, run_id)
    VALUES (v_sched.ns, v_sched.fn_id,
            CASE WHEN v_sched.singleton THEN v_sched.run_key ELSE NULL END,
            v_run);

    UPDATE cron_fires SET run_id = v_run
     WHERE schedule_id = p_schedule_id AND occurrence_at = p_occurrence;

    PERFORM bump_counter(v_sched.ns, 'cron_fired');
    RETURN QUERY SELECT v_run, 'fired'::text;
END $$;

-- Move a schedule on to its next occurrence.
--
-- Separate from `fire_cron_occurrence` because one sweep of a schedule may fire
-- several occurrences (catch-up) and must advance exactly once, at the end. A
-- combined function would have to be told which call was the last one, which is
-- the kind of parameter that is eventually passed wrongly.
CREATE OR REPLACE FUNCTION advance_cron(
    p_schedule_id uuid,
    p_next        timestamptz,
    p_last_fired  timestamptz DEFAULT NULL
) RETURNS void LANGUAGE sql AS $$
    UPDATE cron_schedules
       SET next_fire_at  = p_next,
           last_fired_at = COALESCE(p_last_fired, last_fired_at),
           updated_at    = now()
     WHERE id = p_schedule_id
$$;

-- ---------------------------------------------------------------- registration

-- Upsert a schedule from a function's cron trigger.
--
-- ADR-016: "re-registering a function with a changed cron takes effect from the
-- next occurrence; already-scheduled fires inside the misfire window are
-- honoured." So `next_fire_at` is preserved on an unchanged expression and
-- recomputed by the caller only when the expression or zone actually moved —
-- which is why `p_next_fire_at` is applied conditionally rather than always.
-- Overwriting it on every registration would mean a deploy loop silently
-- postponing a schedule forever, one restart at a time.
CREATE OR REPLACE FUNCTION upsert_cron_schedule(
    p_ns             text,
    p_fn_id          text,
    p_trigger_idx    integer,
    p_expr           text,
    p_tz             text,
    p_catchup        text,
    p_catchup_limit  integer,
    p_misfire_window interval,
    p_singleton      boolean,
    p_run_key        text,
    p_next_fire_at   timestamptz
) RETURNS TABLE (id uuid, definition_changed boolean)
LANGUAGE plpgsql AS $$
DECLARE
    v_existing cron_schedules%ROWTYPE;
    v_id       uuid;
    v_changed  boolean;
BEGIN
    SELECT * INTO v_existing FROM cron_schedules
     WHERE ns = p_ns AND fn_id = p_fn_id AND trigger_idx = p_trigger_idx;

    IF NOT FOUND THEN
        v_id := gen_random_uuid();
        INSERT INTO cron_schedules (
            id, ns, fn_id, trigger_idx, expr, tz, catchup, catchup_limit,
            misfire_window, singleton, run_key, next_fire_at)
        VALUES (
            v_id, p_ns, p_fn_id, p_trigger_idx, p_expr, p_tz, p_catchup,
            p_catchup_limit, p_misfire_window, p_singleton, p_run_key,
            p_next_fire_at);
        RETURN QUERY SELECT v_id, true;
        RETURN;
    END IF;

    v_changed := v_existing.expr IS DISTINCT FROM p_expr
              OR v_existing.tz   IS DISTINCT FROM p_tz;

    UPDATE cron_schedules
       SET expr           = p_expr,
           tz             = p_tz,
           catchup        = p_catchup,
           catchup_limit  = p_catchup_limit,
           misfire_window = p_misfire_window,
           singleton      = p_singleton,
           run_key        = p_run_key,
           -- Only a changed definition moves the next fire.
           next_fire_at   = CASE WHEN v_changed THEN p_next_fire_at
                                 ELSE cron_schedules.next_fire_at END,
           updated_at     = now()
     WHERE cron_schedules.id = v_existing.id;

    RETURN QUERY SELECT v_existing.id, v_changed;
END $$;

-- Retire schedules a function no longer declares.
--
-- Called with the trigger indices the registration *did* declare; anything else
-- for that function goes. Without this, removing a cron trigger from a function
-- and redeploying leaves the old schedule firing, which is indistinguishable
-- from the deploy not having happened.
CREATE OR REPLACE FUNCTION prune_cron_schedules(
    p_ns    text,
    p_fn_id text,
    p_keep  integer[]
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_n integer;
BEGIN
    DELETE FROM cron_schedules
     WHERE ns = p_ns AND fn_id = p_fn_id
       AND NOT (trigger_idx = ANY (COALESCE(p_keep, ARRAY[]::integer[])));
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END $$;

-- ---------------------------------------------------------------- trimming

-- Trim the fire ledger.
--
-- Deleting a `cron_fires` row destroys the at-most-once guarantee for that
-- occurrence, so the cutoff is not a free choice: a row may only go once its
-- occurrence is old enough that no catch-up would ever fire it again. That is
-- `now() - misfire_window` at the earliest, and the retention limit is taken as
-- the later of the two rather than as a replacement for it. An operator who
-- sets retention to a day on a schedule with a week-long misfire window would
-- otherwise re-fire week-old occurrences after a restart, and the evidence of
-- why would have been deleted along with them.
CREATE OR REPLACE FUNCTION trim_cron_fires(p_max integer DEFAULT 10000)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_n    integer;
    v_days integer := COALESCE(engine_limit('cron_fire_retention_days'), 30);
BEGIN
    WITH doomed AS (
        SELECT f.schedule_id, f.occurrence_at
          FROM cron_fires f
          JOIN cron_schedules s ON s.id = f.schedule_id
         WHERE f.occurrence_at < now() - GREATEST(
                   s.misfire_window,
                   make_interval(days => v_days))
         LIMIT p_max
    )
    DELETE FROM cron_fires f
     USING doomed d
     WHERE f.schedule_id = d.schedule_id
       AND f.occurrence_at = d.occurrence_at;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END $$;

-- Pause a schedule that cannot be planned, recording why.
--
-- The alternative — log and carry on — is a hot loop: a due row is re-claimed
-- on every sweep until its `next_fire_at` moves, and a schedule that cannot be
-- planned is precisely one whose `next_fire_at` cannot be computed. Pausing
-- makes it stop and makes it visible; the counter makes it countable.
CREATE OR REPLACE FUNCTION pause_cron_schedule(p_id uuid, p_reason text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_ns text;
BEGIN
    UPDATE cron_schedules
       SET paused = true, last_error = p_reason, updated_at = now()
     WHERE id = p_id
    RETURNING ns INTO v_ns;
    IF v_ns IS NOT NULL THEN
        PERFORM bump_counter(v_ns, 'cron_unschedulable');
    END IF;
END $$;
