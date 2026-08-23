-- Structural invariants. These catch a future edit that silently reopens an R1 race.
DO $$
DECLARE src text; v_n int;
BEGIN
    -- 1. deliver_to_inbox must take the run row lock, and take it FIRST.
    SELECT prosrc INTO src FROM pg_proc WHERE proname='deliver_to_inbox';
    IF position('FOR UPDATE' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  deliver_to_inbox no longer takes the run row lock (R1 race reopened)';
    END IF;
    IF position('FOR UPDATE' in src) > position('INSERT INTO run_inbox' in src) THEN
        RAISE EXCEPTION 'FAIL  deliver_to_inbox takes the lock AFTER inserting (R1 race reopened)';
    END IF;
    RAISE NOTICE 'PASS  deliver_to_inbox serialization point present and first';

    -- 2. commit_ops must take the same lock.
    SELECT prosrc INTO src FROM pg_proc WHERE proname='commit_ops';
    IF position('FOR UPDATE' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  commit_ops no longer takes the run row lock';
    END IF;
    RAISE NOTICE 'PASS  commit_ops takes the matching lock';

    -- 3. No advisory locks anywhere (pooler safety, F-DL-1).
    SELECT count(*) INTO v_n FROM pg_proc
     WHERE prosrc ILIKE '%pg_advisory%' AND pronamespace='public'::regnamespace;
    IF v_n > 0 THEN RAISE EXCEPTION 'FAIL  advisory lock found; breaks transaction-mode pooling'; END IF;
    RAISE NOTICE 'PASS  no advisory locks (pooler-safe)';

    -- 4. Keyed exclusivity must remain a database invariant, not app logic.
    IF NOT EXISTS (SELECT 1 FROM pg_indexes
                   WHERE indexname='runs_singleton_key' AND indexdef ILIKE '%UNIQUE%') THEN
        RAISE EXCEPTION 'FAIL  keyed exclusivity index missing';
    END IF;
    RAISE NOTICE 'PASS  keyed exclusivity enforced by unique index';

    -- 5. Inbox sender dedupe index present.
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='run_inbox_sender_dedupe') THEN
        RAISE EXCEPTION 'FAIL  sender dedupe index missing; retried signals would double-deliver';
    END IF;
    RAISE NOTICE 'PASS  sender dedupe index present';

    -- 6. Blob dedupe must be namespace-scoped, never global.
    IF NOT EXISTS (SELECT 1 FROM pg_indexes
                   WHERE tablename='blobs' AND indexdef ILIKE '%(ns, sha256)%') THEN
        RAISE EXCEPTION 'FAIL  blob dedupe not namespace-scoped; cross-tenant probing possible';
    END IF;
    RAISE NOTICE 'PASS  blob dedupe scoped per namespace';
END $$;


-- Source scanning helper. `prosrc` contains comments, and a comment that merely
-- *names* a function is not a call to it — an earlier version of check 11 failed
-- on the comment in commit_ops explaining which lock deliver_to_inbox takes.
-- Structural tests that cannot tell code from prose produce false failures, and a
-- test that cries wolf gets deleted.
CREATE OR REPLACE FUNCTION prosrc_code(p_name text) RETURNS text
LANGUAGE sql STABLE AS $fn$
    SELECT regexp_replace(prosrc, '--[^\n]*', '', 'g')
      FROM pg_proc WHERE proname = p_name AND pronamespace = 'public'::regnamespace
$fn$;

-- ---------------------------------------------------------------------------
-- Structural invariants for the ops added in migration 006.
--
-- These are the countermeasure to README finding (1): correctness that rests on
-- an undocumented accident. Each one fails the build if a future edit removes a
-- property that no behavioural test would notice going missing.
-- ---------------------------------------------------------------------------
DO $$
DECLARE src text; v_n int;
BEGIN
    -- 7. Exactly one commit_ops. An overload is either an ambiguity error or,
    --    worse, a silent dispatch to an older body that drops invoke, signal and
    --    continue_as_new on the floor without erroring.
    SELECT count(*) INTO v_n FROM pg_proc
     WHERE proname = 'commit_ops' AND pronamespace = 'public'::regnamespace;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'FAIL  % commit_ops overloads; a 3-arg call is ambiguous or silently old', v_n;
    END IF;
    RAISE NOTICE 'PASS  exactly one commit_ops (no shadowing overload)';

    -- 8. commit_ops must take the lock BEFORE it reads the fence. Reading the
    --    fence outside the lock makes the check advisory rather than binding.
    src := prosrc_code('commit_ops');
    IF position('FOR UPDATE' in src) > position('fence_token <> p_fence' in src) THEN
        RAISE EXCEPTION 'FAIL  commit_ops checks the fence outside the row lock';
    END IF;
    RAISE NOTICE 'PASS  commit_ops fence check is under the lock';

    -- 9. commit_ops must handle every op the protocol defines. A missing branch
    --    is silent data loss: the op is accepted, acknowledged and discarded.
    FOREACH src IN ARRAY ARRAY['step','sleep','wait_event','invoke','signal',
                               'continue_as_new','done','error'] LOOP
        IF position(format('v_kind = %L', src) in prosrc_code('commit_ops')) = 0 THEN
            RAISE EXCEPTION 'FAIL  commit_ops has no branch for op %', src;
        END IF;
    END LOOP;
    RAISE NOTICE 'PASS  commit_ops handles every protocol op';

    -- 10. An unknown op must fail the run, never fall through silently.
    src := prosrc_code('commit_ops');
    IF position('unknown_op' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  commit_ops silently ignores unknown ops (protocol §11)';
    END IF;
    RAISE NOTICE 'PASS  unknown ops fail loudly';

    -- 11. Signals must not be delivered inline. Inline delivery takes a second
    --     run row lock while holding the sender's, so two runs signalling each
    --     other deadlock. The relay table is what removes the class of bug.
    src := prosrc_code('commit_ops');
    IF position('signal_outbox' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  commit_ops no longer relays signals; lock-ordering deadlock reintroduced';
    END IF;
    IF position('deliver_to_inbox' in src) > 0 THEN
        RAISE EXCEPTION 'FAIL  commit_ops calls deliver_to_inbox inline while holding its own run lock';
    END IF;
    RAISE NOTICE 'PASS  signals relayed, not delivered under a held lock';

    -- 12. resolve_child_result must lock the PARENT row: a child finishing while
    --     the parent is mid-commit must not interleave with the parent's ops.
    src := prosrc_code('resolve_child_result');
    IF position('FOR UPDATE' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  resolve_child_result does not lock the parent run row';
    END IF;
    RAISE NOTICE 'PASS  child resolution serialises against the parent';

    -- 13. Cascade must be one statement. A per-level loop that commits as it goes
    --     can half-finish and orphan descendants; the protocol requires the
    --     cascade itself to be durable and resumable (§7.5).
    src := prosrc_code('cascade_cancel');
    IF position('RECURSIVE' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  cascade_cancel is no longer a single recursive statement';
    END IF;
    IF position('NOT detached' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  cascade_cancel no longer excludes detached children';
    END IF;
    RAISE NOTICE 'PASS  cascade is one recursive statement and spares detached children';

    -- 14. The inbox bound must count what it drops. An overflow that leaves no
    --     trace destroys the only evidence that it happened (protocol §7.6).
    src := prosrc_code('trim_inbox');
    IF position('bump_counter' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  inbox overflow is silent; no counter is incremented';
    END IF;
    RAISE NOTICE 'PASS  inbox overflow leaves evidence';

    -- 15. Dispatch claiming must be namespace-scoped. A namespace-blind claim
    --     lets one tenant's backlog starve every other, whatever the caller does.
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'claim_runs_ns') THEN
        RAISE EXCEPTION 'FAIL  claim_runs_ns missing; fair dispatch is not expressible';
    END IF;
    src := prosrc_code('claim_runs_ns');
    IF position('SKIP LOCKED' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  claim_runs_ns no longer uses SKIP LOCKED';
    END IF;
    IF position('q.ns = p_ns' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  claim_runs_ns no longer filters by namespace';
    END IF;
    RAISE NOTICE 'PASS  dispatch claiming is namespace-scoped and pooler-safe';

    -- 16. A wait with a timeout must schedule a timer. Without one the timeout
    --     field is accepted and ignored, and the run waits forever.
    src := prosrc_code('commit_ops');
    IF position('wait_timeout' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  wait_event no longer schedules a timeout timer';
    END IF;
    RAISE NOTICE 'PASS  wait timeouts are actually scheduled';
END $$;


-- ---------------------------------------------------------------- cron (009)
--
-- The cron scheduler's guarantees rest on three structural properties. Each is
-- the kind that a plausible refactor removes without failing any behavioural
-- test — the exact shape of finding (1) in the README, where a race was closed
-- only by an incidental row lock and nothing said so.
DO $$
DECLARE src text;
BEGIN
    -- 17. The ledger insert must be the FIRST thing fire_cron_occurrence does.
    --     It is the at-most-once guarantee. Any statement that creates a run or
    --     a queue entry before it opens a window in which two replicas both act
    --     on one occurrence and only afterwards discover the conflict.
    src := prosrc_code('fire_cron_occurrence');
    IF position('INSERT INTO cron_fires' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  fire_cron_occurrence no longer records the occurrence';
    END IF;
    IF position('INSERT INTO runs' in src) > 0
       AND position('INSERT INTO cron_fires' in src) > position('INSERT INTO runs' in src) THEN
        RAISE EXCEPTION 'FAIL  fire_cron_occurrence creates a run BEFORE claiming the occurrence (double-fire reopened)';
    END IF;
    IF position('ON CONFLICT' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  fire_cron_occurrence no longer tolerates a lost race; a duplicate would raise instead of no-op';
    END IF;
    RAISE NOTICE 'PASS  the cron occurrence is claimed before anything is created';

    -- 18. The at-most-once claim must be a key, not a query. A uniqueness check
    --     written as SELECT-then-INSERT is a check-then-act race between two
    --     schedulers, and it passes every single-process test.
    IF NOT EXISTS (
        SELECT 1 FROM pg_index i
          JOIN pg_class c ON c.oid = i.indrelid
         WHERE c.relname = 'cron_fires' AND i.indisprimary
           AND (SELECT count(*) FROM unnest(i.indkey)) = 2
    ) THEN
        RAISE EXCEPTION 'FAIL  cron_fires no longer has a two-column primary key; at-most-once is not enforced by the database';
    END IF;
    RAISE NOTICE 'PASS  at-most-once firing is a primary key, not application care';

    -- 19. Cron claiming must be pooler-safe and must hand back database time.
    --     A scheduler is the component most tempted to elect a leader with an
    --     advisory lock (caught globally by check 3) and most tempted to compute
    --     fire times from its own clock, which on a skewed replica fires the
    --     whole fleet early (F-DL-8).
    src := prosrc_code('claim_due_cron');
    IF position('SKIP LOCKED' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  claim_due_cron no longer uses SKIP LOCKED; two schedulers would block rather than share';
    END IF;
    IF position('now()' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  claim_due_cron no longer returns database time; a replica clock could set fire times';
    END IF;
    -- Namespace scoping is a fairness requirement, exactly as it is for dispatch
    -- (check 15). A blind claim ordered by `next_fire_at` is won by whoever is
    -- furthest behind, so one tenant's backlog silently stops every other
    -- tenant's schedules — nothing fails and no queue grows anywhere visible.
    IF position('s.ns = p_ns' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  claim_due_cron no longer filters by namespace; one busy tenant can starve the rest';
    END IF;
    IF (SELECT count(*) FROM pg_proc WHERE proname = 'claim_due_cron'
         AND pronamespace = 'public'::regnamespace) <> 1 THEN
        RAISE EXCEPTION 'FAIL  claim_due_cron is an overload set; a call can resolve to the wrong one';
    END IF;
    RAISE NOTICE 'PASS  cron claiming is namespace-scoped, pooler-safe and database-timed';

    -- 20. Trimming the ledger must respect the misfire window. Deleting a row
    --     inside it makes its occurrence eligible to fire a second time, and
    --     deletes the evidence of the first.
    src := prosrc_code('trim_cron_fires');
    IF position('misfire_window' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  trim_cron_fires no longer floors its cutoff at the misfire window (double-fire after restart)';
    END IF;
    RAISE NOTICE 'PASS  ledger trimming cannot delete inside the misfire window';
END $$;


-- ---------------------------------------------------------------- compensation (010)
DO $$
DECLARE src text;
BEGIN
    -- 21. Cancelling a run that declares `on_cancel` must leave it queued.
    --
    -- The defect this guards is the one the conformance suite found: `cancel_run`
    -- deleted the queue row unconditionally, so the compensation attempt §7.4
    -- promises was never dispatched and `run.cancelling` could never be true.
    -- Nothing errored — the refund simply did not happen, and the run looked
    -- exactly as it does when it worked.
    src := prosrc_code('cancel_run');
    IF position('on_cancel' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  cancel_run no longer consults on_cancel; compensation is unreachable again';
    END IF;
    IF position('INSERT INTO queue' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  cancel_run no longer queues the compensation attempt; the path will silently never run';
    END IF;
    RAISE NOTICE 'PASS  cancellation schedules the compensation attempt';

    -- 22. The trigger that ends a compensating run as cancelled must exist.
    --
    -- It is invisible control flow, which is why it is asserted here rather than
    -- trusted. Without it a compensation path returning `done` marks the run
    -- `completed` — a run that was cancelled reporting success.
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgname = 'runs_compensation_ends_cancelled' AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'FAIL  the compensation terminal-state trigger is gone; a cancelled run can report completed';
    END IF;
    src := prosrc_code('compensation_ends_cancelled');
    IF position('cancelled' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  the compensation trigger no longer forces the cancelled terminal state';
    END IF;
    RAISE NOTICE 'PASS  a compensating run ends as cancelled whatever its path returned';

    -- 23. Keyed exclusivity must cover the compensation phase.
    --
    -- A run still undoing its work holds its key. Without this a cancel racing
    -- an event starts the next run while the previous one is still releasing
    -- what it reserved.
    -- The mechanism is indirect and therefore easy to break by accident:
    -- `cancel_run` returns the run to `pending`, which is one of the statuses
    -- `runs_singleton_key` covers, so the key stays held for free. Setting any
    -- status outside that set releases the key while the run is still releasing
    -- what it reserved, and the next run starts on top of it — a failure that
    -- would appear only when a cancel raced an event.
    src := prosrc_code('cancel_run');
    IF position('status = ''pending''' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  cancel_run no longer returns a compensating run to pending; it leaves the keyed-exclusivity index and another run can start on its key';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
         WHERE indexname = 'runs_singleton_key' AND indexdef ILIKE '%pending%'
    ) THEN
        RAISE EXCEPTION 'FAIL  keyed exclusivity no longer covers pending runs';
    END IF;
    RAISE NOTICE 'PASS  a compensating run keeps its business key';
END $$;


-- ---------------------------------------------------------------- blob refs (011)
DO $$
DECLARE src text;
BEGIN
    -- 24. A recorded blob reference must be tracked, in the same transaction.
    --
    -- The collector deletes committed blobs with no references. Before this
    -- trigger existed nothing wrote `blob_refs` at all, so the bytes behind
    -- every `$blob` in a live run's journal were collectable from the moment
    -- they were committed — and the run would fail on its next replay with a
    -- missing object, hours after the collection that caused it.
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgname = 'run_steps_record_blob_refs' AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'FAIL  step blob references are no longer recorded; the collector will delete bytes live runs need';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgname = 'runs_record_blob_refs' AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'FAIL  run input/output blob references are no longer recorded';
    END IF;
    RAISE NOTICE 'PASS  blob references are recorded with the row that carries them';

    -- 25. The walk must be recursive.
    --
    -- A step result is arbitrary JSON and a blob can be nested anywhere in it.
    -- A top-level-only walk passes every test written with a flat payload and
    -- loses data on the first realistic one.
    src := prosrc_code('blob_ids_in');
    IF position('RECURSIVE' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  blob_ids_in is no longer recursive; nested references will be missed and their bytes collected';
    END IF;
    RAISE NOTICE 'PASS  the blob reference walk descends into arrays and objects';
END $$;


-- ---------------------------------------------------------------- outcomes (012, 013)
DO $$
DECLARE src text; v_n int;
BEGIN
    -- 26. A step op carrying an error must be recorded as failed.
    --
    -- The `step` branch hardcoded `'completed'` for every op it saw, so a step
    -- whose body raised had no journal row at all — and in a parallel group the
    -- whole envelope was discarded with the error, taking the siblings' results
    -- that had already been produced. Reverting this line makes the engine
    -- silently forget executed work again, which is the failure this project
    -- exists to prevent (ADR-023).
    src := prosrc_code('commit_ops');
    IF position('''failed''::step_status' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  commit_ops no longer records a failed step outcome; a step whose body raised leaves no trace in the journal';
    END IF;
    IF position('v_op->''error''' in src) = 0 THEN
        RAISE EXCEPTION 'FAIL  commit_ops no longer reads the error off a step op; the failure is recorded as a success';
    END IF;
    RAISE NOTICE 'PASS  a failed step body is recorded as a failed step';

    -- 27. No function may take a join-policy parameter again.
    --
    -- `commit_ops` accepted `p_join` and never read it, so every batch behaved
    -- as `all_settled` whatever an app asked for. ADR-023 removed the policies
    -- rather than implement them; this fails the build if one is reintroduced
    -- as a parameter without the behaviour behind it.
    SELECT count(*) INTO v_n
      FROM pg_proc p, unnest(coalesce(p.proargnames, '{}')) AS a(name)
     WHERE p.pronamespace = 'public'::regnamespace
       AND a.name = 'p_join';
    IF v_n > 0 THEN
        RAISE EXCEPTION 'FAIL  a join-policy parameter is back; it was removed because it was accepted and ignored (ADR-023)';
    END IF;
    RAISE NOTICE 'PASS  no join-policy parameter survives';
END $$;
