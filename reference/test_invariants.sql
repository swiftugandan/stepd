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
