-- stepd — schema 011: make a blob reference a database invariant.
--
-- WHY THIS MIGRATION EXISTS
--
-- Protocol §8.3.4: "The server maintains a reference from every step result, run
-- input and emitted event that contains a `$blob`." `blob_refs` existed,
-- `BlobStore::add_ref` existed, `blob_ids` existed to walk a value and find
-- them — and nothing called any of it.
--
-- The collector deletes committed blobs with no references. So the bytes behind
-- a `$blob` in a live run's journal were eligible for deletion from the moment
-- they were committed, and the run would fail on its next replay with a missing
-- object. Nothing would connect the failure to a collection that ran hours
-- earlier.
--
-- WHY A TRIGGER RATHER THAN A CALL IN `commit_ops`
--
-- Atomicity. The reference has to be recorded in the *same transaction* as the
-- step that carries it: any gap is a window in which a crash leaves a referenced
-- blob unreferenced, and a collector cannot tell that apart from garbage.
--
-- A trigger also covers every path that records a step rather than the one path
-- somebody remembered — including paths that do not exist yet. That is the
-- difference between "the commit function is careful" and "a recorded blob
-- reference is always tracked", and only the second survives the next feature.
--
-- The cost is invisible control flow, which is why structural invariant 24
-- asserts the trigger exists and `test_engine_ops.sql` asserts what it does.

-- Every `$blob` id anywhere in a JSON value.
--
-- Recursive because a step result is arbitrary JSON and a blob reference can be
-- nested anywhere in it — inside an array, inside an object, inside another
-- blob's sibling. A walk that only checked the top level would silently miss
-- every real payload shape.
CREATE OR REPLACE FUNCTION blob_ids_in(p_value jsonb)
RETURNS SETOF uuid LANGUAGE sql IMMUTABLE AS $$
    WITH RECURSIVE walk(node) AS (
        SELECT p_value
        UNION ALL
        SELECT child
          FROM walk,
               LATERAL (
                   SELECT value AS child FROM jsonb_array_elements(walk.node)
                    WHERE jsonb_typeof(walk.node) = 'array'
                   UNION ALL
                   SELECT value AS child FROM jsonb_each(walk.node)
                    WHERE jsonb_typeof(walk.node) = 'object'
               ) c
    )
    SELECT (node -> '$blob' ->> 'id')::uuid
      FROM walk
     WHERE jsonb_typeof(node) = 'object'
       AND node ? '$blob'
       AND (node -> '$blob' ->> 'id') IS NOT NULL
$$;

-- Record references carried by a recorded step.
--
-- `ON CONFLICT DO NOTHING` because a step is re-committed idempotently and a
-- reference recorded twice is the same reference. The blob must already exist:
-- a reference to an id that was never reserved is an app inventing one, and the
-- foreign key refusing it is better than a dangling row the collector will
-- puzzle over.
CREATE OR REPLACE FUNCTION record_step_blob_refs() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.result IS NOT NULL THEN
        INSERT INTO blob_refs (blob_id, run_id, step_hash)
        SELECT b, NEW.run_id, NEW.step_hash
          FROM blob_ids_in(NEW.result) b
         WHERE EXISTS (SELECT 1 FROM blobs WHERE id = b)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS run_steps_record_blob_refs ON run_steps;
CREATE TRIGGER run_steps_record_blob_refs
    AFTER INSERT OR UPDATE OF result ON run_steps
    FOR EACH ROW
    EXECUTE FUNCTION record_step_blob_refs();

-- The same for a run's input and output.
--
-- A run's input carries blobs when it was invoked with one, and its output when
-- it returned one. Both outlive every step, and both are read by the console and
-- by a parent resolving a child's result — so a blob referenced only there is
-- exactly as live as one in a journal, and exactly as collectable without this.
--
-- `step_hash` is not nullable, so these use sentinel hashes rather than a schema
-- change. They are outside the 16-hex-character step-hash space, so nothing can
-- collide with a real step and a human reading the table can see what they are.
CREATE OR REPLACE FUNCTION record_run_blob_refs() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.input IS NOT NULL THEN
        INSERT INTO blob_refs (blob_id, run_id, step_hash)
        SELECT b, NEW.id, ':input'
          FROM blob_ids_in(NEW.input) b
         WHERE EXISTS (SELECT 1 FROM blobs WHERE id = b)
        ON CONFLICT DO NOTHING;
    END IF;
    IF NEW.output IS NOT NULL THEN
        INSERT INTO blob_refs (blob_id, run_id, step_hash)
        SELECT b, NEW.id, ':output'
          FROM blob_ids_in(NEW.output) b
         WHERE EXISTS (SELECT 1 FROM blobs WHERE id = b)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS runs_record_blob_refs ON runs;
CREATE TRIGGER runs_record_blob_refs
    AFTER INSERT OR UPDATE OF input, output ON runs
    FOR EACH ROW
    EXECUTE FUNCTION record_run_blob_refs();

-- Backfill anything already recorded.
--
-- A database migrated before this point has live runs whose blobs the collector
-- would take. Doing this here rather than leaving it to an operator means the
-- window closes when the migration runs, not when somebody reads the release
-- note.
INSERT INTO blob_refs (blob_id, run_id, step_hash)
SELECT b, s.run_id, s.step_hash
  FROM run_steps s, LATERAL blob_ids_in(s.result) b
 WHERE s.result IS NOT NULL AND EXISTS (SELECT 1 FROM blobs WHERE id = b)
ON CONFLICT DO NOTHING;

INSERT INTO blob_refs (blob_id, run_id, step_hash)
SELECT b, r.id, ':input'
  FROM runs r, LATERAL blob_ids_in(r.input) b
 WHERE r.input IS NOT NULL AND EXISTS (SELECT 1 FROM blobs WHERE id = b)
ON CONFLICT DO NOTHING;

INSERT INTO blob_refs (blob_id, run_id, step_hash)
SELECT b, r.id, ':output'
  FROM runs r, LATERAL blob_ids_in(r.output) b
 WHERE r.output IS NOT NULL AND EXISTS (SELECT 1 FROM blobs WHERE id = b)
ON CONFLICT DO NOTHING;
