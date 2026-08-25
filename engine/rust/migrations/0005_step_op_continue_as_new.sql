-- no-transaction
--
-- stepd — schema 005: `continue_as_new` becomes a first-class recorded op.
--
-- Separated into its own migration, and marked `-- no-transaction`, because
-- PostgreSQL will not let a value added to an enum be *used* in the same
-- transaction that adds it. Folding this into 006 would fail at apply time on a
-- fresh database and pass on an already-migrated one — the worst kind of
-- migration bug, since it only appears on the deployment that matters.

ALTER TYPE step_op ADD VALUE IF NOT EXISTS 'continue_as_new';
