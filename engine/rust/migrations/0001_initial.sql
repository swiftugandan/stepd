-- stepd — schema 001 (initial)
--
-- Design rules enforced throughout:
--   * NO session-scoped advisory locks anywhere: everything is row-level
--     FOR UPDATE SKIP LOCKED or transaction-scoped, so the engine works behind a
--     transaction-mode pooler (pgbouncer). See PRD F-DL-1.
--   * The op commit is ONE transaction: step results + emitted events + inbox
--     consumption + next schedule. See PRD §7, protocol §7.1.
--   * Fencing prevents a stale attempt from mutating state (protocol §7.3).
--   * Every table is reached through exactly one component trait (PRD §6.3).
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------- enums

CREATE TYPE run_status AS ENUM (
    'pending', 'running', 'sleeping', 'waiting',
    'completed', 'failed', 'cancelled', 'quarantined'
);

CREATE TYPE step_status AS ENUM (
    'pending', 'completed', 'failed', 'timed_out', 'cancelled', 'unknown'
);

CREATE TYPE step_op AS ENUM (
    'step', 'sleep', 'wait_event', 'invoke', 'signal'
);

CREATE TYPE blob_state AS ENUM ('reserved', 'committed');

CREATE TYPE circuit_state AS ENUM ('closed', 'open', 'half_open');

-- ---------------------------------------------------------------- tenancy

CREATE TABLE namespaces (
    id              text PRIMARY KEY,
    retention_cfg   jsonb NOT NULL DEFAULT '{}'::jsonb,
    redaction_cfg   jsonb NOT NULL DEFAULT '{}'::jsonb,
    quota_cfg       jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Deployment binding, supplied by the environment at app start-up (F-CFG-4).
-- Signing keys stored hashed; plaintext lives only in the app's environment.
CREATE TABLE app_bindings (
    id                      uuid PRIMARY KEY,
    ns                      text NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    app_id                  text NOT NULL,
    url                     text NOT NULL,
    env                     text,
    key_hash_current        bytea NOT NULL,
    key_hash_previous       bytea,
    last_seen               timestamptz,
    last_manifest_checksum  text,
    UNIQUE (ns, app_id)
);

-- Code-defined function definitions. Contains NO urls, credentials or env names.
CREATE TABLE functions (
    id              uuid PRIMARY KEY,
    ns              text NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    app_binding_id  uuid NOT NULL REFERENCES app_bindings(id) ON DELETE CASCADE,
    fn_id           text NOT NULL,
    version         text NOT NULL,
    config          jsonb NOT NULL,
    paused          boolean NOT NULL DEFAULT false,
    archived_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ns, fn_id, version)
);
CREATE INDEX functions_active ON functions (ns, fn_id) WHERE archived_at IS NULL;

-- Namespace-scoped auth from v1 (F-SEC-1). Isolation is enforced by scoping every
-- query, not by filtering responses.
CREATE TABLE tokens (
    id          uuid PRIMARY KEY,
    ns          text NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    role        text NOT NULL CHECK (role IN ('viewer', 'operator', 'admin')),
    token_hash  bytea NOT NULL UNIQUE,
    name        text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz,
    revoked_at  timestamptz
);

-- ---------------------------------------------------------------- events

CREATE TABLE events (
    id           uuid NOT NULL,
    ns           text NOT NULL,
    type         text NOT NULL,
    source       text NOT NULL,
    time         timestamptz,
    key          text,
    idem         text,
    subject_key  text,
    data         jsonb NOT NULL DEFAULT '{}'::jsonb,
    received_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, received_at)
) PARTITION BY RANGE (received_at);

CREATE INDEX events_ns_type ON events (ns, type, received_at DESC);
CREATE INDEX events_ns_key  ON events (ns, key, received_at DESC) WHERE key IS NOT NULL;
CREATE UNIQUE INDEX events_idem ON events (ns, idem, received_at) WHERE idem IS NOT NULL;
CREATE INDEX events_subject ON events (ns, subject_key) WHERE subject_key IS NOT NULL;

-- ---------------------------------------------------------------- runs

CREATE TABLE runs (
    id                uuid PRIMARY KEY,
    ns                text NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    fn_id             text NOT NULL,
    fn_version        text,
    status            run_status NOT NULL DEFAULT 'pending',
    key               text,
    trigger_event_id  uuid,
    parent_run_id     uuid REFERENCES runs(id) ON DELETE SET NULL,
    parent_step_hash  text,
    detached          boolean NOT NULL DEFAULT false,
    lineage_id        uuid NOT NULL,
    chain_position    integer NOT NULL DEFAULT 0,
    subject_key       text,
    input             jsonb,
    output            jsonb,
    started_at        timestamptz NOT NULL DEFAULT now(),
    ended_at          timestamptz,
    deadline          timestamptz,
    attempt_no        integer NOT NULL DEFAULT 0,
    error             jsonb,
    error_signature   text,               -- powers poison-pill detection (F-LP-6)
    quarantined_at    timestamptz,
    restored_at       timestamptz,        -- marks runs rewound by PITR (F-DL-5)
    -- fencing: increments on every dispatch; a stale attempt cannot commit
    fence_token       bigint NOT NULL DEFAULT 0,
    lease_owner       text,
    lease_until       timestamptz
);

CREATE INDEX runs_ns_status   ON runs (ns, status, started_at DESC);
CREATE INDEX runs_ns_fn       ON runs (ns, fn_id, started_at DESC);
CREATE INDEX runs_key         ON runs (ns, fn_id, key) WHERE key IS NOT NULL;
CREATE INDEX runs_lineage     ON runs (lineage_id, chain_position);
CREATE INDEX runs_parent      ON runs (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE INDEX runs_subject     ON runs (ns, subject_key) WHERE subject_key IS NOT NULL;
CREATE INDEX runs_lease       ON runs (lease_until) WHERE lease_until IS NOT NULL;

-- At most one active run per (ns, fn_id, key) for singleton functions.
-- A partial unique index makes keyed exclusivity a database invariant rather
-- than something the application has to remember.
CREATE UNIQUE INDEX runs_singleton_key
    ON runs (ns, fn_id, key)
    WHERE key IS NOT NULL
      AND status IN ('pending', 'running', 'sleeping', 'waiting');

-- ---------------------------------------------------------------- steps

CREATE TABLE run_steps (
    run_id       uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_hash    text NOT NULL,
    step_id      text NOT NULL,
    occurrence   integer NOT NULL,
    op           step_op NOT NULL,
    status       step_status NOT NULL,
    result       jsonb,
    meta         jsonb,
    attempts     integer NOT NULL DEFAULT 1,
    started_at   timestamptz,
    ended_at     timestamptz,
    error        jsonb,
    PRIMARY KEY (run_id, step_hash)
);
CREATE INDEX run_steps_pending ON run_steps (run_id) WHERE status = 'pending';

-- ---------------------------------------------------------------- inbox

-- Durable per-run inbox. Closes the lost-signal race: an event directed at a run
-- is retained whether or not a wait is currently registered (protocol §7.6).
CREATE TABLE run_inbox (
    id                     bigserial PRIMARY KEY,
    run_id                 uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    received_at            timestamptz NOT NULL DEFAULT now(),
    event_type             text NOT NULL,
    event                  jsonb NOT NULL,
    consumed_by_step_hash  text,
    sender_run_id          uuid,
    sender_step_hash       text
);
-- FIFO scan for unconsumed entries of a given type
CREATE INDEX run_inbox_unconsumed
    ON run_inbox (run_id, event_type, id)
    WHERE consumed_by_step_hash IS NULL;
-- Sender dedupe: a retried signal op never double-delivers
CREATE UNIQUE INDEX run_inbox_sender_dedupe
    ON run_inbox (run_id, sender_run_id, sender_step_hash)
    WHERE sender_run_id IS NOT NULL;

-- ---------------------------------------------------------------- timers & waits

CREATE TABLE timers (
    id          bigserial PRIMARY KEY,
    run_id      uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_hash   text,
    kind        text NOT NULL,          -- sleep | wait_timeout | run_deadline | cron
    fire_at     timestamptz NOT NULL,
    fired_at    timestamptz
);
CREATE INDEX timers_due ON timers (fire_at) WHERE fired_at IS NULL;

CREATE TABLE waits (
    id           bigserial PRIMARY KEY,
    run_id       uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    ns           text NOT NULL,
    step_hash    text NOT NULL,
    event_type   text NOT NULL,
    expr         text,
    since        timestamptz NOT NULL,
    expires_at   timestamptz,
    prompt       jsonb,
    resolved_at  timestamptz,
    UNIQUE (run_id, step_hash)
);
CREATE INDEX waits_correlate ON waits (ns, event_type) WHERE resolved_at IS NULL;

-- ---------------------------------------------------------------- queue

CREATE TABLE queue (
    id            bigserial PRIMARY KEY,
    ns            text NOT NULL,
    fn_id         text NOT NULL,
    key           text,
    run_id        uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    priority      integer NOT NULL DEFAULT 0,
    available_at  timestamptz NOT NULL DEFAULT now(),
    claimed_by    text,
    claimed_until timestamptz,
    attempts      integer NOT NULL DEFAULT 0
);
-- The dispatch index. Ordering by (priority DESC, available_at) with SKIP LOCKED
-- is the whole claiming strategy; no advisory locks, so it is pooler-safe.
CREATE INDEX queue_dispatch
    ON queue (ns, available_at, priority DESC)
    WHERE claimed_by IS NULL;
CREATE INDEX queue_reclaim ON queue (claimed_until) WHERE claimed_by IS NOT NULL;
CREATE UNIQUE INDEX queue_one_per_run ON queue (run_id);

CREATE TABLE concurrency_slots (
    ns         text NOT NULL,
    scope_key  text NOT NULL,
    in_flight  integer NOT NULL DEFAULT 0,
    lim        integer NOT NULL,
    PRIMARY KEY (ns, scope_key)
);

CREATE TABLE rate_buckets (
    ns          text NOT NULL,
    scope_key   text NOT NULL,
    tokens      double precision NOT NULL,
    refilled_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ns, scope_key)
);

-- ---------------------------------------------------------------- outbox

CREATE TABLE outbox (
    id            bigserial PRIMARY KEY,
    run_id        uuid REFERENCES runs(id) ON DELETE CASCADE,
    ns            text NOT NULL,
    event         jsonb NOT NULL,
    published     boolean NOT NULL DEFAULT false,
    published_at  timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE NOT published;

-- ---------------------------------------------------------------- blobs

CREATE TABLE blobs (
    id            uuid PRIMARY KEY,
    ns            text NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    storage_url   text,
    size          bigint NOT NULL,
    sha256        bytea NOT NULL,
    content_type  text,
    filename      text,
    state         blob_state NOT NULL DEFAULT 'reserved',
    reserved_at   timestamptz NOT NULL DEFAULT now(),
    committed_at  timestamptz,
    -- content-addressed per namespace: dedupe within a tenant, never across
    UNIQUE (ns, sha256)
);
CREATE INDEX blobs_uncommitted ON blobs (reserved_at) WHERE state = 'reserved';

CREATE TABLE blob_refs (
    blob_id    uuid NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    run_id     uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_hash  text NOT NULL,
    PRIMARY KEY (blob_id, run_id, step_hash)
);
CREATE INDEX blob_refs_by_run ON blob_refs (run_id);

-- ---------------------------------------------------------------- health & audit

CREATE TABLE app_health (
    app_binding_id       uuid PRIMARY KEY REFERENCES app_bindings(id) ON DELETE CASCADE,
    circuit              circuit_state NOT NULL DEFAULT 'closed',
    consecutive_failures integer NOT NULL DEFAULT 0,
    opened_at            timestamptz,
    half_open_at         timestamptz,
    ramp_fraction        double precision NOT NULL DEFAULT 1.0
);

CREATE TABLE commands_audit (
    id          bigserial PRIMARY KEY,
    ns          text NOT NULL,
    actor       text NOT NULL,
    command     text NOT NULL,
    target      text,
    at          timestamptz NOT NULL DEFAULT now(),
    request_id  text
);
CREATE INDEX commands_audit_ns ON commands_audit (ns, at DESC);

-- Subject erasure (F-SEC-5)
CREATE TABLE subject_index (
    ns           text NOT NULL,
    subject_key  text NOT NULL,
    entity_kind  text NOT NULL,
    entity_id    text NOT NULL,
    PRIMARY KEY (ns, subject_key, entity_kind, entity_id)
);

CREATE TABLE erasures (
    id              uuid PRIMARY KEY,
    ns              text NOT NULL,
    subject_key     text NOT NULL,
    requested_by    text NOT NULL,
    requested_at    timestamptz NOT NULL DEFAULT now(),
    state           text NOT NULL DEFAULT 'pending',
    progress_cursor text,
    completed_at    timestamptz
);
