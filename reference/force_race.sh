#!/bin/bash
# Force the dangerous interleaving:
#   T1 (commit_ops): checks inbox -> EMPTY ... then stalls before inserting the wait
#   T2 (deliver):    inserts inbox entry, checks waits -> EMPTY (T1 uncommitted)
#   T1: inserts wait, suspends run
# Outcome if unprotected: event sits unconsumed, run parked forever = LOST SIGNAL.
export PGUSER=postgres
P="/usr/lib/postgresql/16/bin/psql -h localhost -p 5433 -d stepd -t -A -q"

RUN=$(python3 -c "import uuid;u=list(str(uuid.uuid4()));u[14]='7';print(''.join(u))")
$P -c "INSERT INTO runs (id,ns,fn_id,lineage_id) VALUES ('$RUN','prod','order-fulfilment','$RUN');
       INSERT INTO queue (ns,fn_id,run_id) VALUES ('prod','order-fulfilment','$RUN');" >/dev/null
$P -c "SELECT claim_runs('racer',1);" >/dev/null
FENCE=$($P -c "SELECT fence_token FROM runs WHERE id='$RUN';")

# T1: simulate commit_ops' wait branch, with a stall between the inbox check and the wait insert
(
$P <<SQL
BEGIN;
SELECT fence_token FROM runs WHERE id='$RUN' FOR UPDATE;
-- inbox check (finds nothing yet)
SELECT count(*) AS inbox_seen FROM run_inbox
 WHERE run_id='$RUN' AND event_type='order.approved' AND consumed_by_step_hash IS NULL;
SELECT pg_sleep(1.5);   -- <<< the window
INSERT INTO run_steps (run_id, step_hash, step_id, occurrence, op, status)
VALUES ('$RUN','9999000000000001','approval',0,'wait_event','pending');
INSERT INTO waits (run_id, ns, step_hash, event_type, since)
VALUES ('$RUN','prod','9999000000000001','order.approved', now());
UPDATE runs SET status='sleeping' WHERE id='$RUN';
COMMIT;
SQL
) &
T1=$!

sleep 0.6
# T2: deliver arrives inside T1's window
$P -c "SELECT deliver_to_inbox('$RUN'::uuid,'order.approved','{\"x\":1}'::jsonb) AS deliver_result;"
wait $T1

echo "--- outcome ---"
$P -c "SELECT
  (SELECT status::text FROM runs WHERE id='$RUN') AS run_status,
  (SELECT status::text FROM run_steps WHERE run_id='$RUN' AND step_hash='9999000000000001') AS step_status,
  (SELECT count(*) FROM waits WHERE run_id='$RUN' AND resolved_at IS NULL) AS waits_parked,
  (SELECT count(*) FROM run_inbox WHERE run_id='$RUN' AND consumed_by_step_hash IS NULL) AS inbox_unconsumed;"
