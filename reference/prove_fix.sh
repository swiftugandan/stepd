#!/bin/bash
export PGUSER=postgres
P="/usr/lib/postgresql/16/bin/psql -h localhost -p 5433 -d stepd -t -A -q"
echo "=== dropping the run_inbox FK, so ONLY the explicit lock can protect us ==="
$P -c "ALTER TABLE run_inbox DROP CONSTRAINT run_inbox_run_id_fkey;" 
echo
echo "=== re-running the forced interleaving ==="
FAILS=0
for i in 1 2 3 4 5; do
  RUN=$(python3 -c "import uuid;u=list(str(uuid.uuid4()));u[14]='7';print(''.join(u))")
  $P -c "INSERT INTO runs (id,ns,fn_id,lineage_id) VALUES ('$RUN','prod','order-fulfilment','$RUN');
         INSERT INTO queue (ns,fn_id,run_id) VALUES ('prod','order-fulfilment','$RUN');" >/dev/null
  $P -c "SELECT claim_runs('racer',1);" >/dev/null
  FENCE=$($P -c "SELECT fence_token FROM runs WHERE id='$RUN';")
  H=$(printf "%016x" $i)

  # T1: commit_ops registering a wait, with a stall in the middle of its transaction
  ( $P >/dev/null <<SQL
BEGIN;
SELECT fence_token FROM runs WHERE id='$RUN' FOR UPDATE;
SELECT count(*) FROM run_inbox WHERE run_id='$RUN' AND event_type='order.approved' AND consumed_by_step_hash IS NULL;
SELECT pg_sleep(1.2);
INSERT INTO run_steps (run_id,step_hash,step_id,occurrence,op,status)
VALUES ('$RUN','$H','approval',0,'wait_event','pending');
INSERT INTO waits (run_id,ns,step_hash,event_type,since)
VALUES ('$RUN','prod','$H','order.approved',now());
UPDATE runs SET status='sleeping' WHERE id='$RUN';
COMMIT;
SQL
  ) &
  T1=$!
  sleep 0.4
  RES=$($P -c "SELECT deliver_to_inbox('$RUN'::uuid,'order.approved','{\"n\":$i}'::jsonb);")
  wait $T1
  OUT=$($P -c "SELECT (SELECT status::text FROM run_steps WHERE run_id='$RUN' AND step_hash='$H')
                   ||'/'|| (SELECT count(*) FROM waits WHERE run_id='$RUN' AND resolved_at IS NULL)
                   ||'/'|| (SELECT count(*) FROM run_inbox WHERE run_id='$RUN' AND consumed_by_step_hash IS NULL);")
  STEP=$(echo $OUT|cut -d/ -f1); PARKED=$(echo $OUT|cut -d/ -f2); UNCONS=$(echo $OUT|cut -d/ -f3)
  if [ "$STEP" = "completed" ] && [ "$PARKED" = "0" ] && [ "$UNCONS" = "0" ]; then
     echo "  trial $i: PASS  (deliver=$RES, step=$STEP, parked=$PARKED, unconsumed=$UNCONS)"
  else
     echo "  trial $i: FAIL  LOST SIGNAL (deliver=$RES, step=$STEP, parked=$PARKED, unconsumed=$UNCONS)"
     FAILS=$((FAILS+1))
  fi
done
echo
echo "=== restoring the FK ==="
$P -c "ALTER TABLE run_inbox ADD CONSTRAINT run_inbox_run_id_fkey FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;"
[ $FAILS -eq 0 ] && echo "RESULT: explicit lock alone closes the race" || echo "RESULT: $FAILS FAILURES"
