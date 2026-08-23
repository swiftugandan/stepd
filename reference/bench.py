#!/usr/bin/env python3
"""Throughput of the commit path — the PRD's ≥1000 step commits/s target (NFR §5).
Measures commit_ops in isolation, then the full dispatch loop, to separate
database cost from dispatcher overhead."""
import json, time, uuid, psycopg2, psycopg2.extras
from dispatcher import uuid7ish

DSN="host=localhost port=5433 dbname=stepd user=postgres"
c=psycopg2.connect(DSN); c.autocommit=True
def q(sql,a=None,f=True):
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql,a or ()); return cur.fetchall() if f and cur.description else None

q("TRUNCATE runs, queue, run_steps, run_inbox, waits, timers, outbox CASCADE;",f=False)

N=1500
ids=[uuid7ish() for _ in range(N)]
q("INSERT INTO runs (id,ns,fn_id,lineage_id) SELECT x,'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)
q("INSERT INTO queue (ns,fn_id,run_id) SELECT 'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)

# --- 1. commit_ops in isolation (one step per run)
q("SELECT claim_runs('bench',%s)",(N,),f=False)
ops=json.dumps([{"op":"step","id":"s","hash":"0000000000000001","data":{"v":1}}])
t0=time.time()
for i in ids:
    q("SELECT commit_ops(%s::uuid,1,%s::jsonb)",(i,ops),f=False)
d=time.time()-t0
print(f"commit_ops (sequential, 1 conn): {N/d:8.0f} commits/s   [{d:.2f}s for {N}]")

# --- 2. batched in one transaction (what a real dispatcher does per tick)
q("TRUNCATE runs, queue, run_steps CASCADE;",f=False)
ids=[uuid7ish() for _ in range(N)]
q("INSERT INTO runs (id,ns,fn_id,lineage_id) SELECT x,'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)
q("INSERT INTO queue (ns,fn_id,run_id) SELECT 'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)
q("SELECT claim_runs('bench',%s)",(N,),f=False)
c.autocommit=False
t0=time.time()
with c.cursor() as cur:
    for i in ids:
        cur.execute("SELECT commit_ops(%s::uuid,1,%s::jsonb)",(i,ops))
c.commit()
d=time.time()-t0
print(f"commit_ops (single transaction): {N/d:8.0f} commits/s   [{d:.2f}s for {N}]")
c.autocommit=True

# --- 3. claim throughput
q("TRUNCATE runs, queue, run_steps CASCADE;",f=False)
ids=[uuid7ish() for _ in range(N)]
q("INSERT INTO runs (id,ns,fn_id,lineage_id) SELECT x,'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)
q("INSERT INTO queue (ns,fn_id,run_id) SELECT 'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)
t0=time.time(); n=0
while True:
    r=q("SELECT run_id FROM claim_runs('bench',100)")
    if not r: break
    n+=len(r)
d=time.time()-t0
print(f"claim_runs (batches of 100):     {n/d:8.0f} claims/s    [{d:.2f}s for {n}]")

# --- 4. inbox delivery (the early-signal path, with its explicit lock)
q("TRUNCATE runs, queue, run_steps, run_inbox CASCADE;",f=False)
ids=[uuid7ish() for _ in range(500)]
q("INSERT INTO runs (id,ns,fn_id,lineage_id) SELECT x,'prod','order-fulfilment',x FROM unnest(%s::uuid[]) x",(ids,),f=False)
t0=time.time()
for i in ids:
    q("SELECT deliver_to_inbox(%s::uuid,'e','{}'::jsonb)",(i,),f=False)
d=time.time()-t0
print(f"deliver_to_inbox:                {len(ids)/d:8.0f} deliveries/s [{d:.2f}s for {len(ids)}]")

print("\nnote: single connection, unoptimised container, no pipelining.")
print("Real deployments batch commits and use a connection pool.")
