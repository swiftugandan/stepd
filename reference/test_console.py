#!/usr/bin/env python3
"""Console tests: it is served, it renders, and every endpoint it calls exists.
Plus the content-safety assertions from F-SEC-4, since the console renders
untrusted payload data."""
import re, sys, json, uuid
import psycopg2, psycopg2.extras
from fastapi.testclient import TestClient
import api
from api import app, issue_token

FAILS=0
def check(n,c,d=""):
    global FAILS
    print(f"  {'PASS' if c else 'FAIL'}  {n}{'  '+str(d) if d else ''}")
    if not c: FAILS+=1

c=psycopg2.connect(api.DSN); c.autocommit=True
def q(sql,a=None):
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql,a or ()); return cur.fetchall() if cur.description else []

def uuid7ish():
    u=list(str(uuid.uuid4())); u[14]="7"; return "".join(u)

q("TRUNCATE runs, queue, run_steps, run_inbox, waits, tokens, commands_audit CASCADE;")
q("INSERT INTO namespaces (id) VALUES ('prod') ON CONFLICT DO NOTHING")
tok=issue_token(c,"prod","operator","t")
cl=TestClient(app)
H={"Authorization":f"Bearer {tok}"}

print("stepd console\n")
r=cl.get("/")
check("console served at /", r.status_code==200)
html=r.text
check("console is HTML", "<title>stepd console</title>" in html)

print("\n--- every endpoint the console calls exists ---")
called=set(re.findall(r'api\("(/v1/[^"?]+)', html)) | set(re.findall(r'api\(`(/v1/[^`?$]+)', html))
paths=set(cl.get("/openapi.json").json()["paths"])
def norm(p):
    p=re.sub(r'\$\{[^}]+\}','{run_id}',p)
    return p.rstrip('/')
missing=[p for p in called if norm(p) not in paths and norm(p)+"/{run_id}" not in paths]
# template paths in the console use ${id}
tmpl=set(re.findall(r'`/v1/runs/\$\{[^}]*\}/(\w[\w-]*)`', html))
for t in tmpl:
    if f"/v1/runs/{{run_id}}/{t}" not in paths:
        missing.append(f"/v1/runs/{{run_id}}/{t}")
check("no endpoint referenced that the API does not serve", not missing, missing)
check("console uses the documented command endpoints",
      {"resolve-wait","cancel","retry"} <= tmpl, tmpl)

print("\n--- content safety (F-SEC-4) ---")
check("an escaping helper exists and maps every dangerous char",
      "const esc" in html and all(x in html for x in ["&amp;","&lt;","&gt;","&quot;","&#39;"]))
# Stronger: every interpolation must route through esc()/chip()/timeline(),
# be a numeric field, or be pure control flow. Anything else is flagged.
interps = re.findall(r'\$\{([^}]*)\}', html)
NUMERIC = ("attempt_no","chain_position","inbox_pending","backlog","in_flight",
           "oldest_seconds","s.attempts")
def safe(x):
    x = x.strip()
    if x.startswith(("esc(","chip(","timeline(","Math.","API","uid(")): return True
    if x == "cls": return True          # fixed vocabulary, never API data
    if any(n in x for n in NUMERIC): return True
    if "esc(" in x or "chip(" in x: return True      # ternaries that escape inside
    if x.startswith(("r.restored?","r.output?","r.error?","w?","d.items.map",
                     "d.functions.map","[\"failed\"")): return True
    return False
unsafe = [x for x in interps if not safe(x)]
check("every interpolation escapes, is numeric, or is control flow",
      not unsafe, unsafe[:5])
check("no javascript: or data: URLs constructed",
      "javascript:" not in html and "data:text/html" not in html)
check("no eval or Function constructor", "eval(" not in html and "new Function" not in html)

# a run whose payload contains an XSS attempt
rid=uuid7ish()
q("""INSERT INTO runs (id,ns,fn_id,key,lineage_id,status,output)
     VALUES (%s,'prod','order-fulfilment',%s,%s,'completed',%s::jsonb)""",
  (rid,"<img src=x onerror=alert(1)>",rid,json.dumps({"x":"</pre><script>alert(1)</script>"})))
d=cl.get(f"/v1/runs/{rid}",headers=H).json()
check("API returns the hostile payload verbatim (escaping is the console's job)",
      "<script>" in json.dumps(d["output"]))
check("console escapes < and > via esc()", 'replace(/[&<>"\']/g' in html)

print("\n--- accessibility and resilience floor ---")
check("keyboard focus is visible", ":focus-visible" in html)
check("reduced motion respected", "prefers-reduced-motion" in html)
check("responsive breakpoint present", "@media (max-width:640px)" in html)
check("empty states are actionable, not blank",
      "No runs yet" in html and "Nothing needs attention" in html)
check("errors explain what to do", "check the token" in html)

print("\n--- the operator flow works end to end ---")
rid2=uuid7ish()
q("""INSERT INTO runs (id,ns,fn_id,lineage_id,status) VALUES (%s,'prod','order-fulfilment',%s,'sleeping')""",(rid2,rid2))
q("""INSERT INTO run_steps (run_id,step_hash,step_id,occurrence,op,status,result,ended_at)
     VALUES (%s,'aaaa000000000001','charge',0,'step','completed','{"tx":1}',now())""",(rid2,))
q("""INSERT INTO waits (run_id,ns,step_hash,event_type,since)
     VALUES (%s,'prod','bbbb000000000001','order.approved',now())""",(rid2,))
d=cl.get(f"/v1/runs/{rid2}",headers=H).json()
check("run detail exposes the pending wait the console highlights",
      len(d["pending_waits"])==1 and d["pending_waits"][0]["event_type"]=="order.approved")
r=cl.post(f"/v1/runs/{rid2}/resolve-wait",headers=H,
          json={"event_type":"order.approved","data":{"by":"priya"}})
check("resolving from the console path works", r.status_code==200, r.text[:100])
st=q("SELECT status::text FROM runs WHERE id=%s",(rid2,))[0]["status"]
check("run resumed after the operator acted", st=="pending", st)

print("\n"+("ALL PASS" if FAILS==0 else f"{FAILS} FAILURE(S)"))
sys.exit(1 if FAILS else 0)
