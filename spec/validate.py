#!/usr/bin/env python3
"""Validate stepd example payloads and negative cases against the JSON Schemas."""
import json, pathlib, sys
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

HERE = pathlib.Path(__file__).parent
SCHEMAS = HERE / "schemas"
EXAMPLES = HERE / "examples"

# Build a registry so relative $refs like "common.schema.json#/$defs/x" resolve.
registry = Registry()
for p in SCHEMAS.glob("*.schema.json"):
    doc = json.loads(p.read_text())
    res = Resource.from_contents(doc)
    registry = registry.with_resource(uri=p.name, resource=res)
    registry = registry.with_resource(uri=doc["$id"], resource=res)

def validator(name):
    """Accepts 'file.schema.json' or 'file.schema.json#/$defs/pointer'."""
    if "#" in name:
        file, ptr = name.split("#", 1)
        doc = {"$ref": name, "$defs": json.loads((SCHEMAS / file).read_text()).get("$defs", {})}
        doc = {"$ref": name}
    else:
        doc = json.loads((SCHEMAS / name).read_text())
    return Draft202012Validator(doc, registry=registry)

CASES = [
    ("app-manifest.schema.json", "app-manifest.example.json"),
    ("function-config.schema.json", "function-config-cron.example.json"),
    ("conformance-manifest.schema.json", "conformance-manifest.example.json"),
    ("attempt-request.schema.json", "attempt-request.example.json"),
    ("attempt-response.schema.json", "attempt-response-parallel.example.json"),
    ("attempt-response.schema.json", "attempt-response-wait.example.json"),
    ("attempt-response.schema.json", "attempt-response-done.example.json"),
    ("attempt-response.schema.json", "attempt-response-error.example.json"),
    ("attempt-response.schema.json", "attempt-response-media.example.json"),
    ("blob-reserve.schema.json#/$defs/request", "blob-reserve-request.example.json"),
    ("blob-reserve.schema.json#/$defs/response", "blob-reserve-response.example.json"),
    ("blob-reserve.schema.json#/$defs/response", "blob-reserve-response-dedup.example.json"),
    ("attempt-response.schema.json", "attempt-response-continue.example.json"),
    ("attempt-response.schema.json", "attempt-response-batch.example.json"),
    ("attempt-response.schema.json", "attempt-response-partial-failure.example.json"),
]

NEGATIVE = [
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55"},
                               {"op": "done"}]},
     "done must appear alone"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "SHORT"}]},
     "hash must be 16 lowercase hex chars"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "teleport", "id": "a", "hash": "3f2a91c4b70e1d55"}]},
     "unknown op type rejected"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "sleep", "id": "s", "hash": "3f2a91c4b70e1d55"}]},
     "sleep needs until or duration"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "sleep", "id": "s", "hash": "3f2a91c4b70e1d55",
                                "until": "2026-01-01T00:00:00Z", "duration": "P1D"}]},
     "sleep cannot have both until and duration"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": []},
     "at least one op required"),
    ("attempt-response.schema.json",
     {"protocol": "2", "ops": [{"op": "done"}]},
     "protocol must be 1"),
    ("function-config.schema.json",
     {"id": "Order Fulfilment", "triggers": [{"type": "event", "event": "x"}]},
     "identifier must be lowercase, no spaces"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "cron"}]},
     "cron trigger needs a cron field"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "cron", "cron": "0 3 * * *", "singleton": True}]},
     "singleton cron needs a run_key to be exclusive on"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "cron", "cron": "0 3 * * *", "catchup": "sometimes"}]},
     "catchup must be one of the three defined policies"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "cron", "cron": "0 3 * * *", "catchup_limit": 0}]},
     "catchup_limit of zero would mean a schedule that never fires"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "cron", "cron": "0 3 * * *",
                                "misfire_window": "1 hour"}]},
     "misfire_window must be an ISO 8601 duration"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "event", "event": "x"}],
      "retries": {"initial": "10 seconds"}},
     "duration must be ISO 8601"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "event", "event": "x"}], "on_cancel": "yes"},
     "on_cancel is a boolean declaration, not a handler name"),
    ("conformance-manifest.schema.json",
     {"protocol": "1", "suites": ["determinism"],
      "statically_prevented": ["gravity"]},
     "an unrecognised statically-prevented hazard is rejected, never ignored"),
    ("conformance-manifest.schema.json",
     {"protocol": "1", "suites": ["memoization", "teleportation"]},
     "an unknown conformance suite is rejected rather than ignored"),
    ("conformance-manifest.schema.json",
     {"protocol": "1", "suites": ["memoization", "memoization"]},
     "a suite declared twice is a mistake, not an emphasis"),
    ("conformance-manifest.schema.json",
     {"protocol": "1"},
     "a conformance manifest must say which suites it claims"),
    ("event.schema.json",
     {"specversion": "1.0", "id": "1", "source": "/a", "type": "t",
      "data": {}, "data_base64": "aGk="},
     "data and data_base64 are mutually exclusive"),
    ("event.schema.json",
     {"specversion": "0.3", "id": "1", "source": "/a", "type": "t"},
     "specversion must be 1.0"),
    ("attempt-request.schema.json",
     {"protocol": "1", "attempt": 1, "fence": "f",
      "run": {"id": "not-a-uuid", "function_id": "f", "namespace": "prod",
              "started_at": "2026-01-01T00:00:00Z"},
      "steps": {}},
     "run id must be UUIDv7"),
    ("attempt-request.schema.json",
     {"protocol": "1", "attempt": 0, "fence": "f",
      "run": {"id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10", "function_id": "f",
              "namespace": "prod", "started_at": "2026-01-01T00:00:00Z"},
      "steps": {}},
     "attempt starts at 1"),
    ("attempt-request.schema.json",
     {"protocol": "1", "attempt": 1, "fence": "f",
      "run": {"id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10", "function_id": "f",
              "namespace": "prod", "started_at": "2026-01-01T00:00:00Z"},
      "steps": {"BADHASH": {"id": "a", "op": "step", "status": "completed"}}},
     "step map keys must be valid hashes"),
    ("blob-reserve.schema.json#/$defs/request",
     {"run_id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10", "size": 1,
      "sha256": "TOOSHORT"},
     "reserve requires a full sha256 digest"),
    ("blob-reserve.schema.json#/$defs/request",
     {"run_id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10", "size": 0,
      "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
     "reserve size must be at least 1 byte"),
    ("blob-reserve.schema.json#/$defs/response",
     {"blob_id": "01926f5a-2200-7aaa-8000-0123456789ab", "deduplicated": False},
     "non-deduplicated reserve must return an upload_url"),
    ("blob-reserve.schema.json#/$defs/response",
     {"blob_id": "01926f5a-2200-7aaa-8000-0123456789ab", "deduplicated": True,
      "upload_url": "https://x/y", "method": "PUT",
      "expires_at": "2026-01-01T00:00:00Z"},
     "deduplicated reserve must not return an upload_url"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55",
       "data": {"$ref": {"size": 10}}}]},
     "$ref requires a uri"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55",
       "data": {"$blob": {"id": "01926f5a-2200-7aaa-8000-0123456789ab", "size": 1}}}]},
     "$blob requires a sha256"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55",
       "data": {"$blob": {"id": "01926f5a-2200-7aaa-8000-0123456789ab", "size": 1,
                          "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                "$ref": {"uri": "s3://x/y"}}}]},
     "a value cannot be both $blob and $ref"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "continue_as_new", "id": "n", "hash": "3f2a91c4b70e1d55"},
                               {"op": "step", "id": "a", "hash": "aabbccddeeff0011"}]},
     "continue_as_new must appear alone"),
    ("attempt-response.schema.json",
     {"protocol": "1", "join": "all",
      "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55"}]},
     "join policies are gone; the field is refused rather than ignored (ADR-023)"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55"},
                               {"op": "error", "retryable": True,
                                "error": {"code": "gateway_down", "message": "503"}}]},
     "a retryable error cannot travel with recorded ops (5.2.2)"),
    ("attempt-response.schema.json",
     {"protocol": "1", "ops": [{"op": "step", "id": "a", "hash": "3f2a91c4b70e1d55",
                                "data": {"ok": True},
                                "error": {"code": "x", "message": "y"}}]},
     "a step has one outcome: a result or a failure, never both"),
    ("op.schema.json",
     {"op": "wait_event", "id": "w", "hash": "3f2a91c4b70e1d55", "event": "x",
      "since": "whenever"},
     "since must be run_start, registration or a timestamp"),
    ("function-config.schema.json",
     {"id": "ok", "triggers": [{"type": "cron", "cron": "0 3 * * *", "catchup": "maybe"}]},
     "cron catchup must be one/skip/all"),
    ("attempt-request.schema.json",
     {"protocol": "1", "attempt": 1, "fence": "f",
      "run": {"id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10", "function_id": "f",
              "namespace": "prod", "started_at": "2026-01-01T00:00:00Z"},
      "steps": {"3f2a91c4b70e1d55": {"id": "a", "op": "step", "status": "weird"}}},
     "recorded step status must be a known value"),
    ("attempt-request.schema.json",
     {"protocol": "1", "attempt": 1, "fence": "f",
      "run": {"id": "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10", "function_id": "f",
              "namespace": "prod", "started_at": "yesterday"},
      "steps": {}},
     "timestamps must be RFC 3339 even without format-checking"),
]

fails = 0
print("POSITIVE CASES")
for schema, example in CASES:
    inst = json.loads((EXAMPLES / example).read_text())
    errs = sorted(validator(schema).iter_errors(inst), key=lambda e: e.path)
    if errs:
        fails += 1
        print(f"  FAIL {example} vs {schema}")
        for e in errs[:5]:
            print(f"       {list(e.path)}: {e.message[:160]}")
    else:
        print(f"  ok   {example} vs {schema}")

print("\nNEGATIVE CASES (must be rejected)")
for schema, inst, why in NEGATIVE:
    errs = list(validator(schema).iter_errors(inst))
    if errs:
        print(f"  ok   rejected: {why}")
    else:
        fails += 1
        print(f"  FAIL accepted but should reject: {why}")

print(f"\n{'ALL PASS' if fails == 0 else str(fails) + ' FAILURE(S)'}")
sys.exit(1 if fails else 0)
