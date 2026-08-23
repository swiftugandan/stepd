# stepd SDK Protocol v1

Open specification for the wire protocol between a **stepd server** (durable orchestrator)
and an **app** hosting workflow code via an SDK.

Licence: Apache-2.0. Anyone may implement a server or an SDK against this spec.

## Contents

| Path | What |
|---|---|
| `PROTOCOL.md` | Normative specification |
| `schemas/` | JSON Schema (2020-12) for every message |
| `examples/` | Valid example payloads, used as fixtures |
| `validate.py` | Validates examples and negative cases against the schemas |

## Schemas

| File | Describes |
|---|---|
| `common.schema.json` | Shared types: ids, durations, hashes, payloads, managed blob refs (`$blob`), external refs (`$ref`), errors |
| `event.schema.json` | CloudEvents 1.0 + `stepd*` extension attributes |
| `function-config.schema.json` | Function definition: triggers, keys, flow control, retries |
| `app-manifest.schema.json` | App registration / discovery document |
| `attempt-request.schema.json` | Server → app: run state for one attempt |
| `attempt-response.schema.json` | App → server: op envelope |
| `op.schema.json` | The seven op types |
| `blob-reserve.schema.json` | Two-phase direct-upload reservation request/response |
| `problem.schema.json` | RFC 9457 error bodies |

## Running the validator

```bash
pip install jsonschema referencing
python3 validate.py
```

## The model in one paragraph

The server drives a run by calling the app once per **attempt**. The request carries every
already-recorded step keyed by a stable **hash** of `(step_id, occurrence)`. The SDK replays
the handler, returning memoized values for known hashes and executing the first unknown step,
then yields one **op** (`step`, `sleep`, `wait_event`, `invoke`, `signal`, `done`, `error`)
or a parallel batch of them. The server commits ops, emitted events and the next-attempt
schedule in a single transaction. Steps are at-least-once to execute, exactly-once to record.

Two failure modes in this design corrupt data *silently* rather than erroring, so both are
handled normatively rather than by convention:

* **Lost signals** — an event sent before the run reaches its `wait_event`. Every run has a
  durable inbox and waits match against it from run start (§7.6).
* **Unstable hashes** — occurrence counters racing under concurrency, causing completed
  steps to re-execute. Occurrence is assigned in program order on a sequential pass, and
  SDKs must fail rather than guess (§6.1).

Implementers should read those two sections before anything else.

## Implementation checklist for a new SDK

1. HTTP handler that accepts `AttemptRequest`, verifies `stepd-signature`, returns `AttemptResponse`.
2. Hash function: `sha256(function_id ‖ 0x1F ‖ step_id ‖ 0x1F ‖ occurrence)[0..8]`, hex.
3. Per-attempt occurrence counters, reset each replay pass.
4. Memo lookup before executing any step closure.
5. Short-circuit control flow to yield an op mid-handler (exception, `Err`, or generator).
6. Lazy blob fetch for `$blob` values, with streaming and `Range` support; pass through `$ref` values untouched.
6b. Two-phase upload: reserve, PUT directly to the store, return the reference. Never POST bytes to the stepd server.
7. `/.well-known/stepd` returning the `AppManifest`.
8. Structured parallel construct that assigns all hashes up front (§6.1) — do not let
   concurrent tasks claim occurrence counters.
9. Never drop a running step future to meet a deadline; abandon the response instead (§7.1.1).
10. Nonce per request, and a replay cache over the signature window (§9).
11. Pass `stepd conformance --app <url>` at level 1, then level 2.
