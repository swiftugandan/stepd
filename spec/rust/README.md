# stepd — the protocol, in Rust

One crate, `stepd-proto`: the wire types, the step-hash algorithm and the
request signature. No I/O, no async runtime, no database — so it can be read as
a specification rather than as an implementation detail.

This is the Rust binding of [`../PROTOCOL.md`](../PROTOCOL.md) and
[`../schemas/`](../schemas). It lives beside them, rather than inside the engine
or the SDK, because both depend on it and a contract owned by one of its
consumers stops being a contract
([ADR-024](../../docs/adr/024-language-trees.md)).

```bash
cargo test --workspace     # 36 tests and a doctest, nothing to start first
```

## What it holds

| | |
|---|---|
| `ops.rs` | `Attempt`, `AttemptResponse`, the eight `Op` variants, and `validate()` — which refuses the retired `join` field by name rather than ignoring it (§11) |
| `hash.rs` | `step_hash(function_id, step_id, occurrence)` — `sha256` over the three fields separated by `0x1F`, truncated to 64 bits |
| `sig.rs` | `stepd-signature: t=,n=,v1=` over `"<t>.<nonce>.<body>"`, HMAC-SHA256, constant-time compare |
| `types.rs` | Run and step status, error bodies, the payload tiers (`$blob`, `$ref`) |

## The constraint that is enforced, not just stated

`stepd-proto` must gain no async runtime, no I/O and no database dependency.
That is what lets a third party build an alternative server or SDK against the
published spec without inheriting this engine, and a dependency added by
accident would break no test — so CI asserts it directly, by grepping
`cargo tree` for `tokio`, `sqlx`, `axum`, `reqwest` and `hyper`
([ADR-006](../../docs/adr/006-crate-boundaries.md)).

## Where it and the schemas disagree

They do, in about a dozen places — `fence` is a string in
`schemas/attempt-request.schema.json` and an `i64` here; `sleep` accepts
`duration` there and only `until` here; `wait_event` carries `expr` and
`timeout` there, `timeout_at` and no `expr` here. The server parses *this*
crate, so this crate is what an SDK must match on the wire today. The divergence
is a defect in one side or the other and is tracked as such; do not assume the
schemas are decorative, and do not assume they are authoritative.
