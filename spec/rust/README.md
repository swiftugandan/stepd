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
cargo test --workspace     # nothing to start first
```

## What it holds

| | |
|---|---|
| `ops.rs` | `Attempt`, `AttemptResponse`, the eight `Op` variants, and `validate()` — which refuses the retired `join` field by name rather than ignoring it (§11) |
| `manifest.rs` | `AppManifest`, `FunctionConfig`, blob reserve, `ProblemBody`, `ConformanceManifest` |
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

## Agreement with the schemas is checked, not assumed

Committed examples under [`../examples/`](../examples) are the documents.
`tests/examples.rs` deserializes them through this crate (and `validate()`s
envelopes and function configs). `tests/schema.rs` serialises crate-constructed
values — including those same examples after a round-trip — and checks them
against the published schemas, so a serde rename cannot pass parse tests and
fail the wire. `spec/validate.py` checks the committed files. Either side
rejecting a document the other accepts fails CI. That is the same design as
[`../fixtures/protocol.json`](../fixtures/protocol.json) for the hash and the
signature.
