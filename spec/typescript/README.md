# stepd — the protocol, in TypeScript

`@stepd/protocol`: the wire types, the step-hash algorithm, the request
signature and envelope validation. No I/O, no HTTP, no timers.

This is the TypeScript binding of [`../PROTOCOL.md`](../PROTOCOL.md), the same
way [`../rust`](../rust) is the Rust one. It lives beside the specification
rather than inside an SDK because both an engine and every SDK implement against
it, and a contract owned by one of its consumers stops being a contract
([ADR-024](../../docs/adr/024-language-trees.md)).

```bash
pnpm install
pnpm test          # 122 tests, nothing to start first
pnpm typecheck
pnpm build         # ESM, CJS and .d.ts
```

## What it holds

| | |
|---|---|
| `types.ts` | `Attempt`, `AttemptResponse`, the eight `Op` variants, `RecordedStep`, `BlobRef`, `ExternalRef`, and the `stepd-*` header names |
| `manifest.ts` | `AppManifest`, `FunctionConfig`, blob reserve, `ProblemBody`, `ConformanceManifest` |
| `hash.ts` | `stepHash(functionId, stepId, occurrence)` and `OccurrenceCounter` |
| `signature.ts` | `sign` and `verify` for `stepd-signature: t=,n=,v1=` |
| `envelope.ts` | `validateEnvelope` — every §5.2 rule, including refusing the retired `join` field by name |
| `attempt.ts` | `decodeAttempt` |

## Two things here are load-bearing

**`stepHash` is synchronous, and has to stay that way.** `ctx.step()` claims an
occurrence at the moment it is *called*, not when its promise is awaited — that
is the single rule the whole SDK design rests on
([ADR-012](../../docs/adr/012-eager-occurrence-claiming.md)) — and it cannot do
that if computing the hash requires an `await`. This is why the package depends
on `@noble/hashes` rather than using WebCrypto, whose `subtle.digest` is async.
It is the only dependency, and `boundaries.test.ts` asserts that.

**`verify` takes `now` as an argument** rather than reading a clock. A signature
check that consults `Date.now()` itself cannot be tested at its boundary, and
every test of it becomes a little bit time-dependent. The same test file asserts
no source here uses a timer at all.

## Agreement with the Rust binding is checked, not assumed

`step_hash` and the signature are the two algorithms a second implementation can
get wrong while looking right. A step hash differing by one byte errors nowhere:
the server holds a result the handler never asks for, the handler asks for one
the server does not have, the step re-executes, and the run completes green.

So the vectors live in [`../fixtures/protocol.json`](../fixtures/protocol.json),
generated from the Rust crate, and **both** bindings assert against that one
file — `test/fixtures.test.ts` here and `tests/fixtures.rs` there. Neither can be
corrected to match the other's bug, which is what would happen if they were
compared to each other.

Twelve hash vectors and eight signature vectors, each carrying a `why`. Two of
the hash vectors are `("a","bc")` and `("ab","c")`, which exist to prove the
`0x1F` separator does something; a Rust test asserts they still differ, because a
pair of vectors that agree has silently stopped testing anything.

## Agreement with the schemas is checked, not assumed

Committed examples under [`../examples/`](../examples) are the documents.
`test/examples.test.ts` decodes every attempt request, validates every
envelope, and type-asserts the remaining wire examples. `spec/validate.py` and
`stepd-proto/tests/examples.rs` check the same files. Either side rejecting a
document the other accepts fails CI.
