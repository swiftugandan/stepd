# ADR-004: Expression language — a documented CEL subset behind `ExprEngine`

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Several protocol fields hold predicates written by application developers:
trigger `expr`, `key_expr`, `wait_event.expr`, `cancel_on`. They run on the ingest
path — once per candidate function per event — inside a 1 ms budget (protocol
§10), must be side-effect free, and in a multi-tenant deployment they arrive from
a registration payload, which makes their content and their nesting depth
attacker-controlled.

CEL is the right family: designed for exactly this, widely known, with published
semantics stepd does not have to invent. The question is which CEL. Its
comprehension macros (`all`, `exists`, `map`, `filter`), duration arithmetic,
protobuf well-known types and user-defined functions are most of the remaining
surface and effectively all of the remaining risk — unbounded evaluation cost
inside a 1 ms budget, and semantics stepd would have to get right to be trusted.

The trap is what happens when someone writes an expression the engine does not
support. Protocol §10 requires an evaluation error to be treated as `false` and
reported as a health warning, because one tenant's bad predicate must not stop
everyone's events. Applied to an *unsupported construct*, that same rule is a
disaster: a trigger that returns `false` looks exactly like a trigger that
legitimately did not match. The function never runs, no error appears anywhere,
and the person debugging it has no thread to pull.

## Decision

`stepd-expr-cel` implements a **documented subset** of CEL and rejects everything
outside it at *compile* time, with a message naming the construct.

The supported surface is tabulated in the crate's module documentation
(`rust/crates/stepd-expr-cel/src/lib.rs`): literals; the `event`, `events`, `run`
and `now` bindings; field access and indexing; comparison; boolean logic;
arithmetic including string concatenation; membership; the conditional operator;
and a fixed function list (`string`, `int`, `double`, `bool`, `size`, `has`,
`startsWith`, `endsWith`, `contains`, `matches`).

Unsupported constructs are refused by name. `parser.rs` holds
`UNSUPPORTED_MACROS = ["all", "exists", "exists_one", "map", "filter"]` and rejects
them wherever they appear; unknown functions and methods get the same treatment.
`MAX_DEPTH = 64` bounds nesting, because a recursive-descent parser without a depth
bound is a stack overflow, and a stack overflow in Rust is an abort, not an error.

The engine sits behind `ExprEngine` in `stepd-core/src/traits.rs` (`compile` /
`eval`, with a `matches` default mapping any error to `false`), so the interpreter
can be swapped for a faster or stricter one without touching the engine. The §10
error rule still applies where it belongs: an expression that compiles but fails at
evaluation — a field the producer did not send — is a non-match, and `eval` still
returns the error so the health warning has something to report.

## Consequences

### What this makes easy

* An unsupported expression produces a message naming the construct, rather than a
  workflow that never runs for reasons nobody can see. That distinction — refusing
  loudly instead of evaluating falsely — is the decision.
* Cost is bounded by construction. Without comprehensions there is no way to write
  an expression whose runtime depends on payload size, so the 1 ms budget is a
  property of the grammar, not of a timeout.
* No `cel-interpreter` dependency, no protobuf toolchain, and full control over the
  parser's depth handling. Swapping in full CEL later is additive: every expression
  valid today stays valid.

### What this makes hard

* stepd owns a parser and an evaluator, including coercion rules. The numeric/string
  coercion in `a_number_and_a_numeric_string_compare_equal` exists because §5.1's own
  example compares `event.data.order_id` to `run.key_suffix` without a `string()`
  call, and refusing would have made the spec wrong — but it diverges from CEL's
  typed semantics and must be documented and kept.
* "CEL" in the protocol now needs a qualifier wherever it appears, and another
  implementer has to read this crate's doc table to match behaviour. Every extension
  to the subset is a compatibility decision, since the rejection message is part of
  the observable contract.

### What we accept

* PRD §6 named `cel-interpreter` as the dependency; there is none. A hand-written
  parser is more code to own and more surface to get wrong than a library.
* **The rejection happens later than the crate documentation claims.** The module
  header says an unsupported expression is rejected "at registration time, where a
  developer sees it". It is not: `register` in
  `rust/crates/stepd-server/src/registry.rs` stores the manifest without compiling
  any expression, and the only `compile` call sites are `trigger_matches` and
  `evaluate_key` on the ingest path, where a failure is a `warn!` log and a
  non-match. The refusal is real and it is by name, but today it surfaces in server
  logs at first event rather than in the developer's terminal at deploy. Validating
  expressions during registration would close this and is not yet done.
* A developer who genuinely needs `exists()` has no route except restructuring the
  predicate or moving the test into the handler.

## Alternatives considered

| Option | Why not |
|---|---|
| Full CEL via `cel-interpreter` | Comprehension macros make evaluation cost a function of payload size, which the 1 ms ingest budget cannot absorb; and the unused surface still has to be understood to be trusted. |
| A subset that silently evaluates unsupported constructs to `false` | Indistinguishable from a trigger that did not match. The function never runs and nothing anywhere says why — the failure this ADR exists to prevent. |
| JSONPath, or a JSON-object matcher | Cannot express the comparisons, string building and conditionals the protocol's own examples use; `key_expr` in particular builds a string. |
| A general scripting language (Lua, Rhai) | Side effects, unbounded runtime, and a sandbox to maintain, all on the ingest path. |
| Compiled Rust predicates in the app | Moves matching out of the server, so a trigger could no longer be evaluated at ingest without calling the app for every event. |

## Verification

* `an_unsupported_construct_is_rejected_at_compile_time_by_name` in
  `rust/crates/stepd-expr-cel/src/lib.rs` compiles `all`, `exists`, `map` and
  `filter` expressions and asserts each error both names the macro and says "not
  supported" — the property this ADR is about.
* `the_protocols_own_examples_evaluate` asserts every expression appearing in
  `spec/PROTOCOL.md` §3 and §5.1 works, so the spec cannot document something the
  reference engine cannot do.
* `an_evaluation_error_is_a_non_match_not_a_crash` asserts the §10 split directly:
  `matches` returns `false` for a predicate over a missing field, while `eval` still
  returns the error so the health warning has content.
* `a_non_boolean_result_does_not_match` asserts `'yes'`, `1` and a map do not match,
  so a typo in a predicate cannot silently start runs.
  `deeply_nested_input_does_not_blow_the_stack` compiles 10,000 nested `!` operators
  and asserts a rejection, pinning `MAX_DEPTH`; `a_syntax_error_names_the_position`
  pins locatable parse errors; `evaluation_is_fast_enough_for_the_ingest_path` is a
  smoke check against the 1 ms budget, present to catch an accidental O(n²) rewrite
  rather than to benchmark.
* The end-to-end path uses a real expression: the fixture in
  `rust/crates/stepd-server/tests/end_to_end.rs` registers `order-fulfilment` with
  `.key("'order:' + string(event.data.order_id)")`, exercised by
  `an_event_drives_a_real_sdk_workflow_to_completion`.
