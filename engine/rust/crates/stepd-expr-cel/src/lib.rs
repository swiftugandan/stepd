//! # The CEL subset stepd evaluates
//!
//! Protocol §10 says all `*_expr` fields are CEL. This crate implements a
//! **documented subset** of it, and refuses everything else at compile time
//! rather than at evaluation time.
//!
//! ## Why a subset, and why it says so
//!
//! Expressions here run on the ingest path, once per candidate function per
//! event, inside a 1 ms budget, and must be side-effect free. The parts of CEL
//! that matter for that — field access, comparison, boolean logic, string
//! building — are a small language. The parts that do not — macros over lists,
//! protobuf well-known types, duration arithmetic, custom functions — are most
//! of the remaining surface and all of the remaining risk.
//!
//! What makes a subset safe is being explicit about it. An expression using an
//! unsupported feature is rejected by [`CelEngine::compile`] with a message
//! naming the construct, at registration time, where a developer sees it. The
//! alternative — silently evaluating to `false` — would look exactly like a
//! trigger that legitimately did not match, and the function would simply never
//! run with nothing anywhere saying why.
//!
//! ## Supported
//!
//! | | |
//! |---|---|
//! | Literals | `true`, `false`, `null`, integers, doubles, single- and double-quoted strings |
//! | Bindings | `event`, `events`, `run`, `now` |
//! | Access | `a.b.c`, `a["b"]`, `a[0]` |
//! | Comparison | `==` `!=` `<` `<=` `>` `>=` |
//! | Logic | `&&` `||` `!` |
//! | Arithmetic | `+` `-` `*` `/` `%` (`+` also concatenates strings) |
//! | Membership | `x in list`, `x in map` |
//! | Conditional | `cond ? a : b` |
//! | Functions | `string(x)`, `int(x)`, `double(x)`, `bool(x)`, `size(x)`, `has(x.y)`, `x.startsWith(s)`, `x.endsWith(s)`, `x.contains(s)`, `x.matches(prefix)` |
//!
//! ## Not supported, and rejected by name
//!
//! List/map comprehension macros (`all`, `exists`, `map`, `filter`), durations
//! and timestamps beyond comparing `now`, type coercion beyond the conversion
//! functions above, protobuf types, and user-defined functions.
//!
//! ## Evaluation errors are not match failures
//!
//! §10 requires an evaluation error to be treated as a non-match and reported as
//! a function health warning. [`ExprEngine::matches`] does exactly that, so an
//! event whose shape a predicate did not anticipate cannot take down ingest —
//! but [`ExprEngine::eval`] still returns the error, so the health warning has
//! something to report.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::sync::Arc;
use stepd_core::traits::{Bindings, CompiledExpr, ExprEngine};
use stepd_core::{Error, Result};

mod parser;
use parser::{Node, Parser};

/// The CEL subset evaluator.
#[derive(Debug, Default, Clone)]
pub struct CelEngine;

impl CelEngine {
    /// A new evaluator. Stateless: compilation produces a value, not a session.
    pub fn new() -> Self {
        Self
    }

    /// Compile and evaluate in one call, for tests and one-off use.
    pub fn eval_str(&self, source: &str, b: &Bindings) -> Result<serde_json::Value> {
        let c = self.compile(source)?;
        self.eval(&c, b)
    }
}

impl ExprEngine for CelEngine {
    fn compile(&self, source: &str) -> Result<CompiledExpr> {
        let node = Parser::new(source).parse()?;
        Ok(CompiledExpr {
            source: source.to_string(),
            program: Arc::new(node),
        })
    }

    fn eval(&self, expr: &CompiledExpr, bindings: &Bindings) -> Result<serde_json::Value> {
        let node = expr
            .program
            .downcast_ref::<Node>()
            .ok_or_else(|| Error::Config("expression was compiled by a different engine".into()))?;
        parser::eval(node, bindings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bindings() -> Bindings {
        Bindings {
            event: Some(json!({
                "type": "order.created",
                "data": { "order_id": 4711, "total": 120.5, "vip": true,
                          "tenant": "acme", "tags": ["a", "b"], "note": null }
            })),
            events: None,
            run: Some(
                json!({ "id": "01J8", "key": "order:4711", "key_suffix": "4711",
                              "function_id": "order-fulfilment" }),
            ),
            now: Some(chrono::Utc::now()),
        }
    }

    fn ev(src: &str) -> serde_json::Value {
        CelEngine::new()
            .eval_str(src, &bindings())
            .unwrap_or_else(|e| panic!("{src}: {e}"))
    }

    fn truthy(src: &str) -> bool {
        let e = CelEngine::new();
        e.matches(&e.compile(src).unwrap(), &bindings())
    }

    #[test]
    fn the_protocols_own_examples_evaluate() {
        // Every expression that appears in PROTOCOL.md §3 must work, or the spec
        // documents something the reference engine cannot do.
        assert!(truthy("event.data.total > 0"));
        assert_eq!(
            ev("'order:' + string(event.data.order_id)"),
            json!("order:4711")
        );
        assert!(truthy("event.data.order_id == run.key_suffix"));
        assert_eq!(ev("event.data.vip ? 10 : 0"), json!(10));
        assert_eq!(ev("event.data.order_id"), json!(4711));
        assert_eq!(ev("event.data.tenant"), json!("acme"));
    }

    #[test]
    fn comparisons_and_logic() {
        assert!(truthy("event.data.total >= 120.5 && event.data.vip"));
        assert!(truthy("event.data.total < 1 || event.data.vip"));
        assert!(truthy("!(event.data.total < 1)"));
        assert!(truthy("event.data.tenant != 'other'"));
        assert!(!truthy("event.data.tenant == 'other'"));
    }

    #[test]
    fn a_number_and_a_numeric_string_compare_equal() {
        // `run.key_suffix` is always a string; `event.data.order_id` is usually a
        // number. Requiring the author to write `string(event.data.order_id) ==
        // run.key_suffix` would be defensible, but the protocol's own example in
        // §5.1 does not, so the engine coerces rather than making the spec wrong.
        assert!(truthy("event.data.order_id == '4711'"));
        assert!(truthy("'4711' == event.data.order_id"));
        assert!(!truthy("event.data.order_id == '4712'"));
    }

    #[test]
    fn membership_and_size() {
        assert!(truthy("'a' in event.data.tags"));
        assert!(!truthy("'z' in event.data.tags"));
        assert!(truthy("'order_id' in event.data"));
        assert_eq!(ev("size(event.data.tags)"), json!(2));
        assert_eq!(ev("size(event.data.tenant)"), json!(4));
    }

    #[test]
    fn string_methods() {
        assert!(truthy("event.type.startsWith('order.')"));
        assert!(truthy("event.type.endsWith('.created')"));
        assert!(truthy("event.type.contains('der.cre')"));
        assert!(!truthy("event.type.startsWith('shipment.')"));
    }

    #[test]
    fn has_distinguishes_absent_from_null() {
        // `null` is a value the producer chose to send; absent is a field they
        // never sent. Collapsing them makes "did they tell us?" unaskable.
        assert!(truthy("has(event.data.note)"));
        assert!(!truthy("has(event.data.nonexistent)"));
        assert_eq!(ev("event.data.note"), json!(null));
    }

    #[test]
    fn arithmetic_including_string_concatenation() {
        assert_eq!(ev("1 + 2"), json!(3));
        assert_eq!(ev("7 % 3"), json!(1));
        assert_eq!(ev("event.data.total * 2"), json!(241.0));
        assert_eq!(ev("'a' + 'b' + 'c'"), json!("abc"));
    }

    #[test]
    fn indexing_by_key_and_position() {
        assert_eq!(ev("event.data['order_id']"), json!(4711));
        assert_eq!(ev("event.data.tags[1]"), json!("b"));
    }

    #[test]
    fn an_unsupported_construct_is_rejected_at_compile_time_by_name() {
        // The whole point of the subset being documented. Silently evaluating to
        // false would be indistinguishable from a trigger that did not match, and
        // the function would never run with nothing saying why.
        let e = CelEngine::new();
        for (src, expect) in [
            ("event.data.tags.all(x, x == 'a')", "all"),
            ("event.data.tags.exists(x, x == 'a')", "exists"),
            ("event.data.tags.map(x, x)", "map"),
            ("event.data.tags.filter(x, true)", "filter"),
        ] {
            let err = e.compile(src).unwrap_err().to_string();
            assert!(
                err.contains(expect) && err.contains("not supported"),
                "compiling {src} should name '{expect}': got {err}"
            );
        }
    }

    #[test]
    fn a_syntax_error_names_the_position() {
        let err = CelEngine::new()
            .compile("event.data. == 1")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("position"),
            "a parse error must be locatable: {err}"
        );
    }

    #[test]
    fn an_evaluation_error_is_a_non_match_not_a_crash() {
        // §10: evaluation errors are treated as false and reported as health
        // warnings. An event whose shape a predicate did not anticipate must not
        // be able to take down ingest.
        let e = CelEngine::new();
        let c = e.compile("event.data.missing.deeper > 1").unwrap();
        assert!(!e.matches(&c, &bindings()), "must not match");
        assert!(
            e.eval(&c, &bindings()).is_err(),
            "but the error is still reportable"
        );
    }

    #[test]
    fn a_non_boolean_result_does_not_match() {
        // `matches` is used for triggers and cancellation; treating a truthy
        // string or a non-zero number as a match would make a typo in a predicate
        // silently start runs.
        let e = CelEngine::new();
        for src in ["'yes'", "1", "event.data"] {
            assert!(
                !e.matches(&e.compile(src).unwrap(), &bindings()),
                "{src} must not match"
            );
        }
    }

    #[test]
    fn an_unbound_name_is_an_error_not_a_null() {
        let e = CelEngine::new();
        assert!(e.eval_str("nonexistent.field", &bindings()).is_err());
    }

    #[test]
    fn missing_bindings_do_not_panic() {
        let e = CelEngine::new();
        let empty = Bindings::default();
        assert!(e.eval_str("event.data.x", &empty).is_err());
        assert!(!e.matches(&e.compile("event.data.x > 1").unwrap(), &empty));
    }

    #[test]
    fn deeply_nested_input_does_not_blow_the_stack() {
        // A predicate arrives from a registration payload, so its depth is
        // attacker-controlled in a multi-tenant deployment.
        let src = "!".repeat(10_000) + "true";
        assert!(
            CelEngine::new().compile(&src).is_err(),
            "depth must be bounded"
        );
    }

    #[test]
    fn evaluation_is_fast_enough_for_the_ingest_path() {
        // §10 budgets 1 ms. This is a smoke check, not a benchmark: it exists to
        // catch an accidental O(n^2) rewrite, not to measure.
        let e = CelEngine::new();
        let c = e
            .compile("event.data.total > 0 && event.type.startsWith('order.')")
            .unwrap();
        let b = bindings();
        let t = std::time::Instant::now();
        for _ in 0..1000 {
            e.matches(&c, &b);
        }
        let per = t.elapsed() / 1000;
        assert!(
            per < std::time::Duration::from_millis(1),
            "{per:?} per evaluation"
        );
    }
}
