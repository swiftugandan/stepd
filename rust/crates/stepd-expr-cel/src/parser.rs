//! Recursive-descent parser and tree-walking evaluator for the documented subset.
//!
//! A tree walker rather than a bytecode VM: expressions are tens of tokens long
//! and run once per candidate function per event, so the interpreter's overhead
//! is invisible next to the JSON access it performs. A VM would buy nothing and
//! cost a whole layer of code in the one place where being obviously correct
//! matters more than being fast.

use serde_json::Value;
use stepd_core::traits::Bindings;
use stepd_core::{Error, Result};

/// Maximum nesting depth.
///
/// Predicates arrive from registration payloads, so in a multi-tenant deployment
/// their depth is attacker-controlled. A recursive-descent parser without a
/// depth bound is a stack overflow — which in Rust is an abort, not an error.
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Node {
    Literal(Value),
    /// A top-level binding: `event`, `events`, `run`, `now`.
    Binding(String),
    Field(Box<Node>, String),
    Index(Box<Node>, Box<Node>),
    Unary(&'static str, Box<Node>),
    Binary(&'static str, Box<Node>, Box<Node>),
    Ternary(Box<Node>, Box<Node>, Box<Node>),
    /// `string(x)`, `size(x)`, …
    Call(String, Vec<Node>),
    /// `x.startsWith(s)` — a method on the value to its left.
    Method(Box<Node>, String, Vec<Node>),
    /// `has(a.b)` needs the *path*, not the value, so it is its own node.
    Has(Box<Node>),
}

// ---------------------------------------------------------------- lexer

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Num(f64, bool), // value, is_integer
    Str(String),
    Op(String),
    Eof,
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(s: &str) -> Self {
        Self {
            chars: s.chars().collect(),
            pos: 0,
        }
    }

    fn tokens(mut self) -> Result<Vec<(Tok, usize)>> {
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            let start = self.pos;
            if self.pos >= self.chars.len() {
                out.push((Tok::Eof, start));
                return Ok(out);
            }
            let c = self.chars[self.pos];
            let tok = match c {
                'a'..='z' | 'A'..='Z' | '_' => {
                    let mut s = String::new();
                    while self.pos < self.chars.len()
                        && (self.chars[self.pos].is_alphanumeric() || self.chars[self.pos] == '_')
                    {
                        s.push(self.chars[self.pos]);
                        self.pos += 1;
                    }
                    Tok::Ident(s)
                }
                '0'..='9' => {
                    let mut s = String::new();
                    let mut is_int = true;
                    while self.pos < self.chars.len()
                        && (self.chars[self.pos].is_ascii_digit() || self.chars[self.pos] == '.')
                    {
                        if self.chars[self.pos] == '.' {
                            // `1.foo` is a field access on an int literal, not a
                            // decimal point; only digits after the dot make it one.
                            if self.pos + 1 >= self.chars.len()
                                || !self.chars[self.pos + 1].is_ascii_digit()
                            {
                                break;
                            }
                            is_int = false;
                        }
                        s.push(self.chars[self.pos]);
                        self.pos += 1;
                    }
                    let n: f64 = s.parse().map_err(|_| {
                        Error::Config(format!("bad number '{s}' at position {start}"))
                    })?;
                    Tok::Num(n, is_int)
                }
                '\'' | '"' => {
                    let quote = c;
                    self.pos += 1;
                    let mut s = String::new();
                    loop {
                        if self.pos >= self.chars.len() {
                            return Err(Error::Config(format!(
                                "unterminated string starting at position {start}"
                            )));
                        }
                        let ch = self.chars[self.pos];
                        if ch == '\\' && self.pos + 1 < self.chars.len() {
                            self.pos += 1;
                            s.push(match self.chars[self.pos] {
                                'n' => '\n',
                                't' => '\t',
                                other => other,
                            });
                            self.pos += 1;
                            continue;
                        }
                        self.pos += 1;
                        if ch == quote {
                            break;
                        }
                        s.push(ch);
                    }
                    Tok::Str(s)
                }
                _ => {
                    let two: String = self.chars[self.pos..].iter().take(2).collect();
                    let op = match two.as_str() {
                        "==" | "!=" | "<=" | ">=" | "&&" | "||" => {
                            self.pos += 2;
                            two
                        }
                        _ => {
                            let one = c.to_string();
                            if !"+-*/%<>!?:.,()[]".contains(c) {
                                return Err(Error::Config(format!(
                                    "unexpected character '{c}' at position {start}"
                                )));
                            }
                            self.pos += 1;
                            one
                        }
                    };
                    Tok::Op(op)
                }
            };
            out.push((tok, start));
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }
}

// ---------------------------------------------------------------- parser

/// Macros CEL defines that this subset does not implement.
///
/// Named individually so the error can say which one, rather than "unsupported
/// syntax" — the developer needs to know whether to rewrite the predicate or to
/// file a request.
const UNSUPPORTED_MACROS: &[&str] = &["all", "exists", "exists_one", "map", "filter"];

pub(crate) struct Parser {
    toks: Vec<(Tok, usize)>,
    pos: usize,
    depth: usize,
}

impl Parser {
    pub(crate) fn new(src: &str) -> Self {
        // Lexing errors surface on the first `parse` call rather than here, so
        // the constructor stays infallible and the caller has one error path.
        let toks = Lexer::new(src)
            .tokens()
            .unwrap_or_else(|e| vec![(Tok::Ident(format!("\u{0}lexerror:{e}")), 0), (Tok::Eof, 0)]);
        Self {
            toks,
            pos: 0,
            depth: 0,
        }
    }

    pub(crate) fn parse(mut self) -> Result<Node> {
        if let (Tok::Ident(s), _) = &self.toks[0] {
            if let Some(msg) = s.strip_prefix('\u{0}') {
                return Err(Error::Config(
                    msg.trim_start_matches("lexerror:").to_string(),
                ));
            }
        }
        let node = self.ternary()?;
        match self.peek() {
            Tok::Eof => Ok(node),
            other => Err(Error::Config(format!(
                "unexpected trailing input {other:?} at position {}",
                self.here()
            ))),
        }
    }

    fn peek(&self) -> Tok {
        self.toks
            .get(self.pos)
            .map(|(t, _)| t.clone())
            .unwrap_or(Tok::Eof)
    }

    fn here(&self) -> usize {
        self.toks.get(self.pos).map(|(_, p)| *p).unwrap_or(0)
    }

    fn eat_op(&mut self, op: &str) -> bool {
        if self.peek() == Tok::Op(op.to_string()) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_op(&mut self, op: &str) -> Result<()> {
        if self.eat_op(op) {
            Ok(())
        } else {
            Err(Error::Config(format!(
                "expected '{op}' at position {}, found {:?}",
                self.here(),
                self.peek()
            )))
        }
    }

    fn enter(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(Error::Config(format!(
                "expression nests deeper than {MAX_DEPTH} levels"
            )));
        }
        Ok(())
    }

    fn ternary(&mut self) -> Result<Node> {
        self.enter()?;
        let cond = self.or()?;
        let out = if self.eat_op("?") {
            let a = self.ternary()?;
            self.expect_op(":")?;
            let b = self.ternary()?;
            Node::Ternary(Box::new(cond), Box::new(a), Box::new(b))
        } else {
            cond
        };
        self.depth -= 1;
        Ok(out)
    }

    fn or(&mut self) -> Result<Node> {
        let mut left = self.and()?;
        while self.eat_op("||") {
            left = Node::Binary("||", Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Node> {
        let mut left = self.comparison()?;
        while self.eat_op("&&") {
            left = Node::Binary("&&", Box::new(left), Box::new(self.comparison()?));
        }
        Ok(left)
    }

    fn comparison(&mut self) -> Result<Node> {
        let mut left = self.additive()?;
        loop {
            let op = match self.peek() {
                Tok::Op(o) if ["==", "!=", "<", "<=", ">", ">="].contains(&o.as_str()) => o,
                Tok::Ident(i) if i == "in" => "in".to_string(),
                _ => break,
            };
            self.pos += 1;
            let right = self.additive()?;
            let s: &'static str = match op.as_str() {
                "==" => "==",
                "!=" => "!=",
                "<" => "<",
                "<=" => "<=",
                ">" => ">",
                ">=" => ">=",
                _ => "in",
            };
            left = Node::Binary(s, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn additive(&mut self) -> Result<Node> {
        let mut left = self.multiplicative()?;
        loop {
            if self.eat_op("+") {
                left = Node::Binary("+", Box::new(left), Box::new(self.multiplicative()?));
            } else if self.eat_op("-") {
                left = Node::Binary("-", Box::new(left), Box::new(self.multiplicative()?));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn multiplicative(&mut self) -> Result<Node> {
        let mut left = self.unary()?;
        loop {
            if self.eat_op("*") {
                left = Node::Binary("*", Box::new(left), Box::new(self.unary()?));
            } else if self.eat_op("/") {
                left = Node::Binary("/", Box::new(left), Box::new(self.unary()?));
            } else if self.eat_op("%") {
                left = Node::Binary("%", Box::new(left), Box::new(self.unary()?));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Node> {
        self.enter()?;
        let out = if self.eat_op("!") {
            Node::Unary("!", Box::new(self.unary()?))
        } else if self.eat_op("-") {
            Node::Unary("-", Box::new(self.unary()?))
        } else {
            self.postfix()?
        };
        self.depth -= 1;
        Ok(out)
    }

    fn postfix(&mut self) -> Result<Node> {
        let mut node = self.primary()?;
        loop {
            if self.eat_op(".") {
                let name = match self.peek() {
                    Tok::Ident(i) => {
                        self.pos += 1;
                        i
                    }
                    other => {
                        return Err(Error::Config(format!(
                            "expected a field name after '.' at position {}, found {other:?}",
                            self.here()
                        )))
                    }
                };
                if self.eat_op("(") {
                    if UNSUPPORTED_MACROS.contains(&name.as_str()) {
                        return Err(Error::Config(format!(
                            "the CEL macro '{name}' is not supported by stepd's expression \
                             subset. Rewrite the predicate without it, or move the check into \
                             the workflow where arbitrary code is allowed."
                        )));
                    }
                    let args = self.args()?;
                    node = Node::Method(Box::new(node), name, args);
                } else {
                    node = Node::Field(Box::new(node), name);
                }
            } else if self.eat_op("[") {
                let idx = self.ternary()?;
                self.expect_op("]")?;
                node = Node::Index(Box::new(node), Box::new(idx));
            } else {
                return Ok(node);
            }
        }
    }

    fn args(&mut self) -> Result<Vec<Node>> {
        let mut out = Vec::new();
        if self.eat_op(")") {
            return Ok(out);
        }
        loop {
            out.push(self.ternary()?);
            if self.eat_op(",") {
                continue;
            }
            self.expect_op(")")?;
            return Ok(out);
        }
    }

    fn primary(&mut self) -> Result<Node> {
        match self.peek() {
            Tok::Num(n, is_int) => {
                self.pos += 1;
                Ok(Node::Literal(if is_int {
                    Value::from(n as i64)
                } else {
                    Value::from(n)
                }))
            }
            Tok::Str(s) => {
                self.pos += 1;
                Ok(Node::Literal(Value::from(s)))
            }
            Tok::Op(o) if o == "(" => {
                self.pos += 1;
                let n = self.ternary()?;
                self.expect_op(")")?;
                Ok(n)
            }
            Tok::Ident(i) => {
                self.pos += 1;
                match i.as_str() {
                    "true" => return Ok(Node::Literal(Value::Bool(true))),
                    "false" => return Ok(Node::Literal(Value::Bool(false))),
                    "null" => return Ok(Node::Literal(Value::Null)),
                    _ => {}
                }
                if self.eat_op("(") {
                    if UNSUPPORTED_MACROS.contains(&i.as_str()) {
                        return Err(Error::Config(format!(
                            "the CEL macro '{i}' is not supported by stepd's expression subset"
                        )));
                    }
                    let args = self.args()?;
                    // `has` takes a *path*, not a value: `has(a.b)` must not
                    // evaluate `a.b` first, or an absent field is an error before
                    // `has` gets a chance to answer the question.
                    if i == "has" {
                        let inner = args.into_iter().next().ok_or_else(|| {
                            Error::Config("has() takes exactly one argument".into())
                        })?;
                        return Ok(Node::Has(Box::new(inner)));
                    }
                    return Ok(Node::Call(i, args));
                }
                Ok(Node::Binding(i))
            }
            other => Err(Error::Config(format!(
                "unexpected {other:?} at position {}",
                self.here()
            ))),
        }
    }
}

// ---------------------------------------------------------------- evaluator

pub(crate) fn eval(node: &Node, b: &Bindings) -> Result<Value> {
    match node {
        Node::Literal(v) => Ok(v.clone()),

        Node::Binding(name) => match name.as_str() {
            "event" => b.event.clone().ok_or_else(|| unbound("event")),
            "events" => b.events.clone().ok_or_else(|| unbound("events")),
            "run" => b.run.clone().ok_or_else(|| unbound("run")),
            "now" => b
                .now
                .map(|t| Value::from(t.timestamp()))
                .ok_or_else(|| unbound("now")),
            other => Err(Error::Config(format!(
                "unknown binding '{other}'. Available: event, events, run, now (protocol §10)"
            ))),
        },

        Node::Field(base, name) => {
            let v = eval(base, b)?;
            v.get(name)
                .cloned()
                .ok_or_else(|| Error::Config(format!("no field '{name}' on {}", kind(&v))))
        }

        Node::Index(base, idx) => {
            let v = eval(base, b)?;
            let i = eval(idx, b)?;
            match (&v, &i) {
                (Value::Object(_), Value::String(k)) => v
                    .get(k.as_str())
                    .cloned()
                    .ok_or_else(|| Error::Config(format!("no key '{k}' in map"))),
                (Value::Array(a), Value::Number(n)) => {
                    let n = n.as_i64().unwrap_or(-1);
                    a.get(n as usize)
                        .cloned()
                        .ok_or_else(|| Error::Config(format!("index {n} out of range")))
                }
                _ => Err(Error::Config(format!(
                    "cannot index {} with {}",
                    kind(&v),
                    kind(&i)
                ))),
            }
        }

        Node::Unary(op, inner) => {
            let v = eval(inner, b)?;
            match *op {
                "!" => Ok(Value::Bool(!truthy(&v)?)),
                _ => num(&v).map(|n| Value::from(-n)),
            }
        }

        Node::Ternary(c, a, d) => {
            if truthy(&eval(c, b)?)? {
                eval(a, b)
            } else {
                eval(d, b)
            }
        }

        Node::Binary(op, l, r) => {
            // Short-circuit before evaluating the right-hand side, so
            // `has(x) && x > 1` is writable and `false && bad.field` does not
            // error. Evaluating both sides first would make every guard useless.
            match *op {
                "&&" => {
                    return if !truthy(&eval(l, b)?)? {
                        Ok(Value::Bool(false))
                    } else {
                        Ok(Value::Bool(truthy(&eval(r, b)?)?))
                    }
                }
                "||" => {
                    return if truthy(&eval(l, b)?)? {
                        Ok(Value::Bool(true))
                    } else {
                        Ok(Value::Bool(truthy(&eval(r, b)?)?))
                    }
                }
                _ => {}
            }

            let a = eval(l, b)?;
            let c = eval(r, b)?;
            match *op {
                "==" => Ok(Value::Bool(loose_eq(&a, &c))),
                "!=" => Ok(Value::Bool(!loose_eq(&a, &c))),
                "<" | "<=" | ">" | ">=" => {
                    let (x, y) = (num(&a)?, num(&c)?);
                    Ok(Value::Bool(match *op {
                        "<" => x < y,
                        "<=" => x <= y,
                        ">" => x > y,
                        _ => x >= y,
                    }))
                }
                "in" => Ok(Value::Bool(match &c {
                    Value::Array(items) => items.iter().any(|i| loose_eq(i, &a)),
                    Value::Object(m) => a.as_str().is_some_and(|k| m.contains_key(k)),
                    other => {
                        return Err(Error::Config(format!(
                            "'in' needs a list or map on the right, found {}",
                            kind(other)
                        )))
                    }
                })),
                "+" => match (&a, &c) {
                    (Value::String(x), _) => Ok(Value::from(format!("{x}{}", to_str(&c)))),
                    (_, Value::String(y)) => Ok(Value::from(format!("{}{y}", to_str(&a)))),
                    _ => arith(&a, &c, |x, y| x + y),
                },
                "-" => arith(&a, &c, |x, y| x - y),
                "*" => arith(&a, &c, |x, y| x * y),
                "/" => {
                    if num(&c)? == 0.0 {
                        return Err(Error::Config("division by zero".into()));
                    }
                    arith(&a, &c, |x, y| x / y)
                }
                "%" => {
                    if num(&c)? == 0.0 {
                        return Err(Error::Config("modulo by zero".into()));
                    }
                    arith(&a, &c, |x, y| x % y)
                }
                other => Err(Error::Config(format!("unsupported operator '{other}'"))),
            }
        }

        Node::Has(path) => {
            // Absent is false; present-but-null is true. Collapsing them makes
            // "did the producer send this field?" unaskable, which is exactly
            // what `has` is for.
            Ok(Value::Bool(eval(path, b).is_ok()))
        }

        Node::Call(name, args) => {
            let vals: Result<Vec<Value>> = args.iter().map(|a| eval(a, b)).collect();
            let vals = vals?;
            let one = |v: &Vec<Value>| -> Result<Value> {
                v.first()
                    .cloned()
                    .ok_or_else(|| Error::Config(format!("{name}() takes one argument")))
            };
            match name.as_str() {
                "string" => Ok(Value::from(to_str(&one(&vals)?))),
                "int" => Ok(Value::from(num(&one(&vals)?)? as i64)),
                "double" => Ok(Value::from(num(&one(&vals)?)?)),
                "bool" => Ok(Value::Bool(truthy(&one(&vals)?)?)),
                "size" => {
                    let v = one(&vals)?;
                    Ok(Value::from(match &v {
                        Value::String(s) => s.chars().count() as i64,
                        Value::Array(a) => a.len() as i64,
                        Value::Object(m) => m.len() as i64,
                        other => {
                            return Err(Error::Config(format!(
                                "size() needs a string, list or map, found {}",
                                kind(other)
                            )))
                        }
                    }))
                }
                other => Err(Error::Config(format!(
                    "the function '{other}' is not supported by stepd's expression subset. \
                     Supported: string, int, double, bool, size, has"
                ))),
            }
        }

        Node::Method(base, name, args) => {
            let v = eval(base, b)?;
            let arg = match args.first() {
                Some(a) => Some(eval(a, b)?),
                None => None,
            };
            let s = v.as_str().ok_or_else(|| {
                Error::Config(format!(
                    "'{name}' is a string method, but the value is {}",
                    kind(&v)
                ))
            })?;
            let a = arg
                .as_ref()
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::Config(format!("'{name}' takes a string argument")))?;
            Ok(Value::Bool(match name.as_str() {
                "startsWith" => s.starts_with(a),
                "endsWith" => s.ends_with(a),
                "contains" => s.contains(a),
                // Deliberately prefix matching, not a regex engine: a regex from
                // a registration payload is a denial-of-service surface, and the
                // 1 ms evaluation budget cannot be honoured with backtracking.
                "matches" => s.starts_with(a),
                other => {
                    return Err(Error::Config(format!(
                        "the method '{other}' is not supported by stepd's expression subset. \
                         Supported: startsWith, endsWith, contains, matches"
                    )))
                }
            }))
        }
    }
}

fn unbound(name: &str) -> Error {
    Error::Config(format!(
        "'{name}' is not bound in this context (protocol §10)"
    ))
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "map",
    }
}

fn truthy(v: &Value) -> Result<bool> {
    v.as_bool()
        .ok_or_else(|| Error::Config(format!("expected a bool, found {}", kind(v))))
}

fn num(v: &Value) -> Result<f64> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| Error::Config("bad number".into())),
        // A numeric string compares as a number: run keys are strings and event
        // ids are usually numbers, and the protocol's own §5.1 example compares
        // them directly.
        Value::String(s) => s
            .parse()
            .map_err(|_| Error::Config(format!("'{s}' is not a number"))),
        other => Err(Error::Config(format!(
            "expected a number, found {}",
            kind(other)
        ))),
    }
}

fn to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

/// Equality that spans the string/number boundary.
///
/// `event.data.order_id == run.key_suffix` is the protocol's own example, and
/// the left side is a JSON number while the right is always a string. Strict
/// equality would make the documented expression false for every event.
fn loose_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (Value::Number(_), Value::String(_)) | (Value::String(_), Value::Number(_)) => {
            matches!((num(a), num(b)), (Ok(x), Ok(y)) if x == y)
        }
        _ => false,
    }
}

fn arith(a: &Value, b: &Value, f: impl Fn(f64, f64) -> f64) -> Result<Value> {
    let (x, y) = (num(a)?, num(b)?);
    let r = f(x, y);
    // Integer in, integer out: `1 + 2` returning `3.0` would serialise as `3.0`
    // and stop matching a key built from an integer id.
    let both_int =
        matches!(a, Value::Number(n) if n.is_i64()) && matches!(b, Value::Number(n) if n.is_i64());
    Ok(if both_int && r.fract() == 0.0 {
        Value::from(r as i64)
    } else {
        Value::from(r)
    })
}
