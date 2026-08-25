# ADR-005: Console — one self-contained HTML file, served under a nonce CSP

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | PRD §14 ADR-005 (React/TS embedded via `rust-embed`) |

## Context

The console is what Priya uses when she has never read the code and needs to find
order 4711, see why it is stuck, and retry it. PRD §4.6 requires it to ship inside
the server binary and be served at `/` (F-UI-9), and §4.13 requires a strict CSP
(F-SEC-4) — the console renders arbitrary payload JSON and app-supplied `$ref`
URIs, so a stored-XSS path exists by construction if escaping ever slips.

The original proposal in the PRD's ADR index was a React/TypeScript application
embedded with `rust-embed`. That is the conventional answer and it carries a
build system: `npm`, a bundler, a lockfile, a hashed-asset directory, and a build
step that must run before `cargo build` or the binary ships a stale UI. It also
puts a version-skew risk between the API and the UI that talks to it into the
release process, and it makes `cargo test` insufficient to know the product works.

Against that, the actual surface is a runs list, a run detail with a step timeline,
a queue view and a DLQ — read-mostly views over a JSON API, with four commands.

This ADR therefore records what was built rather than what was proposed. React/TS
via `rust-embed` was rejected during implementation, not in review. The OpenAPI half
of the original proposal (F-API-1, `utoipa`) is unaffected either way and is not
implemented; the SSE half is affected, and is under "What we accept".

## Decision

The console is a **single self-contained HTML file**, `assets/console.html`
(383 lines, ~17 KB), compiled into the binary with `include_str!` in
`engine/rust/crates/stepd-server/src/console.rs`. No bundler, no `rust-embed`, no asset
directory, no npm in the build. `cargo build` produces the whole product.

It is served under a **nonce-based strict CSP**. Each response mints 128 bits of
randomness, rewrites the file's single `<script>` tag to carry that nonce, and
emits a policy admitting exactly it:

```
default-src 'none'; script-src 'nonce-<128 bits>'; connect-src 'self';
img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'
```

`'unsafe-inline'` is never used in `script-src`. That is the point of the nonce:
blanket `'unsafe-inline'` would admit the console's inline script *and* any script
tag an attacker managed to inject, which is the whole attack. Escaping in the page
is the first layer; the CSP is the second, for the day the first has a hole.

The page also carries `no-store, private` and `referrer-policy: no-referrer`,
because the console takes its bearer token from the query string, which makes the
URL itself a credential.

## Consequences

### What this makes easy

* `cargo build` is the entire build. No Node in CI, no lockfile to audit, no
  hashed-asset manifest, and no way for the shipped UI to disagree with the shipped
  API.
* The CSP is trivially strict because there is exactly one script tag, which is why
  `replacen("<script>", …, 1)` is correct rather than fragile.
* The console is auditable in one sitting. A reviewer can read every line of what
  is served, which is not true of a bundle.
* Reading the file is a compile-time operation, so a missing or truncated asset is
  a build failure rather than a blank page served with a 200.

### What this makes hard

* No component model, no type checking, no test runner for the UI. Correctness of
  the page is asserted indirectly — by substring checks over the asset and by
  end-to-end checks of the response headers.
* The file grows badly. At 383 lines it is comfortable; the F-UI-1 filters, the
  F-UI-5 events explorer and the F-UI-6 functions page are not built yet, and there
  is no obvious point at which "add another view" stops being cheap.
* Restyling means editing a `:root` block of CSS variables by hand.

### What we accept

* **"Self-contained" is not quite true.** The page links Google Fonts from
  `fonts.googleapis.com`, and the CSP admits `fonts.gstatic.com` and
  `style-src 'unsafe-inline'` to make that work. An air-gapped deployment gets
  fallback fonts, and `style-src` is genuinely weaker than `script-src` — style
  injection is a smaller problem than script injection, not no problem.
* **F-UI-3 (live updates via SSE) is not met.** The console polls every four
  seconds (`setInterval(…, 4000)`). Polling is honest about its cost and needs no
  server-side stream, but it is not what the requirement asks for, and on a busy
  namespace it is a repeated full page query rather than a delta.
* The token travels in the query string. `no-store` and `no-referrer` reduce where
  it leaks; they do not stop it appearing in a screenshot or shell history.

## Alternatives considered

| Option | Why not |
|---|---|
| React/TS bundled and embedded with `rust-embed` (the original proposal) | Adds npm, a bundler and a pre-`cargo` build step to a project whose build is otherwise one command, for a UI that is four read-mostly views and four commands. |
| Serve assets from a directory next to the binary | Breaks F-UI-9 and reintroduces version skew: a deploy can update the binary and not the assets, and the failure is a UI silently talking to an API it does not match. |
| `'unsafe-inline'` in `script-src` instead of a nonce | Admits every injected script tag as well as the console's own, which is precisely the attack the CSP exists to stop. |
| Hash-based CSP (`'sha256-…'`) instead of a nonce | Works, and couples the policy to the byte-exact script so that any edit to the console silently breaks the page unless the hash is regenerated. |
| A static nonce baked in at build time | A predictable nonce is not a nonce: an attacker who can guess it marks their own injected script as trusted. |

## Verification

* `engine/rust/crates/stepd-server/src/console.rs` holds the decision and its tests.
  `the_console_is_compiled_into_the_binary` asserts the embedded string contains
  `<title>stepd console</title>` and exceeds 5,000 bytes, so a truncated asset fails
  the build rather than serving a blank page with a 200.
* `every_response_carries_a_fresh_nonce_and_no_unsafe_inline` asserts
  `script-src 'nonce-`, `default-src 'none'`, `frame-ancestors 'none'` (clickjacking)
  and `base-uri 'none'` (base-tag injection) are all present, then calls `serve()`
  twice and asserts the two policies differ — a reused nonce is not a nonce.
* `the_console_escapes_by_default` asserts the page's `esc` helper and each of
  `&amp; &lt; &gt; &quot; &#39;` are present, pinning the first layer that the CSP
  is backup for.
* `the_console_page_is_never_cached` asserts `cache-control: no-store` and
  `referrer-policy: no-referrer`, the two headers that keep the token-bearing URL
  out of shared caches and `Referer` headers.
* `engine/rust/crates/stepd-server/tests/end_to_end.rs::the_console_is_served_with_a_strict_csp`
  repeats the check over real HTTP against a running server, asserting a 200, a CSP
  with `script-src 'nonce-` and without `script-src 'unsafe-inline'`, and a body
  containing `stepd console`.
