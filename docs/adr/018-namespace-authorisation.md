# ADR-018: Namespace authorisation model

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

A namespace was originally a retention and organisation boundary, and namespace
authorisation was scheduled for v1.1. Gap D2 promoted it to v1 on the grounds
that a boundary tenants rely on for isolation is a security boundary whether or
not it was designed as one — and a boundary retrofitted after the read API exists
is retrofitted onto every query at once, which is how one gets missed.

The failure mode to design against is not a missing check but a check placed one
layer too late. If handlers fetch broadly and then filter the response, the data
has already crossed the boundary inside the process, and isolation now depends on
every code path remembering. Such a system leaks anyway, through channels nobody
thinks of as data: a count computed before the filter, a cursor that pages into a
neighbour's rows, and the difference between "forbidden" and "absent".

That last one deserves naming. Returning `403` for a resource that exists in
another namespace and `404` for one that does not is impeccable HTTP and a
cross-tenant oracle: an attacker enumerating run ids learns which are real
without ever reading one.

## Decision

* **Namespace is a `WHERE` clause, never a response filter.** Every query in the
  management and read API is scoped in SQL. Nothing is fetched and then
  discarded. Cursor lookups resolve the cursor row *inside* the same namespace
  scope, so a cursor stolen from another tenant selects nothing rather than
  paging into their data.
* **A forbidden resource returns `404`, not `403`.** The two are deliberately
  indistinguishable across a namespace boundary. `403` is reserved for the case
  where the caller can already see the namespace and merely lacks the role — a
  viewer attempting a cancel — where no cross-tenant fact is disclosed.
* **Tokens are stored only as hashes.** SHA-256 of the bearer value; lookup is by
  hash. A database dump — a backup, a replica, a support export — must not be a
  set of working credentials.
* **A token carries one namespace and one role**, ordered `viewer < operator <
  admin`, so a single comparison expresses every rule.
* **An unknown role is an error, not a downgrade.** Defaulting an unrecognised
  role to least privilege sounds safe and is not: it converts a migration mistake
  into an operator who quietly loses the ability to cancel a run mid-incident,
  with nothing to point at.
* **Isolation extends below the API.** Dispatch claiming is namespace-scoped
  (`claim_runs_ns`), and blob content-addressed dedupe is keyed `(ns, sha256)` so
  a digest cannot be used to probe for another tenant's data.
* **Every mutation is audited**, from any caller including the console. There is
  no privileged UI route; an audit log with exceptions answers "who cancelled
  this run?" with "someone".

## Consequences

### What this makes easy
* Auditing the boundary is reading the SQL. If a query has no `ns = $1`, it is a
  finding, and that is a grep rather than a reasoning exercise.
* Counts, cursors and empty pages are all consistent with the namespace simply
  not containing the data, so there is no side channel to close individually.
* Revoking a token is a single row update, and no plaintext exists to leak.

### What this makes hard
* Any legitimate cross-namespace view — a fleet-wide operator dashboard — has no
  expression in this model and would need a deliberate new mechanism rather than
  a role.
* `404` for a forbidden resource is genuinely confusing when the caller is not an
  attacker but an engineer holding the wrong token. The error detail says the run
  does not exist, because saying anything else would restore the oracle.

### What we accept
* Authentication costs a database round trip per request. Caching would be
  faster and would delay revocation, and a revoked token that still works for
  sixty seconds is the wrong trade during an incident.
* The current `Principal` holds exactly one namespace, while F-SEC-1 describes a
  token granting a role within *one or more*. Multi-namespace tokens are not
  implemented; nothing depends on them yet.

## Alternatives considered

| Option | Why not |
|---|---|
| Fetch broadly, filter the response | The data has already crossed the boundary. Leaks survive through counts, cursors and timing, and correctness depends on every handler remembering. |
| `403` for cross-namespace resources | Tells an attacker which run ids are real. Correct HTTP semantics, cross-tenant information leak. |
| Postgres row-level security | Requires per-request session state (`SET LOCAL role`), which is exactly the session-scoped assumption ADR-019 forbids behind a transaction-mode pooler. |
| Store tokens in plaintext for easy support lookups | Makes every backup and replica a credential store. The support use case is served by a token id, not the secret. |
| Middleware that rewrites queries to add a scope | Invisible at the call site. A handler author cannot see whether their query is scoped, so a query that escapes the rewrite looks identical to one that does not. |
| Default an unknown role to `viewer` | Turns a migration error into a silent privilege loss discovered mid-incident. |

## Verification

* `engine/rust/crates/stepd-server/src/auth.rs` — `Principal { namespace, role }`
  extracted per request from the `tokens` table by `token_hash`, filtered on
  `revoked_at IS NULL` and expiry. Unit tests:
  `roles_are_ordered_so_a_single_comparison_expresses_the_rule`,
  `a_viewer_cannot_perform_an_operator_action`,
  `an_admin_can_perform_every_lesser_action`,
  `only_the_hash_of_a_token_is_ever_stored` (asserts the digest is 32 bytes and
  does not contain the plaintext), and
  `unknown_roles_do_not_silently_become_viewers`.
* `engine/rust/crates/stepd-server/src/api.rs` — every handler binds
  `principal.namespace` into the query. `list_runs` resolves its keyset cursor
  with `WHERE id = $5 AND ns = $1`; `get_run`, `run_steps`, `resolve_wait`,
  `list_events`, `dead_letter` and `list_functions` are all scoped in SQL;
  `audit()` records every mutation.
* `engine/rust/crates/stepd-server/src/problem.rs` — `Problem::not_found` carries the
  reasoning in its doc comment, and `forbidden` is reserved for insufficient
  role. Test: `a_forbidden_resource_is_reported_as_absent`.
* `engine/rust/crates/stepd-server/tests/end_to_end.rs` —
  **`a_token_cannot_see_another_namespace`** mints an admin token in a second
  namespace and asserts `404` on `GET /v1/runs/{id}`, `404` on the cancel command
  ("it must not be cancellable across the boundary"), an *empty* run list rather
  than a filtered one, and that the legitimate token still gets `200`. Alongside
  it: `an_unauthenticated_request_is_refused_but_health_is_not`,
  `a_viewer_can_read_but_not_command`, and `every_command_is_audited`.
* `engine/rust/tests/sql/test_invariants.sql` — check 15 asserts `claim_runs_ns` still
  filters `q.ns = p_ns`; check 6 asserts blob dedupe is `(ns, sha256)` and not
  global, "cross-tenant probing possible" being the failure message.
* **Not verified:** the end-to-end tests require a live PostgreSQL and skip
  without one (`fixture_or_skip!`), and per the README's provenance caveat this
  workspace has not been run against a live database.
