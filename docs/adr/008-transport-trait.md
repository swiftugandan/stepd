# ADR-008: Transport as a trait; signed HTTP push in v1

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

The engine has to get an `Attempt` to an application and get an op envelope back. Two shapes
are established: **push**, where the server calls the app's HTTP endpoint (Inngest), and
**pull**, where workers long-poll the server and claim work (Temporal).

Push wins on developer experience — an app is an HTTP handler, deployable to anything that
serves HTTP, including Lambda — and loses on reachability: an app behind NAT cannot be
called. Pull is the mirror image. The decision that matters is not which one, but whether
choosing wrongly costs a protocol version. BR-18 requires the wire protocol to be
transport-agnostic so an alternative delivery mode can be added without a version bump, and
PRD §11.3 defers pull-worker transport as scope rather than capacity.

Push carries a second obligation that is easy to under-weight. `app.url` arrives in a
registration payload — that is, from a tenant. A server that dereferences tenant-supplied
URLs without restriction is a server-side request forgery primitive with the orchestrator's
own cloud credentials behind it.

## Decision

**Delivery is a trait with one method** — `deliver(&AppTarget, &Attempt) -> Result<AttemptResponse, Error>`
in `rust/crates/stepd-core/src/traits.rs`. Everything transport-specific lives below it. The
dispatcher knows only `Attempt` in, `AttemptResponse` or `Error` out; it holds the retry
policy, the circuit breaker and the fencing check, none of which mention HTTP.

**v1 ships signed HTTP push.** `HttpTransport` (`rust/crates/stepd-transport-http/src/lib.rs`)
signs with HMAC-SHA256 over `"<ts>.<nonce>.<body>"` (`stepd-proto/src/sig.rs`), sends the
protocol, run, attempt and fence headers, and maps each status the protocol gives a distinct
meaning (§2.2) onto a distinct engine outcome — `409` to `Error::StaleFence`, `400` to a
configuration error, `429` to a throttle. Collapsing them would turn a deploy in progress
into a permanently failed run. Responses are verified too: an unverified response is an
unauthenticated instruction to mutate durable run state.

**Pull-worker mode is deferred as an alternative implementation of this trait, not as a
protocol change.** The envelope, the hash algorithm, the signature and the fencing rules are
identical in both directions of initiation; only who opens the socket differs.

**The egress policy is part of this decision, not an add-on.** `EgressPolicy`:

* **Cloud metadata is denied unconditionally** — `169.254.169.254` and `fd00:ec2::/32`.
  Every other rule can be relaxed by an operator who knows their network; this one cannot,
  because no legitimate app endpoint lives there and the cost of being wrong is every
  credential the server holds. `EgressPolicy::development()` still denies it.
* **Default is closed.** Loopback, link-local, RFC 1918, CGNAT, IPv6 unique-local,
  unspecified and broadcast are all refused unless explicitly enabled. A deployment that
  needs private addressing says so via `STEPD_ALLOW_PRIVATE_EGRESS`; one that forgets gets
  an error at registration, not a credential leak.
* **DNS is resolved once**, in `EgressPolicy::resolve`, which returns the single address the
  check was made against. Checking a hostname and resolving it again at connect time is a
  rebinding hole: the second lookup can return the metadata address after the first returned
  something innocuous.
* **Zero redirects.** `reqwest::redirect::Policy::none()`. A redirect is a second URL the
  tenant chose, and the policy checked the first.
* **Schemes are allowlisted** to `http`/`https`, and bodies are capped (8 MiB default),
  checked against both `Content-Length` and the bytes actually read.
* **The check runs at registration as well as at dispatch**, so a bad URL is an error a
  human sees rather than a mysterious run failure days later.

## Consequences

### What this makes easy
* Adding pull mode is a new crate implementing `Transport`, plus server-side claim
  endpoints: no protocol version, no SDK change for existing push apps.
* The dispatch loop is testable with `MemTransport` — no sockets, no TLS, no fixtures — and
  a harness can swap in a transport returning scripted envelopes.
* Lambda and other request-response hosts are first-class targets, because push is what
  they natively are.

### What this makes hard
* Apps behind NAT need `stepd dev --tunnel` (F-DX-3) — the acknowledged cost of push, and
  the main reason pull is on the deferral list rather than off it.
* The server needs outbound reachability to every app, which is a firewall conversation in
  environments where inbound-only workers would not have been, and the app must hold a
  connection open for the whole attempt timeout.

### What we accept
* Push means the server's failure to reach an app is indistinguishable from the app failing;
  the breaker treats both as transport errors, which is right for dispatch and unhelpful for
  diagnosis.
* **The resolve-then-connect pin is not yet enforced at connect time.** `deliver` calls
  `policy.resolve` for the check and then hands the *URL* to `reqwest`, which performs its
  own lookup. The range check is real; the guarantee that the connection goes to the address
  that was checked is not, and closing the window needs `ClientBuilder::resolve` (or a
  custom resolver) pinning the host to the resolved address. Stated plainly here because the
  module doc currently claims the stronger property.
* An unsigned app response is accepted with a warning rather than refused, so an SDK
  predating response signing keeps working. A deliberate compatibility hole; it should
  become a refusal once the SDK floor allows.
* The allowlist bypasses the range rules for the named host, so a mistake in that list is
  caught by nothing else.

## Alternatives considered

| Option | Why not |
|---|---|
| Hard-code HTTP push, no trait | Adding pull later would touch the dispatch loop, the retry policy and the fencing path — the three things this project least wants to reopen — and would invite a version bump for what is a delivery detail. |
| Pull-worker transport in v1 | Larger public surface (claim, heartbeat, lease-renewal endpoints), a worse first-run experience, and no way to deploy a workflow to a request-response host. Deferred, not rejected. |
| gRPC or a broker as the v1 transport | Adds a dependency to every SDK author and every deployment, against a protocol whose goal is that any language can host workflow code over plain HTTP JSON. |
| Allowlist-only egress, no range rules | Unusable where apps come and go, so it would be switched off wholesale — worse than a default-closed range policy. |
| Follow one redirect "for convenience" | The target is chosen by the tenant and checked by nothing. One is the same hole as ten. |

## Verification

* `rust/crates/stepd-transport-http/src/lib.rs`, test
  `cloud_metadata_is_denied_even_when_private_ranges_are_allowed`: both the IPv4 and IPv6
  metadata addresses are refused with `allow_private` and `allow_loopback` on.
* Same file: `link_local_loopback_and_private_are_denied_by_default`,
  `a_public_address_is_allowed`, `development_mode_opens_only_what_it_says`,
  `only_http_and_https_are_dereferenced`, `a_loopback_url_is_refused_under_the_default_policy`,
  `the_allowlist_overrides_the_range_rules`, `the_transport_refuses_to_follow_redirects`.
* `rust/crates/stepd-core/src/traits.rs`: `Transport` names one method and no HTTP type;
  `dispatcher.rs` is generic over `T: Transport`, exercised against `MemTransport` in
  `rust/crates/stepd-core/tests/engine.rs`.
* `rust/crates/stepd-server/src/registry.rs`: `register` calls `policy().resolve(&manifest.url)`
  and returns an `egress_denied` problem before any row is written.
* `rust/crates/stepd-server/src/lib.rs`, test `the_default_egress_policy_fails_closed`.
