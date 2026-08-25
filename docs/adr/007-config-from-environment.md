# ADR-007: Configuration from the environment; manifests carry no deployment config

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

An app registers with stepd by sending an `AppManifest` (protocol §3): the functions it
hosts, their triggers, keys, retries and timeouts. The manifest travels over the network on
every start-up and is persisted by the server in `app_bindings` and `functions`.

That makes the manifest exactly the wrong place for two categories of value.

**Credentials.** A signing key that travels alongside the request it authenticates is not a
secret. Worse, it would then be at rest in the orchestrator's database, in its backups, and
in the console's read model — three places nobody audits when they audit key handling.

**Environment identity.** If a `FunctionConfig` carried the server URL, the namespace or the
environment name, then promoting a build from staging to production would mean editing
workflow code, and a staging app pointed at the wrong `STEPD_SERVER_URL` would overwrite
production's endpoint on start-up. That is one deploy away at all times, and the symptom —
production traffic arriving at a laptop — appears long after the change that caused it.

BR-16 and F-CFG-1…6 state the rule. This ADR records how the split is drawn and where it is
enforced.

## Decision

Two categories, separated at the type level in the SDK and by source in the server.

**Code-defined, travels in the manifest.** `Function` (`sdk/rust/crates/stepd-sdk/src/function.rs`)
holds only what is a property of the workflow: `id`, `version`, `triggers`, `key_expr`,
`singleton`, `retries`, `timeouts`, display `name`. `Function::config()` serialises exactly
those fields and nothing else. There is no builder method that accepts a URL, a token or an
environment name, so a credential cannot reach a `FunctionConfig` by accident — only by
someone adding a field for it.

**Environment-supplied binding.** `App::new(app_id, url)` plus `App::signing_key(key)` hold
the deployment facts. `App::manifest()` emits `protocol`, `app_id`, `url`, `sdk`, `checksum`
and the function array. The URL is in there deliberately: push transport means the server
must know where to call. The key is not, and never is.

**Server configuration is environment-only.** `Config::from_env()`
(`engine/rust/crates/stepd-server/src/lib.rs`) reads `STEPD_DATABASE_URL`, `STEPD_BIND`,
`STEPD_WORKER`, `STEPD_MAX_CONNECTIONS`, `STEPD_SIGNING_KEY`, `STEPD_SIGNING_KEY_PREVIOUS`
and the three egress variables. Nothing is read from a config file, and nothing that governs
behaviour is read from the database or from a manifest, because a value that arrived over
the network is a value a tenant can influence.

**Keys are resolved out of band.** `DbTargetResolver` (`engine/rust/crates/stepd-server/src/registry.rs`)
looks the app's URL up from `app_bindings` but takes the signing keys from `Config` — a
per-app map with a default. The server and the app are configured with the same key by the
orchestrator; neither learns it from the other. Two keys are live at once so rotation is not
a flag day, the first being the one used to sign.

**Missing configuration fails closed, at start-up.** `stepd serve` refuses to boot with no
`STEPD_SIGNING_KEY` unless `--dev`, rather than booting and having every attempt rejected by
a conforming app with a 401 that nobody connects to the missing variable. The SDK's
`App::lint()` reports the mirror-image case on the app side.

## Consequences

### What this makes easy
* The same artefact runs in every environment. `stepd dev`, CI and production differ by
  environment variables only (PRD §6.4).
* Key rotation is two variables and a restart, in either order, on either side.
* A leaked manifest — from a backup, a console screenshot, a support ticket — discloses
  topology, not access.
* The manifest checksum is a function of content, so a restart that changes nothing does not
  look like a config change to the server.

### What this makes hard
* Anything genuinely per-function that also needs a secret — a per-function outbound
  credential, say — has nowhere to live. It belongs in the app's own configuration, and the
  workflow reads it inside a step.
* Configuration is spread across an orchestrator's secret manager rather than being visible
  in one reviewable file, so "what is this deployment actually running with?" is answered by
  `stepd doctor` and the console, not by reading the repository.

### What we accept
* The app's URL does travel in the manifest and is stored. An attacker holding an admin
  token for a namespace can repoint an app and receive its attempts. The mitigations are the
  namespace-scoped admin role and the egress policy check performed at registration
  (ADR-008), not the manifest itself.
* `app_bindings.key_hash_current` is populated in `registry.rs` with `SHA-256(app_id)` — a
  placeholder, precisely because no key material travels. The column reads as if it held a
  key digest and does not; it should either hold a digest of the configured key or be
  dropped. Recorded here rather than left to be rediscovered.
* The Rust SDK has no `App::from_env()` helper yet. The binding fields are constructor
  arguments, so a 12-factor app must read the variables itself. The rule this ADR enforces
  is that they never enter a `FunctionConfig`; that they arrive via `std::env` is currently
  the application's responsibility.

## Alternatives considered

| Option | Why not |
|---|---|
| Deployment config in a checked-in file (`stepd.toml`) | The file is per-environment, so either it is templated at deploy time — which is environment variables with extra steps — or it is edited per environment, which is the promotion hazard above. |
| Signing key in the manifest, encrypted | Something must hold the decryption key, supplied by the environment. The scheme reduces to this decision plus an encryption layer that can be got wrong. |
| Server reads app config from the database at dispatch time | The database row came from a tenant-supplied manifest. Dispatch behaviour would then be tenant-controlled. |
| One signing key, rotate with a flag day | Requires simultaneous restart of server and every app. In practice the rotation is deferred indefinitely, which is worse than either key. |
| Infer the environment name from the hostname | Silent misclassification. A renamed host quietly becomes a different environment. |

## Verification

* `sdk/rust/crates/stepd-sdk/src/function.rs`, test `the_manifest_carries_no_credentials`:
  builds an `App` with `signing_key(b"super-secret")`, serialises `manifest()`, and asserts
  the string does not contain the key. This is the single assertion the whole rule rests on.
* Same file, `an_app_without_a_key_fails_closed`: a missing key is a lint finding, never a
  silent slide into unsigned requests.
* Same file, `the_manifest_checksum_is_stable_across_restarts`: the checksum covers sorted
  content, so hash-map iteration order cannot masquerade as a config change.
* `engine/rust/crates/stepd-server/src/lib.rs`, tests `configuration_comes_only_from_the_environment`,
  `rotation_accepts_two_keys_at_once` and `the_default_egress_policy_fails_closed`.
* `engine/rust/crates/stepd-server/src/registry.rs`: `DbTargetResolver` takes keys from `Config`;
  the doc comment states why they are not read from the manifest.
* `engine/rust/crates/stepd-cli/src/main.rs`, `serve()`: bails with a message naming
  `STEPD_SIGNING_KEY` and protocol §9 rather than starting unconfigured.
