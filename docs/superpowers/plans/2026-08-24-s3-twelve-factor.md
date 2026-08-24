# S3 Backend Twelve-Factor Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three twelve-factor gaps in the S3 blob backend — a
backing service addressable from only one network vantage point, credentials
that cannot be temporary, and a production code path no CI lane runs.

**Architecture:** Two of the three are additive config fields that default to
today's behaviour exactly: a second `Bucket` used only for presigning, and an
optional session token on the credentials. The third re-applies a CI lane that
was written, reviewed as correct, and then reverted for how it landed rather
than for what it said.

**Tech Stack:** Rust, `rusty-s3` 0.10.2, `reqwest`, GitHub Actions, MinIO
`RELEASE.2025-09-07T16-13-09Z`.

**Spec:** No separate spec document. The findings this plan implements are in
the audit recorded in this session and reproduced per-task below, each with the
`file:line` evidence it rests on.

## Global Constraints

- **`.github/workflows/ci.yml` is protected by a hook that blocks Edit and
  Write on that path.** Commit `d59af82` reverted a previous change to it that
  reached the file through Bash instead, on the stated grounds that "a guard
  that can be walked around by choosing a different tool is not a guard."
  **Task 3 must not write that file through Bash, `sed`, `tee`, a heredoc, or
  any other tool chosen to avoid the hook.** Attempt `Edit` and let the hook
  do its job; if it refuses, hand the diff to the file's owner.
- **Don't overstate in docs or comments** (CLAUDE.md). Every comment this plan
  falsifies is listed in the task that falsifies it, and updating it is a step
  in that task, not a follow-up.
- **Per-instance state never goes in a `static`** (CLAUDE.md). Nothing here
  introduces one; the config tests call `blob_backend_from` against a fixed map
  rather than `std::env::set_var`, and new tests must follow that.
- **The workspace builds with `-D warnings`.** `cargo fmt --all && cargo clippy
  --workspace --all-targets -- -D warnings` must pass before each commit.
- **Adding a field to `S3Config` breaks every struct literal.** There is no
  `Default`. Both tasks 1 and 2 add one, so both must update all four
  construction sites listed in the File Structure below.

---

## File Structure

| File | Responsibility | Touched by |
|---|---|---|
| `rust/crates/stepd-blobs-s3/src/lib.rs` | `S3Config`, `S3Backend`, the four `BlobBackend` methods, `check_bucket`, unit tests | 1, 2 |
| `rust/crates/stepd-blobs-s3/tests/live.rs` | Live suite; **constructs `S3Config` literally at ~line 55** | 1, 2 |
| `rust/crates/stepd-server/src/lib.rs` | `S3ConfigInput`, `EndpointInput`, `resolve`, `blob_backend_from`, config tests | 1, 2 |
| `rust/crates/stepd-server/tests/end_to_end.rs` | S3 end-to-end; **constructs `S3Config` literally at ~line 1526**; stale CI comment at 1840-1847 | 1, 2, 3 |
| `.env.example` | Operator-facing env documentation | 1, 2 |
| `compose.yaml` | Stale claim that a second endpoint needs a code change | 1 |
| `docs/blob-backends.md` | Object-store compatibility notes | 2 |
| `README.md` | Honest-gaps section on CI coverage | 3 |
| `.github/workflows/ci.yml` | **Hook-protected.** The S3 lane | 3 |

---

### Task 1: Presign against a public endpoint (Factor IV)

**Finding:** `read_url` (`stepd-blobs-s3/src/lib.rs:308`) and `upload_target`
(`:259`) presign against `self.bucket`, built from the single
`STEPD_BLOB_S3_ENDPOINT`. That same endpoint is what `stored` (`:323`),
`delete` (`:394`) and `check_bucket` (`:523`) call for their own metadata
requests. So the object store must answer at one identical address from both
this process and every application. `compose.yaml` already documents the
consequence and calls the fix "a code change rather than a compose one".

**Files:**
- Modify: `rust/crates/stepd-blobs-s3/src/lib.rs` (struct at `:104`, `:143`; `with_timeouts` at `:167`; call sites at `:270`, `:310`, `:325`, `:396`, `:523`)
- Modify: `rust/crates/stepd-server/src/lib.rs` (`S3ConfigInput` at `:175`, `Debug` at `:189`, `resolve` at `:213`, `blob_backend_from` at `:496`, `probe_s3_config` at `:895`)
- Modify: `rust/crates/stepd-blobs-s3/tests/live.rs` (`config()` at ~`:55`)
- Modify: `rust/crates/stepd-server/tests/end_to_end.rs` (`s3_config()` at ~`:1526`)
- Modify: `.env.example`, `compose.yaml`
- Test: same files (`#[cfg(test)]` modules, in-crate)

**Interfaces:**
- Produces: `S3Config { pub public_endpoint: Option<Url>, .. }`;
  `S3ConfigInput { pub public_endpoint: EndpointInput, .. }`;
  `S3Backend { internal: Bucket, presign: Bucket, .. }` (private fields).
- Consumes: nothing from other tasks. Task 2 adds a second field to the same
  two structs and must keep this one.

- [ ] **Step 1: Write the failing tests**

In the `#[cfg(test)] mod tests` of `rust/crates/stepd-blobs-s3/src/lib.rs`:

```rust
/// A config whose two endpoints differ, so a test can tell which one signed.
fn split_endpoint_config() -> S3Config {
    S3Config {
        endpoint: "http://minio:9000".parse().unwrap(),
        public_endpoint: Some("https://blobs.example.com".parse().unwrap()),
        region: "us-east-1".into(),
        bucket: "stepd".into(),
        access_key: "probe".into(),
        secret_key: "probeprobe".into(),
        path_style: true,
    }
}

#[test]
fn a_read_url_is_signed_against_the_public_endpoint_when_one_is_set() {
    let b = S3Backend::new(split_endpoint_config()).expect("a complete config builds");
    let url = b
        .read_url(Uuid::nil(), 0, Duration::seconds(60))
        .expect("a positive ttl signs");
    assert!(
        url.starts_with("https://blobs.example.com/"),
        "an app-facing URL must carry the address apps can reach; got {url}"
    );
}

#[tokio::test]
async fn an_upload_target_is_signed_against_the_public_endpoint_when_one_is_set() {
    let b = S3Backend::new(split_endpoint_config()).expect("a complete config builds");
    let spec = BlobSpec {
        size: 5,
        sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
        content_type: None,
    };
    let t = b
        .upload_target(Uuid::nil(), &spec, Duration::seconds(60))
        .await
        .expect("a complete spec signs");
    assert!(
        t.url.starts_with("https://blobs.example.com/"),
        "got {}",
        t.url
    );
}

#[test]
fn no_public_endpoint_signs_against_the_only_endpoint_there_is() {
    // The default must be byte-for-byte today's behaviour: an operator who
    // never sets the new variable must not get a different URL than before.
    let cfg = S3Config {
        public_endpoint: None,
        ..split_endpoint_config()
    };
    let b = S3Backend::new(cfg).expect("a complete config builds");
    let url = b
        .read_url(Uuid::nil(), 0, Duration::seconds(60))
        .expect("a positive ttl signs");
    assert!(url.starts_with("http://minio:9000/"), "got {url}");
}
```

In the `#[cfg(test)] mod tests` of `rust/crates/stepd-server/src/lib.rs`:

```rust
#[test]
fn a_public_endpoint_that_is_set_but_unusable_is_refused_by_name() {
    // Same reasoning as `an_endpoint_that_is_set_but_unusable_is_not_reported
    // _as_unset`: silently dropping an unparseable value hands apps URLs
    // pointing at the wrong host, and reporting it as unset sends the
    // operator to look at a variable that is plainly right there.
    let c = S3ConfigInput {
        public_endpoint: EndpointInput::Unparsed("blobs.example.com".into()),
        ..probe_s3_config()
    };
    let err = c.resolve().expect_err("an unparseable public endpoint must not resolve");
    assert!(
        err.to_string().contains("STEPD_BLOB_S3_PUBLIC_ENDPOINT")
            && err.to_string().contains("blobs.example.com"),
        "got {err}"
    );
}

#[test]
fn an_unset_public_endpoint_resolves_to_no_override() {
    let c = probe_s3_config().resolve().expect("the probe config is complete");
    assert_eq!(c.public_endpoint, None);
}

#[test]
fn a_public_endpoint_reaches_the_config_from_the_environment() {
    let cfg = blob_backend_from(lookup(&[
        ("STEPD_BLOB_BACKEND", "s3"),
        ("STEPD_BLOB_S3_ENDPOINT", "http://minio:9000"),
        ("STEPD_BLOB_S3_PUBLIC_ENDPOINT", "https://blobs.example.com"),
        ("STEPD_BLOB_S3_BUCKET", "stepd"),
        ("STEPD_BLOB_S3_ACCESS_KEY", "probe"),
        ("STEPD_BLOB_S3_SECRET_KEY", "probeprobe"),
    ]));
    match cfg {
        BlobBackendConfig::S3(s) => assert_eq!(
            s.public_endpoint,
            EndpointInput::Url("https://blobs.example.com".parse().unwrap())
        ),
        other => panic!("expected the S3 backend, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd rust && cargo test -p stepd-blobs-s3 --lib && cargo test -p stepd-server --lib
```

Expected: FAIL to **compile** — `S3Config` has no field `public_endpoint`,
`S3ConfigInput` has no field `public_endpoint`. A compile failure is the
correct red here; there is no runtime behaviour to observe yet.

- [ ] **Step 3: Add the field to `S3Config` and split the bucket**

In `rust/crates/stepd-blobs-s3/src/lib.rs`, add to `S3Config` after `endpoint`:

```rust
    /// Base URL to sign *app-facing* URLs against, when applications reach the
    /// object store at a different address than this process does.
    ///
    /// `None` signs against [`Self::endpoint`], which is what a deployment
    /// where both sides share one address wants and is the behaviour this
    /// backend had before the field existed.
    ///
    /// The split exists because one address cannot serve both vantage points.
    /// The server calls the store directly for `HeadObject` and
    /// `DeleteObject`, so it needs an address reachable from wherever the
    /// server runs — a cluster-internal service name, typically. Every
    /// presigned URL is handed to an application that may be nowhere near
    /// that network, and carries whatever host it was signed with, because
    /// SigV4 signs the host. Before this field, `minio:9000` worked for a
    /// sibling container and not a host app, and `localhost:9000` the
    /// reverse, with no value that worked for both.
    pub public_endpoint: Option<Url>,
```

Add it to the hand-written `Debug` impl beside `endpoint` — it is an address,
not a credential, and an operator debugging a signature mismatch needs to see
which one signed:

```rust
            .field("endpoint", &self.endpoint)
            .field("public_endpoint", &self.public_endpoint)
```

Replace the `S3Backend` struct's `bucket` field with two:

```rust
pub struct S3Backend {
    /// Addressed by this process: `HeadObject`, `DeleteObject`, `check_bucket`.
    internal: Bucket,
    /// Addressed by applications: every presigned URL this backend mints.
    /// Identical to [`Self::internal`] unless `public_endpoint` was set.
    presign: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}
```

In `with_timeouts`, replace the single `Bucket::new` with:

```rust
        let internal = Bucket::new(
            config.endpoint,
            style,
            config.bucket.clone(),
            config.region.clone(),
        )
        .map_err(|e| Error::Config(format!("blob store endpoint is unusable: {e}")))?;
        // Cloned rather than re-derived when there is no override, so the two
        // are the same value by construction and cannot drift into signing
        // against subtly different bases.
        let presign = match config.public_endpoint {
            None => internal.clone(),
            Some(url) => Bucket::new(url, style, config.bucket, config.region).map_err(|e| {
                Error::Config(format!("blob store public endpoint is unusable: {e}"))
            })?,
        };
```

and the returned struct:

```rust
        Ok(Self {
            internal,
            presign,
            credentials: Credentials::new(config.access_key, config.secret_key),
            http,
        })
```

- [ ] **Step 4: Point each call site at the right bucket**

Exactly two use `presign`; three use `internal`. Getting one wrong is the
whole bug this task exists to prevent, so change them deliberately:

| Method | Line | Bucket | Why |
|---|---|---|---|
| `upload_target` | `:270` | `&self.presign` | the URL is handed to an app |
| `read_url` | `:310` | `&self.presign` | the URL is handed to an app |
| `stored` | `:325` | `&self.internal` | this process sends the request |
| `delete` | `:396` | `&self.internal` | this process sends the request |
| `check_bucket` | `:523` | `&self.internal` | this process sends the request |

- [ ] **Step 5: Add the field to `S3ConfigInput` and read it**

In `rust/crates/stepd-server/src/lib.rs`, add to `S3ConfigInput` after
`endpoint`:

```rust
    /// Base URL apps are handed, as `STEPD_BLOB_S3_PUBLIC_ENDPOINT` left it.
    ///
    /// [`EndpointInput`] rather than `Option<Url>` for the same reason
    /// `endpoint` uses it — a value that will not parse must be quoted back,
    /// not reported as unset. Unlike `endpoint`, [`EndpointInput::Unset`] is
    /// legitimate here and means "sign against the endpoint".
    pub public_endpoint: EndpointInput,
```

Add `.field("public_endpoint", &self.public_endpoint)` to its `Debug` impl.

In `resolve`, before the `anyhow::ensure!`:

```rust
        let public_endpoint = match &self.public_endpoint {
            EndpointInput::Unset => None,
            EndpointInput::Url(u) => Some(u.clone()),
            EndpointInput::Unparsed(raw) => {
                problems.push(format!(
                    "STEPD_BLOB_S3_PUBLIC_ENDPOINT is set to {raw:?}, which is not a URL \
                     — it needs a scheme and a host, as in https://blobs.example.com"
                ));
                None
            }
        };
```

and add `public_endpoint,` to the returned `S3Config`.

In `blob_backend_from`, after the `endpoint` field:

```rust
            public_endpoint: match lookup("STEPD_BLOB_S3_PUBLIC_ENDPOINT") {
                None => EndpointInput::Unset,
                Some(raw) => match raw.parse::<Url>() {
                    Ok(url) => EndpointInput::Url(url),
                    Err(_) => EndpointInput::Unparsed(raw),
                },
            },
```

Add `public_endpoint: EndpointInput::Unset,` to `probe_s3_config`.

- [ ] **Step 6: Fix the two test-file construction sites**

`rust/crates/stepd-blobs-s3/tests/live.rs`, in `config()`, add to the
`S3Config` literal:

```rust
        // The live suite talks to one address as both server and client, which
        // is the shape this field exists to stop being the only one possible.
        public_endpoint: std::env::var("STEPD_TEST_S3_PUBLIC_ENDPOINT")
            .ok()
            .map(|v| v.parse().expect("STEPD_TEST_S3_PUBLIC_ENDPOINT is a URL")),
```

`rust/crates/stepd-server/tests/end_to_end.rs`, in `s3_config()`, add:

```rust
        public_endpoint: None,
```

- [ ] **Step 7: Run the tests to verify they pass**

```bash
cd rust && cargo test -p stepd-blobs-s3 && cargo test -p stepd-server
```

Expected: PASS, including the six new tests. The live and end-to-end S3 suites
will print their SKIPPED notice without `STEPD_TEST_S3_ENDPOINT`; that is
expected here and is what Task 3 fixes.

- [ ] **Step 8: Correct the two comments this made false**

`compose.yaml` — the block on the `stepd` service currently says there is one
endpoint serving both vantage points and that serving both "would need a
second, public endpoint to sign with, which is a code change rather than a
compose one." Replace that paragraph with:

```yaml
      # STEPD_BLOB_S3_ENDPOINT is the address THIS SERVER calls for HeadObject
      # and DeleteObject. STEPD_BLOB_S3_PUBLIC_ENDPOINT is the address every
      # presigned URL carries, for the app to reach. Set the second only when
      # they differ: on this compose network the server needs `minio:9000`,
      # while an app on the host needs `localhost:9000`, and before the second
      # variable existed no single value served both.
      # - STEPD_BLOB_S3_ENDPOINT=http://minio:9000
      # - STEPD_BLOB_S3_PUBLIC_ENDPOINT=http://localhost:9000
```

`.env.example` — add beneath `STEPD_BLOB_S3_ENDPOINT`:

```
# Optional. The address presigned URLs carry, when apps reach the object store
# somewhere other than this server does. Unset signs against the endpoint
# above, which is right whenever both sides share one address.
# STEPD_BLOB_S3_PUBLIC_ENDPOINT=https://blobs.example.com
```

- [ ] **Step 9: Commit**

```bash
cd /Users/p.munaawa/Documents/projects/labs/stepd
cargo fmt --all --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings
git add rust/crates/stepd-blobs-s3 rust/crates/stepd-server compose.yaml .env.example
git commit -m "blobs: presign against the address apps can reach, not the one the server uses

One endpoint had to serve two network vantage points: the server's own
HeadObject and DeleteObject, and the host baked into every presigned URL
an app is handed. No single value served both when the two sit on
different networks, which is the ordinary production shape."
```

---

### Task 2: Let credentials be temporary (Factor III)

**Finding:** `Credentials::new(access_key, secret_key)`
(`stepd-blobs-s3/src/lib.rs:186`) is the only constructor used. `rusty-s3`
0.10.2 also ships `Credentials::new_with_token` for session credentials
(`credentials/mod.rs:42`), and nothing in the workspace mentions a session
token — verified by grep. That rules out IRSA, EC2/ECS instance profiles and
AssumeRole, so every deploy needs a permanent access key pair in its
environment.

**Scope, decided:** the env-var route only. `stepd` gains a way to *accept* a
temporary credential; it does not learn to *obtain or refresh* one. Whoever
injects the token owns its expiry. This is a deliberate line, and Step 6 writes
it down rather than letting an operator infer a refresh that does not happen.

**Files:**
- Modify: `rust/crates/stepd-blobs-s3/src/lib.rs` (struct `:104`, `with_timeouts` `:167`)
- Modify: `rust/crates/stepd-server/src/lib.rs` (`S3ConfigInput` `:175`, `resolve` `:213`, `blob_backend_from` `:496`, `probe_s3_config` `:895`)
- Modify: `rust/crates/stepd-blobs-s3/tests/live.rs`, `rust/crates/stepd-server/tests/end_to_end.rs`
- Modify: `.env.example`, `docs/blob-backends.md`

**Interfaces:**
- Consumes: Task 1's `public_endpoint` field on both structs — keep it.
- Produces: `S3Config { pub session_token: Option<String>, .. }`;
  `S3ConfigInput { pub session_token: Option<String>, .. }`.

- [ ] **Step 1: Write the failing tests**

In `rust/crates/stepd-blobs-s3/src/lib.rs` tests:

```rust
#[test]
fn a_session_token_reaches_the_signature() {
    let cfg = S3Config {
        session_token: Some("FQoGZXIvYXdzEExampleToken".into()),
        ..split_endpoint_config()
    };
    let b = S3Backend::new(cfg).expect("a complete config builds");
    let url = b
        .read_url(Uuid::nil(), 0, Duration::seconds(60))
        .expect("a positive ttl signs");
    assert!(
        url.contains("X-Amz-Security-Token"),
        "a temporary credential is only usable if its token is in the query; got {url}"
    );
}

#[test]
fn no_session_token_signs_without_one() {
    // A permanent key pair must sign exactly as it did before this field
    // existed. An empty or absent token that reached the query as a parameter
    // would be rejected by the store as a signature error naming nothing.
    let cfg = S3Config {
        session_token: None,
        ..split_endpoint_config()
    };
    let b = S3Backend::new(cfg).expect("a complete config builds");
    let url = b
        .read_url(Uuid::nil(), 0, Duration::seconds(60))
        .expect("a positive ttl signs");
    assert!(!url.contains("X-Amz-Security-Token"), "got {url}");
}

#[test]
fn a_session_token_does_not_reach_a_debug_line() {
    // It is a credential, exactly as much as the secret key is, and the
    // hand-written Debug on this struct exists so credentials do not reach a
    // log the first time someone prints a config.
    let cfg = S3Config {
        session_token: Some("FQoGZXIvYXdzEExampleToken".into()),
        ..split_endpoint_config()
    };
    let printed = format!("{cfg:?}");
    assert!(!printed.contains("ExampleToken"), "got {printed}");
}
```

In `rust/crates/stepd-server/src/lib.rs` tests:

```rust
#[test]
fn an_empty_session_token_is_no_token_rather_than_an_empty_one() {
    // `NAME=` in a .env file is a set variable with an empty value. Carrying
    // that through as `Some("")` would sign an empty X-Amz-Security-Token,
    // which the store rejects with a signature error that names neither the
    // token nor the variable.
    let cfg = blob_backend_from(lookup(&[
        ("STEPD_BLOB_BACKEND", "s3"),
        ("STEPD_BLOB_S3_ENDPOINT", "http://minio:9000"),
        ("STEPD_BLOB_S3_BUCKET", "stepd"),
        ("STEPD_BLOB_S3_ACCESS_KEY", "probe"),
        ("STEPD_BLOB_S3_SECRET_KEY", "probeprobe"),
        ("STEPD_BLOB_S3_SESSION_TOKEN", "   "),
    ]));
    match cfg {
        BlobBackendConfig::S3(s) => assert_eq!(s.session_token, None),
        other => panic!("expected the S3 backend, got {other:?}"),
    }
}

#[test]
fn a_session_token_reaches_the_config_from_the_environment() {
    let cfg = blob_backend_from(lookup(&[
        ("STEPD_BLOB_BACKEND", "s3"),
        ("STEPD_BLOB_S3_ENDPOINT", "http://minio:9000"),
        ("STEPD_BLOB_S3_BUCKET", "stepd"),
        ("STEPD_BLOB_S3_ACCESS_KEY", "probe"),
        ("STEPD_BLOB_S3_SECRET_KEY", "probeprobe"),
        ("STEPD_BLOB_S3_SESSION_TOKEN", "FQoGZXIvYXdzEExampleToken"),
    ]));
    match cfg {
        BlobBackendConfig::S3(s) => {
            assert_eq!(s.session_token.as_deref(), Some("FQoGZXIvYXdzEExampleToken"))
        }
        other => panic!("expected the S3 backend, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd rust && cargo test -p stepd-blobs-s3 --lib && cargo test -p stepd-server --lib
```

Expected: FAIL to compile — no field `session_token` on either struct.

- [ ] **Step 3: Add the field and use the token-bearing constructor**

In `rust/crates/stepd-blobs-s3/src/lib.rs`, add to `S3Config` after
`secret_key`:

```rust
    /// Session token, for a temporary credential.
    ///
    /// `Some` selects SigV4's `X-Amz-Security-Token`, which is what makes an
    /// AssumeRole, IRSA or instance-profile credential usable at all. `None`
    /// is a permanent key pair and signs exactly as this backend did before
    /// the field existed.
    ///
    /// This backend never obtains or refreshes one: it reads what it was
    /// given, at construction, and holds it. A token that expires is a token
    /// whose presigned URLs start failing, and the fix is a restart or a
    /// sidecar that rewrites the environment — see `docs/blob-backends.md`.
    pub session_token: Option<String>,
```

**Do not add it to the `Debug` impl.** `finish_non_exhaustive` already elides
it; the test in Step 1 is what keeps that true.

In `with_timeouts`, replace the credentials line in the returned struct with a
binding above it:

```rust
        // `new_with_token` rather than `new` whenever a token is present:
        // SigV4 rejects a temporary credential presented without its token,
        // with a signature error that names neither.
        let credentials = match config.session_token {
            Some(token) => {
                Credentials::new_with_token(config.access_key, config.secret_key, token)
            }
            None => Credentials::new(config.access_key, config.secret_key),
        };
```

and use `credentials,` in the struct literal.

- [ ] **Step 4: Read it from the environment**

In `rust/crates/stepd-server/src/lib.rs`, add to `S3ConfigInput` after
`secret_key`:

```rust
    /// Session token for a temporary credential, if there is one.
    pub session_token: Option<String>,
```

In `resolve`, add `session_token: self.session_token.clone(),` to the returned
`S3Config`. It joins no `problems` check: a token is genuinely optional, and a
deployment using a permanent key pair is not half-configured for lacking one.

In `blob_backend_from`, after `secret_key`:

```rust
            // Filtered, not passed through: `STEPD_BLOB_S3_SESSION_TOKEN=` in
            // an .env file is a set variable holding an empty string, and
            // `Some("")` would sign an empty token rather than none.
            session_token: lookup("STEPD_BLOB_S3_SESSION_TOKEN")
                .filter(|v| !v.trim().is_empty()),
```

Add `session_token: None,` to `probe_s3_config`, to `live.rs`'s `config()`, and
to `end_to_end.rs`'s `s3_config()`.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd rust && cargo test -p stepd-blobs-s3 && cargo test -p stepd-server
```

Expected: PASS, including the five new tests.

- [ ] **Step 6: Document the expiry this does not handle**

`.env.example`, beneath `STEPD_BLOB_S3_SECRET_KEY`:

```
# Optional. Set alongside the two above for a TEMPORARY credential — an
# AssumeRole, IRSA or instance-profile one. Without it those credentials are
# rejected by the store with a signature error that names neither them nor
# this variable.
#
# stepd reads this once, at startup, and never refreshes it. When the token
# expires, presigned URLs start failing; something outside stepd has to
# re-inject a fresh one and restart the process. There is no background
# refresh and no call to STS.
# STEPD_BLOB_S3_SESSION_TOKEN=
```

`docs/blob-backends.md`, as a new section at the end:

```markdown
## Temporary credentials

`STEPD_BLOB_S3_SESSION_TOKEN` carries a session token into the SigV4
signature, which is what makes an AssumeRole, IRSA or instance-profile
credential usable here at all. Set it alongside the access key and secret key;
leave it out for a permanent key pair.

What this does not do is as important as what it does. The token is read once,
when the backend is constructed, and held for the life of the process. Nothing
here calls STS, watches an expiry, or refreshes anything. So a token that
expires while `stepd serve` is running produces presigned URLs the object
store rejects, and the only fix is a fresh token in the environment and a
restart. If that is not acceptable for a deployment, a permanent key pair
scoped tightly to the bucket is the honest alternative — not this field plus
an assumption it will renew itself.
```

- [ ] **Step 7: Commit**

```bash
cd /Users/p.munaawa/Documents/projects/labs/stepd
cargo fmt --all --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings
git add rust/crates/stepd-blobs-s3 rust/crates/stepd-server .env.example docs/blob-backends.md
git commit -m "blobs: accept a session token, so the credential can be temporary

Only Credentials::new was ever used, so an AssumeRole, IRSA or
instance-profile credential could not be expressed at all and every
deployment needed a permanent key pair in its environment. This accepts a
token; it does not obtain or refresh one, and says so where an operator
will read it."
```

---

### Task 3: Run the S3 path in CI (Factor X)

**Finding:** the path production runs is the one no automated lane runs. The
default everywhere (`stepd dev`, CI, `docker compose up`) is the filesystem
relay; the S3 backend is a different code path, with `RelayBytes` deliberately
unimplemented so the relay route is not even mounted
(`stepd-blobs-s3/src/lib.rs:23-26`). Verified directly: `STEPD_TEST_S3`,
`STEPD_BLOB_BACKEND`, and any minio/rustfs image each appear **0 times** in
`.github/workflows/ci.yml`.

**This lane already exists and was reviewed as correct.** Commit `4f519a1`
added it; `d59af82` reverted it because it was written through Bash to get
around the hook protecting that file, not because anything in it was wrong —
"The change itself looks right and is kept in the task report; the way it
landed is not something to keep." So this task re-applies known-good content
through the sanctioned route.

> **Read the Global Constraints before starting this task.** Do not write
> `.github/workflows/ci.yml` through Bash, `sed`, `tee`, a heredoc, or a
> subagent chosen because it has a different tool set. Use `Edit`. If the hook
> refuses, stop and hand the diff to the file's owner — that refusal is the
> guard working, and routing around it is the exact thing `d59af82` reverted.

**Files:**
- Modify: `.github/workflows/ci.yml` (**hook-protected**; job `integration`, env block after `:107`, steps after `:110` and after `:153`)
- Modify: `README.md` (honest-gaps bullet on the S3 evidence)
- Modify: `rust/crates/stepd-server/tests/end_to_end.rs:1840-1847`

**Interfaces:**
- Consumes: nothing. Tasks 1 and 2 leave the lane's env and steps unchanged —
  both new variables are optional and unset in CI, so the lane exercises the
  same single-endpoint, permanent-credential shape it was written for.

- [ ] **Step 1: Recover the exact reverted lane**

```bash
cd /Users/p.munaawa/Documents/projects/labs/stepd
git show 4f519a1 -- .github/workflows/ci.yml
```

This is the content to re-apply. Read it in full before editing; it is three
hunks against the `integration` job — a job-level `env` addition, a MinIO
startup step, and a test step.

- [ ] **Step 2: Apply hunk 1 — job-level env**

Using `Edit`, in the `integration` job's `env:` block, after
`STEPD_TEST_DATABASE_URL`:

```yaml
      # Bucket and credentials match `stepd-blobs-s3/tests/live.rs`'s own
      # defaults, so both that suite and the step below reach the same
      # server the same way. Set at job level, not step level, so
      # `cargo test --workspace` further down already exercises the S3
      # path too -- the explicit step near the end of this job exists for
      # attribution, not because it is the only place these tests run.
      STEPD_TEST_S3_ENDPOINT: http://127.0.0.1:9000
      STEPD_TEST_S3_BUCKET: stepd
      STEPD_TEST_S3_ACCESS_KEY: probe
      STEPD_TEST_S3_SECRET_KEY: probeprobe
```

- [ ] **Step 3: Apply hunk 2 — start MinIO**

Using `Edit`, as a new step immediately before the `protocol schemas` step:

```yaml
      - name: start MinIO for the S3 blob backend lane
        # Not a `services:` container: an S3-compatible image needs a command
        # (`server /data`) to start serving at all, and the `services:` block
        # has no field for one -- only `docker run` does, so that is what
        # this is. Pinned by tag, not `latest`: docs/blob-backends.md records
        # this exact release as one confirmed to reject a presigned PUT whose
        # body does not match its signed checksum, which is the property the
        # whole S3 backend rests on. A lane whose backing image floats would
        # report a failure that is not this branch's.
        run: |
          docker run -d --name stepd-s3 -p 9000:9000 \
            -e MINIO_ROOT_USER="$STEPD_TEST_S3_ACCESS_KEY" \
            -e MINIO_ROOT_PASSWORD="$STEPD_TEST_S3_SECRET_KEY" \
            quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z server /data
          timeout 30 sh -c \
            'until curl -sf http://127.0.0.1:9000/minio/health/live; do sleep 1; done'
```

No bucket-creation step is needed: both suites create it themselves and
tolerate one already present — `live.rs`'s `ensure_bucket` and
`end_to_end.rs`'s own copy at `:1542`.

- [ ] **Step 4: Apply hunk 3 — the attributable test step**

Using `Edit`, as a new step immediately before the `conformance battery
against the reference app` step:

```yaml
      - name: blob backend against S3-compatible storage
        # BR-19: bulk payload data must never traverse the control plane.
        # `cargo test --workspace` above already runs both suites, because
        # the S3 env is set at job level; this step exists so a failure on
        # this specific path is attributable at a glance, the same reason
        # the conformance battery below is its own step rather than folded
        # into the one above.
        run: cargo test -p stepd-blobs-s3 && cargo test -p stepd-server --test end_to_end no_object_bytes
        working-directory: rust
```

- [ ] **Step 5: Verify the lane locally before trusting CI**

CI is not the place to find out the YAML is wrong. Reproduce the lane's
environment against a local MinIO and run exactly what it runs:

```bash
cd /Users/p.munaawa/Documents/projects/labs/stepd
podman run -d --name ci-probe-s3 -p 9002:9000 \
  -e MINIO_ROOT_USER=probe -e MINIO_ROOT_PASSWORD=probeprobe \
  quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z server /data
timeout 30 sh -c 'until curl -sf http://127.0.0.1:9002/minio/health/live; do sleep 1; done'

export STEPD_TEST_S3_ENDPOINT=http://127.0.0.1:9002
export STEPD_TEST_S3_BUCKET=stepd
export STEPD_TEST_S3_ACCESS_KEY=probe
export STEPD_TEST_S3_SECRET_KEY=probeprobe
cd rust && cargo test -p stepd-blobs-s3
```

Expected: PASS, and — this is the point — **no `SKIPPED:` line**. A run that
printed the skip notice has proved nothing. Then, with a database available:

```bash
cargo test -p stepd-server --test end_to_end no_object_bytes
```

Expected: PASS, no skip notice. Tear down: `podman rm -f ci-probe-s3`.

Also confirm the YAML parses:

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ci.yml parses')"
```

- [ ] **Step 6: Correct the three places that say this evidence does not exist**

All three become false the moment the lane merges, and a comment describing a
property the code lacks is worse than none.

`rust/crates/stepd-server/tests/end_to_end.rs:1840-1847` — replace the "On CI"
paragraph with:

```rust
    // On CI: `tier 2 · integration` sets both `STEPD_TEST_DATABASE_URL` and
    // `STEPD_TEST_S3_*`, and starts a pinned MinIO, so this test executes and
    // runs rather than skipping. "There" being pushes to `main` and pull
    // requests — `ci.yml:20-25` is the whole trigger, so a push to a branch
    // with no PR open still runs no lane at all.
```

`README.md` — the honest-gaps bullet beginning "**BR-19 on the S3 path is
proved by tests nothing runs automatically.**" The heading and its closing
sentence ("So this is evidence that exists and passes locally, and no evidence
that is produced automatically") are now wrong. Retitle it **"BR-19 on the S3
path is proved against one object store."** and replace that closing sentence
with:

```markdown
  `tier 2 · integration` starts a pinned MinIO and sets `STEPD_TEST_S3_*`, so
  both suites run on every push to `main` and every pull request. What is still
  narrow is the sample: one server, one release. `docs/blob-backends.md`
  records that RustFS `v1.0.0-beta.12` also rejects a mismatched presigned PUT
  but names the wrong header when it does, and no lane runs it.
```

Also update the `Managed blobs` row of the Status table, whose evidence column
says the S3 suites "need `STEPD_TEST_S3_*` and nothing automatic sets it".

- [ ] **Step 7: Commit**

```bash
cd /Users/p.munaawa/Documents/projects/labs/stepd
git add .github/workflows/ci.yml README.md rust/crates/stepd-server/tests/end_to_end.rs
git commit -m "ci: prove bytes skip the control plane on the S3 backend

Re-applies the lane from 4f519a1, which d59af82 reverted for reaching a
hook-protected file through Bash rather than for anything it said. The S3
backend is the path production runs and no lane ran it, so BR-19 on that
path was evidence that existed locally and nowhere else.

Also corrects the three comments that said this evidence does not exist."
```

- [ ] **Step 8: Watch the first run**

```bash
gh run list --limit 3 --json databaseId,status,conclusion,displayTitle
gh run view <id> --json jobs -q '.jobs[] | "\(.conclusion)\t\(.name)"'
```

If `tier 2 · integration` is red, `gh run view <id> --log-failed`. Do not
merge past a red lane: CLAUDE.md is explicit that a red lane means a claim the
README cites as evidence is not currently being produced, which outranks new
work.

---

## Self-Review

**Spec coverage.** Three findings, three tasks: Factor IV → Task 1, Factor III
→ Task 2, Factor X → Task 3. The two things I chose *not* to build are stated
where the choice binds — Task 2's scope note rules out an STS refresh loop, and
Task 3's Step 6 rewrite keeps the "one server, one release" narrowness visible
in the README rather than letting the new lane read as full coverage.

**Placeholder scan.** No TBDs. Every code step carries the actual code; every
comment rewrite carries the replacement text.

**Type consistency.** `S3Config` ends with both `public_endpoint:
Option<Url>` (Task 1) and `session_token: Option<String>` (Task 2);
`S3ConfigInput` with `public_endpoint: EndpointInput` and `session_token:
Option<String>`. Task 2's tests build on Task 1's `split_endpoint_config()`
helper via `..`, so **Task 2 depends on Task 1 having landed** — running them
out of order fails to compile. `S3Backend`'s field rename `bucket` →
`internal`/`presign` happens once, in Task 1 Step 3, and all five call sites
move in Step 4. The four `S3Config` literal sites are listed in the File
Structure table and touched by both tasks.

**One risk worth naming.** Task 1 Step 4 is the only step where a plausible
wrong answer compiles cleanly: pointing `stored` at `presign` would build,
pass every unit test, and fail only against a deployment where the two
endpoints actually differ — which CI does not exercise, because the lane sets
no public endpoint. The table in that step is the mitigation; a reviewer should
check those five lines by hand.

---

## Execution Handoff

Plan complete and saved to
`docs/superpowers/plans/2026-08-24-s3-twelve-factor.md`.
