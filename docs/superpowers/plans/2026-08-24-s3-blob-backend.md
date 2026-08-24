# S3 Blob Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make BR-19 true on the upload path — managed-blob bytes reach an
S3-compatible object store directly, never through the stepd server.

**Architecture:** Split `PostgresBlobStore` into the index (stays in Postgres:
the `blobs` row, per-namespace dedupe, `blob_refs`, collection) and a
`BlobBackend` seam that owns bytes and URL minting. Two backends implement it:
the filesystem-plus-relay one that ships today, and a new S3 one that presigns.
Digest verification moves into the object store — the presigned PUT carries a
signed `x-amz-checksum-sha256` and `Content-Length`, and `commit_blob` confirms
via `HeadObject` metadata, so no object bytes are read by the server.

**Tech Stack:** Rust 2021, `rusty-s3` (sans-IO SigV4 signer, BSD-2-Clause) plus
the `reqwest` already in the workspace, sqlx/Postgres, axum.

**Spec:** [issue #3](https://github.com/swiftugandan/stepd/issues/3). Read it
first — it names four things about the current tree that contradict the "drop-in
trait impl" framing, and this plan is shaped by them. Background:
[`docs/adr/010-payload-tiering.md`](../../adr/010-payload-tiering.md),
[`spec/PROTOCOL.md`](../../../spec/PROTOCOL.md) §8.2–8.5.

## Global Constraints

- **Bytes never traverse the control plane on a presigning backend.** A
  `commit_blob` that downloads the object satisfies every other line in this
  plan and fails its purpose. Not an acceptable fallback (issue #3).
- **`stepd-core` names no concrete backend.** The CI lane "stepd-core names no
  concrete backend" greps `cargo tree -p stepd-core` for `sqlx|reqwest|axum`.
  The `BlobBackend` trait goes in `stepd-core`; every S3 type goes in a new
  crate.
- **`stepd-proto` gains no runtime or I/O dependency.** Same lane, greps for
  `tokio|sqlx|axum|reqwest|hyper`.
- **Per-instance state never goes in a `static`.** (CLAUDE.md; three independent
  recurrences.) Backend handles live in `ServerState` / `PostgresBlobStore`.
- **The commit path lives in SQL.** Do not move blob-reference recording out of
  the trigger in `rust/migrations/0011_blob_refs.sql`.
- **`cargo deny check` must pass.** `deny.toml` allows only licences something
  already needs. `rusty-s3` is BSD-2-Clause and must be added to `allow` with a
  comment saying which crate requires it, in the same style as the others.
- **Don't overstate in docs or comments.** (CLAUDE.md.) A comment describing a
  property the code lacks is worse than none.
- Commands run from `rust/` unless stated. Database tests need
  `STEPD_TEST_DATABASE_URL`; without it they skip loudly and the run is not green.
- Every task ends `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
  before the commit. CI sets `RUSTFLAGS: -D warnings`.

---

### Task 0: Prove the checksum-verified presigned PUT works at all

The whole design rests on an object store accepting a presigned PUT whose
signature covers `x-amz-checksum-sha256`, rejecting bytes that do not match, and
reporting the checksum back on `HeadObject`. That is [a known sharp
edge](https://github.com/aws/aws-sdk-js-v3/issues/3906) even on real S3, and
RustFS is at release-candidate maturity with no documented answer. Find out
before building a seam around it. **This task's output is a committed compatibility
note, not code** — anything you write to probe is throwaway.

**Files:**
- Create: `docs/blob-backends.md`
- Scratch (do not commit): a throwaway binary under `/tmp`

- [ ] **Step 1: Start both candidate servers**

```bash
podman run -d --name probe-minio -p 9000:9000 \
  -e MINIO_ROOT_USER=probe -e MINIO_ROOT_PASSWORD=probeprobe \
  quay.io/minio/minio server /data

podman run -d --name probe-rustfs -p 9010:9000 \
  -e RUSTFS_ACCESS_KEY=probe -e RUSTFS_SECRET_KEY=probeprobe \
  rustfs/rustfs:latest
```

RustFS's env-var names and default port are not assumed to be stable — if the
container does not come up, read `podman logs probe-rustfs` and record what the
image actually wants in `docs/blob-backends.md`. That finding is part of the
deliverable.

- [ ] **Step 2: Write a throwaway probe**

In `/tmp/s3probe`, `cargo init`, add `rusty-s3 = "0.10"`, `reqwest` with
`rustls-tls`, `tokio = { version = "1", features = ["full"] }`, `sha2`,
`base64 = "0.22"`, `url = "2"`. The probe must, against a base URL and
credentials taken from argv:

1. create a bucket;
2. build `PutObject`, set `headers_mut()` to include
   `x-amz-checksum-sha256: <base64 of the sha256 of the body>` and
   `content-length: <len>`, `sign(Duration::from_secs(300))`;
3. PUT the correct bytes with exactly those headers — expect 2xx;
4. PUT *different* bytes of the same length with the same URL and headers —
   **expect a 4xx**, because this is the property the design depends on;
5. `HeadObject` with `x-amz-checksum-mode: ENABLED` and print every response
   header;
6. presign a `GetObject`, fetch it with `Range: bytes=0-3`, and confirm a
   `206` with `Content-Range`.

Use `UrlStyle::Path` — virtual-host addressing needs DNS these servers do not have.

- [ ] **Step 3: Run it against both servers and record what happened**

```bash
cd /tmp/s3probe
cargo run -- http://127.0.0.1:9000 probe probeprobe   # minio
cargo run -- http://127.0.0.1:9010 probe probeprobe   # rustfs
```

- [ ] **Step 4: Write the compatibility note**

Create `docs/blob-backends.md` with a table: server, version/tag, presigned PUT
with signed checksum header, **rejects mismatched bytes**, checksum returned by
`HeadObject`, presigned ranged GET. One row per server, plus a prose paragraph
per failure describing exactly what it did — an error body, a silent accept,
a missing header. Say plainly which servers qualify as presigning backends.

A server that accepts mismatched bytes does **not** qualify. Record that rather
than working around it; the `can_presign` answer in Task 3 is where that decision
is expressed in code.

- [ ] **Step 5: Commit**

```bash
git add docs/blob-backends.md
git commit -m "docs: what each S3-compatible server does with a checksum-signed presigned PUT"
```

- [ ] **Step 6: Stop and report before continuing**

If neither server rejects mismatched bytes, the design in Tasks 4–6 does not
hold and the plan needs revisiting before more is built on it. Say so rather than
proceeding.

---

### Task 1: Extract the `BlobBackend` seam, filesystem behaviour unchanged

Pure refactor. No behaviour changes, no new configuration, no S3. The existing
blob tests are the specification: they must pass untouched.

**Files:**
- Modify: `rust/crates/stepd-core/src/traits.rs` (add the trait after `BlobStore`)
- Create: `rust/crates/stepd-store-postgres/src/blobs/filesystem.rs`
- Modify: `rust/crates/stepd-store-postgres/src/blobs.rs`
- Test: `rust/crates/stepd-store-postgres/src/blobs/filesystem.rs` (`mod tests`)

**Interfaces:**
- Produces: `stepd_core::traits::BlobBackend` with
  `async fn upload_target(&self, id: Uuid, spec: &BlobSpec, ttl: Duration) -> Result<UploadTarget>`,
  `fn read_url(&self, id: Uuid, size: i64, ttl: Duration) -> Result<String>`,
  `async fn stored(&self, id: Uuid) -> Result<Option<StoredObject>>`,
  `async fn delete(&self, id: Uuid) -> Result<()>`,
  `fn can_presign(&self) -> bool`.
- Produces: `UploadTarget { url: String, method: String, headers: Vec<(String, String)>, expires_at: DateTime<Utc> }`
  and `StoredObject { size: i64, sha256: Option<String> }`.
- Produces: `stepd_store_postgres::blobs::FilesystemBackend::new(root: PathBuf, caps: Capability) -> Self`.

`read_url` is deliberately **synchronous and infallible-ish**: `attach_read_urls`
(`rust/crates/stepd-store-postgres/src/lib.rs:374`) walks a journal from a sync
context, and SigV4 presigning is pure HMAC with no network, so both backends can
answer without `await`. Making it async would force that walk async — a much
larger change that should be argued for, not discovered.

`stored` returns `sha256: Option<String>` because the filesystem backend must
hash to know it and S3 reports it from metadata. `None` means "this backend
cannot tell you without reading the bytes" and the caller falls back to reading
them — which is correct for the filesystem, and is why Task 4 asserts S3 never
returns `None`.

- [ ] **Step 1: Write the failing test**

In a new `rust/crates/stepd-store-postgres/src/blobs/filesystem.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn backend() -> (tempfile::TempDir, FilesystemBackend) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let caps = Capability::new("http://localhost:8080", b"k".to_vec());
        (
            FilesystemBackend::new(dir.path().to_path_buf(), caps),
            // NB: return the TempDir too, or it is deleted before the test runs.
        )
    }

    #[tokio::test]
    async fn a_stored_object_reports_its_size_but_not_a_digest() {
        // The filesystem cannot answer "what is this object's sha256" without
        // reading it, so it says so rather than lying or hashing eagerly. An S3
        // backend answers from metadata; that difference is the whole point of
        // `Option` here.
        let (_dir, b) = backend();
        b.put_bytes(Uuid::nil(), b"hello").await.expect("stored");
        let got = b.stored(Uuid::nil()).await.expect("queried").expect("present");
        assert_eq!(got.size, 5);
        assert_eq!(got.sha256, None);
    }

    #[tokio::test]
    async fn a_missing_object_is_absent_rather_than_an_error() {
        let (_dir, b) = backend();
        assert!(b.stored(Uuid::nil()).await.expect("queried").is_none());
    }

    #[test]
    fn the_filesystem_backend_admits_it_cannot_presign() {
        // This is what mounts the relay route in Task 3. If it ever returns
        // true, bytes stop being relayed and start being lost.
        let (_dir, b) = backend();
        assert!(!b.can_presign());
    }
}
```

Fix the `backend()` helper to return `(TempDir, FilesystemBackend)` properly —
the comment above marks the trap deliberately.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p stepd-store-postgres --lib filesystem
```

Expected: FAIL, `cannot find type FilesystemBackend`.

- [ ] **Step 3: Add the trait to `stepd-core`**

In `rust/crates/stepd-core/src/traits.rs`, directly after the `BlobStore` block:

```rust
/// Where a blob's bytes live, and who mints the URLs that reach them.
///
/// Split out of [`BlobStore`] because the index is the same everywhere — the
/// row, the per-namespace dedupe, the references recorded by trigger — and only
/// the bytes and the URLs vary. Writing a second `BlobStore` to change where
/// bytes live would duplicate the correctness-bearing half to swap the
/// mechanical one.
#[async_trait]
pub trait BlobBackend: Send + Sync + 'static {
    /// Where to PUT the bytes for a reserved blob, and what to send with them.
    ///
    /// A presigning backend MUST bind the declared size and digest into what it
    /// returns, so the store itself refuses bytes that do not match. That is
    /// what lets `stored` answer without reading the object.
    async fn upload_target(&self, id: Uuid, spec: &BlobSpec, ttl: chrono::Duration)
        -> Result<UploadTarget>;

    /// A read-scoped, short-lived URL.
    ///
    /// Synchronous because the journal walk that mints these
    /// (`attach_read_urls`) is synchronous, and neither an HMAC capability nor
    /// SigV4 needs the network to sign.
    fn read_url(&self, id: Uuid, size: i64, ttl: chrono::Duration) -> Result<String>;

    /// What the backend holds for `id`, without transferring the object.
    ///
    /// `sha256` is `None` when the backend cannot answer from metadata; the
    /// caller then falls back to reading the bytes, which is correct for a local
    /// filesystem and defeats the purpose on object storage.
    async fn stored(&self, id: Uuid) -> Result<Option<StoredObject>>;

    /// Remove an object. Absent is success: collection must be idempotent.
    async fn delete(&self, id: Uuid) -> Result<()>;

    /// Whether this backend issues URLs that reach the bytes directly.
    ///
    /// `false` mounts protocol §8.3.2's relay and its warning. A backend that
    /// answers `true` without truly presigning does not slow anything down — it
    /// hands apps URLs that go nowhere.
    fn can_presign(&self) -> bool;
}

/// Where to send bytes for a reserved blob.
#[derive(Debug, Clone)]
pub struct UploadTarget {
    /// URL to send them to.
    pub url: String,
    /// HTTP method.
    pub method: String,
    /// Headers the caller must send verbatim. On a presigning backend these are
    /// signed, so altering or dropping one makes the upload fail rather than
    /// succeed unverified.
    pub headers: Vec<(String, String)>,
    /// When the URL stops working.
    pub expires_at: DateTime<Utc>,
}

/// What a backend holds, as metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    /// Size in bytes.
    pub size: i64,
    /// Lowercase hex SHA-256, when the backend knows it without reading bytes.
    pub sha256: Option<String>,
}
```

- [ ] **Step 4: Move the filesystem bytes into the new module**

Turn `blobs.rs` into `blobs/mod.rs` + `blobs/filesystem.rs`. Move `blob_path`,
`is_within`, `put_bytes` and `get_bytes` into `filesystem.rs` as inherent methods
on `FilesystemBackend`, then implement `BlobBackend` for it:

```rust
/// Bytes on a local filesystem root, reached through the server's relay.
///
/// What makes `stepd dev` work with no cloud account, and what CI's default
/// lane uses. It cannot presign — there is no signer in front of a directory —
/// so §8.3.2's relay applies and `can_presign` says so.
pub struct FilesystemBackend {
    root: PathBuf,
    caps: Capability,
}

#[async_trait]
impl BlobBackend for FilesystemBackend {
    async fn upload_target(&self, id: Uuid, spec: &BlobSpec, ttl: Duration) -> Result<UploadTarget> {
        let (url, expires_at) = self.caps.url(id, "write", spec.size, ttl);
        let mut headers = vec![("content-length".into(), spec.size.to_string())];
        if let Some(ct) = &spec.content_type {
            headers.push(("content-type".into(), ct.clone()));
        }
        Ok(UploadTarget { url, method: "PUT".into(), headers, expires_at })
    }

    fn read_url(&self, id: Uuid, size: i64, ttl: Duration) -> Result<String> {
        Ok(self.caps.url(id, "read", size, ttl).0)
    }

    async fn stored(&self, id: Uuid) -> Result<Option<StoredObject>> {
        match tokio::fs::metadata(self.path(id)).await {
            Ok(m) => Ok(Some(StoredObject { size: m.len() as i64, sha256: None })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Store(e.to_string())),
        }
    }

    async fn delete(&self, id: Uuid) -> Result<()> {
        match tokio::fs::remove_file(self.path(id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Store(e.to_string())),
        }
    }

    fn can_presign(&self) -> bool {
        false
    }
}
```

Keep `Capability` exactly as it is. Its four tests still pass with no database,
which is the property that makes the signing rules testable at all.

- [ ] **Step 5: Make `PostgresBlobStore` delegate**

Replace the `root: PathBuf` and `caps: Capability` fields with
`backend: Arc<dyn BlobBackend>`, plus `caps: Capability` retained **only** so the
server's relay route can verify capabilities. In the `BlobStore` impl:

- `reserve` calls `self.backend.upload_target(id, &spec, Duration::seconds(300))`
  and maps it into `Reservation::Upload`.
- `presign_read` calls `self.backend.read_url(id, size, ttl)` after the same
  `state='committed'` lookup it does now.
- `commit_blob` calls `self.backend.stored(id)`, and:
  - `None` → `Error::Store("no such blob {id}")`;
  - `Some(o)` where `o.size != declared_size` → the existing
    `blob_digest_mismatch` error, unchanged in wording;
  - `Some(o)` with `sha256: Some(d)` → compare `d` to the declared digest, no
    bytes read;
  - `Some(o)` with `sha256: None` → read the bytes and hash them, exactly as
    today. **Leave a comment saying this branch is the filesystem's, and that a
    presigning backend reaching it means verification silently moved back into
    the control plane.**
- `collect` calls `self.backend.delete(id)` in place of `remove_file`, keeping
  bytes-before-row ordering.

Add a constructor that takes a backend, and keep `PostgresBlobStore::new(pool,
root, base_url, key)` as a thin wrapper building a `FilesystemBackend`, so the
four call sites outside this module
(`stepd-server/src/lib.rs:262,326`, `stepd-server/src/blobs.rs:324`,
`stepd-store-postgres/src/lib.rs:135`) do not change in this task.

- [ ] **Step 6: Run the whole blob surface**

```bash
export STEPD_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/stepd_rust"
cargo test -p stepd-store-postgres --lib
cargo test -p stepd-server --test end_to_end a_blob
cargo test -p stepd-conformance --test battery -- --nocapture
```

Expected: PASS, including `a_blob_round_trips_through_the_real_transfer_endpoints`
and `a_blob_capability_cannot_be_repurposed`, **unmodified**. If you changed a
test to make it pass, you changed behaviour and this task's premise is broken.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add rust/crates/stepd-core/src/traits.rs rust/crates/stepd-store-postgres/src/blobs.rs rust/crates/stepd-store-postgres/src/blobs/
git commit -m "blobs: separate where bytes live from the index that tracks them"
```

---

### Task 2: Route the read path through the backend

Today `PostgresStore` holds a `Capability` and `attach_read_urls` mints relay
URLs directly (`lib.rs:129`, `lib.rs:374`). `BlobStore::presign_read` exists and
nothing production calls it. After this task the journal walk goes through the
backend, so an S3 backend's URLs actually reach attempts.

**Files:**
- Modify: `rust/crates/stepd-store-postgres/src/lib.rs:129`, `:374`
- Modify: `rust/crates/stepd-store-postgres/src/blobs.rs` (`attach_read_urls`)
- Modify: `rust/crates/stepd-server/src/lib.rs:316`
- Test: `rust/crates/stepd-store-postgres/src/blobs.rs` (`mod tests`)

**Interfaces:**
- Consumes: `BlobBackend::read_url` from Task 1.
- Produces: `attach_read_urls(backend: &dyn BlobBackend, value: &mut serde_json::Value, ttl: Duration)`
  — same recursion, same in-place mutation, different minter.
- Produces: `PostgresStore::with_blob_backend(backend: Arc<dyn BlobBackend>) -> Self`,
  replacing `with_blob_capability`.

- [ ] **Step 1: Write the failing test**

Add to `blobs.rs`'s `mod tests`:

```rust
/// A backend that mints an unmistakable URL, so a test can prove which minter ran.
struct StubBackend;

#[async_trait]
impl BlobBackend for StubBackend {
    async fn upload_target(&self, _: Uuid, _: &BlobSpec, _: Duration) -> Result<UploadTarget> {
        unimplemented!("not exercised by the read path")
    }
    fn read_url(&self, id: Uuid, size: i64, _: Duration) -> Result<String> {
        Ok(format!("stub://{id}/{size}"))
    }
    async fn stored(&self, _: Uuid) -> Result<Option<StoredObject>> { Ok(None) }
    async fn delete(&self, _: Uuid) -> Result<()> { Ok(()) }
    fn can_presign(&self) -> bool { true }
}

#[test]
fn read_urls_are_minted_by_the_backend_not_by_the_relay_capability() {
    // The bug this guards: an S3 backend is configured, and attempts keep
    // receiving relay URLs because the journal walk still holds a Capability.
    // Every byte then goes through the server exactly as before, silently.
    let id = Uuid::now_v7();
    let mut v = serde_json::json!({
        "receipts": [ { "file": { "$blob": { "id": id, "size": 7, "sha256": "ab" } } } ]
    });
    attach_read_urls(&StubBackend, &mut v, Duration::seconds(60));
    assert_eq!(
        v["receipts"][0]["file"]["$blob"]["url"],
        serde_json::json!(format!("stub://{id}/7")),
        "the backend's URL must reach the attempt"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p stepd-store-postgres --lib read_urls_are_minted
```

Expected: FAIL — `attach_read_urls` takes `&Capability`.

- [ ] **Step 3: Change the signature and the field**

`attach_read_urls` takes `&dyn BlobBackend` and calls `backend.read_url(id, size, ttl)`.
A `read_url` error means no `url` key is inserted for that reference — the SDK
already reports that precisely (`BlobError::NoReadUrl`), which beats failing the
whole attempt because one of forty references could not be signed. Log it at
`warn` with the blob id.

In `lib.rs`, replace `blob_caps: Option<Arc<Capability>>` with
`blob_backend: Option<Arc<dyn BlobBackend>>` and `with_blob_capability` with
`with_blob_backend`. Update the call at `:374`.

- [ ] **Step 4: Update the server's wiring**

In `rust/crates/stepd-server/src/lib.rs`, build the `FilesystemBackend` once and
hand the same `Arc` to both `store.with_blob_backend(..)` and the
`PostgresBlobStore`. Two backends constructed from the same configuration would
work today and diverge the moment one takes a different code path — the store
minting reads against one bucket while uploads land in another is exactly the
silent-corruption shape this project keeps finding.

- [ ] **Step 5: Run everything that ships a journal**

```bash
cargo test -p stepd-store-postgres --lib
cargo test -p stepd-server --test end_to_end
cargo test -p stepd-conformance --test battery -- --nocapture
```

Expected: PASS. `a_blob_round_trips_through_the_real_transfer_endpoints`
exercises exactly this path and must still pass unmodified.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add rust/crates/stepd-store-postgres/src rust/crates/stepd-server/src/lib.rs
git commit -m "blobs: mint read URLs through the backend, so presign_read is the code that runs"
```

---

### Task 3: Mount the relay only for a backend that cannot presign

§8.3.2 calls the relay a compatibility fallback. Today it is mounted
unconditionally and warns on every request. After this task the route exists
only when the configured backend genuinely cannot presign — which is what the
issue's "the relay warning fires only when a store genuinely cannot presign"
asks for.

**Files:**
- Modify: `rust/crates/stepd-server/src/blobs.rs:44-52` (`router`), `:184`, `:243`
- Modify: `rust/crates/stepd-server/src/lib.rs:406`
- Test: `rust/crates/stepd-server/tests/end_to_end.rs`

**Interfaces:**
- Consumes: `BlobBackend::can_presign` from Task 1.
- Produces: `blobs::router(relay: bool) -> Router<ServerState>` — always mounts
  `POST /v1/blobs:reserve`; mounts `/v1/blobs/{id}/content` only when `relay`.

- [ ] **Step 1: Write the failing test**

In `end_to_end.rs`:

```rust
#[tokio::test]
async fn a_presigning_backend_does_not_expose_the_relay_route() {
    // The relay is a fallback (§8.3.2). Leaving it mounted next to a backend
    // that presigns leaves a second, unwarned path to the same bytes — and the
    // capability it accepts is signed with a different key than the one the
    // object store checks.
    let f = fixture_with_presigning_backend().await;
    let res = reqwest::Client::new()
        .put(format!("{}/v1/blobs/{}/content?dir=write&size=1&exp=1&sig=x",
                     f.base_url, Uuid::now_v7()))
        .body("x")
        .send()
        .await
        .expect("the server answered");
    assert_eq!(res.status(), 404, "the relay route must not exist here");
}
```

`fixture_with_presigning_backend` builds the existing fixture with a stub backend
whose `can_presign()` is `true`. Follow the fixture pattern already in this file;
do not introduce a `static` for it (CLAUDE.md — per-instance state in state, not
statics; this has gone wrong three times).

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p stepd-server --test end_to_end a_presigning_backend
```

Expected: FAIL with 400 (`bad_capability`) rather than 404 — the route is mounted.

- [ ] **Step 3: Make the router conditional**

```rust
/// Mount the transfer endpoints.
///
/// `relay` mounts §8.3.2's compatibility route. It is passed rather than
/// assumed because a backend that presigns has no use for it, and an unused
/// route that accepts bytes is a second way in that nothing warns about.
pub fn router(relay: bool) -> Router<ServerState> {
    let r = Router::new().route("/v1/blobs:reserve", post(reserve));
    if !relay {
        return r;
    }
    r.route("/v1/blobs/{id}/content", get(read_content).put(write_content))
}
```

At `lib.rs:406`, pass `blobs.as_ref().map(|b| !b.can_presign()).unwrap_or(false)`.

Move the `warn!` at `blobs.rs:221` out of the per-request path and into startup —
once, naming the backend — **and leave the per-request warning in place too**.
§8.3.2 says servers SHOULD warn when the relay is used, and a startup line does
not tell an operator that it is still happening at three in the morning.

- [ ] **Step 4: Run it**

```bash
cargo test -p stepd-server --test end_to_end
```

Expected: PASS, including the existing relay tests, which run against the
filesystem backend and therefore still have their route.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add rust/crates/stepd-server
git commit -m "blobs: mount the relay only where a backend cannot presign (§8.3.2)"
```

---

### Task 4: The S3 backend

**Files:**
- Create: `rust/crates/stepd-blobs-s3/Cargo.toml`
- Create: `rust/crates/stepd-blobs-s3/src/lib.rs`
- Modify: `rust/Cargo.toml` (workspace members, `rusty-s3` and `base64` in
  `[workspace.dependencies]`)
- Modify: `rust/deny.toml` (BSD-2-Clause)
- Test: `rust/crates/stepd-blobs-s3/tests/live.rs`

**Interfaces:**
- Consumes: `BlobBackend`, `UploadTarget`, `StoredObject` from Task 1.
- Produces: `stepd_blobs_s3::S3Backend::new(config: S3Config) -> Result<S3Backend>`
  and `S3Config { endpoint: Url, region: String, bucket: String, access_key: String, secret_key: String, path_style: bool }`.

Object key is the blob id, not the digest: `blobs/{shard}/{id}`, matching the
filesystem's sharding. Keying by digest would make dedupe implicit and collection
wrong — two `blobs` rows in different namespaces can share a digest, and deleting
one must not delete the other's bytes. Per-namespace dedupe stays a Postgres
lookup, where the cross-tenant reasoning in ADR-010 already lives.

- [ ] **Step 1: Write the failing tests**

Two unit tests that need no network, in `src/lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> S3Backend {
        S3Backend::new(S3Config {
            endpoint: "http://127.0.0.1:9000".parse().unwrap(),
            region: "us-east-1".into(),
            bucket: "stepd".into(),
            access_key: "probe".into(),
            secret_key: "probeprobe".into(),
            path_style: true,
        })
        .expect("a backend builds from static configuration")
    }

    #[tokio::test]
    async fn an_upload_target_binds_the_digest_and_the_length() {
        // This is the property that lets `stored` answer from metadata. If the
        // checksum header stops being signed, S3 accepts any bytes and
        // verification silently moves back into the control plane.
        let spec = BlobSpec {
            size: 5,
            sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            content_type: Some("text/plain".into()),
            filename: None,
        };
        let t = backend()
            .upload_target(Uuid::nil(), &spec, chrono::Duration::seconds(300))
            .await
            .expect("a target");

        let names: Vec<String> = t.headers.iter().map(|(k, _)| k.to_lowercase()).collect();
        assert!(names.iter().any(|k| k == "x-amz-checksum-sha256"), "got {names:?}");
        assert!(names.iter().any(|k| k == "content-length"), "got {names:?}");

        let signed = t.url.split("X-Amz-SignedHeaders=").nth(1).expect("signed headers");
        assert!(signed.contains("x-amz-checksum-sha256"),
                "the checksum must be signed, not merely sent: {signed}");
        assert!(signed.contains("content-length"),
                "the length must be signed, or a URL stores more than was reserved: {signed}");
    }

    #[test]
    fn the_declared_digest_is_sent_base64_not_hex() {
        // S3 wants base64 of the raw digest; sending the hex string is accepted
        // as a header and then never matches, so every upload fails at commit
        // with a mismatch that looks like app corruption.
        let hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(
            checksum_header(hex).expect("valid hex"),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
    }
}
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cargo test -p stepd-blobs-s3
```

Expected: FAIL — the crate does not exist.

- [ ] **Step 3: Create the crate**

`Cargo.toml`, matching the workspace style of `stepd-transport-http`:

```toml
[package]
name = "stepd-blobs-s3"
description = "S3-compatible blob backend: presigned upload and download, digest verified by the object store."
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
rust-version.workspace = true

[dependencies]
stepd-core.workspace = true
async-trait.workspace = true
chrono.workspace = true
uuid.workspace = true
tracing.workspace = true
reqwest.workspace = true
hex.workspace = true
rusty-s3 = "0.10"
base64 = "0.22"
url = "2"
```

Add `"crates/stepd-blobs-s3"` to `members`, and `rusty-s3`/`base64`/`url` to
`[workspace.dependencies]`.

In `deny.toml`, add to `allow` **with a comment naming what needs it**, matching
the file's existing style:

```toml
    "BSD-2-Clause",       # rusty-s3, the SigV4 signer behind the S3 blob backend
```

- [ ] **Step 4: Implement**

```rust
/// Lowercase hex SHA-256 → the base64 form S3 wants in `x-amz-checksum-sha256`.
fn checksum_header(hex_digest: &str) -> Result<String> {
    use base64::Engine;
    let raw = hex::decode(hex_digest)
        .map_err(|_| Error::Config("sha256 must be lowercase hex".into()))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
}
```

`upload_target`: build `PutObject::new(&bucket, &key)`, insert
`x-amz-checksum-sha256` and `content-length` via `headers_mut()`, plus
`content-type` when the spec has one, then `sign(ttl)`. Return the signed
headers in `UploadTarget.headers` so the SDK replays them verbatim — it already
does (`stepd-sdk/src/blobs.rs`).

`read_url`: `GetObject::new(..).sign(ttl)`. `Range` is added by the reader and is
not signed, which is what keeps §8.3.3's ranged reads working through a
presigned URL.

`stored`: presign a `HeadObject` with `x-amz-checksum-mode: ENABLED`, send it
with `reqwest`, and read `content-length` and `x-amz-checksum-sha256` from the
response headers. Decode the base64 back to lowercase hex. **404 → `Ok(None)`.**
Never call `GetObject` here — the point of this method is that no object bytes
move.

`delete`: presigned `DeleteObject`; 404 is success.

`can_presign`: `true`.

- [ ] **Step 5: Check the SDK does not send `content-length` twice**

`PutBuilder::send` (`rust/crates/stepd-sdk/src/blobs.rs`) replays every
reservation header onto the request, and `reqwest` sets `content-length` itself
from the body. Against the relay that is harmless — the route reads the body and
compares lengths. Against a signed request a duplicated or conflicting header is
a signature mismatch, and the error S3 returns says nothing about which header
caused it.

Write a test that asserts on what actually goes on the wire:

```rust
#[tokio::test]
async fn the_upload_sends_each_reservation_header_exactly_once() {
    // A duplicated content-length breaks SigV4 and the error names nothing
    // useful. Assert on the request, not on the upload succeeding — a relay
    // upload succeeds either way, which is why this went unnoticed.
    let seen = record_headers_of_next_put().await;   // a one-shot recording server
    assert_eq!(seen.get_all("content-length").iter().count(), 1, "got {seen:?}");
}
```

If `reqwest` does duplicate it, filter `content-length` out of the replayed
headers in the SDK and say in a comment why the server still sends it in the
reservation: the value is signed, so the *signature* binds the length even when
the header comes from the client's own body handling.

- [ ] **Step 6: Write the live test**

`tests/live.rs`, gated exactly the way the database tests are — skipping loudly
rather than passing quietly:

```rust
/// Skips loudly without `STEPD_TEST_S3_ENDPOINT`. A green run that skipped this
/// is not a green run — the same rule as STEPD_TEST_DATABASE_URL.
fn config() -> Option<S3Config> {
    let endpoint = match std::env::var("STEPD_TEST_S3_ENDPOINT") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("SKIPPED: set STEPD_TEST_S3_ENDPOINT to run the live S3 tests");
            return None;
        }
    };
    // ... access key, secret, bucket from STEPD_TEST_S3_* with the probe defaults
}

#[tokio::test]
async fn the_store_refuses_bytes_that_do_not_match_the_declared_digest() {
    let Some(cfg) = config() else { return };
    let b = S3Backend::new(cfg).expect("a backend");
    let spec = /* size 5, sha256 of b"hello" */;
    let t = b.upload_target(Uuid::now_v7(), &spec, chrono::Duration::seconds(300)).await.unwrap();

    let res = put_with(&t, b"world").await;   // same length, different bytes
    assert!(res.status().is_client_error(),
            "the object store must reject these, or the server has to read them to find out");
}

#[tokio::test]
async fn a_committed_object_reports_its_digest_without_transferring_it() {
    let Some(cfg) = config() else { return };
    // upload the correct bytes, then:
    let got = b.stored(id).await.unwrap().expect("present");
    assert_eq!(got.size, 5);
    assert_eq!(got.sha256.as_deref(), Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"));
}

#[tokio::test]
async fn a_presigned_read_serves_a_range() {
    let Some(cfg) = config() else { return };
    // GET the read_url with `Range: bytes=0-2`, expect 206 and 3 bytes.
}
```

- [ ] **Step 7: Run both, unit and live**

```bash
cargo test -p stepd-blobs-s3                       # unit only; live tests skip loudly
podman run -d --name stepd-s3 -p 9000:9000 \
  -e MINIO_ROOT_USER=probe -e MINIO_ROOT_PASSWORD=probeprobe \
  quay.io/minio/minio server /data
export STEPD_TEST_S3_ENDPOINT=http://127.0.0.1:9000
cargo test -p stepd-blobs-s3                       # now the live tests run
cargo deny check                                    # BSD-2-Clause must be allowed
```

Use whichever server Task 0's note says qualifies. If both do, run both.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add rust/crates/stepd-blobs-s3 rust/Cargo.toml rust/Cargo.lock rust/deny.toml
git commit -m "blobs: an S3 backend whose digest check never moves the object"
```

---

### Task 5: Configuration, wiring and doctor

**Files:**
- Modify: `rust/crates/stepd-server/src/lib.rs` (`Config`, `from_env`, `validate`, `build`)
- Modify: `rust/crates/stepd-cli/src/doctor.rs:84`, `:316`
- Modify: `.env.example`, `compose.yaml`
- Test: `rust/crates/stepd-server/src/lib.rs` (`mod tests`)

**Interfaces:**
- Consumes: `S3Backend::new`, `S3Config` (Task 4); `FilesystemBackend` (Task 1).
- Produces: `Config.blob_backend: BlobBackendConfig`, an enum of
  `Filesystem { root: PathBuf }` and `S3(S3Config)`.

Env vars, following the existing `STEPD_BLOB_*` naming
(`stepd-server/src/lib.rs:190-202`): `STEPD_BLOB_BACKEND` (`fs` default, or
`s3`), `STEPD_BLOB_S3_ENDPOINT`, `STEPD_BLOB_S3_REGION`, `STEPD_BLOB_S3_BUCKET`,
`STEPD_BLOB_S3_ACCESS_KEY`, `STEPD_BLOB_S3_SECRET_KEY`,
`STEPD_BLOB_S3_PATH_STYLE`.

Read `STEPD_BLOB_S3_PATH_STYLE` **by value, not presence.** The three egress
flags are read by presence and that is already a documented trap
(`.env.example`, `compose.yaml`); adding a fourth variable with the opposite
convention and a similar name is how someone sets `=0` and gets virtual-host
addressing against a server with no DNS for it.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn selecting_s3_without_a_bucket_is_refused_rather_than_defaulted() {
    // A half-configured S3 backend that starts is a server handing apps upload
    // URLs pointing at a bucket that is not there. Failing at startup names the
    // missing variable; failing later names a run.
    let mut c = Config::default();
    c.blob_key = vec![1, 2, 3];
    c.blob_backend = BlobBackendConfig::S3(S3Config { bucket: String::new(), ..probe_config() });
    let err = c.validate().expect_err("a bucketless S3 config must not validate");
    assert!(err.to_string().contains("STEPD_BLOB_S3_BUCKET"), "got {err}");
}
```

Check `validate`'s current signature before writing this — today it warns rather
than returning an error for the missing signing key. Extend it in the shape it
already has; if it returns `()`, this test asserts on `Server::build` failing
instead. Do not silently change the missing-key behaviour, which is deliberate
(`lib.rs:230`).

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p stepd-server --lib selecting_s3_without_a_bucket
```

- [ ] **Step 3: Implement config, parsing and validation**

Build the backend once in `Server::build` and share the one `Arc` between
`store.with_blob_backend(..)` and `PostgresBlobStore` (Task 2, Step 4).

- [ ] **Step 4: Extend doctor**

Add a check beside `orphaned_blobs` (`doctor.rs:84`) that, when the backend is
S3, issues a `HeadBucket` and reports: reachable and authorised, reachable but
403, or unreachable. Follow `Finding::ok`/`warn`/`crit` as the file uses them.
An operator whose bucket credentials are wrong currently learns it from a failed
run; `doctor` is meant to be runnable when the server will not start, which is
when this matters.

- [ ] **Step 5: Document the variables**

Add an `# ---- managed blobs: S3 backend` block to `.env.example` in the file's
existing commented style, saying what each variable does and that leaving
`STEPD_BLOB_BACKEND` unset keeps the filesystem-plus-relay default. Add a
commented `minio`-or-`rustfs` service to `compose.yaml` — commented, because the
default `docker compose up` should stay the two-service stack the README
describes.

- [ ] **Step 6: Run it**

```bash
cargo test -p stepd-server
cargo run -p stepd-cli -- doctor
```

- [ ] **Step 7: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add rust/crates/stepd-server/src/lib.rs rust/crates/stepd-cli/src/doctor.rs .env.example compose.yaml
git commit -m "blobs: select and validate the backend from configuration"
```

---

### Task 6: Prove it in CI

**Files:**
- Modify: `.github/workflows/ci.yml`
- Test: `rust/crates/stepd-server/tests/end_to_end.rs`

- [ ] **Step 1: Write the failing test**

An end-to-end test that reserves against an S3 backend, uploads with the
returned URL and headers, commits, and asserts on the shape of what happened:

```rust
#[tokio::test]
async fn no_object_bytes_reach_the_server_on_the_s3_path() {
    // The issue's whole point (BR-19). A commit_blob that downloads the object
    // passes every other assertion in this file and fails this one.
    let Some(f) = fixture_with_s3().await else { return };   // skips loudly
    let (blob_id, url, headers) = reserve(&f, b"hello").await;

    assert!(!url.starts_with(&f.base_url),
            "the upload URL must not point at the control plane: {url}");

    put_direct(&url, &headers, b"hello").await;
    let committed = commit(&f, blob_id).await.expect("committed");
    assert_eq!(committed.sha256, sha256_hex(b"hello"));

    assert_eq!(f.server_bytes_transferred(), 0,
               "the server moved object bytes; verification is back in the control plane");
}
```

`server_bytes_transferred` needs a counter the fixture can read. Put it in the
fixture's state — **not a `static`** (CLAUDE.md; three prior recurrences, each
found by two tests interfering). If threading a counter proves invasive, assert
instead that the relay route is absent (Task 3) *and* that `stored` returned a
digest from metadata, which together mean no byte path existed.

- [ ] **Step 2: Run it and watch it fail or skip**

```bash
cargo test -p stepd-server --test end_to_end no_object_bytes
```

Without an S3 endpoint it must print SKIPPED and return, matching this project's
rule that a suite which skips says so.

- [ ] **Step 3: Add the CI lane**

Extend tier 2 rather than adding a tier: this is integration work, it needs a
container and a few seconds, and the tier budgets are stated in the file header
for a reason. Add the object-storage service alongside `postgres`, set
`STEPD_TEST_S3_*` in the job `env`, and add a step:

```yaml
      - name: blob backend against S3-compatible storage
        # BR-19 says bulk data never traverses the control plane. Until this
        # lane existed, that claim rested on a store nobody had written.
        run: cargo test -p stepd-blobs-s3 && cargo test -p stepd-server --test end_to_end no_object_bytes
        working-directory: rust
```

Pin the image by tag, not `latest` — a lane whose backing service changes under
it reports a failure that is not yours.

Keep a lane that still runs the **filesystem** backend. It is the shipped default
for `stepd dev` and the relay must not rot; the existing tier 2 step covers it as
long as it is not switched over to S3 wholesale.

- [ ] **Step 4: Push and read the result**

```bash
git push
gh run list --limit 3 --json databaseId,status,conclusion,displayTitle
gh run view <id> --json jobs -q '.jobs[] | "\(.conclusion)\t\(.name)"'
```

Do not proceed on a red lane. Note that `tier 3 · through pgbouncer` and
`tier 4 · soak` were already red before this work started, for reasons unrelated
to blobs — confirm any failure you see is one of those two before assuming it is
pre-existing.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml rust/crates/stepd-server/tests/end_to_end.rs
git commit -m "ci: prove bytes skip the control plane on the S3 backend"
```

---

### Task 7: Make the documentation true again

The README and ADR-010 currently describe the world before this change, and
ADR-010 has a bullet that was already stale before it.

**Files:**
- Modify: `README.md:46`, `:64-68`, `:281`
- Modify: `docs/adr/010-payload-tiering.md`
- Modify: `docs/GAPS.md`

- [ ] **Step 1: README**

Remove "The blob relay is the fallback path, not the fast one" from *Honest
gaps*. Replace the status-table row for Managed blobs with one naming both
backends. In the configuration section (`:281`), document the S3 variables next
to the existing `STEPD_BLOB_*` ones.

Say what is true and no more. If the S3 backend was proved against one server and
not another, the README says which — `docs/blob-backends.md` from Task 0 is the
detail, and the README links to it.

- [ ] **Step 2: ADR-010**

Under *What we accept*, two bullets are now wrong:

- "Digest verification reads the whole object back on commit — … a full download
  on S3 unless the store's own checksum headers are used." Rewrite: the checksum
  headers **are** used; the filesystem backend still reads locally because it has
  no metadata to consult.
- "The blob HTTP surface is not yet wired." It has been wired since before this
  work; both routes exist in `rust/crates/stepd-server/src/blobs.rs`. Delete it.

Also revisit *What this makes easy*: "S3 is a drop-in" was not true and is what
made this issue look small. Say what it actually took — a seam between the index
and the backend.

Add to *Verification*: the tests from Tasks 1–6, by name.

- [ ] **Step 3: GAPS**

`docs/GAPS.md` records spec resolutions rather than always code. Add a row for
this in the register's existing format, citing BR-19 and the CI lane that now
produces the evidence.

- [ ] **Step 4: Check the docs against the code**

Re-read each claim you wrote and point at the code or test that makes it true.
A comment describing a property the code lacks is worse than none, because it
stops the next reader checking (CLAUDE.md).

- [ ] **Step 5: Commit and close**

```bash
git add README.md docs/adr/010-payload-tiering.md docs/GAPS.md
git commit -m "docs: the relay is no longer the only path

Closes #3"
```

---

## Notes for the executor

- **Task 0 can invalidate Tasks 4–6.** It is first for that reason. Report rather
  than working around a server that accepts mismatched bytes.
- **Tasks 1–3 are behaviour-preserving.** If a pre-existing test needs editing to
  pass, stop: that is a behaviour change wearing a refactor's clothes.
- **`main` had two red lanes before this work** — `tier 3 · through pgbouncer`
  (cron fires but no run row appears, `end_to_end.rs:877`) and `tier 4 · soak`
  (`runs_singleton_key` unique violation, `simulation.rs:676`). Neither touches
  blobs. Don't adopt them, and don't mistake them for your own breakage.
