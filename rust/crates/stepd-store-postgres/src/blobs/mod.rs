//! Managed blobs (protocol §8.3).
//!
//! The index lives in Postgres; the bytes do not. stepd is a control plane, and
//! §8.5 forbids the server from interpreting payload bytes at all — its only
//! three interactions with blob content are digest verification on commit,
//! URL minting on read, and deletion on collection. This module does exactly
//! those three things and nothing else.
//!
//! Where the bytes live and who mints the transfer URLs is a
//! [`stepd_core::traits::BlobBackend`]; this module owns only the index —
//! the row, the per-namespace dedupe, the references recorded by trigger — so
//! a second backend (S3, say) is a new implementation of that trait, not a
//! second implementation of this one.
//!
//! ## Why the URLs are signed rather than session-authenticated
//!
//! An upload URL is handed to an app that may not hold a console token, and is
//! meant to be usable exactly once, for one blob, with one content type, up to
//! one size. A capability encoded in the URL and verified with an HMAC expresses
//! all of that; a session cookie expresses none of it. The signature covers the
//! blob id, the direction, the declared size and the expiry, so a leaked write
//! URL cannot be turned into a read URL, cannot be pointed at another blob, and
//! cannot be used to upload something larger than was reserved.
//!
//! Content substitution is separately impossible: the digest is verified before
//! the blob becomes readable, so even a stolen upload URL used in time can only
//! store bytes that hash to what the app already declared.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::sync::Arc;
use stepd_core::traits::{BlobBackend, BlobSpec, BlobStore, RelayBytes, Reservation};
use stepd_core::{Error, Result};
use stepd_proto::BlobRef;
use uuid::Uuid;

use crate::db;

mod filesystem;
pub use filesystem::FilesystemBackend;

/// Blob index in Postgres; bytes and transfer URLs come from a [`BlobBackend`].
///
/// The server builds a [`FilesystemBackend`] and passes it to
/// [`PostgresBlobStore::with_backend_and_relay`], which is what makes `stepd
/// dev` work with no cloud account and what CI uses. Nothing in this struct's
/// `BlobStore` methods knows that, though — they only ever call through the
/// trait, which is what makes another backend a drop-in swap.
#[derive(Clone)]
pub struct PostgresBlobStore {
    pool: sqlx::PgPool,
    /// Where bytes live and who mints transfer URLs.
    backend: Arc<dyn BlobBackend>,
    /// Set only when the backend can also relay bytes through this process —
    /// today, always, since [`FilesystemBackend`] is the only backend and
    /// cannot presign. The server's transfer endpoints and `commit_blob`'s
    /// no-digest-from-metadata fallback use it; a presigning backend does not
    /// implement [`RelayBytes`] at all, and Task 3 stops mounting those
    /// endpoints for one. Named by capability rather than by concrete type —
    /// naming `FilesystemBackend` here would rebuild the coupling this seam
    /// exists to remove, and would stop any future relay-capable backend that
    /// is not the filesystem from ever using it.
    relay: Option<Arc<dyn RelayBytes>>,
    /// Mints and checks the transfer capabilities.
    caps: Capability,
    /// Ceiling on a single managed blob (protocol §8.2).
    max_size: i64,
}

/// Mints and verifies the signed transfer URLs.
///
/// Deliberately separate from the store, and holding no database handle: the
/// signing rules are the security-critical part, and they are exhaustively
/// testable only if testing them does not require a Postgres to be running.
/// A security check whose tests are awkward to run is a security check whose
/// tests get skipped.
#[derive(Clone)]
pub struct Capability {
    /// Base URL the app should call. The server hosts the transfer endpoints.
    base_url: String,
    /// Key for the HMAC.
    key: Vec<u8>,
}

impl Capability {
    /// Build a minter for `base_url` signing with `key`.
    pub fn new(base_url: impl Into<String>, key: Vec<u8>) -> Self {
        Self {
            base_url: base_url.into(),
            key,
        }
    }

    /// Sign a capability: this id, this direction, this size, until this instant.
    fn sign(&self, id: Uuid, dir: &str, size: i64, expires: i64) -> String {
        use hmac::{Hmac, Mac};
        let mut mac = <Hmac<Sha256>>::new_from_slice(&self.key).expect("HMAC accepts any key");
        mac.update(format!("{id}.{dir}.{size}.{expires}").as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Verify a capability presented by a caller.
    ///
    /// Returns the declared size so the transfer endpoint can enforce it: the
    /// size is part of the capability precisely so that a write URL cannot be
    /// used to store more than was reserved.
    pub fn verify(&self, id: Uuid, dir: &str, size: i64, expires: i64, sig: &str) -> Result<i64> {
        use subtle::ConstantTimeEq;
        if Utc::now().timestamp() > expires {
            return Err(Error::Config("blob url expired".into()));
        }
        let expected = self.sign(id, dir, size, expires);
        // Constant-time: a byte-by-byte comparison leaks the correct prefix.
        let ok: bool = expected.as_bytes().ct_eq(sig.as_bytes()).into();
        if !ok {
            return Err(Error::Config("blob url signature invalid".into()));
        }
        Ok(size)
    }

    /// Mint a URL valid for `ttl`.
    pub fn url(&self, id: Uuid, dir: &str, size: i64, ttl: Duration) -> (String, DateTime<Utc>) {
        let expires_at = Utc::now() + ttl;
        let exp = expires_at.timestamp();
        let sig = self.sign(id, dir, size, exp);
        (
            format!(
                "{}/v1/blobs/{id}/content?dir={dir}&size={size}&exp={exp}&sig={sig}",
                self.base_url.trim_end_matches('/')
            ),
            expires_at,
        )
    }
}

/// Add a fresh read URL to every `$blob` in a value, in place.
///
/// Protocol §8.3.1: `url` is never supplied by the app and must not be persisted
/// by an SDK. The server mints it per attempt, which is what makes a short TTL
/// safe — a reference replayed on attempt forty gets a URL minted for attempt
/// forty, and the one from attempt one expired thirty-nine attempts ago.
///
/// Recursive, for the same reason [`blob_ids`] is: a blob can be nested anywhere
/// in a step result, and a walk that only checked the top level would hand back
/// references the app cannot dereference, from payload shapes that are perfectly
/// ordinary.
///
/// A `read_url` failure on one reference does not fail the whole attempt: the
/// `url` key is simply left off that reference and the failure is logged. The
/// SDK already reports a missing read URL precisely (`BlobError::NoReadUrl`),
/// and failing the entire attempt because one of forty references could not be
/// signed would trade that precise, local error for a broad one that tells the
/// app nothing about which reference or why.
pub fn attach_read_urls(backend: &dyn BlobBackend, value: &mut serde_json::Value, ttl: Duration) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::Object(blob)) = map.get_mut("$blob") {
                let id = blob
                    .get("id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<Uuid>().ok());
                let size = blob.get("size").and_then(|v| v.as_i64());
                if let (Some(id), Some(size)) = (id, size) {
                    match backend.read_url(id, size, ttl) {
                        Ok(url) => {
                            blob.insert("url".into(), serde_json::Value::String(url));
                        }
                        Err(e) => {
                            // §8.3.1: `url` is never supplied by the app and must not be
                            // persisted by an SDK, so a value already sitting here did not
                            // come from us. A reference the backend refused to sign must
                            // reach the attempt with no URL at all, not with whatever else
                            // was in that key — a range read does not verify the digest of
                            // what it fetches, so a stale or attacker-supplied URL left in
                            // place would be trusted with nothing left to catch it.
                            blob.remove("url");
                            tracing::warn!(
                                blob = %id, error = %e,
                                "failed to mint a read url for this blob reference; \
                                 leaving it off, the SDK reports it as a missing read url"
                            );
                        }
                    }
                }
                return;
            }
            for (_, v) in map.iter_mut() {
                attach_read_urls(backend, v, ttl);
            }
        }
        serde_json::Value::Array(items) => {
            for v in items {
                attach_read_urls(backend, v, ttl);
            }
        }
        _ => {}
    }
}

/// Ceiling on a single managed blob (protocol §8.2), for stores built with
/// [`PostgresBlobStore::with_backend`] that do not override it with
/// [`PostgresBlobStore::with_max_size`].
const DEFAULT_MAX_SIZE: i64 = 100 * 1024 * 1024;

impl PostgresBlobStore {
    /// Build a store whose bytes live wherever `backend` puts them, with no
    /// relay: the backend must presign, or the transfer endpoints will refuse
    /// every request (Task 3 stops mounting them for a backend like this).
    ///
    /// `caps` still verifies the capabilities on the server's transfer
    /// endpoints, independent of which backend is minting them.
    pub fn with_backend(
        pool: sqlx::PgPool,
        backend: Arc<dyn BlobBackend>,
        caps: Capability,
    ) -> Self {
        Self::with_backend_and_relay(pool, backend, None, caps)
    }

    /// Build a store from a backend the caller already constructed, sharing
    /// its relay capability (if any) too.
    ///
    /// Exists so a caller that hands the same backend `Arc` to more than one
    /// place — the server wires the identical `Arc` into both this store and
    /// [`crate::PostgresStore::with_blob_backend`] — can also carry the relay
    /// through, which [`PostgresBlobStore::with_backend`] always sets to
    /// `None`. Building two backends from one configuration instead would work
    /// today and diverge the moment one took a different code path: reads
    /// minted against one bucket while uploads land in another, silently.
    pub fn with_backend_and_relay(
        pool: sqlx::PgPool,
        backend: Arc<dyn BlobBackend>,
        relay: Option<Arc<dyn RelayBytes>>,
        caps: Capability,
    ) -> Self {
        Self {
            pool,
            backend,
            relay,
            caps,
            max_size: DEFAULT_MAX_SIZE,
        }
    }

    /// The capability minter, for the server's transfer endpoint.
    pub fn capability(&self) -> &Capability {
        &self.caps
    }

    /// Whether the backend issues URLs that reach the bytes directly.
    ///
    /// A passthrough to [`BlobBackend::can_presign`], so the server can decide
    /// whether to mount protocol §8.3.2's relay route without naming a concrete
    /// backend type.
    pub fn can_presign(&self) -> bool {
        self.backend.can_presign()
    }

    /// The backend's short name, for logs and diagnostics.
    ///
    /// A passthrough to [`BlobBackend::name`], for the same reason
    /// [`PostgresBlobStore::can_presign`] is one: the server decides what to
    /// mount and what to say about it without naming a concrete backend type.
    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }

    /// Override the single-blob ceiling.
    pub fn with_max_size(mut self, bytes: i64) -> Self {
        self.max_size = bytes;
        self
    }

    /// Store bytes for a reserved blob. Called by the server's transfer endpoint
    /// after it has verified the capability.
    ///
    /// Only meaningful when the backend relays bytes through this process; see
    /// `relay`.
    pub async fn put_bytes(&self, id: Uuid, bytes: &[u8]) -> Result<()> {
        match &self.relay {
            Some(relay) => relay.put_bytes(id, bytes).await,
            None => Err(Error::Store(
                "this blob store's backend does not relay bytes through the server".into(),
            )),
        }
    }

    /// Read a committed blob, optionally a byte range (protocol §8.3.3).
    ///
    /// Only meaningful when the backend relays bytes through this process; see
    /// `relay`.
    pub async fn get_bytes(&self, id: Uuid, range: Option<(u64, u64)>) -> Result<Vec<u8>> {
        match &self.relay {
            Some(relay) => relay.get_bytes(id, range).await,
            None => Err(Error::Store(
                "this blob store's backend does not relay bytes through the server".into(),
            )),
        }
    }
}

/// Lowercase hex SHA-256, the form used everywhere in the protocol.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[async_trait]
impl BlobStore for PostgresBlobStore {
    async fn reserve(&self, namespace: &str, spec: BlobSpec) -> Result<Reservation> {
        if spec.size <= 0 || spec.size > self.max_size {
            return Err(Error::Config(format!(
                "payload_too_large: {} bytes exceeds the managed-blob ceiling of {}; \
                 use an external $ref for data the application already stores",
                spec.size, self.max_size
            )));
        }
        let digest = hex::decode(&spec.sha256)
            .map_err(|_| Error::Config("sha256 must be lowercase hex".into()))?;

        // Content addressing is scoped per namespace, never globally: a global
        // scope would let one tenant probe for another's data by reserving a
        // digest and observing whether the upload was skipped.
        let existing: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM blobs WHERE ns=$1 AND sha256=$2 AND state='committed'",
        )
        .bind(namespace)
        .bind(&digest)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;

        if let Some(id) = existing {
            return Ok(Reservation::Deduplicated { id });
        }

        let id = Uuid::now_v7();
        sqlx::query(
            r#"INSERT INTO blobs (id, ns, size, sha256, content_type, filename, state)
               VALUES ($1,$2,$3,$4,$5,$6,'reserved')"#,
        )
        .bind(id)
        .bind(namespace)
        .bind(spec.size)
        .bind(&digest)
        .bind(&spec.content_type)
        .bind(&spec.filename)
        .execute(&self.pool)
        .await
        .map_err(db)?;

        let target = self
            .backend
            .upload_target(id, &spec, Duration::seconds(300))
            .await?;
        Ok(Reservation::Upload {
            id,
            url: target.url,
            method: target.method,
            headers: target.headers,
            expires_at: target.expires_at,
        })
    }

    async fn commit_blob(&self, id: Uuid) -> Result<BlobRef> {
        let row = sqlx::query(
            "SELECT ns, size, sha256, content_type, filename, state::text AS state
               FROM blobs WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound(format!("no such blob {id}")))?;

        let declared_size: i64 = row.get("size");
        let declared_digest: Vec<u8> = row.get("sha256");

        // Verify before the blob becomes readable. Without this a compromised
        // upload URL could substitute different content for a reference that has
        // already been committed into a step result, and every later read of that
        // step would return the substituted bytes with no sign anything changed.
        let stored = self
            .backend
            .stored(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("no such blob {id}")))?;

        if stored.size != declared_size {
            return Err(Error::Config(format!(
                "blob_digest_mismatch: {id} declared {declared_size} bytes, stored {}",
                stored.size
            )));
        }

        match stored.sha256 {
            // The backend read the digest from metadata; no bytes to read here.
            Some(digest) => {
                if digest != hex::encode(&declared_digest) {
                    return Err(Error::Config(format!(
                        "blob_digest_mismatch: {id} content does not match the declared sha256"
                    )));
                }
            }
            // Only the filesystem backend reaches this branch: it cannot answer
            // "what is this object's sha256" without reading the bytes, so it
            // says so via `None` rather than lying or hashing eagerly inside
            // `stored`. A presigning backend answering `None` here would mean
            // digest verification silently moved back into the control plane,
            // defeating the reason bytes bypass it in the first place.
            None => {
                let bytes = self.get_bytes(id, None).await?;
                let actual = Sha256::digest(&bytes);
                if actual.as_slice() != declared_digest.as_slice() {
                    return Err(Error::Config(format!(
                        "blob_digest_mismatch: {id} content does not match the declared sha256"
                    )));
                }
            }
        }

        sqlx::query("UPDATE blobs SET state='committed', committed_at=now() WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(db)?;

        Ok(BlobRef {
            id,
            size: declared_size,
            sha256: hex::encode(&declared_digest),
            content_type: row.get("content_type"),
            filename: row.get("filename"),
            // Never populated here. `url` is minted per attempt on read and must
            // not be persisted by an SDK (protocol §8.3.1).
            url: None,
        })
    }

    async fn presign_read(&self, id: Uuid, ttl: Duration) -> Result<String> {
        let size: Option<i64> =
            sqlx::query_scalar("SELECT size FROM blobs WHERE id=$1 AND state='committed'")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db)?;
        let size = size.ok_or_else(|| Error::Store(format!("blob {id} is not readable")))?;
        self.backend.read_url(id, size, ttl)
    }

    async fn add_ref(&self, id: Uuid, run: Uuid, step_hash: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO blob_refs (blob_id, run_id, step_hash) VALUES ($1,$2,$3)
             ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .bind(run)
        .bind(step_hash)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn collect(&self, before: DateTime<Utc>) -> Result<u64> {
        // Two populations, deleted for different reasons: reservations that were
        // never completed (the app crashed between reserving and uploading), and
        // committed blobs nothing references any more.
        //
        // A blob referenced by both a parent and a child run survives until both
        // are collected, which falls out of counting references rather than runs.
        let ids: Vec<Uuid> = sqlx::query_scalar(
            r#"SELECT b.id FROM blobs b
                WHERE (b.state = 'reserved' AND b.reserved_at < $1)
                   OR (b.state = 'committed'
                       AND NOT EXISTS (SELECT 1 FROM blob_refs r WHERE r.blob_id = b.id)
                       AND b.committed_at < $1)"#,
        )
        .bind(before)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        let mut n = 0;
        for id in ids {
            // Bytes first, then the row: deleting the row first and the bytes
            // second can leave an orphaned file with nothing left that knows
            // its name. If the delete fails for a reason other than "already
            // gone" (`BlobBackend::delete` already treats that as success),
            // the row must survive it rather than be removed anyway — it is
            // now the only remaining record that bytes still sit under `id`.
            // Removing the row here would create exactly the orphan the
            // ordering exists to prevent, just one step later than doing it
            // in the other order would.
            //
            // That record is not currently surfaced for both populations
            // this query sweeps, though: `stepd doctor`'s `orphaned_blobs`
            // check only counts `state='reserved'` rows past its age
            // threshold, so a failed delete on a `state='committed'` row (the
            // second half of the `WHERE` above) leaves a row nothing reports.
            // Keeping the row is still correct — it is strictly better than
            // losing the only record entirely — but doctor covering it is a
            // gap, not a property this code has.
            //
            // One bad object must not block the rest of the sweep either, so
            // this logs and moves on rather than propagating: a store with
            // one undeletable object would otherwise never collect anything
            // again.
            if let Err(e) = self.backend.delete(id).await {
                tracing::warn!(
                    blob = %id, error = %e,
                    "failed to delete blob bytes during collection; leaving its row in place"
                );
                continue;
            }
            sqlx::query("DELETE FROM blobs WHERE id=$1")
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(db)?;
            n += 1;
        }
        Ok(n)
    }
}

/// Walk a JSON value and collect every `$blob` id it references.
///
/// Used on commit to build the reference graph, and on read to know which
/// references need a fresh URL minted. Recursive over arrays and objects because
/// a step result is arbitrary JSON and a blob can be nested anywhere in it.
pub fn blob_ids(value: &serde_json::Value, out: &mut Vec<Uuid>) {
    match value {
        serde_json::Value::Object(m) => {
            if let Some(b) = m.get("$blob") {
                if let Some(id) = b.get("id").and_then(|v| v.as_str()) {
                    if let Ok(u) = Uuid::parse_str(id) {
                        out.push(u);
                    }
                }
            }
            for v in m.values() {
                blob_ids(v, out);
            }
        }
        serde_json::Value::Array(a) => {
            for v in a {
                blob_ids(v, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps() -> Capability {
        // No pool, no database, no runtime: the signing rules are the security
        // critical part and must be testable with nothing running.
        Capability::new("http://localhost:8080", b"k".to_vec())
    }

    #[test]
    fn a_write_capability_cannot_be_replayed_as_a_read() {
        let c = caps();
        let id = Uuid::now_v7();
        let exp = Utc::now().timestamp() + 300;
        let write = c.sign(id, "write", 10, exp);
        assert!(c.verify(id, "read", 10, exp, &write).is_err());
        assert!(c.verify(id, "write", 10, exp, &write).is_ok());
    }

    #[test]
    fn a_capability_is_bound_to_one_blob_and_one_size() {
        let c = caps();
        let id = Uuid::now_v7();
        let other = Uuid::now_v7();
        let exp = Utc::now().timestamp() + 300;
        let sig = c.sign(id, "write", 10, exp);
        assert!(
            c.verify(other, "write", 10, exp, &sig).is_err(),
            "id must be bound"
        );
        assert!(
            c.verify(id, "write", 11, exp, &sig).is_err(),
            "size must be bound, or a write URL uploads more than was reserved"
        );
    }

    #[test]
    fn an_expired_capability_is_refused_even_with_a_valid_mac() {
        let c = caps();
        let id = Uuid::now_v7();
        let exp = Utc::now().timestamp() - 1;
        let sig = c.sign(id, "read", 10, exp);
        assert!(c.verify(id, "read", 10, exp, &sig).is_err());
    }

    #[test]
    fn a_minted_url_verifies_against_its_own_signature() {
        let c = caps();
        let id = Uuid::now_v7();
        let (url, expires) = c.url(id, "read", 42, Duration::seconds(300));
        assert!(url.contains(&id.to_string()));
        let sig = url.split("sig=").nth(1).unwrap();
        assert!(c.verify(id, "read", 42, expires.timestamp(), sig).is_ok());
    }

    #[test]
    fn blob_references_are_found_wherever_they_are_nested() {
        let id = Uuid::now_v7();
        let v = serde_json::json!({
            "receipts": [ { "file": { "$blob": { "id": id, "size": 1, "sha256": "ab" } } } ],
            "note": "no blob here"
        });
        let mut out = vec![];
        blob_ids(&v, &mut out);
        assert_eq!(
            out,
            vec![id],
            "a blob nested in an array inside an object must be found"
        );
    }

    #[test]
    fn sha256_hex_matches_the_protocol_form() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    use stepd_core::traits::{StoredObject, UploadTarget};

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
        async fn stored(&self, _: Uuid) -> Result<Option<StoredObject>> {
            Ok(None)
        }
        async fn delete(&self, _: Uuid) -> Result<()> {
            Ok(())
        }
        fn can_presign(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "stub"
        }
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

    /// A backend whose `read_url` fails for exactly one id and succeeds for
    /// any other, so a test can prove a failure on one reference does not
    /// stop the walk from reaching a sibling. Per-instance state (`fails` is
    /// a field, set fresh by each test), never a `static` — two tests sharing
    /// mutable state through one has broken this codebase more than once.
    struct FailingBackend {
        fails: Uuid,
    }

    #[async_trait]
    impl BlobBackend for FailingBackend {
        async fn upload_target(&self, _: Uuid, _: &BlobSpec, _: Duration) -> Result<UploadTarget> {
            unimplemented!("not exercised by the read path")
        }
        fn read_url(&self, id: Uuid, size: i64, _: Duration) -> Result<String> {
            if id == self.fails {
                Err(Error::Store(format!("cannot sign a read url for {id}")))
            } else {
                Ok(format!("stub://{id}/{size}"))
            }
        }
        async fn stored(&self, _: Uuid) -> Result<Option<StoredObject>> {
            Ok(None)
        }
        async fn delete(&self, _: Uuid) -> Result<()> {
            Ok(())
        }
        fn can_presign(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "failing-stub"
        }
    }

    #[test]
    fn a_read_url_failure_on_one_reference_does_not_stop_the_walk() {
        // Guards the skip-and-continue resolution: failing to sign one of
        // several references must not fail the whole attempt. The sibling
        // getting its url is the part that actually proves the walk
        // continued rather than bailing on the first failure — a test that
        // only checked the failing reference would pass just as well under
        // a `return` on the first error.
        let failing = Uuid::now_v7();
        let ok = Uuid::now_v7();
        let mut v = serde_json::json!({
            "broken": { "$blob": { "id": failing, "size": 7, "sha256": "ab" } },
            "fine": { "$blob": { "id": ok, "size": 3, "sha256": "cd" } },
        });
        let backend = FailingBackend { fails: failing };
        attach_read_urls(&backend, &mut v, Duration::seconds(60));
        assert!(
            v["broken"]["$blob"].get("url").is_none(),
            "a reference the backend could not sign must carry no url"
        );
        assert_eq!(
            v["fine"]["$blob"]["url"],
            serde_json::json!(format!("stub://{ok}/3")),
            "a sibling reference must still get a fresh url after another one failed"
        );
    }

    #[test]
    fn a_read_url_failure_clears_any_preexisting_url_rather_than_leaving_it() {
        // Guards against a stale or app-supplied `url` surviving a signing
        // failure. Unreachable with today's filesystem backend, which cannot
        // fail, but load-bearing once a presigning backend can: the SDK does
        // not verify a digest on a ranged read, so a URL left in place here
        // would be trusted with nothing left to catch it.
        let id = Uuid::now_v7();
        let mut v = serde_json::json!({
            "$blob": {
                "id": id, "size": 7, "sha256": "ab",
                "url": "http://stale.example/leaked",
            }
        });
        let backend = FailingBackend { fails: id };
        attach_read_urls(&backend, &mut v, Duration::seconds(60));
        assert!(
            v["$blob"].get("url").is_none(),
            "a value already present under `url` did not come from us and must not survive"
        );
    }
}
