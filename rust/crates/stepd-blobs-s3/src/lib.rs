//! Blob bytes in an S3-compatible object store (protocol §8.3).
//!
//! This is the backend that makes BR-19 true on the upload path. The
//! filesystem backend cannot sign anything, so §8.3.2's relay applies and every
//! payload byte crosses the control plane; this one hands the app a presigned
//! URL and the bytes go straight to the object store.
//!
//! ## Why the server never reads an object
//!
//! `commit_blob` has to establish that the stored bytes hash to the digest the
//! app declared. Downloading the object to hash it would satisfy that and
//! defeat the entire purpose — the bytes would simply travel through the server
//! on commit instead of on upload.
//!
//! Instead the verification is pushed into the object store. The presigned
//! `PutObject` signs `x-amz-checksum-sha256` and `content-length`, so the store
//! itself refuses a body that does not match, and [`BlobBackend::stored`] only
//! has to ask what it holds — a `HeadObject`, which moves metadata and no
//! content. Whether a given server actually enforces the signed checksum is not
//! something to assume: [`docs/blob-backends.md`][compat] records what two
//! candidate servers were observed to do.
//!
//! Because the checksum is signed, this backend deliberately does *not*
//! implement [`stepd_core::traits::RelayBytes`]. A backend that can presign has
//! nothing to relay, and being unable to answer those methods is what keeps the
//! relay route unmounted for it.
//!
//! [compat]: https://github.com/swiftugandan/stepd/blob/main/docs/blob-backends.md
//!
//! ## Why the key is the blob id
//!
//! Objects live at `blobs/{shard}/{shard}/{id}`, keyed by the blob id and never
//! by the content digest. Keying by digest would make deduplication implicit and
//! collection wrong: two `blobs` rows in different namespaces can legitimately
//! share a digest, and collecting one must not delete the other's bytes.
//! Per-namespace deduplication stays a Postgres lookup, where ADR-010's
//! cross-tenant reasoning already lives.

#![deny(missing_docs)]

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rusty_s3::actions::{DeleteObject, GetObject, HeadObject, PutObject};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use stepd_core::traits::{BlobBackend, BlobSpec, StoredObject, UploadTarget};
use stepd_core::{Error, Result};
use url::Url;
use uuid::Uuid;

/// How long the backend's own signed requests stay valid.
///
/// These URLs are minted and sent by this process in the same breath, so they
/// need to outlive one round trip and nothing more. They are never handed to an
/// application; the TTL an app sees is the one the caller passes to
/// [`BlobBackend::upload_target`] or [`BlobBackend::read_url`].
const INTERNAL_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long this backend waits to establish a connection to the object store.
///
/// See [`REQUEST_TIMEOUT`] for why both of these exist and why they are
/// constants. [`REQUEST_TIMEOUT`] already bounds the whole request,
/// connecting included, and would eventually catch a stalled handshake too —
/// but only at its budget. A DROPping firewall or a stale NAT entry stalls
/// the *handshake* specifically, and a shorter bound on just that phase fails
/// fast on it instead of waiting out the full request budget for something
/// that was never going to answer. `reqwest` applies neither by default.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long this backend waits for a whole metadata request to finish.
///
/// Every request this process sends to the object store is metadata: a
/// `HeadObject` from [`BlobBackend::stored`] and [`S3Backend::check_bucket`],
/// a `DeleteObject` from [`BlobBackend::delete`]. No payload byte is on either
/// wire — that is the entire point of presigning — so the budget is a small
/// round trip's, not a transfer's, and is deliberately far below
/// `STEPD_ATTEMPT_TIMEOUT_SECONDS` (60 by default), which covers an app
/// actually doing work.
///
/// Without a bound here, one request that never answers stops the engine, not
/// just the blob: `stored` is reached from `commit_blob` ← `verify_blobs` ←
/// `Dispatcher::drive` ← `tick_namespace` ← `tick`, and both of those loops
/// are sequential `for` loops over leases and then over namespaces, driven by
/// a single task. The circuit breaker in front of them covers the app
/// transport only, so it never opens for this. `delete` has the same shape
/// from the housekeeping loop, whose own contract in `stepd_core::traits` is
/// that timers and lease reclamation keep making progress when no app is
/// reachable at all. `HttpTransport` bounds its own requests with
/// `attempt_timeout` for exactly this reason.
///
/// Constants rather than `S3Config` fields: the value that is correct here is
/// a property of the operation, not of the deployment — a `HeadObject` that
/// has not answered in fifteen seconds is not going to, wherever the bucket
/// is. Making it an operator knob would add a supported way to reconstruct the
/// unbounded case by setting it high, and the symptom of that setting is a
/// stalled engine rather than a slow blob, which is not a trade an operator is
/// in a position to make from the outside. If a real deployment is ever found
/// where a metadata round trip legitimately needs longer, raise the constant
/// here — where the reasoning is — rather than moving the decision out.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// How to reach an S3-compatible object store.
///
/// [`Debug`] is implemented by hand rather than derived, so that the secret key
/// does not reach a log the first time someone prints a `Config`.
#[derive(Clone)]
pub struct S3Config {
    /// Base URL of the service, e.g. `https://s3.eu-west-1.amazonaws.com`.
    pub endpoint: Url,
    /// Region name used in the signature. Non-AWS servers usually ignore the
    /// value but still require it to match what was signed.
    pub region: String,
    /// Bucket holding the objects. It must already exist; this backend never
    /// creates one.
    pub bucket: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Address the bucket as a path segment rather than a hostname.
    ///
    /// Required for most self-hosted servers, which have no wildcard DNS to
    /// resolve `bucket.host` with.
    pub path_style: bool,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("path_style", &self.path_style)
            .finish_non_exhaustive()
    }
}

/// Blob bytes in an S3-compatible object store.
///
/// Presigns every transfer, so no object byte passes through this process.
/// See the module documentation for why that survives digest verification.
///
/// The derived [`Debug`] is safe to print: `rusty_s3::Credentials` redacts the
/// secret in its own.
#[derive(Debug, Clone)]
pub struct S3Backend {
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}

impl S3Backend {
    /// Build a backend for `config`. Signs locally; contacts nothing here.
    pub fn new(config: S3Config) -> Result<Self> {
        Self::with_timeouts(config, CONNECT_TIMEOUT, REQUEST_TIMEOUT)
    }

    /// [`S3Backend::new`] with the timeouts supplied rather than taken from
    /// [`CONNECT_TIMEOUT`] and [`REQUEST_TIMEOUT`].
    ///
    /// Private, and the seam exists so the timeouts can be *proved* rather
    /// than read: a test can point a backend at a socket that accepts and
    /// never answers and assert that the call returns, without the test
    /// taking the production budget to do it. The alternative — asserting on
    /// the constants — would pass just as well against a client that was
    /// never told about them.
    fn with_timeouts(
        config: S3Config,
        connect: std::time::Duration,
        request: std::time::Duration,
    ) -> Result<Self> {
        let style = if config.path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };
        let bucket = Bucket::new(config.endpoint, style, config.bucket, config.region)
            .map_err(|e| Error::Config(format!("blob store endpoint is unusable: {e}")))?;
        let http = reqwest::Client::builder()
            .connect_timeout(connect)
            .timeout(request)
            .build()
            .map_err(|e| Error::Transport(format!("building the blob store client: {e}")))?;
        Ok(Self {
            bucket,
            credentials: Credentials::new(config.access_key, config.secret_key),
            http,
        })
    }

    /// Where a blob's bytes live in the bucket.
    ///
    /// Sharded on the first bytes of the id, matching the filesystem backend,
    /// so that a listing tool or a lifecycle rule sees the same shape whichever
    /// backend produced the objects.
    fn key(id: Uuid) -> String {
        let s = id.simple().to_string();
        format!("blobs/{}/{}/{}", &s[0..2], &s[2..4], s)
    }
}

/// Lowercase hex SHA-256 → the base64 form S3 wants in `x-amz-checksum-sha256`.
///
/// Sending the hex string instead is accepted as a header and then never
/// matches, so every upload fails at commit with a mismatch that reads like
/// application corruption.
fn checksum_header(hex_digest: &str) -> Result<String> {
    use base64::Engine;
    let raw = hex::decode(hex_digest)
        .map_err(|_| Error::Config("sha256 must be lowercase hex".into()))?;
    if raw.len() != 32 {
        return Err(Error::Config(format!(
            "sha256 must be 32 bytes, got {}",
            raw.len()
        )));
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
}

/// The base64 in `x-amz-checksum-sha256` → the lowercase hex the protocol uses.
fn digest_from_header(value: &str) -> Result<String> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| Error::Store(format!("x-amz-checksum-sha256 is not base64: {value}")))?;
    if raw.len() != 32 {
        return Err(Error::Store(format!(
            "x-amz-checksum-sha256 decoded to {} bytes, which is not a sha256",
            raw.len()
        )));
    }
    Ok(hex::encode(raw))
}

/// A presign lifetime as the signer wants it.
///
/// A non-positive TTL would sign a URL that has already expired, which fails at
/// the object store with a signature error that names nothing about the TTL.
fn presign_ttl(ttl: Duration) -> Result<std::time::Duration> {
    ttl.to_std().map_err(|_| {
        Error::Config(format!(
            "a presign ttl must be positive, got {} seconds",
            ttl.num_seconds()
        ))
    })
}

#[async_trait]
impl BlobBackend for S3Backend {
    /// A presigned `PutObject` that binds the declared digest and length.
    ///
    /// Both headers are signed, not merely suggested. That is what lets
    /// `stored` answer from metadata: the object store refuses a body whose
    /// SHA-256 differs from the signed `x-amz-checksum-sha256`, and refuses a
    /// request whose `content-length` differs from the signed one, so by the
    /// time an object exists under this key it already matches what was
    /// reserved. If the checksum stopped being signed, the store would accept
    /// any bytes and verification would move back into the control plane.
    async fn upload_target(
        &self,
        id: Uuid,
        spec: &BlobSpec,
        ttl: Duration,
    ) -> Result<UploadTarget> {
        let expires_in = presign_ttl(ttl)?;
        let checksum = checksum_header(&spec.sha256)?;
        let length = spec.size.to_string();
        let key = Self::key(id);

        let mut action = PutObject::new(&self.bucket, Some(&self.credentials), &key);
        let headers = action.headers_mut();
        headers.insert("x-amz-checksum-sha256", checksum.clone());
        headers.insert("content-length", length.clone());
        if let Some(ct) = &spec.content_type {
            headers.insert("content-type", ct.clone());
        }
        let url = action.sign(expires_in);

        // Returned so the caller replays them verbatim. A signed header that is
        // altered or dropped makes the upload fail rather than succeed
        // unverified, which is the point.
        let mut headers = vec![
            ("x-amz-checksum-sha256".to_string(), checksum),
            ("content-length".to_string(), length),
        ];
        if let Some(ct) = &spec.content_type {
            headers.push(("content-type".to_string(), ct.clone()));
        }

        Ok(UploadTarget {
            url: url.to_string(),
            method: "PUT".to_string(),
            headers,
            expires_at: Utc::now() + ttl,
        })
    }

    /// A presigned `GetObject`.
    ///
    /// `size` is unused: an S3 presigned GET is scoped to one object, and how
    /// much of it the reader takes is decided by a `Range` header that is not
    /// among the signed headers. That is what keeps §8.3.3's ranged reads
    /// working through a presigned URL — and it is also why the caller must not
    /// treat this URL as a size limit the way the relay capability's is.
    ///
    /// Synchronous, because SigV4 presigning is HMAC over strings and touches
    /// no network.
    fn read_url(&self, id: Uuid, _size: i64, ttl: Duration) -> Result<String> {
        let key = Self::key(id);
        let action = GetObject::new(&self.bucket, Some(&self.credentials), &key);
        Ok(action.sign(presign_ttl(ttl)?).to_string())
    }

    /// What the store holds for `id`, read from object metadata.
    ///
    /// A `HeadObject`, never a `GetObject`: this method exists so that
    /// verification costs one small round trip instead of the whole object.
    /// A missing checksum is an error rather than `Ok(None)` for the digest,
    /// because `None` means "read the bytes to find out" and reading the bytes
    /// is the one thing this backend must not do. It is
    /// [`Error::Unsupported`], which [`Error::is_retryable`] answers `false`
    /// for: the condition is a property of the server, not a moment in it.
    async fn stored(&self, id: Uuid) -> Result<Option<StoredObject>> {
        let key = Self::key(id);
        let mut action = HeadObject::new(&self.bucket, Some(&self.credentials), &key);
        action
            .headers_mut()
            .insert("x-amz-checksum-mode", "ENABLED");
        let url = action.sign(INTERNAL_TTL);

        let res = self
            .http
            .head(url)
            // Signed above, so it has to be on the wire with the same value.
            .header("x-amz-checksum-mode", "ENABLED")
            .send()
            .await
            .map_err(|e| Error::Transport(format!("HeadObject for blob {id}: {e}")))?;

        // Absent is a legitimate answer: an upload that never happened, or an
        // object already collected.
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !res.status().is_success() {
            return Err(Error::Store(format!(
                "HeadObject for blob {id} returned {}",
                res.status()
            )));
        }

        let size: i64 = res
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| {
                Error::Store(format!(
                    "HeadObject for blob {id} reported no usable content-length"
                ))
            })?;

        // `Unsupported`, not `Store`: `Error::is_retryable` is true for
        // `Store`, and this condition is permanent. An object that was stored
        // without a checksum does not grow one, so on a server that accepts
        // the signed `x-amz-checksum-sha256` but does not return it on
        // `HeadObject` — the class `docs/blob-backends.md` exists to warn
        // about — retrying means every blob-bearing run re-executing its
        // steps up to `quarantine_after` times before being quarantined,
        // while `doctor` goes on reporting the bucket reachable.
        let checksum = res
            .headers()
            .get("x-amz-checksum-sha256")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "the object store reports no x-amz-checksum-sha256 for blob {id}; \
                     this backend verifies digests from object metadata and will not \
                     download the object to hash it. A server that accepts the signed \
                     checksum but does not report it back cannot be used as a presigning \
                     backend — see docs/blob-backends.md for the servers this has been \
                     verified against, and use STEPD_BLOB_BACKEND=fs to relay bytes \
                     through the server instead"
                ))
            })?;

        Ok(Some(StoredObject {
            size,
            sha256: Some(digest_from_header(checksum)?),
        }))
    }

    /// Delete the object. Already gone is success, so collection is idempotent.
    async fn delete(&self, id: Uuid) -> Result<()> {
        let key = Self::key(id);
        let action = DeleteObject::new(&self.bucket, Some(&self.credentials), &key);
        let url = action.sign(INTERNAL_TTL);

        let res = self
            .http
            .delete(url)
            .send()
            .await
            .map_err(|e| Error::Transport(format!("DeleteObject for blob {id}: {e}")))?;
        if res.status().is_success() || res.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(Error::Store(format!(
                "DeleteObject for blob {id} returned {}",
                res.status()
            )))
        }
    }

    fn can_presign(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "s3"
    }
}

/// What probing the object store with this backend's credentials found.
///
/// Three states, not a bare `Result`: "reachable but these credentials are
/// refused" and "never got an answer at all" call for different remedies —
/// the first says check the access key, the second says check the endpoint
/// and network path — and collapsing them into one error string makes an
/// operator try the wrong fix first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketCheck {
    /// The probe `HeadObject` came back 2xx or 404: the endpoint answered and
    /// these credentials may read there. A 404 is included deliberately — the
    /// probed key is chosen to never exist, so 404 is the expected success
    /// case, not a failure.
    ///
    /// Says nothing about writing. The probe is a read, so credentials
    /// granting `GetObject`/`HeadObject`/`DeleteObject` and not `PutObject`
    /// reach this variant and then fail every upload. See
    /// [`S3Backend::check_bucket`] for the rest of what this cannot tell you.
    Reachable,
    /// The probe returned 403.
    ///
    /// Inconclusive, not damning; at least three things produce it.
    ///
    /// Real AWS S3 answers `HeadObject` on a non-existent key with 403 rather
    /// than 404 unless the caller also holds bucket-level `s3:ListBucket` — a
    /// permission this backend never otherwise needs and a least-privileged
    /// policy would correctly omit. So this same 403 is produced by wrong
    /// credentials *and* by exactly-right, correctly-scoped ones.
    ///
    /// A clock is the third. SigV4 rejects a request whose `X-Amz-Date` is
    /// more than about fifteen minutes from the store's own clock, and an
    /// expired presign is rejected the same way — both with a 403 that names
    /// the signature, not the clock. `stepd doctor`'s `clock_skew` check will
    /// not catch it: that one compares this host against Postgres, and the
    /// object store is a third clock nothing here compares against.
    ///
    /// See [`S3Backend::check_bucket`].
    Forbidden,
    /// No usable answer — wrong endpoint, network failure, or a status other
    /// than success, 404 or 403.
    Unreachable(String),
}

/// Id the doctor probe reads, chosen to be one this backend can never have
/// stored: [`S3Backend::upload_target`] is only ever handed a
/// [`uuid::Uuid::now_v7`] id, which is time-ordered and never all-zero.
const DOCTOR_PROBE_ID: Uuid = Uuid::nil();

impl S3Backend {
    /// Ask the object store whether this backend's credentials can reach and
    /// read from the configured endpoint and bucket, with a `HeadObject` on
    /// an id that can never exist rather than a `HeadBucket`.
    ///
    /// Deliberately not `HeadBucket`: that action needs bucket-level
    /// `s3:ListBucket`, a permission this backend does not otherwise use —
    /// every real operation here is `PutObject`/`GetObject`/`HeadObject`/
    /// `DeleteObject` scoped to `bucket/*`. A policy scoped to exactly what
    /// this backend needs would then make `HeadBucket` answer 403 while every
    /// real transfer succeeds: a correctly least-privileged deployment
    /// failing its own health check.
    ///
    /// `HeadObject` does not fully escape that problem, and callers of this
    /// method must not assume it does. It needs no permission this backend
    /// does not already require — but on real AWS S3 (confirmed against the
    /// documented behaviour, not just inferred from the permission model),
    /// `HeadObject` on a key that does not exist itself answers 403 rather
    /// than 404 *unless the caller also holds `s3:ListBucket`* — the same
    /// permission this whole probe exists to avoid requiring. A clock far
    /// enough out from the object store's produces the same 403 through
    /// SigV4's own freshness window. So [`BucketCheck::Forbidden`] from this
    /// probe does not mean "these credentials are wrong"; it means "wrong
    /// credentials, or right credentials correctly scoped to exactly what
    /// this backend uses, or a skewed clock." A caller must treat it as
    /// inconclusive, not as a confirmed failure — see the variant's own doc
    /// comment.
    ///
    /// Two further costs, even setting the 403 ambiguity aside.
    ///
    /// This is a read, so it says nothing about writing. A bucket policy
    /// granting `GetObject`, `HeadObject` and `DeleteObject` but not
    /// `PutObject` — the likeliest way to get a least-privileged policy
    /// *nearly* right — answers this probe 404 and passes as
    /// [`BucketCheck::Reachable`], and then every app's first upload fails at
    /// the object store with a 403 nothing here predicted. The probe stays
    /// read-only anyway: a write probe would have to leave an object behind
    /// or delete one, and a health check that writes to the bucket it is
    /// checking is a worse trade than a health check with a stated blind
    /// spot. Callers must state it rather than report an unqualified pass.
    ///
    /// And it cannot tell an absent bucket apart from a bucket that exists
    /// but holds nothing at the probed key, since MinIO- and RustFS-style
    /// stores answer both with 404. It answers "can these credentials read
    /// from where the config says the bucket is", not "does the bucket
    /// exist"; callers should say so rather than imply the stronger claim.
    ///
    /// For `stepd doctor`: an unreachable endpoint today surfaces only when
    /// an app's first upload fails, a system away from whoever configured it.
    /// This lets an operator learn it at the same moment they learn
    /// everything else `doctor` checks.
    pub async fn check_bucket(&self) -> BucketCheck {
        let key = Self::key(DOCTOR_PROBE_ID);
        let mut action = HeadObject::new(&self.bucket, Some(&self.credentials), &key);
        action
            .headers_mut()
            .insert("x-amz-checksum-mode", "ENABLED");
        let url = action.sign(INTERNAL_TTL);
        match self
            .http
            .head(url)
            .header("x-amz-checksum-mode", "ENABLED")
            .send()
            .await
        {
            Ok(res)
                if res.status().is_success() || res.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                BucketCheck::Reachable
            }
            Ok(res) if res.status() == reqwest::StatusCode::FORBIDDEN => BucketCheck::Forbidden,
            Ok(res) => BucketCheck::Unreachable(format!("HeadObject returned {}", res.status())),
            Err(e) => BucketCheck::Unreachable(e.to_string()),
        }
    }
}

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
        assert!(
            names.iter().any(|k| k == "x-amz-checksum-sha256"),
            "got {names:?}"
        );
        assert!(names.iter().any(|k| k == "content-length"), "got {names:?}");

        let signed = t
            .url
            .split("X-Amz-SignedHeaders=")
            .nth(1)
            .expect("signed headers");
        assert!(
            signed.contains("x-amz-checksum-sha256"),
            "the checksum must be signed, not merely sent: {signed}"
        );
        assert!(
            signed.contains("content-length"),
            "the length must be signed, or a URL stores more than was reserved: {signed}"
        );
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

    #[test]
    fn a_checksum_read_back_from_metadata_is_the_digest_that_was_declared() {
        // The two directions must agree, or `commit_blob` compares a correctly
        // stored object against the wrong string and reports it as corruption.
        let hex = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let header = checksum_header(hex).expect("valid hex");
        assert_eq!(digest_from_header(&header).expect("valid base64"), hex);
    }

    #[test]
    fn objects_are_keyed_by_blob_id_rather_than_by_digest() {
        // Two namespaces may legitimately hold the same bytes under different
        // blob ids. Keyed by digest they would share one object and collecting
        // either would delete the other's bytes; keyed by id they do not.
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        assert_ne!(S3Backend::key(a), S3Backend::key(b));
        assert!(S3Backend::key(a).ends_with(&a.simple().to_string()));
        assert_eq!(
            S3Backend::key(a).matches('/').count(),
            3,
            "prefix plus two shard levels: {}",
            S3Backend::key(a)
        );
    }

    #[test]
    fn neither_the_config_nor_the_backend_prints_its_secret_key() {
        // Configuration gets logged. A derived `Debug` on either of these would
        // put a long-lived object-store credential into whatever collects the
        // logs, where nothing rotates it and nothing knows it is there.
        let cfg = S3Config {
            endpoint: "http://127.0.0.1:9000".parse().unwrap(),
            region: "us-east-1".into(),
            bucket: "stepd".into(),
            access_key: "probe".into(),
            secret_key: "probeprobe".into(),
            path_style: true,
        };
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("probeprobe"), "got {printed}");
        assert!(printed.contains("probe"), "the key id is not the secret");

        let printed = format!("{:?}", backend());
        assert!(!printed.contains("probeprobe"), "got {printed}");
    }

    #[test]
    fn the_s3_backend_presigns_and_says_which_backend_it_is() {
        // `can_presign` is what leaves §8.3.2's relay unmounted; `name` is what
        // the start-up line reports when a backend cannot presign. A backend
        // that answered "filesystem" here would make that line a lie.
        let b = backend();
        assert!(b.can_presign());
        assert_eq!(b.name(), "s3");
    }

    #[tokio::test]
    async fn a_bucket_check_against_nothing_listening_is_unreachable_not_a_panic() {
        // No server on this port: the point is that `check_bucket` returns a
        // value rather than propagating a transport error `doctor` would have
        // to unwrap.
        let b = S3Backend::new(S3Config {
            endpoint: "http://127.0.0.1:1".parse().unwrap(),
            region: "us-east-1".into(),
            bucket: "stepd".into(),
            access_key: "probe".into(),
            secret_key: "probeprobe".into(),
            path_style: true,
        })
        .expect("a backend builds from static configuration");
        assert!(matches!(
            b.check_bucket().await,
            BucketCheck::Unreachable(_)
        ));
    }

    #[tokio::test]
    async fn a_store_that_accepts_a_connection_and_never_answers_does_not_block_forever() {
        // The failure this bounds is not a slow blob, it is a stopped engine:
        // `stored` is reached from `commit_blob` ← `verify_blobs` ←
        // `Dispatcher::drive`, and the two loops above that are sequential
        // `for` loops over leases and then over namespaces on one task. A
        // `HeadObject` that never returns therefore holds up dispatch for
        // every namespace, and the circuit breaker in front of it watches the
        // app transport, not this one.
        //
        // Deliberately a socket that *accepts* and then says nothing, not a
        // closed port: a closed port already fails fast without any timeout,
        // so a test against one passes with the timeouts removed.
        // `a_bucket_check_against_nothing_listening_is_unreachable_not_a_panic`
        // above is that other case, and it is a different property.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("a bound address");
        let _sink = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((conn, _)) = listener.accept().await {
                // Held, never read from and never written to. Dropping the
                // socket here would send a FIN and let the client fail
                // promptly, which is the case this test is not about.
                held.push(conn);
            }
        });

        // Short budgets so the assertion is about behaviour rather than about
        // waiting out the production ones; `new` is what applies those.
        let backend = S3Backend::with_timeouts(
            S3Config {
                endpoint: format!("http://{addr}").parse().expect("a url"),
                region: "us-east-1".into(),
                bucket: "stepd".into(),
                access_key: "probe".into(),
                secret_key: "probeprobe".into(),
                path_style: true,
            },
            std::time::Duration::from_millis(200),
            std::time::Duration::from_millis(200),
        )
        .expect("a backend builds from static configuration");

        // The outer bound is the assertion, not a safety net: without a
        // timeout on the client the inner call never returns, and a test that
        // simply awaited it would hang the suite instead of going red.
        // Generous by two orders of magnitude against the 200ms budget, so a
        // loaded CI runner cannot fail it either way.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            backend.stored(Uuid::nil()),
        )
        .await
        .expect("the backend must give up on its own rather than wait forever");
        assert!(
            result.is_err(),
            "a request that never gets an answer must end as an error"
        );
    }

    #[test]
    fn an_expired_ttl_is_refused_rather_than_signed() {
        // Signing a URL that has already expired produces a signature error at
        // the object store that names nothing about the TTL.
        let b = backend();
        assert!(b
            .read_url(Uuid::nil(), 5, chrono::Duration::seconds(-1))
            .is_err());
    }
}
