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
        let style = if config.path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };
        let bucket = Bucket::new(config.endpoint, style, config.bucket, config.region)
            .map_err(|e| Error::Config(format!("blob store endpoint is unusable: {e}")))?;
        let http = reqwest::Client::builder()
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
    /// is the one thing this backend must not do.
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

        let checksum = res
            .headers()
            .get("x-amz-checksum-sha256")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                Error::Store(format!(
                    "the object store reports no x-amz-checksum-sha256 for blob {id}; \
                     this backend verifies digests from object metadata and will not \
                     download the object to hash it"
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
