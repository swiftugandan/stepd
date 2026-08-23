//! Managed blobs from the app's side (protocol §8.3).
//!
//! The rest of this SDK is a server: it receives attempts and answers them. This
//! is the one place an app calls stepd, because the two-phase upload requires it
//! — the app asks for a reservation, uploads the bytes somewhere the server never
//! sees them, and hands back a reference.
//!
//! ## Reading is lazy, and that is the whole point
//!
//! [`Blob`] is a value, not a handle. Constructing one costs nothing, replaying
//! one costs nothing, and passing one between steps costs nothing. Bytes move
//! only when you call [`Blobs::read`].
//!
//! That is what keeps replay cheap. A run with forty blob-bearing steps replays
//! on attempt forty-one by decoding forty references — not by re-downloading
//! forty objects, which is what a handle that fetched on construction would do,
//! and which would make a large workflow quadratically expensive in exactly the
//! situation where it is already having a bad day.
//!
//! ## Uploading belongs inside a step
//!
//! An upload is a side effect. Doing it inside `ctx.step` records the reference
//! in the journal, so a retry memoises it and the bytes are stored once:
//!
//! ```ignore
//! let receipt: Blob = ctx.step("receipt", || async {
//!     blobs.put(ctx.run().id, &pdf).content_type("application/pdf").await
//! }).await?;
//! ```
//!
//! Uploading outside a step still works and re-uploads on every attempt. Content
//! addressing makes that cheap rather than wrong — the second reservation is
//! deduplicated and the upload is skipped — but the reference will not be in the
//! journal, so nothing keeps the bytes alive.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use stepd_proto::BlobRef;
use uuid::Uuid;

/// A reference to managed bytes, as it appears in a step result.
///
/// Serialises as `{"$blob": {…}}`, which is what the server walks for to build
/// the reference graph. Nothing else about the shape is the app's business.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    /// The reference itself.
    #[serde(rename = "$blob")]
    pub inner: BlobRef,
}

impl Blob {
    /// Size in bytes, as declared at upload.
    pub fn size(&self) -> i64 {
        self.inner.size
    }

    /// Lowercase hex SHA-256 of the content.
    pub fn sha256(&self) -> &str {
        &self.inner.sha256
    }

    /// Media type, if one was given.
    pub fn content_type(&self) -> Option<&str> {
        self.inner.content_type.as_deref()
    }
}

/// What went wrong moving bytes.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// The stepd server refused or could not be reached.
    #[error("blob transfer failed: {0}")]
    Transport(String),
    /// The server answered, unhappily.
    #[error("stepd refused the blob operation: {0}")]
    Refused(String),
    /// A reference with no usable read URL.
    ///
    /// The URL is minted per attempt and never persisted (§8.3.1), so a
    /// reference the app kept across attempts, or built itself, has none.
    #[error("this blob reference carries no read URL; it must come from a step result of the current attempt (§8.3.1)")]
    NoReadUrl,
    /// The bytes read back do not hash to what the reference declares.
    #[error("blob {0} content does not match its declared digest")]
    DigestMismatch(Uuid),
}

/// The app's client for the two transfer endpoints.
///
/// Holds the stepd base URL and an operator token. Built once and shared;
/// cloning is cheap.
#[derive(Debug, Clone)]
pub struct Blobs {
    http: reqwest::Client,
    base: String,
    token: String,
    verify_on_read: bool,
}

impl Blobs {
    /// A client for `stepd_url`, authenticating with `token`.
    pub fn new(stepd_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .expect("a default reqwest client builds"),
            base: stepd_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            // On by default. The server verifies on commit, which stops content
            // substitution at rest; this catches a truncated or corrupted
            // *transfer*, which the server cannot see. It costs one hash of data
            // already in memory.
            verify_on_read: true,
        }
    }

    /// Stop hashing content on read.
    ///
    /// Worth it only for large objects read in full on a hot path, and it gives
    /// up the one check that a transfer arrived intact.
    pub fn without_read_verification(mut self) -> Self {
        self.verify_on_read = false;
        self
    }

    /// Store bytes and get a reference back.
    pub fn put<'a>(&'a self, run: Uuid, bytes: &'a [u8]) -> PutBuilder<'a> {
        PutBuilder {
            blobs: self,
            run,
            bytes,
            content_type: None,
            filename: None,
        }
    }

    /// Read a blob in full.
    pub async fn read(&self, blob: &Blob) -> Result<Vec<u8>, BlobError> {
        let bytes = self.fetch(blob, None).await?;
        if self.verify_on_read {
            let actual = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes));
            if actual != blob.inner.sha256 {
                return Err(BlobError::DigestMismatch(blob.inner.id));
            }
        }
        Ok(bytes)
    }

    /// Read a byte range, inclusive of both ends (§8.3.3).
    ///
    /// The reason `Range` is in the protocol rather than left to implementations:
    /// a step that needs a file's header should not pull a hundred megabytes to
    /// read the first kilobyte of it. The digest is not checked here, because a
    /// range does not hash to the whole object's digest.
    pub async fn read_range(&self, blob: &Blob, from: u64, to: u64) -> Result<Vec<u8>, BlobError> {
        self.fetch(blob, Some((from, to))).await
    }

    async fn fetch(&self, blob: &Blob, range: Option<(u64, u64)>) -> Result<Vec<u8>, BlobError> {
        let url = blob.inner.url.as_deref().ok_or(BlobError::NoReadUrl)?;
        let mut req = self.http.get(url);
        if let Some((from, to)) = range {
            req = req.header("range", format!("bytes={from}-{to}"));
        }
        let res = req
            .send()
            .await
            .map_err(|e| BlobError::Transport(e.to_string()))?;
        if !res.status().is_success() {
            return Err(BlobError::Refused(format!(
                "{}: {}",
                res.status(),
                res.text().await.unwrap_or_default()
            )));
        }
        Ok(res
            .bytes()
            .await
            .map_err(|e| BlobError::Transport(e.to_string()))?
            .to_vec())
    }
}

/// Configures an upload before it happens.
pub struct PutBuilder<'a> {
    blobs: &'a Blobs,
    run: Uuid,
    bytes: &'a [u8],
    content_type: Option<String>,
    filename: Option<String>,
}

impl<'a> PutBuilder<'a> {
    /// Media type, used by the console to choose a preview.
    pub fn content_type(mut self, ct: impl Into<String>) -> Self {
        self.content_type = Some(ct.into());
        self
    }

    /// Original filename. Presentational only; never used as a path.
    pub fn filename(mut self, name: impl Into<String>) -> Self {
        self.filename = Some(name.into());
        self
    }

    /// Reserve, upload if needed, and return the reference.
    pub async fn send(self) -> Result<Blob, BlobError> {
        let b = self.blobs;
        let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(self.bytes));

        #[derive(Deserialize)]
        struct ReserveResponse {
            blob_id: Uuid,
            deduplicated: bool,
            upload_url: Option<String>,
            #[serde(default)]
            headers: std::collections::BTreeMap<String, String>,
        }

        let res = b
            .http
            .post(format!("{}/v1/blobs:reserve", b.base))
            .bearer_auth(&b.token)
            .json(&serde_json::json!({
                "run_id": self.run,
                "size": self.bytes.len() as i64,
                "sha256": digest,
                "content_type": self.content_type,
                "filename": self.filename,
            }))
            .send()
            .await
            .map_err(|e| BlobError::Transport(e.to_string()))?;

        if !res.status().is_success() {
            return Err(BlobError::Refused(format!(
                "{}: {}",
                res.status(),
                res.text().await.unwrap_or_default()
            )));
        }
        let reservation: ReserveResponse = res
            .json()
            .await
            .map_err(|e| BlobError::Transport(e.to_string()))?;

        // Deduplicated: these exact bytes are already stored in this namespace,
        // and §8.3.2 says the app MUST skip the upload. This is what makes
        // replaying an event or retrying a run cheap rather than merely correct
        // — the second attempt moves no bytes at all.
        if !reservation.deduplicated {
            let url = reservation.upload_url.ok_or_else(|| {
                BlobError::Refused(
                    "the reservation was not deduplicated and carries no upload URL".into(),
                )
            })?;
            let mut req = b.http.put(url).body(self.bytes.to_vec());
            for (k, v) in &reservation.headers {
                req = req.header(k, v);
            }
            let up = req
                .send()
                .await
                .map_err(|e| BlobError::Transport(e.to_string()))?;
            if !up.status().is_success() {
                return Err(BlobError::Refused(format!(
                    "upload rejected {}: {}",
                    up.status(),
                    up.text().await.unwrap_or_default()
                )));
            }
        }

        Ok(Blob {
            inner: BlobRef {
                id: reservation.blob_id,
                size: self.bytes.len() as i64,
                sha256: digest,
                content_type: self.content_type,
                filename: self.filename,
                // Deliberately absent. §8.3.1: `url` is minted by the server per
                // attempt and MUST NOT be persisted by an SDK. Setting it here
                // would put a five-minute URL into the journal, where it would be
                // replayed hours later and fail in a way that reads like the blob
                // having disappeared.
                url: None,
            },
        })
    }
}

impl<'a> std::future::IntoFuture for PutBuilder<'a> {
    type Output = Result<Blob, BlobError>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_ref() -> BlobRef {
        BlobRef {
            id: Uuid::nil(),
            size: 3,
            sha256: "a".repeat(64),
            content_type: Some("text/plain".into()),
            filename: None,
            url: None,
        }
    }

    #[test]
    fn a_blob_serialises_as_the_protocol_reference_shape() {
        // The server walks step results for `$blob` to build the reference
        // graph. A different key means the bytes are never referenced and the
        // collector takes them while the run is still live.
        let v = serde_json::to_value(Blob { inner: a_ref() }).unwrap();
        assert!(v.get("$blob").is_some(), "got {v}");
        assert_eq!(v["$blob"]["size"], 3);
    }

    #[test]
    fn a_reference_round_trips_through_a_step_result() {
        let blob = Blob { inner: a_ref() };
        let encoded = serde_json::to_value(&blob).unwrap();
        let decoded: Blob = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.sha256(), blob.sha256());
        assert_eq!(decoded.size(), 3);
    }

    #[test]
    fn a_read_url_is_never_serialised_into_the_journal() {
        // §8.3.1. A URL persisted with the step is a five-minute credential
        // replayed hours later; the failure looks like the blob disappearing.
        let mut r = a_ref();
        r.url = Some("https://example.invalid/short-lived".into());
        let v = serde_json::to_value(Blob { inner: r }).unwrap();
        // `url` round-trips when present, because the server puts it there on
        // the way out — the SDK simply never originates one.
        assert!(v["$blob"]["url"].is_string());

        let fresh = serde_json::to_value(Blob { inner: a_ref() }).unwrap();
        assert!(
            fresh["$blob"].get("url").is_none(),
            "an SDK-built reference must carry no URL"
        );
    }

    #[tokio::test]
    async fn reading_a_reference_with_no_url_says_why() {
        // The likeliest way to hit this is holding a `Blob` across attempts, so
        // the message names the rule rather than reporting a null dereference.
        let blobs = Blobs::new("http://127.0.0.1:1", "t");
        let err = blobs.read(&Blob { inner: a_ref() }).await.unwrap_err();
        assert!(matches!(err, BlobError::NoReadUrl), "got {err}");
        assert!(err.to_string().contains("§8.3.1"));
    }
}
