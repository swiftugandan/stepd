//! The S3 backend against a real S3-compatible server.
//!
//! These are the tests that can fail for reasons a unit test cannot see: the
//! store accepting bytes it should have refused, a checksum that never comes
//! back on `HeadObject`, a presigned GET that ignores `Range`. Everything the
//! design rests on lives here rather than in `src/lib.rs`, because signing a
//! URL correctly and having a server honour it are different claims.
//!
//! Run them against a server [`docs/blob-backends.md`] says qualifies:
//!
//! ```sh
//! podman run -d --name stepd-s3 -p 9000:9000 \
//!   -e MINIO_ROOT_USER=probe -e MINIO_ROOT_PASSWORD=probeprobe \
//!   quay.io/minio/minio server /data
//! export STEPD_TEST_S3_ENDPOINT=http://127.0.0.1:9000
//! cargo test -p stepd-blobs-s3
//! ```

use rusty_s3::actions::{CreateBucket, PutObject};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use stepd_blobs_s3::{S3Backend, S3Config};
use stepd_core::traits::{BlobBackend, BlobSpec};
use url::Url;
use uuid::Uuid;

/// The five bytes every test here uploads, and what they hash to.
const BODY: &[u8] = b"hello";
const BODY_SHA256: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

/// Five different bytes, for the substitution attempts.
const OTHER: &[u8] = b"world";
const OTHER_SHA256: &str = "486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7";

/// Configuration for the live server, or `None` with a loud reason.
///
/// Skips loudly without `STEPD_TEST_S3_ENDPOINT`. A green run that skipped this
/// is not a green run — the same rule as `STEPD_TEST_DATABASE_URL`, and for the
/// same reason: the claims these tests carry are claims about a real object
/// store, and nothing else in the build produces them.
fn config() -> Option<S3Config> {
    let endpoint = match std::env::var("STEPD_TEST_S3_ENDPOINT") {
        Ok(v) => v,
        Err(_) => {
            eprintln!(
                "SKIPPED: set STEPD_TEST_S3_ENDPOINT to run the live S3 tests. \
                 Without them nothing in this build checks that an object store \
                 actually refuses bytes that do not match the signed checksum, \
                 which is the property the whole backend rests on."
            );
            return None;
        }
    };
    let var = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    Some(S3Config {
        endpoint: endpoint.parse().expect("STEPD_TEST_S3_ENDPOINT is a URL"),
        // The live suite reaches the store as both server and client at one
        // address, which is the shape this field exists to stop being the
        // only one possible. Overridable so a future lane can split them.
        public_endpoint: std::env::var("STEPD_TEST_S3_PUBLIC_ENDPOINT")
            .ok()
            .map(|v| v.parse().expect("STEPD_TEST_S3_PUBLIC_ENDPOINT is a URL")),
        region: var("STEPD_TEST_S3_REGION", "us-east-1"),
        bucket: var("STEPD_TEST_S3_BUCKET", "stepd"),
        access_key: var("STEPD_TEST_S3_ACCESS_KEY", "probe"),
        secret_key: var("STEPD_TEST_S3_SECRET_KEY", "probeprobe"),
        // MinIO's root credentials are a permanent pair; a live lane against a
        // store issuing temporary ones would set this.
        session_token: std::env::var("STEPD_TEST_S3_SESSION_TOKEN").ok(),
        path_style: true,
    })
}

/// Create the test bucket, tolerating one that is already there.
///
/// `S3Backend` never creates a bucket — a control plane that can create storage
/// is a control plane that can create it in the wrong place — so the tests do
/// it, once each, idempotently.
async fn ensure_bucket(cfg: &S3Config) {
    let bucket = Bucket::new(
        cfg.endpoint.clone(),
        UrlStyle::Path,
        cfg.bucket.clone(),
        cfg.region.clone(),
    )
    .expect("a usable bucket url");
    let credentials = Credentials::new(cfg.access_key.clone(), cfg.secret_key.clone());
    let url = CreateBucket::new(&bucket, &credentials).sign(std::time::Duration::from_secs(60));
    let res = reqwest::Client::new()
        .put(url)
        .send()
        .await
        .expect("the test object store answers");
    assert!(
        res.status().is_success() || res.status() == reqwest::StatusCode::CONFLICT,
        "creating the test bucket returned {}",
        res.status()
    );
}

/// The spec an app would declare for [`BODY`].
fn spec() -> BlobSpec {
    BlobSpec {
        size: BODY.len() as i64,
        sha256: BODY_SHA256.into(),
        content_type: Some("text/plain".into()),
        filename: None,
    }
}

/// Upload `bytes` to an [`UploadTarget`], replaying its headers the way the SDK
/// does.
///
/// Mirrors `stepd_sdk::blobs::PutBuilder::send` deliberately — every reservation
/// header goes on the request verbatim — so that these tests fail if the header
/// set the backend reserves stops being one a real client can send. The SDK is
/// not depended on here: an object-store backend that reached for the app-side
/// SDK would be a layering inversion, and the shared thing is a protocol rule,
/// not code.
async fn put_with(
    target: &stepd_core::traits::UploadTarget,
    bytes: &'static [u8],
) -> reqwest::Response {
    put_headers(&target.url, &target.headers, bytes).await
}

/// PUT `bytes` to `url` with exactly `headers`, whatever they say.
///
/// Split out of [`put_with`] so a test can send a header set the backend did
/// not reserve — which is what a client tampering with an upload URL would do,
/// and therefore the only way to test that it fails.
async fn put_headers(
    url: &str,
    headers: &[(String, String)],
    bytes: &'static [u8],
) -> reqwest::Response {
    let mut req = reqwest::Client::new().put(url).body(bytes);
    for (k, v) in headers {
        req = req.header(k, v);
    }
    req.send().await.expect("the object store answers")
}

/// A lowercase hex SHA-256 in the base64 form `x-amz-checksum-sha256` carries.
///
/// The unit tests pin this encoding against a literal; here it is a
/// convenience, so that a test can name the digest of the bytes it is actually
/// sending rather than a magic string.
fn checksum_of(hex_digest: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(hex::decode(hex_digest).expect("a hex digest"))
}

/// The object key the backend uses for `id`, read back out of the backend.
///
/// Taken from a presigned GET rather than reconstructed here on purpose: a
/// hand-copied copy of the shard layout would make the test that needs this
/// fail for an unrelated reason the day the layout changes, and what that test
/// claims has nothing to do with where objects live.
fn key_of(backend: &S3Backend, bucket: &str, id: Uuid) -> String {
    let signed = backend
        .read_url(id, 1, chrono::Duration::seconds(60))
        .expect("a read url");
    Url::parse(&signed)
        .expect("a presigned url")
        .path()
        .trim_start_matches('/')
        .strip_prefix(&format!("{bucket}/"))
        .expect("a path-style url names the bucket before the key")
        .to_string()
}

#[tokio::test]
async fn the_store_refuses_bytes_that_do_not_match_the_declared_digest() {
    let Some(cfg) = config() else { return };
    ensure_bucket(&cfg).await;
    let b = S3Backend::new(cfg).expect("a backend");

    let id = Uuid::now_v7();
    let t = b
        .upload_target(id, &spec(), chrono::Duration::seconds(300))
        .await
        .expect("an upload target");

    // Same length, different bytes, reserved headers replayed honestly: the
    // signature still verifies, so this reaches the store's own checksum check
    // rather than bouncing off SigV4.
    let res = put_with(&t, OTHER).await;

    // Pinned to 400, not merely "a client error". A 403 here would be
    // `SignatureDoesNotMatch` — which `docs/blob-backends.md` records is exactly
    // how both qualifying servers rejected a *different-length* body, and which
    // a signing regression would produce for these bytes too. Accepting it would
    // leave this test green while it stopped certifying the property the whole
    // backend rests on. Both servers returned 400 for this case.
    assert_eq!(
        res.status().as_u16(),
        400,
        "the object store must reject these on the checksum, or the server has \
         to read them to find out; 403 here means the signature failed, which \
         is not what this test claims"
    );

    // And nothing was stored, so a commit against this id cannot succeed either.
    assert!(
        b.stored(id).await.expect("queried").is_none(),
        "a refused upload must leave no object behind"
    );
}

#[tokio::test]
async fn a_client_cannot_swap_the_checksum_for_one_matching_its_own_bytes() {
    let Some(cfg) = config() else { return };
    ensure_bucket(&cfg).await;
    let b = S3Backend::new(cfg).expect("a backend");

    let id = Uuid::now_v7();
    let t = b
        .upload_target(id, &spec(), chrono::Duration::seconds(300))
        .await
        .expect("an upload target");

    // The threat the design is built against: whoever holds the upload URL
    // substitutes different content. Sending mismatched bytes with the reserved
    // headers is only half of it — the obvious next move is to send the
    // checksum of the substituted bytes, so that the store's own check passes.
    //
    // That is what the *signature* stops, and only because
    // `x-amz-checksum-sha256` is inside `X-Amz-SignedHeaders` rather than merely
    // in the header list the backend hands back. Without this test that
    // distinction rests on a substring match in a unit test.
    let tampered: Vec<(String, String)> = t
        .headers
        .iter()
        .map(|(k, v)| {
            if k.eq_ignore_ascii_case("x-amz-checksum-sha256") {
                (k.clone(), checksum_of(OTHER_SHA256))
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect();
    assert!(
        tampered
            .iter()
            .any(|(_, v)| v == &checksum_of(OTHER_SHA256)),
        "the reserved headers must contain a checksum to substitute"
    );

    let res = put_headers(&t.url, &tampered, OTHER).await;

    // 403, not 400, and the difference matters: a signed header that was altered
    // never reaches the store's checksum comparison, because the request no
    // longer verifies. A 400 here would mean the checksum header was *not*
    // covered by the signature and the store merely happened to disagree with
    // it — a URL that could be re-pointed at any content.
    assert_eq!(
        res.status().as_u16(),
        403,
        "altering a signed header must invalidate the signature; got {}",
        res.status()
    );
    assert!(
        b.stored(id).await.expect("queried").is_none(),
        "a refused upload must leave no object behind"
    );
}

#[tokio::test]
async fn an_object_the_store_reports_no_checksum_for_is_an_error_not_a_fallback() {
    let Some(cfg) = config() else { return };
    ensure_bucket(&cfg).await;
    let raw = cfg.clone();
    let b = S3Backend::new(cfg).expect("a backend");

    // Store an object the way something other than this backend would: a plain
    // presigned PUT with no checksum header at all.
    let id = Uuid::now_v7();
    let key = key_of(&b, &raw.bucket, id);
    let bucket = Bucket::new(
        raw.endpoint.clone(),
        UrlStyle::Path,
        raw.bucket.clone(),
        raw.region.clone(),
    )
    .expect("a usable bucket url");
    let credentials = Credentials::new(raw.access_key.clone(), raw.secret_key.clone());
    let url =
        PutObject::new(&bucket, Some(&credentials), &key).sign(std::time::Duration::from_secs(300));
    let res = reqwest::Client::new()
        .put(url)
        .body(BODY)
        .send()
        .await
        .expect("the object store answers");
    assert!(
        res.status().is_success(),
        "the unchecksummed upload should land; got {}",
        res.status()
    );

    // It is there, at the key this backend would use.
    let read = b
        .read_url(id, BODY.len() as i64, chrono::Duration::seconds(300))
        .expect("a read url");
    assert!(reqwest::Client::new()
        .get(&read)
        .send()
        .await
        .expect("the object store answers")
        .status()
        .is_success());

    // And `stored` refuses it rather than reporting `sha256: None`. `None` is a
    // statement about the backend's capability, not about one object: it routes
    // `commit_blob` into reading the bytes to hash them, which is the single
    // thing this backend exists not to do. An object the store cannot vouch for
    // was never verified on the way in, so failing closed here is also the only
    // safe answer.
    let err = b
        .stored(id)
        .await
        .expect_err("an object with no checksum cannot be verified from metadata");
    let message = err.to_string();
    assert!(
        message.contains("x-amz-checksum-sha256"),
        "the error must name what was missing: {message}"
    );
    assert!(
        message.contains("will not"),
        "and must say the backend refuses rather than that it failed: {message}"
    );
    assert!(
        message.contains("docs/blob-backends.md"),
        "and must point at the compatibility note, which is where the remedy is: {message}"
    );
    // The classification, not just the message. Reported as `Error::Store` this
    // was retryable, and a server that accepts the signed checksum but does not
    // report it back on HeadObject — the class `docs/blob-backends.md` exists to
    // warn about — would therefore fail no configuration check and instead make
    // every blob-bearing run re-execute its steps up to `quarantine_after` times
    // before quarantining. The condition is permanent: an object stored without
    // a checksum does not grow one.
    assert!(
        !err.is_retryable(),
        "a permanent condition must not be retried: {message}"
    );

    b.delete(id).await.expect("clean up");
}

#[tokio::test]
async fn a_committed_object_reports_its_digest_without_transferring_it() {
    let Some(cfg) = config() else { return };
    ensure_bucket(&cfg).await;
    let b = S3Backend::new(cfg).expect("a backend");

    let id = Uuid::now_v7();
    let t = b
        .upload_target(id, &spec(), chrono::Duration::seconds(300))
        .await
        .expect("an upload target");
    let res = put_with(&t, BODY).await;
    assert!(
        res.status().is_success(),
        "upload returned {}",
        res.status()
    );

    // This is the method `commit_blob` calls. It answers from `HeadObject`
    // metadata; a backend that downloaded the object to hash it would pass this
    // assertion and defeat the entire design, which is why the digest being
    // `Some` at all is the load-bearing part.
    let got = b.stored(id).await.expect("queried").expect("present");
    assert_eq!(got.size, BODY.len() as i64);
    assert_eq!(
        got.sha256.as_deref(),
        Some(BODY_SHA256),
        "the store must report the digest itself, in the protocol's hex form"
    );
}

#[tokio::test]
async fn a_presigned_read_serves_a_range() {
    let Some(cfg) = config() else { return };
    ensure_bucket(&cfg).await;
    let b = S3Backend::new(cfg).expect("a backend");

    let id = Uuid::now_v7();
    let t = b
        .upload_target(id, &spec(), chrono::Duration::seconds(300))
        .await
        .expect("an upload target");
    assert!(put_with(&t, BODY).await.status().is_success());

    // §8.3.3: `Range` is applied by the reader and is not one of the signed
    // headers, which is what lets a ranged read work through a presigned URL at
    // all. If it were signed, every ranged read would fail the signature.
    let url = b
        .read_url(id, BODY.len() as i64, chrono::Duration::seconds(300))
        .expect("a read url");
    let res = reqwest::Client::new()
        .get(&url)
        .header("range", "bytes=0-2")
        .send()
        .await
        .expect("the object store answers");
    assert_eq!(res.status().as_u16(), 206, "expected a partial response");
    assert_eq!(res.bytes().await.expect("a body").as_ref(), &BODY[0..3]);
}

#[tokio::test]
async fn deleting_an_object_is_idempotent() {
    let Some(cfg) = config() else { return };
    ensure_bucket(&cfg).await;
    let b = S3Backend::new(cfg).expect("a backend");

    let id = Uuid::now_v7();
    let t = b
        .upload_target(id, &spec(), chrono::Duration::seconds(300))
        .await
        .expect("an upload target");
    assert!(put_with(&t, BODY).await.status().is_success());

    b.delete(id).await.expect("the object is deleted");
    assert!(
        b.stored(id).await.expect("queried").is_none(),
        "a deleted object must read as absent, not as an error"
    );
    // Collection retries, and a sweep that failed on an object another sweep
    // already removed would never finish.
    b.delete(id).await.expect("deleting again is still success");
}
