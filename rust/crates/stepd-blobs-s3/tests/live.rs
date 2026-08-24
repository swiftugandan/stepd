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

use rusty_s3::actions::CreateBucket;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use stepd_blobs_s3::{S3Backend, S3Config};
use stepd_core::traits::{BlobBackend, BlobSpec};
use uuid::Uuid;

/// The five bytes every test here uploads, and what they hash to.
const BODY: &[u8] = b"hello";
const BODY_SHA256: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

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
        region: var("STEPD_TEST_S3_REGION", "us-east-1"),
        bucket: var("STEPD_TEST_S3_BUCKET", "stepd"),
        access_key: var("STEPD_TEST_S3_ACCESS_KEY", "probe"),
        secret_key: var("STEPD_TEST_S3_SECRET_KEY", "probeprobe"),
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
    let mut req = reqwest::Client::new().put(&target.url).body(bytes);
    for (k, v) in &target.headers {
        req = req.header(k, v);
    }
    req.send().await.expect("the object store answers")
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

    // Same length, different bytes: the signature still verifies, so this
    // reaches the store's own checksum check rather than bouncing off SigV4.
    let res = put_with(&t, b"world").await;
    assert!(
        res.status().is_client_error(),
        "the object store must reject these, or the server has to read them to \
         find out; got {}",
        res.status()
    );

    // And nothing was stored, so a commit against this id cannot succeed either.
    assert!(
        b.stored(id).await.expect("queried").is_none(),
        "a refused upload must leave no object behind"
    );
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
