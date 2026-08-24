//! Bytes on a local filesystem, reached through the server's relay
//! (protocol §8.3.2).

use async_trait::async_trait;
use chrono::Duration;
use std::path::{Path, PathBuf};
use stepd_core::traits::{BlobBackend, BlobSpec, RelayBytes, StoredObject, UploadTarget};
use stepd_core::{Error, Result};
use uuid::Uuid;

use super::Capability;

/// Bytes on a local filesystem root, reached through the server's relay.
///
/// What makes `stepd dev` work with no cloud account, and what CI's default
/// lane uses. It cannot presign — there is no signer in front of a directory —
/// so §8.3.2's relay applies and `can_presign` says so.
pub struct FilesystemBackend {
    root: PathBuf,
    caps: Capability,
}

impl FilesystemBackend {
    /// Build a backend rooted at `root`, minting URLs with `caps`.
    pub fn new(root: PathBuf, caps: Capability) -> Self {
        Self { root, caps }
    }

    /// Where a blob's bytes live under `root`.
    ///
    /// Sharded by the first bytes of the id so a large namespace does not
    /// produce a single directory with millions of entries, which several
    /// filesystems handle badly and every `ls` handles worse.
    fn path(&self, id: Uuid) -> PathBuf {
        let s = id.simple().to_string();
        self.root.join(&s[0..2]).join(&s[2..4]).join(s)
    }

    /// Whether a path is inside `root`, after normalisation.
    ///
    /// Not called from `put_bytes`/`get_bytes` today: blob ids are
    /// server-generated UUIDs, so `path` cannot be made to traverse outside
    /// `root` to begin with. Carried over from before this module existed,
    /// where it was public and so exempt from the dead-code lint by accident
    /// rather than by a caller; kept, allowed, and named honestly rather than
    /// deleted, since a path check costs nothing and the failure it would
    /// prevent is arbitrary file read.
    #[allow(dead_code)]
    fn is_within(&self, path: &Path) -> bool {
        match (self.root.canonicalize(), path.canonicalize()) {
            (Ok(r), Ok(p)) => p.starts_with(r),
            _ => path.starts_with(&self.root),
        }
    }
}

#[async_trait]
impl RelayBytes for FilesystemBackend {
    /// Store bytes for a reserved blob. Called by the server's relay endpoint
    /// after it has verified the capability.
    async fn put_bytes(&self, id: Uuid, bytes: &[u8]) -> Result<()> {
        let path = self.path(id);
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
        }
        // Write to a temporary name and rename into place, so a torn write never
        // presents itself as a complete blob to a concurrent reader.
        let tmp = path.with_extension("partial");
        tokio::fs::write(&tmp, bytes)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    /// Read a committed blob, optionally a byte range (protocol §8.3.3).
    async fn get_bytes(&self, id: Uuid, range: Option<(u64, u64)>) -> Result<Vec<u8>> {
        let bytes = tokio::fs::read(self.path(id))
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(match range {
            Some((from, to)) => {
                let from = (from as usize).min(bytes.len());
                let to = ((to as usize) + 1).min(bytes.len());
                bytes[from..to.max(from)].to_vec()
            }
            None => bytes,
        })
    }
}

#[async_trait]
impl BlobBackend for FilesystemBackend {
    async fn upload_target(
        &self,
        id: Uuid,
        spec: &BlobSpec,
        ttl: Duration,
    ) -> Result<UploadTarget> {
        let (url, expires_at) = self.caps.url(id, "write", spec.size, ttl);
        let mut headers = vec![("content-length".into(), spec.size.to_string())];
        if let Some(ct) = &spec.content_type {
            headers.push(("content-type".into(), ct.clone()));
        }
        Ok(UploadTarget {
            url,
            method: "PUT".into(),
            headers,
            expires_at,
        })
    }

    fn read_url(&self, id: Uuid, size: i64, ttl: Duration) -> Result<String> {
        Ok(self.caps.url(id, "read", size, ttl).0)
    }

    async fn stored(&self, id: Uuid) -> Result<Option<StoredObject>> {
        match tokio::fs::metadata(self.path(id)).await {
            Ok(m) => Ok(Some(StoredObject {
                size: m.len() as i64,
                sha256: None,
            })),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> (tempfile::TempDir, FilesystemBackend) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let caps = Capability::new("http://localhost:8080", b"k".to_vec());
        let b = FilesystemBackend::new(dir.path().to_path_buf(), caps);
        (dir, b)
    }

    #[tokio::test]
    async fn a_stored_object_reports_its_size_but_not_a_digest() {
        // The filesystem cannot answer "what is this object's sha256" without
        // reading it, so it says so rather than lying or hashing eagerly. An S3
        // backend answers from metadata; that difference is the whole point of
        // `Option` here.
        let (_dir, b) = backend();
        b.put_bytes(Uuid::nil(), b"hello").await.expect("stored");
        let got = b
            .stored(Uuid::nil())
            .await
            .expect("queried")
            .expect("present");
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

    #[test]
    fn blob_paths_shard_and_stay_under_the_root() {
        let root = Path::new("/tmp/stepd-blobs");
        let b = FilesystemBackend::new(
            root.to_path_buf(),
            Capability::new("http://localhost:8080", b"k".to_vec()),
        );
        let p = b.path(Uuid::now_v7());
        assert!(p.starts_with(root));
        assert_eq!(p.components().count(), 6, "root + two shard levels + file");
        assert!(b.is_within(&p));
    }
}
