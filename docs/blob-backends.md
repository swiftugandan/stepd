# Object store compatibility: presigned PUT with a signed checksum header

The S3 blob backend design (see the [S3 blob backend
plan](.superpowers/sdd/2026-08-24-s3-blob-backend/)) presigns a `PutObject`
whose signature covers `x-amz-checksum-sha256`, and relies on the object
store rejecting a PUT whose body does not match that header. That property is
a [known sharp edge even on real
S3](https://github.com/aws/aws-sdk-js-v3/issues/3906) — some S3-compatible
servers accept the header, include it in the signature, and then never
actually verify the bytes against it. If a server does that, presigned
uploads to it are not tamper-evident and it cannot be used as a presigning
backend under this design; the durable write path would have to keep relaying
bytes through the control plane for that server instead.

This note records what two candidate servers actually did, checked with a
throwaway probe (not committed) built against `rusty-s3` 0.10, path-style
addressing, one 47-byte body, and a 300-second presign expiry.

## Result

| Server | Version | Presigned PUT w/ signed `x-amz-checksum-sha256` | Rejects mismatched bytes | Checksum on `HeadObject` | Presigned ranged GET |
|---|---|---|---|---|---|
| MinIO | `RELEASE.2025-09-07T16-13-09Z` | Yes | **Yes** | Yes | Yes (206 + `Content-Range`) |
| RustFS | `v1.0.0-beta.12` | Yes | **Yes** | Yes | Yes (206 + `Content-Range`) |

Both servers qualify as presigning backends: both reject a same-length body
that does not match the signed checksum, both return the checksum on
`HeadObject` when `x-amz-checksum-mode: ENABLED` is sent, and both serve a
presigned ranged `GET` correctly.

## What each server actually did

**MinIO** (`quay.io/minio/minio:latest`, started with the invocation in the
task brief unchanged) rejected the same-length mismatched body with `400 Bad
Request` and an XML body:

```
<Error><Code>XAmzContentChecksumMismatch</Code><Message>The provided
'x-amz-checksum' header does not match what was computed.</Message>...</Error>
```

This is an explicit, on-topic error: MinIO computed the checksum of the
received bytes and compared it against the signed header.

The different-length control case did **not** reach MinIO's checksum logic at
all: because `content-length` was one of the signed headers, sending an
honestly-declared shorter body (with a correct, matching `content-length`
header) invalidated the SigV4 signature before the object logic ran, and MinIO
returned `403 Forbidden` / `SignatureDoesNotMatch`. This is still a rejection
of the tampered request, just via a different mechanism than the checksum
check — worth noting because a first attempt at this control case (keeping the
*original* signed `content-length` value while sending a body of a different
actual length) produced a malformed request that MinIO did not fail fast on:
it held the connection for the full lock-wait window and returned `503
Service Unavailable` / `RequestTimeout` after 60 seconds. That is a transport
artifact of a mismatched declared-vs-actual body length, not a finding about
checksum enforcement, and is not reflected in the table above.

**RustFS** (`rustfs/rustfs:latest`, `1.0.0-beta.12`) rejected the same-length
mismatched body with `400 Bad Request`:

```
<Error><Code>BadDigest</Code><Message>The Content-Md5 you specified did not
match what we received.</Message></Error>
```

The rejection is real and immediate, but the error message is inaccurate: the
probe never sent a `Content-MD5` header, only `x-amz-checksum-sha256`. RustFS
is checking the right thing (the response confirmed a real digest mismatch,
not a signature failure — the same-length body carries a valid signature,
since only its content, not its headers, changed) but reports it under the
wrong name. Do not use this server's error text as a basis for a written claim
about which header it validated.

The different-length control case behaved the same way as MinIO's, for the
same reason (`content-length` is signed): `403 Forbidden` /
`SignatureDoesNotMatch`, no 60-second stall observed.

## RustFS startup

The task brief's invocation was a best guess at RustFS's env-var names and
port; it turned out to be correct. `podman run -d --name probe-rustfs -p
9010:9000 -e RUSTFS_ACCESS_KEY=probe -e RUSTFS_SECRET_KEY=probeprobe
rustfs/rustfs:latest` came up cleanly on the first try, listening on the
container's port 9000 (S3 API) as mapped. This was confirmed by reading the
image's `/entrypoint.sh`: it reads `RUSTFS_ACCESS_KEY` /
`RUSTFS_SECRET_KEY` directly (falling back to a built-in default credential
with only a warning, not a hard failure, if neither is set — so a
misconfigured deployment would run with default credentials rather than
refuse to start). The image also exposes a separate console port (9001,
unused by this probe) alongside the S3 API on 9000.

## Method

Probe: `rusty-s3` 0.10 (`UrlStyle::Path`), `reqwest` (rustls-tls), against a
47-byte body. Per server: `CreateBucket`, then a `PutObject` presign with
`headers_mut()` set to `x-amz-checksum-sha256: <base64 sha256 of body>` and
`content-length: <len>`, signed for 300s. Sequence: PUT the correct bytes
(expect 2xx), PUT a same-length body with one byte flipped against the same
URL and headers (the gating check — expect 4xx), PUT a different-length body
against the same URL and headers (control case), re-PUT the correct bytes,
`HeadObject` with `x-amz-checksum-mode: ENABLED` (print all response
headers), then a presigned `GetObject` fetched with `Range: bytes=0-3`
(expect 206 + `Content-Range`). The probe source is throwaway and was not
committed.
