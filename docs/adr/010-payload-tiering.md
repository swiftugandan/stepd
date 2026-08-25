# ADR-010: Payload tiering, direct upload and blob lifecycle

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Workflows carry data: an invoice PDF, a scan, an hour of video. A durable execution engine
records every step result, so whatever a step returns is written to the journal and shipped
back to the app in the `steps` map on every subsequent attempt.

Taken naively that has three compounding costs. A 40 MiB result in Postgres is toasted, then
re-read on each of the next forty attempts. It blows the 4 MiB attempt body limit (§8.2), so
the attempt fails or the state is truncated — and silent truncation makes the SDK re-execute
steps whose results were merely not sent, the exact silent-corruption class this project
exists to avoid. And routing bytes through the orchestrator makes control-plane throughput a
function of customer payload size, so one tenant's video degrades dispatch for everyone.

Meanwhile most payloads are small, and forcing every workflow through object storage to
accommodate the few that are not is a bad trade for the many that are.

## Decision

**Three tiers, selected by size** (protocol §8.1, F-BLB-1):

| Tier | Size | Bytes live | Owner |
|---|---|---|---|
| Inline | < 1 MiB | JSON in the step result, in Postgres | stepd |
| Managed blob | 1–100 MiB | blob store, `$blob` reference in the result | stepd, refcounted |
| External `$ref` | > 100 MiB | wherever the application already keeps them | the application |

**Bulk data does not traverse the stepd server.** Managed blobs use a two-phase direct upload
(§8.3.2): the app reserves (`BlobStore::reserve`, declaring size, SHA-256 and content type),
PUTs the bytes to the URL it is handed, then returns `{"$blob": {...}}` in the step result.
On a backend that presigns — `stepd-blobs-s3` — that URL addresses the object store and the
server sees the digest, never the bytes. The bundled filesystem backend cannot sign anything,
so §8.3.2's compatibility relay applies to it and the bytes do cross this process; that is a
deliberate exception, it is warned about, and it is what makes `stepd dev` work with no cloud
account.

**The upload and read URLs are signed capabilities, and the MAC covers the whole
capability.** `Capability::sign` in `engine/rust/crates/stepd-store-postgres/src/blobs/mod.rs`
HMACs `"{id}.{dir}.{size}.{expires}"`. Every field is load-bearing:

* `id` — a leaked URL cannot be pointed at another blob.
* `dir` — a write capability cannot be replayed as a read. This is the difference between a
  leaked upload URL being a nuisance and being an exfiltration primitive.
* `size` — the transfer endpoint enforces the declared size, so a write URL cannot store
  more than was reserved.
* `expires` — checked before the MAC comparison, which is itself constant-time
  (`subtle::ConstantTimeEq`), because a byte-by-byte compare leaks the correct prefix.

A session cookie expresses none of that, and the app holding an upload URL may hold no
console token at all. This capability is what authorises a transfer *through the server* —
the §8.3.2 relay. A backend that presigns mints its own URL instead, with the same
properties carried by its own signature: `stepd-blobs-s3` signs the object key, the method,
the declared length and the digest, and the URL expires.

**Content is verified before the blob is committed** (§8.3.2). `commit_blob` asks the backend
what it holds (`BlobBackend::stored`), compares length and SHA-256 against what was declared,
and only then sets `state='committed'`. Without it a compromised upload URL used in time
could substitute different content for a reference already committed into a step result, and
every later read would return the substituted bytes with nothing indicating a change. Which
call answers "and what is its digest" is the backend's business: object storage reads it from
metadata the store computed itself, the filesystem has no metadata and reads the bytes.

*Committed*, not *readable* — the two are not the same here, and the spec's word is the
stronger one. `attach_read_urls` mints a read URL from the backend with no database lookup
and no state check at all, so a `reserved` blob whose digest nobody has checked is already
dereferenceable. What commit actually gates is collection: an uncommitted row is taken by the
reservation sweep. Closing the gap between the two would mean `attach_read_urls` consulting
`state`, which it does not.

**Content addressing is per namespace, never global.** `UNIQUE (ns, sha256)` on `blobs`
(`engine/rust/migrations/0001_initial.sql`); `reserve` looks up `WHERE ns=$1 AND sha256=$2`. A
global scope would let one tenant probe for another's data by reserving a digest and seeing
whether the upload was skipped — an oracle over every byte sequence an attacker can guess.
Dedupe is what makes retrying a run cheap; it is not worth a cross-tenant side channel.

**Lifecycle is reference counting plus retention.** `blob_refs (blob_id, run_id, step_hash)`
records every reference; `blob_ids` walks a step result recursively, because a `$blob` can be
nested anywhere in arbitrary JSON. `collect` deletes two populations — reservations never
completed, and committed blobs nothing references any more — bytes first then the row, so a
crash mid-delete leaves a row pointing at nothing rather than a file nothing knows the name
of. **`$ref` is opaque**: the server never fetches, validates, transcodes or mints
credentials for one (§8.4), and its only interactions with blob content anywhere are digest
verification, URL minting and deletion (§8.5).

## Consequences

### What this makes easy
* Control-plane throughput is independent of payload size **on a presigning backend**: a
  tenant moving 100 GiB of video through an S3 store moves none of it through stepd. On the
  filesystem backend the relay applies and this does not hold — which is why the backend is
  an operator's configuration choice and not an implementation detail.
* Replay stays cheap — a run with forty blob-bearing steps re-downloads nothing on attempt
  forty-one, because SDKs dereference lazily (§8.3.3).
* Swapping where bytes live is a `BlobBackend` implementation: `PostgresBlobStore` keeps the
  index in Postgres — the row, the per-namespace dedupe, the `blob_refs` recorded by trigger,
  collection — and the backend owns the bytes and the URLs. `FilesystemBackend` writes to a
  local root, which is what makes `stepd dev` work with no cloud account; `stepd-blobs-s3`
  presigns against an object store.

  This ADR originally said "S3 is a drop-in", and that is what made the work look small. It
  was not one. `BlobStore` mixed the index with byte transfer, so a second implementation
  would have duplicated the correctness-bearing half to change the mechanical one. Four
  things had to move before an S3 backend was correct rather than merely present:
  `BlobBackend` had to be split out of `BlobStore`; raw transfer had to move to a separate
  `RelayBytes` trait, so a presigning backend is *unable* to answer the relay's methods
  rather than merely not asked to; `Server::router` had to stop mounting the §8.3.2 route
  when `can_presign()` is true, since an unused route that accepts bytes is a second way in;
  and `Dispatcher::commit` had to start verifying `$blob` references at op-commit, because on
  a presigning backend nothing else in the tree calls `commit_blob` at all — without it the
  rows stayed `reserved` and the collector took their bytes 24 hours later. A seam described
  in a sentence is not a seam that exists.
* The signing rules are testable with nothing running — `Capability` holds no pool and no
  runtime, so its four tests need no database.

### What this makes hard
* An app must choose a tier, and choosing wrongly is a `payload_too_large` failure rather
  than a slow success — the error names the step and suggests the next tier (F-BLB-11).
* Two round trips before a step result exists; a crash between them leaves a reservation
  only GC cleans up.

### What we accept
* Digest verification costs a whole-object read **on the filesystem backend**, which has no
  metadata to consult and so reaches `commit_blob`'s `stored() == None` arm and hashes the
  bytes. That is the real cost, and the reason the ceiling is 100 MiB. On S3 it does not
  apply: the presigned PUT binds `x-amz-checksum-sha256` and `content-length` into the
  signature, so the store rejects mismatched bytes itself and `stored` answers from a
  `HeadObject`. The server issues only `HeadObject` and `DeleteObject` against an object
  store; it never transfers an object.
* **One narrow shape of that regression escapes every test:** an `S3Backend::stored` that
  keeps the `HeadObject` and keeps erroring for an object the store reports no checksum for,
  and simply *adds* a `GetObject` beside them. It would pass everything —
  `a_committed_object_reports_its_digest_without_transferring_it` measures the answer and not
  the transfer, and says so in its own comment, and
  `no_object_bytes_reach_the_server_on_the_s3_path` counts bytes on the server's
  client-facing socket, which server-to-store traffic never crosses — while putting the
  payload back on the wire between the server and the store. The neighbouring shapes are
  caught. *Replacing* the `HeadObject` with a GET-and-hash fails
  `an_object_the_store_reports_no_checksum_for_is_an_error_not_a_fallback`, whose object
  carries no checksum and which asserts an error there rather than an answer. And `stored`
  returning `None`, which routes `commit_blob` into its read-and-hash arm, fails that same
  test, and on a real deployment fails the commit loudly regardless, because that arm calls
  `PostgresBlobStore::get_bytes` and `Server::build` gives the S3 store `relay: None`, which
  makes `get_bytes` an error rather than a download.
* **§8.3.2's verify-before-readable is enforced at one entry point, not everywhere.**
  `Dispatcher::commit` verifies the `$blob` references in an attempt envelope's ops and
  emitted events — which includes the `invoke` and `continue_as_new` run inputs. A `$blob`
  arriving in an ingested event or a `resolve-wait` payload reaches no `commit_blob` call, so
  on a presigning backend its row stays `reserved` and the collector takes the bytes after
  the reservation window. §8.3.4 — the *reference* obligation — is a different clause and is
  not affected: `runs_record_blob_refs` fires on `INSERT OR UPDATE OF input, output ON runs`,
  so those blobs do get their `blob_refs` rows.
* **`commit_blob` performs no namespace check**, and neither does `attach_read_urls` or
  `presign_read` — a blob is looked up by id alone. Op-commit is the first place an
  app-supplied id drives a `reserved → committed` transition, so this is the first place it
  matters. Content substitution stays impossible regardless: the digest is fixed at
  reservation and is what the commit checks.
* Reference counting is only as correct as the walk populating `blob_refs`: a blob referenced
  from somewhere `blob_ids` does not reach is collected while still referenced.
* `Capability` uses one key for every namespace, so rotating it invalidates every outstanding
  URL at once. Per-namespace keys would confine that, at the cost of a key schedule.
* The S3 backend depends on the object store enforcing a checksum it signed, and not every
  S3-compatible server does. `docs/blob-backends.md` records what MinIO
  `RELEASE.2025-09-07T16-13-09Z` and RustFS `v1.0.0-beta.12` were observed to do — both
  reject a mismatched body, but RustFS is a beta release and names the wrong header
  (`Content-Md5`) when it does.

## Alternatives considered

| Option | Why not |
|---|---|
| One tier: everything inline | Toasted values, journals exceeding the attempt body limit, and a database sized by customer payloads rather than workflow count. |
| Upload through the server | Control-plane throughput becomes a function of payload size. Kept only as `RelayBytes` (`put_bytes`/`get_bytes`), for stores that cannot presign, warned about at start-up and on every relayed upload (F-BLB-13). |
| Presign with the object store's own signer | Rejected as *the* mechanism: it ties the capability's shape to each backend's signer. Kept as an option inside a backend, which is what `stepd-blobs-s3` now is — and the concern this row raised, that a store-native signer would not bind the reserved size, turned out not to hold for SigV4, which binds `content-length` and `x-amz-checksum-sha256` when they are signed headers. |
| Sign only the blob id | A leaked write URL becomes a read URL for the same object, and can store more than was reserved. Each field in the MAC removes a specific attack. |
| Global content addressing | A cross-tenant existence oracle: reserve a digest, observe whether the upload was skipped. |
| Verify the digest on first read | The reference is committed into a step result by then, so the run has proceeded on a value the server never checked. |

## Verification

* `engine/rust/crates/stepd-store-postgres/src/blobs/mod.rs`: `a_write_capability_cannot_be_replayed_as_a_read`
  (a `write` signature fails verification as `read`) and
  `a_capability_is_bound_to_one_blob_and_one_size` (substituting the id or the declared size
  fails, "or a write URL uploads more than was reserved").
* Same file: `an_expired_capability_is_refused_even_with_a_valid_mac` and
  `a_minted_url_verifies_against_its_own_signature`. Also in that file, and not tests:
  `reserve` scopes the digest lookup by namespace and carries the comment on the probing
  attack; `commit_blob` checks size and digest before `state='committed'`, returning
  `blob_digest_mismatch`; `collect` deletes bytes before rows.
* `engine/rust/crates/stepd-store-postgres/src/blobs/filesystem.rs`, the backend behind
  `stepd dev`: `blob_paths_shard_and_stay_under_the_root` with `is_within`, against path
  traversal; `a_stored_object_reports_its_size_but_not_a_digest` (the `None` that sends
  `commit_blob` down the read-and-hash arm); `a_missing_object_is_absent_rather_than_an_error`;
  `the_filesystem_backend_admits_it_cannot_presign`.
* `engine/rust/crates/stepd-core/src/blobs.rs`, where the reference walk now lives so the engine and
  the store share one: `blob_references_are_found_wherever_they_are_nested` (a `$blob` inside
  an array inside an object is found — what the reference count depends on).
* `engine/rust/crates/stepd-core/tests/engine.rs`, for verify-at-op-commit rather than on first read:
  `a_blob_reference_that_fails_verification_fails_the_run_and_records_no_ops`,
  `a_run_with_no_blob_references_commits_the_same_with_or_without_a_blob_store`, and
  `an_already_committed_blob_reference_commits_normally`, whose store-side counterpart is
  `committing_an_already_committed_blob_does_not_look_at_the_object_again` in
  `engine/rust/crates/stepd-store-postgres/tests/live.rs`.
* Read URLs come from the backend, not from the relay capability:
  `read_urls_are_minted_by_the_backend_not_by_the_relay_capability`,
  `a_read_url_failure_on_one_reference_does_not_stop_the_walk` and
  `a_read_url_failure_clears_any_preexisting_url_rather_than_leaving_it` (the last because a
  stale `url` left in place would be trusted by a range read that verifies nothing), all in
  `engine/rust/crates/stepd-store-postgres/src/blobs/mod.rs`.
* The relay route exists only where it is needed:
  `a_presigning_backend_does_not_expose_the_relay_route` in
  `engine/rust/crates/stepd-server/tests/end_to_end.rs`, and the same 404 asserted against a real S3
  backend inside `no_object_bytes_reach_the_server_on_the_s3_path`.
* `engine/rust/crates/stepd-blobs-s3/src/lib.rs`, offline: `an_upload_target_binds_the_digest_and_the_length`,
  `the_declared_digest_is_sent_base64_not_hex`,
  `a_checksum_read_back_from_metadata_is_the_digest_that_was_declared`,
  `objects_are_keyed_by_blob_id_rather_than_by_digest` (keying by digest would make
  collection delete another namespace's bytes), `an_expired_ttl_is_refused_rather_than_signed`,
  `neither_the_config_nor_the_backend_prints_its_secret_key`,
  `the_s3_backend_presigns_and_says_which_backend_it_is`, and
  `a_bucket_check_against_nothing_listening_is_unreachable_not_a_panic`.
* `engine/rust/crates/stepd-server/src/lib.rs`, for configuration: `the_default_blob_backend_is_the_filesystem`,
  `selecting_s3_without_a_bucket_is_refused_rather_than_defaulted`,
  `selecting_s3_with_no_endpoint_is_refused_rather_than_silently_accepted`,
  `selecting_s3_with_no_credentials_is_also_refused`, `a_fully_configured_s3_backend_validates`,
  `blob_backend_is_read_case_and_whitespace_insensitively`, and
  `an_unrecognised_blob_backend_falls_back_to_the_filesystem_rather_than_hanging`.
* **Against a live object store, and only where one is configured.**
  `engine/rust/crates/stepd-blobs-s3/tests/live.rs` —
  `the_store_refuses_bytes_that_do_not_match_the_declared_digest`,
  `a_client_cannot_swap_the_checksum_for_one_matching_its_own_bytes`,
  `an_object_the_store_reports_no_checksum_for_is_an_error_not_a_fallback` (the one that
  fails if `stored` starts reporting `None` and routing commits into the read-and-hash arm),
  `a_committed_object_reports_its_digest_without_transferring_it` (which asserts the digest
  is right, *not* that no transfer happened — see "What we accept"),
  `a_presigned_read_serves_a_range`, `deleting_an_object_is_idempotent` — and
  `no_object_bytes_reach_the_server_on_the_s3_path` in
  `engine/rust/crates/stepd-server/tests/end_to_end.rs`, which drives a real run through a real SDK
  app and asserts that everything crossing the server's own socket stayed under 32 KiB while
  a 256 KiB payload reached the object store. All of these skip loudly without
  `STEPD_TEST_S3_*`, which no lane in `.github/workflows/ci.yml` sets. The end-to-end one
  needs `STEPD_TEST_DATABASE_URL` as well; CI does set that (`ci.yml:108`), so
  `tier 2 · integration` runs the test on pushes to `main` and on pull requests
  (`ci.yml:20-25`), where it skips for want of the S3 variables. Evidence that passes
  locally, then, and nothing automatic.
* `engine/rust/migrations/0001_initial.sql`: `blobs` carries `UNIQUE (ns, sha256)` commented "dedupe
  within a tenant, never across"; `blob_refs` is keyed `(blob_id, run_id, step_hash)`.
* `spec/PROTOCOL.md` §8 is the normative statement; `engine/rust/crates/stepd-cli/src/doctor.rs`
  reports reservations older than 24 h as bytes nothing references, and on an S3 backend adds
  a probe that the bucket answers with the configured credentials.
