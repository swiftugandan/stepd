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

**Bulk data never traverses the stepd server.** Managed blobs use a two-phase direct upload
(§8.3.2): the app reserves (`BlobStore::reserve`, declaring size, SHA-256 and content type),
PUTs the bytes straight to the store, then returns `{"$blob": {...}}` in the step result.
The server sees the digest, never the bytes.

**The upload and read URLs are signed capabilities, and the MAC covers the whole
capability.** `Capability::sign` in `rust/crates/stepd-store-postgres/src/blobs.rs` HMACs
`"{id}.{dir}.{size}.{expires}"`. Every field is load-bearing:

* `id` — a leaked URL cannot be pointed at another blob.
* `dir` — a write capability cannot be replayed as a read. This is the difference between a
  leaked upload URL being a nuisance and being an exfiltration primitive.
* `size` — the transfer endpoint enforces the declared size, so a write URL cannot store
  more than was reserved.
* `expires` — checked before the MAC comparison, which is itself constant-time
  (`subtle::ConstantTimeEq`), because a byte-by-byte compare leaks the correct prefix.

A session cookie expresses none of that, and the app holding an upload URL may hold no
console token at all.

**Content is verified before the blob becomes readable.** `commit_blob` re-reads the stored
bytes, compares length and SHA-256 against what was declared, and only then sets
`state='committed'`. Without it a compromised upload URL used in time could substitute
different content for a reference already committed into a step result, and every later read
would return the substituted bytes with nothing indicating a change.

**Content addressing is per namespace, never global.** `UNIQUE (ns, sha256)` on `blobs`
(`rust/migrations/0001_initial.sql`); `reserve` looks up `WHERE ns=$1 AND sha256=$2`. A
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
* Control-plane throughput is independent of payload size: a tenant moving 100 GiB of video
  moves none of it through stepd.
* Replay stays cheap — a run with forty blob-bearing steps re-downloads nothing on attempt
  forty-one, because SDKs dereference lazily (§8.3.3).
* Swapping the backend is a `BlobStore` implementation: `PostgresBlobStore` indexes in
  Postgres and writes bytes to a filesystem root, which is what makes `stepd dev` work with
  no cloud account, and S3 is a drop-in.
* The signing rules are testable with nothing running — `Capability` holds no pool and no
  runtime, so its four tests need no database.

### What this makes hard
* An app must choose a tier, and choosing wrongly is a `payload_too_large` failure rather
  than a slow success — the error names the step and suggests the next tier (F-BLB-11).
* Two round trips before a step result exists; a crash between them leaves a reservation
  only GC cleans up.

### What we accept
* **The blob HTTP surface is not yet wired.** `rust/crates/stepd-server/src/api.rs` exposes
  neither `POST /v1/blobs:reserve` nor `PUT /v1/blobs/{id}/content`. The store, the
  capability minter, the schema and the `doctor` check all exist; the endpoints that would
  let an app use them do not. The decision is settled, the wiring is outstanding.
* Digest verification reads the whole object back on commit — a local read on the filesystem
  backend, a full download on S3 unless the store's own checksum headers are used. A real
  cost, and the reason the ceiling is 100 MiB.
* Reference counting is only as correct as the walk populating `blob_refs`: a blob referenced
  from somewhere `blob_ids` does not reach is collected while still referenced.
* `Capability` uses one key for every namespace, so rotating it invalidates every outstanding
  URL at once. Per-namespace keys would confine that, at the cost of a key schedule.

## Alternatives considered

| Option | Why not |
|---|---|
| One tier: everything inline | Toasted values, journals exceeding the attempt body limit, and a database sized by customer payloads rather than workflow count. |
| Upload through the server | Control-plane throughput becomes a function of payload size. Kept only as `relay_put`, for stores that cannot presign, with a warning metric (F-BLB-13). |
| Presign with the object store's own signer | Ties the capability's shape to each backend's signer, and none binds the *reserved size*. A store-native signer stays available inside a `BlobStore` implementation. |
| Sign only the blob id | A leaked write URL becomes a read URL for the same object, and can store more than was reserved. Each field in the MAC removes a specific attack. |
| Global content addressing | A cross-tenant existence oracle: reserve a digest, observe whether the upload was skipped. |
| Verify the digest on first read | The reference is committed into a step result by then, so the run has proceeded on a value the server never checked. |

## Verification

* `rust/crates/stepd-store-postgres/src/blobs.rs`: `a_write_capability_cannot_be_replayed_as_a_read`
  (a `write` signature fails verification as `read`) and
  `a_capability_is_bound_to_one_blob_and_one_size` (substituting the id or the declared size
  fails, "or a write URL uploads more than was reserved").
* Same file: `an_expired_capability_is_refused_even_with_a_valid_mac`,
  `a_minted_url_verifies_against_its_own_signature`,
  `blob_references_are_found_wherever_they_are_nested` (a `$blob` inside an array inside an
  object is found — what the reference count depends on), and
  `blob_paths_shard_and_stay_under_the_root` with `is_within`, against path traversal.
* Same module: `reserve` scopes the digest lookup by namespace and carries the comment on the
  probing attack; `commit_blob` checks size and digest before `state='committed'`, returning
  `blob_digest_mismatch`; `collect` deletes bytes before rows.
* `rust/migrations/0001_initial.sql`: `blobs` carries `UNIQUE (ns, sha256)` commented "dedupe
  within a tenant, never across"; `blob_refs` is keyed `(blob_id, run_id, step_hash)`.
* `spec/PROTOCOL.md` §8 is the normative statement; `rust/crates/stepd-cli/src/doctor.rs`
  reports reservations older than 24 h as bytes nothing references.
