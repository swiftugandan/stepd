import { sha256Bytes, type BlobRef } from '@stepd/protocol';

/**
 * A managed blob (§8.3).
 *
 * A **value**, not a handle. Constructing or replaying one costs nothing, and
 * bytes move only when something dereferences it — which is what keeps replay
 * cheap: a run with forty blob-bearing steps replays on attempt forty-one by
 * decoding forty references and downloading nothing.
 */
export class Blob {
  constructor(readonly ref: BlobRef) {}

  get id(): string {
    return this.ref.$blob.id;
  }
  get size(): number {
    return this.ref.$blob.size;
  }
  get sha256(): string {
    return this.ref.$blob.sha256;
  }
  get contentType(): string | undefined {
    return this.ref.$blob.content_type;
  }

  /** The reference as it goes into a step result. */
  toJSON(): BlobRef {
    return this.ref;
  }

  /** Read a `$blob` back out of a step result. */
  static from(value: unknown): Blob {
    if (
      typeof value === 'object' &&
      value !== null &&
      '$blob' in value &&
      typeof (value as BlobRef).$blob?.id === 'string'
    ) {
      return new Blob(value as BlobRef);
    }
    throw new TypeError('not a $blob reference');
  }
}

export class BlobError extends Error {
  override readonly name = 'BlobError';
}

interface Reservation {
  blob_id: string;
  deduplicated: boolean;
  upload_url?: string;
  method?: 'PUT' | 'POST';
  headers?: Record<string, string>;
  expires_at?: string;
  relay?: boolean;
}

export interface PutOptions {
  contentType?: string;
  filename?: string;
  stepId?: string;
}

/**
 * The two-phase upload client (§8.3.2).
 *
 * The only place an app calls stepd rather than answering it. Bytes never go to
 * the stepd server: it hands out a URL addressing the store, and the app uploads
 * there directly.
 *
 * Upload **inside a step**. That is what records the reference in the journal,
 * and the journal reference is what keeps the bytes alive — an upload outside a
 * step works and re-uploads on every attempt, but nothing holds a reference to
 * it and the collector will eventually take it.
 */
export class Blobs {
  readonly #base: string;
  readonly #token: string;
  readonly #verifyReads: boolean;

  constructor(baseUrl: string, token: string, options: { verifyReads?: boolean } = {}) {
    this.#base = baseUrl.replace(/\/+$/, '');
    this.#token = token;
    this.#verifyReads = options.verifyReads ?? true;
  }

  /** Reserve, upload, and return the reference to put in a step result. */
  async put(runId: string, bytes: Uint8Array, options: PutOptions = {}): Promise<Blob> {
    const digest = sha256Bytes(bytes);

    const reserved = await this.#reserve({
      run_id: runId,
      size: bytes.byteLength,
      sha256: digest,
      ...(options.stepId === undefined ? {} : { step_id: options.stepId }),
      ...(options.contentType === undefined ? {} : { content_type: options.contentType }),
      ...(options.filename === undefined ? {} : { filename: options.filename }),
    });

    // Content addressing: the server already holds these bytes, so §8.3.2 says
    // skip the transfer. This is what makes a retry cheap rather than merely
    // correct.
    if (!reserved.deduplicated) {
      if (reserved.upload_url === undefined) {
        throw new BlobError(
          'the server did not deduplicate and did not supply an upload URL; ' +
            'one of the two is required (§8.3.2)',
        );
      }
      await this.#upload(reserved, bytes);
    }

    // No `url`. §8.3.1: a read URL is minted by the server per attempt and MUST
    // NOT be persisted by an SDK — journalling a five-minute URL makes it fail
    // hours later in a way that reads like the blob having disappeared.
    return new Blob({
      $blob: {
        id: reserved.blob_id,
        size: bytes.byteLength,
        sha256: digest,
        ...(options.contentType === undefined ? {} : { content_type: options.contentType }),
        ...(options.filename === undefined ? {} : { filename: options.filename }),
      },
    });
  }

  /** Read the whole object, verifying the digest unless told not to. */
  async read(blob: Blob): Promise<Uint8Array> {
    const bytes = await this.#fetchBytes(blob, undefined);
    if (this.#verifyReads) {
      const got = sha256Bytes(bytes);
      if (got !== blob.sha256) {
        // The one corruption the server cannot see: it never held these bytes,
        // so a truncated transfer is only detectable here.
        throw new BlobError(
          `blob ${blob.id} did not match its digest: expected ${blob.sha256}, got ${got}`,
        );
      }
    }
    return bytes;
  }

  /**
   * Read a byte range. Both ends inclusive, as HTTP means them.
   *
   * The digest is **not** checked: a range does not hash to the object's digest,
   * and pretending otherwise would either fail every range read or quietly do
   * nothing.
   */
  async readRange(blob: Blob, from: number, to: number): Promise<Uint8Array> {
    return await this.#fetchBytes(blob, `bytes=${from}-${to}`);
  }

  async #reserve(body: Record<string, unknown>): Promise<Reservation> {
    const url = `${this.#base}/v1/blobs:reserve`;
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${this.#token}`,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new BlobError(`reserve failed at ${url}: ${res.status} ${await res.text()}`);
    }
    return (await res.json()) as Reservation;
  }

  async #upload(reserved: Reservation, bytes: Uint8Array): Promise<void> {
    // Every reservation header, replayed verbatim and exactly once. A presigning
    // backend signs these, so dropping one makes the upload fail — and sending
    // one twice breaks SigV4 just as thoroughly. A `Headers` object is used
    // rather than an array of pairs precisely because `set` cannot duplicate.
    const headers = new Headers();
    for (const [k, v] of Object.entries(reserved.headers ?? {})) headers.set(k, v);

    const res = await fetch(reserved.upload_url!, {
      method: reserved.method ?? 'PUT',
      headers,
      body: bytes,
    });
    if (!res.ok) {
      throw new BlobError(
        `upload failed at ${reserved.upload_url}: ${res.status} ${await res.text()}`,
      );
    }
  }

  async #fetchBytes(blob: Blob, range: string | undefined): Promise<Uint8Array> {
    const url = blob.ref.$blob.url;
    if (url === undefined) {
      throw new BlobError(
        `blob ${blob.id} carries no read URL. The server mints one per attempt and an SDK ` +
          `must not persist it (§8.3.1) — the usual cause is holding a Blob across attempts ` +
          `instead of reading the reference out of this attempt's step results.`,
      );
    }
    const res = await fetch(url, range === undefined ? {} : { headers: { range } });
    if (!res.ok) {
      throw new BlobError(`read failed at ${url}: ${res.status}`);
    }
    return new Uint8Array(await res.arrayBuffer());
  }
}
