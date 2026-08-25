import { sha256Bytes } from '@stepd/protocol';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Blob, BlobError, Blobs } from '../src/index.js';

const RUN = '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10';
const PAYLOAD = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
const DIGEST = sha256Bytes(PAYLOAD);

interface Call {
  url: string;
  method: string;
  headers: Array<[string, string]>;
  body?: unknown;
}

function stubFetch(responder: (call: Call) => Response): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal('fetch', async (input: string | URL | Request, init: RequestInit = {}) => {
    const headers: Array<[string, string]> = [];
    new Headers(init.headers).forEach((v, k) => headers.push([k, v]));
    const call: Call = {
      url: String(input),
      method: init.method ?? 'GET',
      headers,
      body: init.body,
    };
    calls.push(call);
    return responder(call);
  });
  return calls;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('two-phase upload', () => {
  it('reserves, uploads, and returns a reference with no url', async () => {
    const calls = stubFetch((call) => {
      if (call.url.endsWith('/v1/blobs:reserve')) {
        return Response.json({
          blob_id: 'blob-1',
          deduplicated: false,
          upload_url: 'https://store.example/put?sig=abc',
          method: 'PUT',
          headers: { 'content-type': 'image/png', 'x-amz-checksum-sha256': 'AAA=' },
        });
      }
      return new Response('', { status: 200 });
    });

    const blob = await new Blobs('http://stepd', 'tok').put(RUN, PAYLOAD, {
      contentType: 'image/png',
    });

    expect(calls[0]!.url).toBe('http://stepd/v1/blobs:reserve');
    expect(calls[0]!.headers).toContainEqual(['authorization', 'Bearer tok']);
    expect(JSON.parse(calls[0]!.body as string)).toMatchObject({
      run_id: RUN,
      size: 8,
      sha256: DIGEST,
      content_type: 'image/png',
    });

    expect(calls[1]!.url).toBe('https://store.example/put?sig=abc');
    expect(calls[1]!.method).toBe('PUT');

    // §8.3.1: the app never supplies a url, and must never persist one. A
    // five-minute URL in the journal fails hours later in a way that reads like
    // the blob having disappeared.
    expect(blob.toJSON().$blob.url).toBeUndefined();
    expect(blob.id).toBe('blob-1');
    expect(blob.sha256).toBe(DIGEST);
  });

  it('replays every reservation header exactly once', async () => {
    // A presigning backend signs these headers. Dropping one makes the upload
    // fail; sending one twice breaks SigV4 just as thoroughly, and a duplicated
    // content-length is the way that actually happens.
    const calls = stubFetch((call) =>
      call.url.endsWith('/v1/blobs:reserve')
        ? Response.json({
            blob_id: 'b',
            deduplicated: false,
            upload_url: 'https://store/put',
            method: 'PUT',
            headers: {
              'content-type': 'application/octet-stream',
              'content-length': '8',
              'x-amz-checksum-sha256': 'AAA=',
            },
          })
        : new Response('', { status: 200 }),
    );

    await new Blobs('http://stepd', 'tok').put(RUN, PAYLOAD);

    const names = calls[1]!.headers.map(([k]) => k);
    expect(new Set(names).size).toBe(names.length);
    expect(names).toContain('x-amz-checksum-sha256');
  });

  it('skips the upload entirely when the server deduplicates', async () => {
    // Content addressing is what makes a retry cheap rather than merely correct.
    const calls = stubFetch(() => Response.json({ blob_id: 'existing', deduplicated: true }));
    const blob = await new Blobs('http://stepd', 'tok').put(RUN, PAYLOAD);
    expect(calls).toHaveLength(1);
    expect(blob.id).toBe('existing');
  });

  it('refuses a reservation that neither deduplicates nor supplies a url', async () => {
    stubFetch(() => Response.json({ blob_id: 'b', deduplicated: false }));
    await expect(new Blobs('http://stepd', 'tok').put(RUN, PAYLOAD)).rejects.toThrow(BlobError);
  });

  it('surfaces a failed upload rather than returning a reference to nothing', async () => {
    stubFetch((call) =>
      call.url.endsWith('/v1/blobs:reserve')
        ? Response.json({ blob_id: 'b', deduplicated: false, upload_url: 'https://store/put' })
        : new Response('denied', { status: 403 }),
    );
    await expect(new Blobs('http://stepd', 'tok').put(RUN, PAYLOAD)).rejects.toThrow(/403/);
  });
});

describe('reading', () => {
  const ref = (url?: string): Blob =>
    new Blob({
      $blob: { id: 'b', size: PAYLOAD.length, sha256: DIGEST, ...(url === undefined ? {} : { url }) },
    });

  it('verifies the digest on a full read', async () => {
    // The one corruption the server cannot see: it never held these bytes, so a
    // truncated transfer is only detectable here.
    stubFetch(() => new Response(PAYLOAD.slice(0, 4)));
    await expect(new Blobs('http://stepd', 'tok').read(ref('https://store/get'))).rejects.toThrow(
      /did not match its digest/,
    );
  });

  it('accepts a full read that matches', async () => {
    stubFetch(() => new Response(PAYLOAD));
    const out = await new Blobs('http://stepd', 'tok').read(ref('https://store/get'));
    expect([...out]).toEqual([...PAYLOAD]);
  });

  it('sends an inclusive Range and does not check the digest', async () => {
    // A range does not hash to the object's digest. Checking it would fail every
    // range read; pretending to check would be worse.
    const calls = stubFetch(() => new Response(PAYLOAD.slice(0, 4)));
    const out = await new Blobs('http://stepd', 'tok').readRange(ref('https://store/get'), 0, 3);
    expect(calls[0]!.headers).toContainEqual(['range', 'bytes=0-3']);
    expect(out).toHaveLength(4);
  });

  it('names §8.3.1 when the reference carries no url', async () => {
    // The likeliest cause is holding a Blob across attempts, and the message has
    // to say so — otherwise it reads as the blob having disappeared.
    stubFetch(() => new Response(''));
    await expect(new Blobs('http://stepd', 'tok').read(ref())).rejects.toThrow(/§8.3.1/);
  });

  it('can be told not to verify', async () => {
    stubFetch(() => new Response(PAYLOAD.slice(0, 4)));
    const out = await new Blobs('http://stepd', 'tok', { verifyReads: false }).read(
      ref('https://store/get'),
    );
    expect(out).toHaveLength(4);
  });
});

describe('Blob.from', () => {
  it('reads a reference out of a step result', () => {
    const value = { $blob: { id: 'b', size: 1, sha256: DIGEST } };
    expect(Blob.from(value).id).toBe('b');
  });

  it('refuses anything else', () => {
    for (const bad of [null, 42, {}, { $ref: 's3://x' }]) {
      expect(() => Blob.from(bad)).toThrow(TypeError);
    }
  });
});
