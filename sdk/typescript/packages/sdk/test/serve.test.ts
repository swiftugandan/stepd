import { HEADER_NONCE, HEADER_SDK, HEADER_SIGNATURE, sign, verify } from '@stepd/protocol';
import { describe, expect, it } from 'vitest';
import { App, NonceCache, createHandler, fn } from '../src/index.js';

const KEY = 'shared-secret';
const NOW = 1_700_000_000;

function appWith(...functions: ReturnType<typeof fn>[]): App {
  const app = new App({ appId: 'test', url: 'http://app' }).signingKey(KEY);
  for (const f of functions) app.function(f);
  return app;
}

const attemptBody = (functionId = 'w', over: Record<string, unknown> = {}): string =>
  JSON.stringify({
    protocol: '1',
    attempt: 1,
    fence: 1,
    run: {
      id: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10',
      function_id: functionId,
      namespace: 'test',
      started_at: '2026-01-01T00:00:00Z',
      lineage_id: '01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10',
    },
    steps: {},
    ...over,
  });

function signedRequest(body: string, nonce = 'n1'): Request {
  return new Request('http://app/', {
    method: 'POST',
    body,
    headers: { [HEADER_SIGNATURE]: sign(KEY, body, NOW, nonce) },
  });
}

const handlerFor = (app: App, nonces = new NonceCache()) =>
  createHandler(app, { now: () => NOW, nonce: () => 'response-nonce', nonces });

describe('the attempt endpoint', () => {
  it('runs a handler and signs the response', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => 'finished'));
    const res = await handlerFor(app)(signedRequest(attemptBody()));

    expect(res.status).toBe(200);
    expect(res.headers.get(HEADER_SDK)).toMatch(/^typescript\//);

    const text = await res.text();
    expect(JSON.parse(text)).toMatchObject({
      protocol: '1',
      ops: [{ op: 'done', data: 'finished' }],
    });

    // Signing is required in both directions: an unverified response is an
    // unauthenticated instruction to mutate durable run state.
    const header = res.headers.get(HEADER_SIGNATURE);
    expect(header).not.toBeNull();
    expect(res.headers.get(HEADER_NONCE)).toBe('response-nonce');
    expect(verify([KEY], header!, text, NOW).ok).toBe(true);
  });

  it('serves the manifest for pull discovery', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(new Request('http://app/.well-known/stepd'));
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ protocol: '1', app_id: 'test' });
  });
});

describe('the status mapping, which tells the server what to do', () => {
  it('answers 401 to an unsigned request', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(
      new Request('http://app/', { method: 'POST', body: attemptBody() }),
    );
    expect(res.status).toBe(401);
    expect(res.headers.get('content-type')).toBe('application/problem+json');
    expect(await res.json()).toMatchObject({ code: 'unsigned', status: 401 });
  });

  it('answers 401 to a bad signature, not 400', async () => {
    // 401 makes the server alert and retry; 400 would fail the run outright,
    // which is the wrong answer to a key rotation gone wrong.
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const body = attemptBody();
    const res = await handlerFor(app)(
      new Request('http://app/', {
        method: 'POST',
        body,
        headers: { [HEADER_SIGNATURE]: sign('wrong-key', body, NOW, 'n1') },
      }),
    );
    expect(res.status).toBe(401);
    expect(await res.json()).toMatchObject({ code: 'bad_signature' });
  });

  it('answers 400 to a body it cannot parse', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(signedRequest('not json'));
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ code: 'malformed' });
  });

  it('answers 400 to the wrong protocol version', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(signedRequest(attemptBody('w', { protocol: '2' })));
    expect(res.status).toBe(400);
  });

  it('answers 404 to an unknown function, so a deploy in progress recovers', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(signedRequest(attemptBody('not-registered')));
    expect(res.status).toBe(404);
    expect(await res.json()).toMatchObject({ code: 'unknown_function' });
  });

  it('answers 400 to a truncated journal rather than replaying a partial one', async () => {
    // Replaying against a partial journal re-executes every step the app could
    // not see, the run still completes, and nothing errors.
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(signedRequest(attemptBody('w', { state_truncated: true })));
    expect(res.status).toBe(400);
    expect(((await res.json()) as { title: string }).title).toContain('§8.6');
  });

  it('answers 400 to a body that is not valid UTF-8', async () => {
    // An invalid byte must not become U+FFFD: that would change the body the
    // signature was computed over.
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const res = await handlerFor(app)(
      new Request('http://app/', { method: 'POST', body: new Uint8Array([0xff, 0xfe, 0x00]) }),
    );
    expect(res.status).toBe(400);
  });
});

describe('replay defence', () => {
  it('rejects a replayed nonce inside the window', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const handle = handlerFor(app);
    const body = attemptBody();
    const first = await handle(signedRequest(body, 'same-nonce'));
    expect(first.status).toBe(200);
    const second = await handle(signedRequest(body, 'same-nonce'));
    expect(second.status).toBe(401);
    expect(await second.json()).toMatchObject({ code: 'bad_signature' });
  });

  it('does not let a forged request poison the cache', async () => {
    // The ordering property. If an unverified nonce were inserted, an attacker
    // could burn the nonces a legitimate sender was about to use and lock them
    // out — a denial of service that needs no key at all.
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const nonces = new NonceCache();
    const handle = handlerFor(app, nonces);
    const body = attemptBody();

    const forged = await handle(
      new Request('http://app/', {
        method: 'POST',
        body,
        headers: { [HEADER_SIGNATURE]: sign('wrong-key', body, NOW, 'victim-nonce') },
      }),
    );
    expect(forged.status).toBe(401);
    expect(nonces.size).toBe(0);

    // The legitimate sender's turn: same nonce, real key. Must be accepted.
    const genuine = await handle(signedRequest(body, 'victim-nonce'));
    expect(genuine.status).toBe(200);
  });

  it('rejects a signature outside the tolerance window', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => null));
    const body = attemptBody();
    const res = await handlerFor(app)(
      new Request('http://app/', {
        method: 'POST',
        body,
        headers: { [HEADER_SIGNATURE]: sign(KEY, body, NOW - 3_600, 'old') },
      }),
    );
    expect(res.status).toBe(401);
  });
});

describe('dev mode', () => {
  it('accepts unsigned requests only when asked explicitly', async () => {
    // A missing key must fail closed, so this is a separate opt-in rather than
    // something that happens when no key is configured.
    const app = new App({ appId: 'test', url: 'http://app', devMode: true }).function(
      fn('w').onEvent('e').run(() => 'ok'),
    );
    const res = await createHandler(app, { now: () => NOW })(
      new Request('http://app/', { method: 'POST', body: attemptBody() }),
    );
    expect(res.status).toBe(200);
    // No key, so no signature — the server would reject this in production,
    // which is the point of it being loopback-only.
    expect(res.headers.get(HEADER_SIGNATURE)).toBeNull();
  });
});

describe('the envelope the SDK emits', () => {
  it('puts a non-retryable error last, after the work the pass recorded', async () => {
    const app = appWith(
      fn('w')
        .onEvent('e')
        .run(async (ctx) => {
          await ctx.step('a', () => 1).catch(() => undefined);
          const { fatal } = await import('@stepd/sdk-core');
          throw fatal('stop here');
        }),
    );
    const res = await handlerFor(app)(signedRequest(attemptBody()));
    const body = (await res.json()) as { ops: Array<{ op: string }> };
    expect(body.ops.map((o) => o.op)).toEqual(['step', 'error']);
  });

  it('reports orphaned steps rather than staying silent about a rename', async () => {
    const app = appWith(fn('w').onEvent('e').run(() => 'done'));
    const res = await handlerFor(app)(
      signedRequest(
        attemptBody('w', {
          steps: {
            deadbeefdeadbeef: { id: 'old-name', op: 'step', status: 'completed', data: 1 },
          },
        }),
      ),
    );
    expect(((await res.json()) as { orphaned_steps: number }).orphaned_steps).toBe(1);
  });
});
