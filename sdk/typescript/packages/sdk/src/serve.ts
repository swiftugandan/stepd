import {
  DEFAULT_TOLERANCE_SECS,
  HEADER_NONCE,
  HEADER_PROTOCOL,
  HEADER_SDK,
  HEADER_SIGNATURE,
  PROTOCOL_VERSION,
  decodeAttempt,
  describeEnvelopeError,
  sign,
  validateEnvelope,
  verify,
  type AttemptResponse,
  type Op,
} from '@stepd/protocol';
import { Ctx, runPass } from '@stepd/sdk-core';
import { App, SDK_VERSION } from './app.js';
import { NonceCache } from './nonce.js';

/** Why a request was refused, and what the server should do about it. */
export type ServeError =
  | { kind: 'unsigned' }
  | { kind: 'bad_signature'; why: string }
  | { kind: 'malformed'; why: string }
  | { kind: 'unknown_function'; id: string };

/**
 * The status mapping is load-bearing, not cosmetic (§2.2).
 *
 * Each class tells the server to do something different, and collapsing them
 * would turn a deploy in progress into a permanently failed run.
 */
function statusOf(e: ServeError): number {
  switch (e.kind) {
    // 401, not 400: an unsigned or badly-signed request is an authentication
    // failure, and the server alerts and retries rather than failing the run.
    case 'unsigned':
    case 'bad_signature':
      return 401;
    // 400 fails the run non-retryably: a body we cannot parse this attempt will
    // not parse on the next one either.
    case 'malformed':
      return 400;
    // 404 marks the function unhealthy and retries — the usual cause is a
    // deploy in progress, which fixes itself.
    case 'unknown_function':
      return 404;
  }
}

function titleOf(e: ServeError): string {
  switch (e.kind) {
    case 'unsigned':
      return 'request was not signed';
    case 'bad_signature':
      return `signature rejected: ${e.why}`;
    case 'malformed':
      return e.why;
    case 'unknown_function':
      return `unknown function '${e.id}'`;
  }
}

/** RFC 9457, the same shape the server produces, so a log does not say who wrote it. */
function problem(e: ServeError): Response {
  const status = statusOf(e);
  return new Response(
    JSON.stringify({ type: 'about:blank', title: titleOf(e), status, code: e.kind }),
    { status, headers: { 'content-type': 'application/problem+json' } },
  );
}

export interface ServeOptions {
  /** Injected so a test can drive the signature window; seconds since the epoch. */
  now?: () => number;
  /** Injected so a test can assert on the nonce a response carries. */
  nonce?: () => string;
  nonces?: NonceCache;
}

/**
 * Verify a request's signature and nonce.
 *
 * Exported so an alternative transport — a Lambda entry point, a queue consumer
 * — reuses the part that must not be got wrong instead of reimplementing it.
 */
export function verifyRequest(
  app: App,
  headers: Headers,
  body: string,
  nonces: NonceCache,
  now: number,
): ServeError | null {
  const header = headers.get(HEADER_SIGNATURE);
  if (header === null) {
    return app.devMode ? null : { kind: 'unsigned' };
  }

  const result = verify([...app.keys], header, body, now, DEFAULT_TOLERANCE_SECS);
  if (!result.ok) return { kind: 'bad_signature', why: result.reason };

  // Only now. An attacker must not be able to poison the cache with nonces they
  // never had a valid signature for, which would lock out the legitimate sender.
  if (!nonces.checkAndInsert(result.nonce, now)) {
    return { kind: 'bad_signature', why: 'replay' };
  }
  return null;
}

/**
 * The app's HTTP surface, as a web-standard handler.
 *
 * Two routes, both required by §3: the attempt endpoint at the root, and the
 * pull-discovery manifest. Built on `Request`/`Response` so the same function
 * runs under `node:http`, Bun, Deno, a Cloudflare Worker or a Next.js route.
 */
export function createHandler(
  app: App,
  options: ServeOptions = {},
): (request: Request) => Promise<Response> {
  const nonces = options.nonces ?? new NonceCache();
  const nowSecs = options.now ?? (() => Math.floor(Date.now() / 1000));
  const newNonce = options.nonce ?? (() => crypto.randomUUID());

  // Findings are warnings, not start-up failures: a function with no trigger is
  // a mistake, but refusing to boot over it would take down an app whose other
  // forty functions are fine.
  for (const finding of app.lint()) {
    console.warn(`[stepd::lint] ${finding}`);
  }

  return async function handle(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'GET' && url.pathname === '/.well-known/stepd') {
      return Response.json(app.manifest());
    }
    if (request.method !== 'POST' || url.pathname !== '/') {
      return problem({ kind: 'malformed', why: `no route for ${request.method} ${url.pathname}` });
    }

    // Decoded strictly: an invalid byte must not become U+FFFD and change the
    // body the signature was computed over.
    let body: string;
    try {
      body = new TextDecoder('utf-8', { fatal: true }).decode(await request.arrayBuffer());
    } catch {
      return problem({ kind: 'malformed', why: 'request body was not valid UTF-8' });
    }

    const now = nowSecs();
    const rejected = verifyRequest(app, request.headers, body, nonces, now);
    if (rejected !== null) return problem(rejected);

    let raw: unknown;
    try {
      raw = JSON.parse(body);
    } catch (e) {
      return problem({
        kind: 'malformed',
        why: `request body was not JSON: ${e instanceof Error ? e.message : String(e)}`,
      });
    }

    const decoded = decodeAttempt(raw);
    if (!decoded.ok) {
      return problem({
        kind: 'malformed',
        why: `attempt request could not be read: ${JSON.stringify(decoded.error)}`,
      });
    }
    const attempt = decoded.attempt;

    // §8.6. Replaying against a partial journal re-executes every step the app
    // could not see, the run still completes, and nothing errors — so an SDK
    // that cannot page must refuse rather than proceed on what it holds.
    if (attempt.state_truncated) {
      return problem({
        kind: 'malformed',
        why:
          'the attempt journal was truncated and this SDK build cannot yet page it; ' +
          'raise STEPD_ATTEMPT_STATE_LIMIT on the server or split the run with ' +
          'continue_as_new (protocol §8.6)',
      });
    }

    const fn = app.handlerFor(attempt.run.function_id);
    const handler = fn?.handler;
    if (handler === undefined) {
      return problem({ kind: 'unknown_function', id: attempt.run.function_id });
    }

    const ctx = new Ctx({
      functionId: attempt.run.function_id,
      run: attempt.run,
      memo: attempt.steps,
      attempt: attempt.attempt,
      pass: attempt.attempt,
    });
    const { outcome, emit, orphaned } = await runPass(ctx, handler);

    let ops: Op[];
    switch (outcome.kind) {
      case 'done':
        ops = [{ op: 'done', data: outcome.data }];
        break;
      case 'yield':
        ops = outcome.ops;
        break;
      case 'error':
        // The error goes last, after whatever the pass managed to record. A
        // group with two successful members and one that raised commits all
        // three outcomes and then fails the run; sending the error on its own
        // would fail a run having thrown away work it did (§5.2.2).
        ops = [...outcome.ops, { op: 'error', retryable: outcome.retryable, error: outcome.error }];
        break;
    }

    const response: AttemptResponse = {
      protocol: PROTOCOL_VERSION,
      ops,
      emit,
      orphaned_steps: orphaned.length,
    };

    if (orphaned.length > 0) {
      // Answers "why did my step re-run?" without the developer reconstructing
      // it from two deploys' worth of source.
      console.warn(
        `[stepd] run ${attempt.run.id}: ${orphaned.length} recorded step(s) were not ` +
          `encountered this pass — a step id was renamed or removed, and its side effect ` +
          `will re-execute under the new id`,
      );
    }

    // Validate our own envelope. A malformed batch rejected at the server fails
    // the run with a message about the server; failing here names the SDK, which
    // is where the bug is.
    const invalid = validateEnvelope(response);
    if (invalid !== null) {
      return problem({
        kind: 'malformed',
        why: `this SDK produced an invalid envelope: ${describeEnvelopeError(invalid)}`,
      });
    }

    const text = JSON.stringify(response);
    const headers = new Headers({
      'content-type': 'application/json',
      [HEADER_PROTOCOL]: PROTOCOL_VERSION,
      [HEADER_SDK]: SDK_VERSION,
    });

    // Signing is required in both directions (§9): an unverified response is an
    // unauthenticated instruction to mutate durable run state.
    const [key] = app.keys;
    if (key !== undefined) {
      const nonce = newNonce();
      headers.set(HEADER_SIGNATURE, sign(key, text, now, nonce));
      headers.set(HEADER_NONCE, nonce);
    }

    return new Response(text, { status: 200, headers });
  };
}
