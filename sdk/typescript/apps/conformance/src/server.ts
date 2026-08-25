import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { PROTOCOL_VERSION } from '@stepd/protocol';
import { SDK_VERSION, createHandler, type App } from '@stepd/sdk';
import { SUITES, STATICALLY_PREVENTED } from './app.ts';
import type { AppState } from './state.ts';

/**
 * Bridge `node:http` to the web-standard handler the SDK exposes.
 *
 * The SDK's own surface is `(Request) => Promise<Response>` so it runs unchanged
 * on Bun, Deno, a Worker or a Next.js route. This adapter is the Node half, and
 * it is here rather than in the SDK only because the SDK package does not have a
 * framework adapter yet.
 */
function toRequest(req: IncomingMessage, body: Buffer): Request {
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === 'string') headers.set(k, v);
    else if (Array.isArray(v)) for (const one of v) headers.append(k, one);
  }
  const method = req.method ?? 'GET';
  return new Request(`http://127.0.0.1${req.url ?? '/'}`, {
    method,
    headers,
    ...(method === 'GET' || method === 'HEAD' ? {} : { body }),
  });
}

async function send(res: ServerResponse, response: Response): Promise<void> {
  const body = Buffer.from(await response.arrayBuffer());
  const headers: Record<string, string> = {};
  response.headers.forEach((v, k) => {
    headers[k] = v;
  });
  res.writeHead(response.status, headers);
  res.end(body);
}

function json(res: ServerResponse, status: number, value: unknown): void {
  const body = JSON.stringify(value);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(body);
}

function readBody(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

export function serve(app: App, state: AppState, port: number): ReturnType<typeof createServer> {
  const handle = createHandler(app);

  const server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url ?? '/', 'http://127.0.0.1');

      // ------------------------------------------------ §12.1 probe routes
      //
      // Here rather than inside the SDK, and this app is a separate binary. They
      // take no authentication and expose execution detail; an SDK that made
      // them a default route would ship a debug endpoint on every production app
      // anyone ever wrote with it.
      if (req.method === 'GET' && url.pathname === '/.well-known/stepd-conformance') {
        json(res, 200, {
          protocol: PROTOCOL_VERSION,
          sdk: SDK_VERSION,
          suites: SUITES,
          statically_prevented: STATICALLY_PREVENTED,
        });
        return;
      }

      if (req.method === 'GET' && url.pathname === '/_conformance/effects') {
        const run = url.searchParams.get('run') ?? '';
        json(res, 200, { effects: state.effectsOf(run) });
        return;
      }

      if (req.method === 'POST' && url.pathname === '/_conformance/reset') {
        state.reset();
        json(res, 200, { reset: true });
        return;
      }

      if (req.method === 'POST' && url.pathname === '/_conformance/configure') {
        const body = JSON.parse((await readBody(req)).toString('utf8')) as {
          api_base?: string;
          token?: string;
        };
        if (typeof body.api_base !== 'string' || typeof body.token !== 'string') {
          json(res, 400, { error: 'expected {api_base, token}' });
          return;
        }
        state.configure(body.api_base, body.token);
        // The SDK pages a truncated journal itself (§8.6), before the pass runs,
        // so the address has to reach the `App` and not only this app's own
        // state. Configuring one and not the other is how `truncation` and
        // `blobs` end up disagreeing about whether the app was set up.
        app.journalSource(body.api_base, body.token);
        json(res, 200, { configured: true });
        return;
      }

      // ------------------------------------------------------- the app itself
      const body = await readBody(req);
      await send(res, await handle(toRequest(req, body)));
    })().catch((e: unknown) => {
      // Never let a handler fault take the process down: the abandonment case
      // deliberately leaves a request hanging forever, and a crash there would
      // look like a protocol failure in every later suite.
      console.error('[conformance] request failed', e);
      if (!res.headersSent) res.writeHead(500);
      res.end();
    });
  });

  // §7.1.1 again, at the socket level. Node's default is no timeout, which is
  // what the abandonment case needs: the step must finish even though nobody is
  // listening for the answer.
  server.requestTimeout = 0;
  server.headersTimeout = 0;
  server.timeout = 0;

  server.listen(port, '127.0.0.1');
  return server;
}
