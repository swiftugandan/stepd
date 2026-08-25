import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { PROTOCOL_VERSION } from '@stepd/protocol';
import { SDK_VERSION, type App } from '@stepd/sdk';
import { nodeListener } from '@stepd/sdk/node';
import { SUITES, STATICALLY_PREVENTED } from './app.ts';
import type { AppState } from './state.ts';

/**
 * The app under test, plus the §12.1 probe routes in front of it.
 *
 * The probes are here and not in the SDK on purpose. They take no
 * authentication and expose execution detail, so an SDK that made them a default
 * route would ship a debug endpoint on every production app anyone ever wrote
 * with it. This is a separate binary for the same reason.
 *
 * Everything below them is `nodeListener` from `@stepd/sdk/node` — the shipped
 * adapter, not a copy of it. An adapter no application uses is an adapter nobody
 * has run.
 */

function json(res: ServerResponse, status: number, value: unknown): void {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(value));
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
  const attempts = nodeListener(app);

  const server = createServer((req, res) => {
    const url = new URL(req.url ?? '/', 'http://127.0.0.1');

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
      json(res, 200, { effects: state.effectsOf(url.searchParams.get('run') ?? '') });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/_conformance/reset') {
      state.reset();
      json(res, 200, { reset: true });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/_conformance/configure') {
      void readBody(req)
        .then((buf) => {
          const body = JSON.parse(buf.toString('utf8')) as { api_base?: string; token?: string };
          if (typeof body.api_base !== 'string' || typeof body.token !== 'string') {
            json(res, 400, { error: 'expected {api_base, token}' });
            return;
          }
          state.configure(body.api_base, body.token);
          // The SDK pages a truncated journal itself (§8.6), before the pass
          // runs, so the address has to reach the `App` and not only this app's
          // own state. Configuring one and not the other is how `truncation` and
          // `blobs` end up disagreeing about whether the app was set up.
          app.journalSource(body.api_base, body.token);
          json(res, 200, { configured: true });
        })
        .catch(() => json(res, 400, { error: 'unreadable body' }));
      return;
    }

    attempts(req, res);
  });

  // §7.1.1 at the socket level. Node's defaults would cut off the attempt the
  // abandonment case deliberately leaves hanging, and the rule is that the step
  // finishes even though nobody is listening for the answer.
  server.requestTimeout = 0;
  server.headersTimeout = 0;
  server.timeout = 0;

  server.listen(port, '127.0.0.1');
  return server;
}
