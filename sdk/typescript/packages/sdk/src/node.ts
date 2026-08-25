import type { IncomingMessage, RequestListener, ServerResponse } from 'node:http';
import type { App } from './app.js';
import { createHandler, type ServeOptions } from './serve.js';

/**
 * The `node:http` adapter.
 *
 * The SDK's own surface is `(Request) => Promise<Response>`, so it runs unchanged
 * on Bun, Deno, a Cloudflare Worker or a Next.js route handler. This module is
 * the Node bridge, and it is a separate entry point so that importing the SDK
 * does not pull `node:http` into a bundle targeting a runtime that has no such
 * module.
 */

function toRequest(req: IncomingMessage, body: Buffer): Request {
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === 'string') headers.set(k, v);
    else if (Array.isArray(v)) for (const one of v) headers.append(k, one);
  }
  const method = req.method ?? 'GET';
  const host = req.headers.host ?? '127.0.0.1';
  return new Request(`http://${host}${req.url ?? '/'}`, {
    method,
    headers,
    ...(method === 'GET' || method === 'HEAD' ? {} : { body }),
  });
}

function readBody(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
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

/**
 * A request listener for `http.createServer`.
 *
 * ```ts
 * import { createServer } from 'node:http';
 * import { nodeListener } from '@stepd/sdk/node';
 *
 * createServer(nodeListener(app)).listen(3000);
 * ```
 *
 * **Do not set a request timeout on the server.** §7.1.1 forbids dropping a
 * running step to meet a deadline: the step must finish even though nobody is
 * listening for the answer any more, and the server treats no-response as
 * `unknown` and retries. Node's defaults are permissive enough, but a
 * `server.requestTimeout` set elsewhere would break the guarantee silently.
 */
export function nodeListener(app: App, options: ServeOptions = {}): RequestListener {
  const handle = createHandler(app, options);
  return (req, res) => {
    void (async () => {
      await send(res, await handle(toRequest(req, await readBody(req))));
    })().catch((e: unknown) => {
      // A fault here must not take the process down. The one attempt that is
      // *meant* to hang forever is a protocol requirement, not a bug, and a
      // crash on any other request would look like a protocol failure in
      // everything that ran afterwards.
      console.error('[stepd] request failed', e);
      if (!res.headersSent) res.writeHead(500);
      res.end();
    });
  };
}
