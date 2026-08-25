import { buildApp } from './app.ts';
import { serve } from './server.ts';
import { AppState } from './state.ts';

const port = Number(process.env.PORT ?? 9944);
const signingKey = process.env.STEPD_SIGNING_KEY ?? 'stepd-conformance';
const url = `http://127.0.0.1:${port}`;

// Per instance, never module-level. See `AppState`.
const state = new AppState();
serve(buildApp(state, url, signingKey), state, port);

console.log(`conformance app listening on ${url}`);
