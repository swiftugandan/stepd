import { sha256Hex, type Json } from '@stepd/protocol';
import type { Function } from './function.js';

/** This SDK's identifier, sent as `stepd-sdk` and in the manifest (§3). */
export const SDK_VERSION = 'typescript/0.1.0';

export interface AppOptions {
  appId: string;
  /** The endpoint stepd will call. Registered in the manifest. */
  url: string;
  /**
   * Accept unsigned requests. Loopback development only.
   *
   * A separate, explicit call rather than something that happens when no key is
   * configured: a missing key must fail closed (§9).
   */
  devMode?: boolean;
}

/** A set of functions served at one URL. */
export class App {
  readonly appId: string;
  readonly url: string;
  readonly devMode: boolean;
  readonly #keys: Array<Uint8Array | string> = [];
  readonly #functions = new Map<string, Function>();

  constructor(options: AppOptions) {
    this.appId = options.appId;
    this.url = options.url;
    this.devMode = options.devMode ?? false;
  }

  /** Add a signing key. Call twice during rotation: both verify, the first signs. */
  signingKey(key: Uint8Array | string): this {
    this.#keys.push(key);
    return this;
  }

  function(f: Function): this {
    this.#functions.set(f.id, f);
    return this;
  }

  get keys(): ReadonlyArray<Uint8Array | string> {
    return this.#keys;
  }

  handlerFor(id: string): Function | undefined {
    return this.#functions.get(id);
  }

  /** The `AppManifest` this app registers with (§3). */
  manifest(): Json {
    const fns = [...this.#functions.values()]
      .map((f) => f.config())
      // Sorted so the checksum is a function of content, not of insertion order
      // — otherwise every restart looks like a config change and the server
      // re-registers for nothing.
      .sort((a, b) =>
        String((a as Record<string, Json>).id).localeCompare(String((b as Record<string, Json>).id)),
      );

    return {
      protocol: '1',
      app_id: this.appId,
      url: this.url,
      sdk: SDK_VERSION,
      checksum: `sha256:${sha256Hex(JSON.stringify(fns))}`,
      functions: fns,
    };
  }

  lint(): string[] {
    const out = [...this.#functions.values()].flatMap((f) => f.lint());
    if (this.#keys.length === 0 && !this.devMode) {
      out.push(
        `app '${this.appId}' has no signing key and is not in dev mode; every attempt will be ` +
          `rejected as unsigned (protocol §9)`,
      );
    }
    return out;
  }
}
