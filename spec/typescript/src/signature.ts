import { hmac } from '@noble/hashes/hmac';
import { sha256 } from '@noble/hashes/sha256';
import { bytesToHex, utf8ToBytes } from '@noble/hashes/utils';

/** Default acceptance window either side of the signature timestamp (§9). */
export const DEFAULT_TOLERANCE_SECS = 300;

/** Why a signature was rejected. Replay is the caller's to detect; see {@link verify}. */
export type SignatureRejection = 'malformed' | 'expired' | 'mismatch';

export type VerifyResult =
  | { ok: true; nonce: string; timestamp: number }
  | { ok: false; reason: SignatureRejection };

function toBytes(key: Uint8Array | string): Uint8Array {
  return typeof key === 'string' ? utf8ToBytes(key) : key;
}

/**
 * Produce a `stepd-signature` header value (§9).
 *
 * `t=<unix>,n=<nonce>,v1=<hex>` where the MAC covers `"<t>.<nonce>.<body>"`.
 *
 * The nonce is not decoration. A timestamp window alone permits replay of a
 * captured body for the width of that window, so receivers must also keep a
 * seen-nonce cache — and including the nonce in the MAC is what stops an
 * attacker swapping in a fresh one.
 */
export function sign(
  key: Uint8Array | string,
  body: string,
  unixTs: number,
  nonce: string,
): string {
  const mac = hmac(sha256, toBytes(key), utf8ToBytes(`${unixTs}.${nonce}.${body}`));
  return `t=${unixTs},n=${nonce},v1=${bytesToHex(mac)}`;
}

/** Constant time for equal-length inputs; length alone is not a secret here. */
function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i]! ^ b[i]!;
  return diff === 0;
}

function hexToBytes(hex: string): Uint8Array | null {
  if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) return null;
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

/**
 * Verify a `stepd-signature` header against one or more accepted keys.
 *
 * Several keys are accepted so key rotation needs no flag day: during rotation
 * both the current and the previous key are live.
 *
 * **This does not detect replay.** It returns the nonce so the caller can check
 * it against a cache covering the tolerance window — and the caller must do that
 * check *after* this returns `ok`, never before. Inserting an unverified nonce
 * lets an attacker poison the cache with values they never had a valid signature
 * for, and lock out the legitimate sender.
 */
export function verify(
  keys: Array<Uint8Array | string>,
  header: string,
  body: string,
  nowUnix: number,
  toleranceSecs: number = DEFAULT_TOLERANCE_SECS,
): VerifyResult {
  let ts: number | undefined;
  let nonce: string | undefined;
  let macHex: string | undefined;

  for (const part of header.split(',')) {
    const at = part.indexOf('=');
    if (at < 0) continue;
    const k = part.slice(0, at);
    const v = part.slice(at + 1).trim();
    // Unknown components are ignored: §11 says additive fields must not break a
    // receiver, and a signature scheme is exactly where that matters.
    if (k === 't') {
      const n = Number(v);
      if (Number.isInteger(n)) ts = n;
    } else if (k === 'n') {
      nonce = v;
    } else if (k === 'v1') {
      macHex = v;
    }
  }

  if (ts === undefined || nonce === undefined || macHex === undefined) {
    return { ok: false, reason: 'malformed' };
  }
  // Symmetric: a timestamp far in the future is as suspicious as one far in the
  // past, and accepting it would widen the replay window arbitrarily.
  if (Math.abs(nowUnix - ts) > toleranceSecs) {
    return { ok: false, reason: 'expired' };
  }

  const provided = hexToBytes(macHex);
  if (provided === null) return { ok: false, reason: 'malformed' };

  const signed = utf8ToBytes(`${ts}.${nonce}.${body}`);
  for (const key of keys) {
    if (equalBytes(hmac(sha256, toBytes(key), signed), provided)) {
      return { ok: true, nonce, timestamp: ts };
    }
  }
  return { ok: false, reason: 'mismatch' };
}
