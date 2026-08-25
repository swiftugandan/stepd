/**
 * Registration, blob-reserve, problem and conformance documents.
 *
 * Attempt/op/event live in `types.ts`. These are the other messages the
 * published schemas describe — the same documents `stepd-proto::manifest`
 * binds in Rust.
 */

import type { Json } from './types.js';

/** A feature an app may declare on its manifest. */
export type Capability =
  | 'parallel'
  | 'blobs'
  | 'signal'
  | 'invoke'
  | 'batch'
  | 'cancel'
  | 'streaming';

/** What an app registers with the server (protocol §3). */
export interface AppManifest {
  protocol: string;
  app_id: string;
  url: string;
  sdk?: string;
  checksum?: string;
  env?: string;
  capabilities?: Capability[];
  functions: FunctionConfig[];
}

/** How a run of this function starts. */
export type Trigger =
  | { type: 'event'; event: string; expr?: string }
  | {
      type: 'cron';
      cron: string;
      tz?: string;
      catchup?: CatchUp;
      catchup_limit?: number;
      misfire_window?: string;
      singleton?: boolean;
      run_key?: string;
    }
  | { type: 'invoke' };

/** Misfire policy for a cron trigger. */
export type CatchUp = 'one' | 'skip' | 'all';

/** A function definition (protocol §3). */
export interface FunctionConfig {
  id: string;
  version?: string;
  name?: string;
  description?: string;
  triggers: Trigger[];
  key_expr?: string;
  idempotency_expr?: string;
  priority_expr?: string;
  concurrency?: ConcurrencyLimit[];
  rate_limit?: RateLimit;
  debounce?: Debounce;
  batch?: Batch;
  retries?: Retries;
  timeouts?: Timeouts;
  cancel_on?: CancelOn[];
  on_failure?: string;
  singleton?: boolean;
  input_schema?: Json;
  output_schema?: Json;
  inbox?: Inbox;
  limits?: Limits;
  on_cancel?: boolean;
}

export interface ConcurrencyLimit {
  limit: number;
  key_expr?: string;
  scope?: 'function' | 'namespace';
}

export interface RateLimit {
  limit: number;
  period: string;
  key_expr?: string;
  burst?: number;
}

export interface Debounce {
  period: string;
  key_expr?: string;
  max_delay?: string;
}

export interface Batch {
  max_size: number;
  timeout?: string;
  key_expr?: string;
}

export interface Retries {
  max_attempts?: number;
  backoff?: 'exponential' | 'linear' | 'constant';
  initial?: string;
  max?: string;
  jitter?: boolean;
}

export interface Timeouts {
  attempt?: string;
  run?: string;
  start?: string;
}

export interface CancelOn {
  event: string;
  expr?: string;
  timeout?: string;
}

export interface Inbox {
  max_entries?: number;
  on_overflow?: 'drop_oldest' | 'fail_run';
}

export interface Limits {
  invoke_depth?: number;
  invoke_fanout?: number;
  chain_length?: number;
  max_steps?: number;
}

/** `POST /v1/blobs:reserve` request. */
export interface BlobReserveRequest {
  run_id: string;
  step_id?: string;
  size: number;
  sha256: string;
  content_type?: string;
  filename?: string;
}

/** `POST /v1/blobs:reserve` response. */
export interface BlobReserveResponse {
  blob_id: string;
  deduplicated: boolean;
  upload_url?: string;
  method?: 'PUT' | 'POST';
  headers?: Record<string, string>;
  expires_at?: string;
  relay?: boolean;
}

/** RFC 9457 Problem Details, as stepd puts it on the wire. */
export interface ProblemBody {
  type?: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
  code?: string;
  run_id?: string;
  op?: string;
}

/** Suites an app may declare at `/.well-known/stepd-conformance`. */
export type ConformanceSuite =
  | 'memoization'
  | 'loops'
  | 'determinism'
  | 'parallel'
  | 'sleep'
  | 'wait'
  | 'early_signal'
  | 'invoke'
  | 'cascade'
  | 'continue_as_new'
  | 'errors'
  | 'cancel'
  | 'abandonment'
  | 'blobs'
  | 'refs'
  | 'fencing'
  | 'signature'
  | 'truncation'
  | 'cron';

/** A hazard an implementation makes unrepresentable. */
export type StaticHazard = 'offpath_claim';

/** `GET /.well-known/stepd-conformance`. */
export interface ConformanceManifest {
  protocol: string;
  sdk?: string;
  suites: ConformanceSuite[];
  statically_prevented?: StaticHazard[];
}
