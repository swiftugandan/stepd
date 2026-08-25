import { describe, expect, it } from 'vitest';
import {
  PROTOCOL_VERSION,
  describeEnvelopeError,
  validateEnvelope,
  type AttemptResponse,
  type Op,
} from '../src/index.js';

const step = (id: string, hash: string): Op => ({ op: 'step', id, hash });
const envelope = (ops: Op[], extra: Partial<AttemptResponse> = {}): AttemptResponse => ({
  protocol: PROTOCOL_VERSION,
  ops,
  ...extra,
});

describe('validateEnvelope', () => {
  it('accepts a single op', () => {
    expect(validateEnvelope(envelope([step('a', '1111111111111111')]))).toBeNull();
  });

  it('accepts a parallel batch of distinct steps', () => {
    expect(
      validateEnvelope(
        envelope([step('a', '1111111111111111'), step('b', '2222222222222222')]),
      ),
    ).toBeNull();
  });

  it('rejects an empty envelope', () => {
    expect(validateEnvelope(envelope([]))).toEqual({ kind: 'empty' });
  });

  it('rejects the wrong protocol version', () => {
    expect(validateEnvelope(envelope([step('a', '1111111111111111')], { protocol: '2' }))).toEqual(
      { kind: 'bad_version', protocol: '2' },
    );
  });

  it('rejects `done` batched with anything', () => {
    expect(validateEnvelope(envelope([step('a', '1111111111111111'), { op: 'done' }]))).toEqual({
      kind: 'not_alone',
      op: 'done',
    });
  });

  it('rejects `continue_as_new` batched with anything', () => {
    expect(
      validateEnvelope(
        envelope([
          step('a', '1111111111111111'),
          { op: 'continue_as_new', id: 'n', hash: '3333333333333333' },
        ]),
      ),
    ).toEqual({ kind: 'not_alone', op: 'continue_as_new' });
  });

  it('accepts `done` alone', () => {
    expect(validateEnvelope(envelope([{ op: 'done', data: null }]))).toBeNull();
  });

  it('rejects an error that is not last', () => {
    // Anywhere but the end would have the engine fail the run and then keep
    // recording steps into it.
    expect(
      validateEnvelope(
        envelope([
          { op: 'error', retryable: false, error: { message: 'x' } },
          step('a', '1111111111111111'),
        ]),
      ),
    ).toEqual({ kind: 'must_be_last', op: 'error' });
  });

  it('accepts a non-retryable error riding at the end of a batch', () => {
    // §5.2.2: record everything, then fail. Dropping the ops would lose executed
    // work; dropping the error would lose why the run stopped.
    expect(
      validateEnvelope(
        envelope([
          step('a', '1111111111111111'),
          { op: 'error', retryable: false, error: { message: 'declined' } },
        ]),
      ),
    ).toBeNull();
  });

  it('rejects a retryable error batched with recorded ops', () => {
    expect(
      validateEnvelope(
        envelope([
          step('a', '1111111111111111'),
          { op: 'error', retryable: true, error: { message: 'gateway down' } },
        ]),
      ),
    ).toEqual({ kind: 'retryable_error_batched' });
  });

  it('treats a missing `retryable` as true, as §5.1 says', () => {
    // The default is the dangerous direction, so it has to be tested rather than
    // assumed: an SDK omitting the field gets a retry, not a terminal failure.
    expect(
      validateEnvelope(
        envelope([step('a', '1111111111111111'), { op: 'error', error: { message: 'x' } }]),
      ),
    ).toEqual({ kind: 'retryable_error_batched' });
  });

  it('accepts a retryable error alone', () => {
    expect(
      validateEnvelope(envelope([{ op: 'error', retryable: true, error: { message: 'x' } }])),
    ).toBeNull();
  });

  it('rejects two ops claiming the same hash', () => {
    expect(
      validateEnvelope(envelope([step('a', '1111111111111111'), step('b', '1111111111111111')])),
    ).toEqual({ kind: 'duplicate_hash', hash: '1111111111111111' });
  });

  it('refuses the retired `join` field by name rather than ignoring it', () => {
    // §11 says ignore unknown fields; a *retired* one had a meaning, and being
    // ignored is the state it was removed for being in.
    expect(
      validateEnvelope(envelope([step('a', '1111111111111111')], { join: 'all' })),
    ).toEqual({ kind: 'retired_field', field: 'join' });
  });

  it('does not mistake an absent join for a present one', () => {
    expect(validateEnvelope(envelope([step('a', '1111111111111111')], { join: null }))).toBeNull();
  });

  it('describes every error kind it can produce', () => {
    // A message nobody generated is a message nobody has read. Each of these
    // ends up in an app's 400 body, which is all an SDK author gets to debug
    // with.
    const kinds: Parameters<typeof describeEnvelopeError>[0][] = [
      { kind: 'empty' },
      { kind: 'not_alone', op: 'done' },
      { kind: 'must_be_last', op: 'error' },
      { kind: 'duplicate_hash', hash: 'aaaa' },
      { kind: 'bad_version', protocol: '9' },
      { kind: 'retired_field', field: 'join' },
      { kind: 'retryable_error_batched' },
    ];
    for (const k of kinds) {
      expect(describeEnvelopeError(k).length).toBeGreaterThan(10);
    }
  });
});
