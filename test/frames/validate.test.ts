import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import type { StompVersion } from '../../src/codec/frame.ts';
import {
  StompValidationError,
  assertValid,
  validateClientFrame,
  validateServerFrame,
  type ValidationResult,
} from '../../src/frames/validate.ts';
import type { Frame } from '../../src/codec/frame.ts';
import { frame } from '../helpers.ts';

const codes = (result: ValidationResult<Frame>, severity: 'error' | 'warning' = 'error') =>
  result.issues.filter((issue) => issue.severity === severity).map((issue) => issue.code);

const client = (f: Frame, version: StompVersion = '1.2', mode: 'strict' | 'lenient' = 'strict') =>
  validateClientFrame(f, { version, mode });

const server = (f: Frame, version: StompVersion = '1.2', mode: 'strict' | 'lenient' = 'strict') =>
  validateServerFrame(f, { version, mode });

describe('frame validation', () => {
  it('accepts a valid SEND frame', () => {
    const result = client(frame('SEND', { destination: '/a' }, 'hi'));
    assert.equal(result.valid, true);
    assert.deepEqual(result.issues, []);
  });

  it('rejects unknown commands and commands sent in the wrong direction', () => {
    assert.deepEqual(codes(client(frame('FOO'))), ['unknown-command']);
    assert.deepEqual(codes(client(frame('MESSAGE'))), ['unexpected-command']);
    assert.deepEqual(codes(server(frame('SEND', { destination: '/a' }))), ['unexpected-command']);
  });

  it('requires headers', () => {
    const result = client(frame('SEND'));
    assert.equal(result.valid, false);
    assert.deepEqual(result.issues, [
      { severity: 'error', code: 'missing-header', message: 'SEND frames require a "destination" header', header: 'destination' },
    ]);
  });

  it('treats empty required headers as soft violations', () => {
    assert.deepEqual(codes(client(frame('SEND', { destination: '' }))), ['empty-header']);
    const lenient = client(frame('SEND', { destination: '' }), '1.2', 'lenient');
    assert.equal(lenient.valid, true);
    assert.deepEqual(codes(lenient, 'warning'), ['empty-header']);
  });

  it('requires a subscription id from 1.1 onwards', () => {
    assert.equal(client(frame('SUBSCRIBE', { destination: '/a' }), '1.0').valid, true);
    assert.deepEqual(codes(client(frame('SUBSCRIBE', { destination: '/a' }), '1.1')), ['missing-header']);
    assert.equal(client(frame('SUBSCRIBE', { destination: '/a', id: '0' }), '1.2').valid, true);
  });

  it('validates ack modes', () => {
    assert.deepEqual(codes(client(frame('SUBSCRIBE', { destination: '/a', id: '0', ack: 'nope' }))), ['invalid-header']);
    assert.deepEqual(
      codes(client(frame('SUBSCRIBE', { destination: '/a', ack: 'client-individual' }), '1.0')),
      ['invalid-header'],
    );
    assert.equal(client(frame('SUBSCRIBE', { destination: '/a', id: '0', ack: 'client-individual' }), '1.1').valid, true);
  });

  it('requires an id or destination to UNSUBSCRIBE in 1.0, and an id later', () => {
    assert.deepEqual(codes(client(frame('UNSUBSCRIBE'), '1.0')), ['missing-header']);
    assert.equal(client(frame('UNSUBSCRIBE', { destination: '/a' }), '1.0').valid, true);
    assert.deepEqual(codes(client(frame('UNSUBSCRIBE', { destination: '/a' }), '1.2')), ['missing-header']);
  });

  it('applies per-version ACK and NACK headers', () => {
    assert.equal(client(frame('ACK', { id: '1' }), '1.2').valid, true);
    assert.deepEqual(codes(client(frame('ACK', { 'message-id': '1', subscription: '0' }), '1.2')), ['missing-header']);
    assert.equal(client(frame('ACK', { 'message-id': '1', subscription: '0' }), '1.1').valid, true);
    assert.deepEqual(codes(client(frame('ACK', { 'message-id': '1' }), '1.1')), ['missing-header']);
    assert.equal(client(frame('ACK', { 'message-id': '1' }), '1.0').valid, true);
    assert.equal(client(frame('NACK', { id: '1' }), '1.2').valid, true);
    assert.deepEqual(codes(client(frame('NACK', { 'message-id': '1' }), '1.0')), ['unsupported-command']);
  });

  it('requires transaction ids', () => {
    for (const command of ['BEGIN', 'COMMIT', 'ABORT']) {
      assert.deepEqual(codes(client(frame(command))), ['missing-header']);
      assert.equal(client(frame(command, { transaction: 'tx' })).valid, true);
    }
  });

  it('only allows bodies on SEND, MESSAGE and ERROR', () => {
    assert.deepEqual(codes(client(frame('SUBSCRIBE', { destination: '/a', id: '0' }, 'x'))), ['unexpected-body']);
    const lenient = client(frame('SUBSCRIBE', { destination: '/a', id: '0' }, 'x'), '1.2', 'lenient');
    assert.equal(lenient.valid, true);
    assert.deepEqual(codes(lenient, 'warning'), ['unexpected-body']);
    assert.equal(server(frame('ERROR', { message: 'oops' }, 'details')).valid, true);
  });

  it('checks content-length against the body', () => {
    assert.deepEqual(codes(client(frame('SEND', { destination: '/a', 'content-length': '3' }, 'hi'))), [
      'content-length-mismatch',
    ]);
    assert.deepEqual(codes(client(frame('SEND', { destination: '/a', 'content-length': 'x' }, 'hi'))), [
      'invalid-header',
    ]);
    assert.equal(client(frame('SEND', { destination: '/a', 'content-length': '2' }, 'hi')).valid, true);
  });

  describe('negotiation frames', () => {
    it('accepts a 1.0 CONNECT without headers', () => {
      assert.equal(client(frame('CONNECT'), '1.0').valid, true);
    });

    it('ignores the negotiated version for negotiation frames', () => {
      assert.equal(client(frame('STOMP', { 'accept-version': '1.2', host: 'h' }), '1.0').valid, true);
    });

    it('requires host for 1.1+ CONNECT and STOMP frames in strict mode', () => {
      assert.deepEqual(codes(client(frame('CONNECT', { 'accept-version': '1.1,1.2' }))), ['missing-header']);
      assert.equal(client(frame('CONNECT', { 'accept-version': '1.0' })).valid, true);
      const lenient = client(frame('CONNECT', { 'accept-version': '1.2' }), '1.2', 'lenient');
      assert.equal(lenient.valid, true);
      assert.deepEqual(codes(lenient, 'warning'), ['missing-header']);
    });

    it('requires accept-version on STOMP frames', () => {
      assert.deepEqual(codes(client(frame('STOMP', { host: 'h' }))), ['missing-header']);
    });

    it('validates accept-version and heart-beat formats', () => {
      assert.deepEqual(codes(client(frame('CONNECT', { 'accept-version': '1.2,banana', host: 'h' }))), [
        'invalid-header',
      ]);
      assert.deepEqual(codes(client(frame('CONNECT', { 'heart-beat': '10, 10' }))), ['invalid-header']);
      assert.equal(client(frame('CONNECT', { 'heart-beat': '0,1000' })).valid, true);
    });

    it('validates the CONNECTED version', () => {
      assert.equal(server(frame('CONNECTED', { version: '1.2', 'heart-beat': '0,0' })).valid, true);
      assert.deepEqual(codes(server(frame('CONNECTED', { version: '2.0' }), '1.2', 'lenient')), ['invalid-header']);
    });
  });

  it('requires subscription on MESSAGE frames from 1.1 onwards', () => {
    const headers = { destination: '/a', 'message-id': '1' };
    assert.equal(server(frame('MESSAGE', headers), '1.0').valid, true);
    assert.deepEqual(codes(server(frame('MESSAGE', headers), '1.1')), ['missing-header']);
    assert.equal(server(frame('MESSAGE', { ...headers, subscription: '0' }), '1.2').valid, true);
  });

  it('narrows typed frames per version', () => {
    const result = validateClientFrame(frame('ACK', { id: '7' }), { version: '1.2' });
    const acked = assertValid(result);
    if (acked.command === 'ACK') {
      // With version 1.2, the `id` header is typed as a required string.
      const id: string = acked.headers.id;
      assert.equal(id, '7');
    } else {
      assert.fail('expected an ACK frame');
    }
  });

  it('assertValid throws a StompValidationError with all issues', () => {
    assert.throws(
      () => assertValid(client(frame('SUBSCRIBE', {}, 'x'))),
      (err: unknown) =>
        err instanceof StompValidationError &&
        err.issues.length === 3 &&
        err.message.includes('"destination"') &&
        err.message.includes('"id"'),
    );
  });
});
