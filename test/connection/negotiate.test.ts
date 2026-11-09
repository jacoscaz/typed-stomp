import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  formatHeartBeat,
  negotiateHeartBeat,
  negotiateVersion,
  parseAcceptVersion,
  parseHeartBeat,
} from '../../src/connection/negotiate.ts';

describe('version negotiation', () => {
  it('parses accept-version headers', () => {
    assert.deepEqual(parseAcceptVersion(undefined), ['1.0']);
    assert.deepEqual(parseAcceptVersion('1.0,1.1, 1.2,2.0,1.1'), ['1.0', '1.1', '1.2']);
    assert.deepEqual(parseAcceptVersion('banana'), []);
  });

  it('picks the highest common version', () => {
    assert.equal(negotiateVersion('1.0,1.1,1.2'), '1.2');
    assert.equal(negotiateVersion('1.2,1.0'), '1.2');
    assert.equal(negotiateVersion(undefined), '1.0');
    assert.equal(negotiateVersion('1.0,1.1,1.2', ['1.0', '1.1']), '1.1');
    assert.equal(negotiateVersion('1.2', ['1.0', '1.1']), undefined);
    assert.equal(negotiateVersion('3.0'), undefined);
  });
});

describe('heart-beat negotiation', () => {
  it('parses and formats heart-beat headers', () => {
    assert.deepEqual(parseHeartBeat('100,200'), [100, 200]);
    assert.deepEqual(parseHeartBeat(undefined), [0, 0]);
    assert.deepEqual(parseHeartBeat('100, 200'), [0, 0]);
    assert.deepEqual(parseHeartBeat('99999999999999999999,1'), [0, 0]);
    assert.equal(formatHeartBeat([1000, 0]), '1000,0');
    assert.equal(formatHeartBeat([-5, 1.7]), '0,1');
  });

  it('computes intervals as in the specification', () => {
    // Client can send every 1000 ms and wants to receive every 2000 ms;
    // server can send every 500 ms and wants to receive every 3000 ms.
    assert.deepEqual(negotiateHeartBeat([1000, 2000], [500, 3000]), { outgoing: 3000, incoming: 2000 });
    // The server's view is the mirror image: what the client sends, the
    // server receives, and vice versa.
    assert.deepEqual(negotiateHeartBeat([500, 3000], [1000, 2000]), { outgoing: 2000, incoming: 3000 });
  });

  it('disables a direction when either side opts out', () => {
    assert.deepEqual(negotiateHeartBeat([0, 2000], [500, 3000]), { outgoing: 0, incoming: 2000 });
    assert.deepEqual(negotiateHeartBeat([1000, 0], [500, 3000]), { outgoing: 3000, incoming: 0 });
    assert.deepEqual(negotiateHeartBeat([1000, 2000], [0, 0]), { outgoing: 0, incoming: 0 });
  });
});
