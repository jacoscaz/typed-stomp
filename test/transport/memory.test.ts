import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { TransportClosedError } from '../../src/transport/errors.ts';
import { MemoryTransport } from '../../src/transport/memory.ts';
import type { TransportCloseInfo, TransportData } from '../../src/transport/types.ts';
import { flush } from '../helpers.ts';

function attach(transport: MemoryTransport) {
  const messages: TransportData[] = [];
  const closes: TransportCloseInfo[] = [];
  transport.attach({ onMessage: (data) => messages.push(data), onClose: (info) => closes.push(info) });
  return { messages, closes };
}

describe('MemoryTransport', () => {
  it('delivers messages asynchronously and in order', async () => {
    const [a, b] = MemoryTransport.pair();
    const received = attach(b);
    a.send('one');
    a.send(Uint8Array.of(2));
    assert.deepEqual(received.messages, []);
    await flush();
    assert.deepEqual(received.messages, ['one', Uint8Array.of(2)]);
  });

  it('copies binary data', async () => {
    const [a, b] = MemoryTransport.pair();
    const received = attach(b);
    const data = Uint8Array.of(1, 2, 3);
    a.send(data);
    data[0] = 9;
    await flush();
    assert.deepEqual(received.messages, [Uint8Array.of(1, 2, 3)]);
  });

  it('buffers events until handlers are attached', async () => {
    const [a, b] = MemoryTransport.pair();
    a.send('early');
    a.close();
    await flush();
    const received = attach(b);
    assert.deepEqual(received.messages, ['early']);
    assert.equal(received.closes.length, 1);
  });

  it('refuses a second attach', () => {
    const [a] = MemoryTransport.pair();
    attach(a);
    assert.throws(() => attach(a), /already attached/);
  });

  it('closes both ends exactly once, after pending messages', async () => {
    const [a, b] = MemoryTransport.pair();
    const sideA = attach(a);
    const sideB = attach(b);
    a.send('last');
    a.close(4001, 'bye');
    a.close();
    assert.equal(a.closed, true);
    assert.throws(() => a.send('x'), TransportClosedError);
    await flush();
    assert.deepEqual(sideB.messages, ['last']);
    assert.deepEqual(sideA.closes, [{ code: 4001, reason: 'bye', clean: true }]);
    assert.deepEqual(sideB.closes, [{ code: 4001, reason: 'bye', clean: true }]);
    assert.equal(b.closed, true);
  });

  it('simulates abrupt connection loss', async () => {
    const [a, b] = MemoryTransport.pair();
    const sideA = attach(a);
    const sideB = attach(b);
    const error = new Error('cable cut');
    a.terminate(error);
    await flush();
    assert.deepEqual(sideA.closes, [{ code: 1006, reason: '', clean: false, error }]);
    assert.deepEqual(sideB.closes, [{ code: 1006, reason: '', clean: false, error }]);
  });
});
