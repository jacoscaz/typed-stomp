import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Dispatcher, type DispatcherOptions } from '../../src/client/dispatcher.ts';
import { flush } from '../helpers.ts';

function dispatcher(options: Partial<DispatcherOptions<number>> = {}) {
  const handled: number[] = [];
  const errors: [unknown, number][] = [];
  const dropped: number[] = [];
  const overflowed: number[] = [];
  const d = new Dispatcher<number>({
    handler: (item) => {
      handled.push(item);
    },
    onError: (error, item) => errors.push([error, item]),
    onDrop: (item) => dropped.push(item),
    onOverflow: (item) => overflowed.push(item),
    ...options,
  });
  return { d, handled, errors, dropped, overflowed };
}

/** A handler whose calls stay pending until released. */
function deferredHandler() {
  const started: number[] = [];
  const pending = new Map<number, () => void>();
  let active = 0;
  let maxActive = 0;
  return {
    started,
    get maxActive() {
      return maxActive;
    },
    handler: (item: number) => {
      started.push(item);
      active += 1;
      maxActive = Math.max(maxActive, active);
      return new Promise<void>((resolve) =>
        pending.set(item, () => {
          active -= 1;
          resolve();
        }),
      );
    },
    release(item: number) {
      pending.get(item)?.();
      pending.delete(item);
    },
  };
}

describe('Dispatcher', () => {
  it('handles synchronous items immediately and in order', () => {
    const { d, handled } = dispatcher();
    d.push(1);
    d.push(2);
    assert.deepEqual(handled, [1, 2]);
    assert.equal(d.buffered, 0);
  });

  it('waits for asynchronous handlers with concurrency 1', async () => {
    const h = deferredHandler();
    const { d } = dispatcher({ handler: h.handler });
    d.push(1);
    d.push(2);
    d.push(3);
    assert.deepEqual(h.started, [1]);
    assert.equal(d.buffered, 2);
    assert.equal(d.inFlight, 1);
    h.release(1);
    await flush();
    assert.deepEqual(h.started, [1, 2]);
    h.release(2);
    await flush();
    h.release(3);
    await d.idle();
    assert.equal(h.maxActive, 1);
  });

  it('respects higher concurrency', async () => {
    const h = deferredHandler();
    const { d } = dispatcher({ handler: h.handler, concurrency: 2 });
    for (let i = 1; i <= 4; i += 1) {
      d.push(i);
    }
    assert.deepEqual(h.started, [1, 2]);
    h.release(2);
    await flush();
    assert.deepEqual(h.started, [1, 2, 3]);
    h.release(1);
    h.release(3);
    await flush();
    h.release(4);
    await d.idle();
    assert.equal(h.maxActive, 2);
  });

  it('pauses and resumes', () => {
    const { d, handled } = dispatcher();
    d.pause();
    d.push(1);
    d.push(2);
    assert.deepEqual(handled, []);
    assert.equal(d.buffered, 2);
    d.resume();
    assert.deepEqual(handled, [1, 2]);
  });

  it('reports handler errors and keeps going', async () => {
    const boom = new Error('boom');
    const { d, errors, handled } = dispatcher({
      handler: (item) => {
        handled.push(item);
        if (item === 1) {
          throw boom;
        }
        if (item === 2) {
          return Promise.reject(boom);
        }
        return undefined;
      },
    });
    d.push(1);
    d.push(2);
    d.push(3);
    await d.idle();
    assert.deepEqual(handled, [1, 2, 3]);
    assert.deepEqual(errors, [
      [boom, 1],
      [boom, 2],
    ]);
  });

  describe('overflow', () => {
    it('drops the newest item', () => {
      const { d, dropped } = dispatcher({ limit: 2, overflow: 'drop-newest' });
      d.pause();
      assert.equal(d.push(1), true);
      assert.equal(d.push(2), true);
      assert.equal(d.push(3), false);
      assert.deepEqual(dropped, [3]);
      assert.deepEqual(d.clear(), [1, 2]);
    });

    it('drops the oldest item', () => {
      const { d, dropped } = dispatcher({ limit: 2, overflow: 'drop-oldest' });
      d.pause();
      d.push(1);
      d.push(2);
      assert.equal(d.push(3), true);
      assert.deepEqual(dropped, [1]);
      assert.deepEqual(d.clear(), [2, 3]);
    });

    it('reports an overflow', () => {
      const { d, overflowed } = dispatcher({ limit: 1, overflow: 'error' });
      d.pause();
      d.push(1);
      assert.equal(d.push(2), false);
      assert.deepEqual(overflowed, [2]);
      assert.equal(d.buffered, 1);
    });

    it('does not count items that can start right away', () => {
      const { d, handled, overflowed } = dispatcher({ limit: 0 });
      d.push(1);
      assert.deepEqual(handled, [1]);
      assert.deepEqual(overflowed, []);
    });
  });

  it('stops accepting items once closed, and resolves idle', async () => {
    const h = deferredHandler();
    const { d } = dispatcher({ handler: h.handler });
    d.push(1);
    d.push(2);
    const idle = d.idle();
    assert.deepEqual(d.close(), [2]);
    assert.equal(d.push(3), false);
    h.release(1);
    await idle;
    assert.deepEqual(h.started, [1]);
  });

  it('handles many synchronous items without deep recursion', () => {
    const { d, handled } = dispatcher();
    d.pause();
    for (let i = 0; i < 100_000; i += 1) {
      d.push(i);
    }
    d.resume();
    assert.equal(handled.length, 100_000);
    assert.equal(handled[99_999], 99_999);
  });
});
