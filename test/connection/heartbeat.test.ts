import assert from 'node:assert/strict';
import { describe, it, type TestContext } from 'node:test';
import { HeartbeatMonitor } from '../../src/connection/heartbeat.ts';

function monitor(t: TestContext, options: { outgoing: number; incoming: number; tolerance?: number }) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const state = { lastSent: 0, lastReceived: 0, sent: [] as number[], timeouts: [] as number[] };
  const heartbeat = new HeartbeatMonitor({
    outgoing: options.outgoing,
    incoming: options.incoming,
    tolerance: options.tolerance ?? 1,
    lastSent: () => state.lastSent,
    lastReceived: () => state.lastReceived,
    send: () => {
      state.lastSent = Date.now();
      state.sent.push(Date.now());
    },
    timeout: (elapsed) => state.timeouts.push(elapsed),
  });
  return { heartbeat, state };
}

describe('HeartbeatMonitor', () => {
  it('sends at the outgoing interval while idle', (t) => {
    const { heartbeat, state } = monitor(t, { outgoing: 100, incoming: 0 });
    heartbeat.start();
    // Mock timers report the end-of-tick time to all timers fired within one
    // tick() call, so advance in small steps.
    for (let i = 0; i < 35; i += 1) {
      t.mock.timers.tick(10);
    }
    assert.deepEqual(state.sent, [100, 200, 300]);
    heartbeat.stop();
    t.mock.timers.tick(1000);
    assert.equal(state.sent.length, 3);
  });

  it('skips heart-beats while other data is being sent', (t) => {
    const { heartbeat, state } = monitor(t, { outgoing: 100, incoming: 0 });
    heartbeat.start();
    for (let now = 50; now <= 500; now += 50) {
      t.mock.timers.tick(50);
      state.lastSent = now;
    }
    assert.deepEqual(state.sent, []);
    heartbeat.stop();
  });

  it('times out once, after the incoming interval times the tolerance', (t) => {
    const { heartbeat, state } = monitor(t, { outgoing: 0, incoming: 100, tolerance: 2 });
    heartbeat.start();
    t.mock.timers.tick(150);
    state.lastReceived = 150;
    t.mock.timers.tick(199);
    assert.deepEqual(state.timeouts, []);
    t.mock.timers.tick(1);
    assert.deepEqual(state.timeouts, [200]);
    assert.equal(heartbeat.running, false);
    t.mock.timers.tick(1000);
    assert.deepEqual(state.timeouts, [200]);
  });
});
