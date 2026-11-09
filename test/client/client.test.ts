import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { backoffDelay } from '../../src/client/backoff.ts';
import { Client, type ClientEvents, type ClientOptions } from '../../src/client/client.ts';
import {
  StompBufferOverflowError,
  StompNotConnectedError,
  StompServerError,
} from '../../src/client/errors.ts';
import type { StompMessage } from '../../src/client/message.ts';
import { StompConnectionClosedError } from '../../src/connection/errors.ts';
import { flush } from '../helpers.ts';
import { FakeServer, delay, until, type FakeServerOptions } from './fake-server.ts';

const fastReconnect = { initialDelay: 5, maxDelay: 20, jitter: 0 } as const;

function setup(serverOptions: FakeServerOptions = {}, clientOptions: Partial<ClientOptions> = {}) {
  const server = new FakeServer(serverOptions);
  const client = new Client({
    transport: () => server.transport(),
    heartbeat: { outgoing: 0, incoming: 0 },
    reconnect: fastReconnect,
    ...clientOptions,
  });
  const events: { [K in keyof ClientEvents]: ClientEvents[K][] } = {
    connect: [],
    disconnect: [],
    reconnecting: [],
    error: [],
    warning: [],
    close: [],
  };
  for (const type of Object.keys(events) as (keyof ClientEvents)[]) {
    client.on(type, (event) => (events[type] as unknown[]).push(event));
  }
  return { server, client, events };
}

describe('Client', () => {
  it('requires exactly one of url and transport', () => {
    assert.throws(() => new Client({}), TypeError);
    assert.throws(() => new Client({ url: 'ws://x', transport: () => Promise.reject(new Error()) }), TypeError);
  });

  it('connects and emits connect', async () => {
    const { server, client, events } = setup({}, { connectHeaders: { login: 'guest' } });
    assert.equal(client.state, 'idle');
    await client.connect();
    assert.equal(client.state, 'connected');
    assert.equal(client.connected, true);
    assert.deepEqual(events.connect, [{ version: '1.2', server: 'fake/1.0', session: 'session-0' }]);
    assert.equal(server.last.frames[0]?.headers['login'], 'guest');
    await client.close();
  });

  it('calls a connectHeaders function before every attempt', async () => {
    let calls = 0;
    const { server, client } = setup({}, { connectHeaders: async () => ({ passcode: `token-${calls++}` }) });
    await client.connect();
    server.last.kill();
    await until(() => client.connected && server.connections.length === 2);
    assert.deepEqual(
      server.connections.map((connection) => connection.frames[0]?.headers['passcode']),
      ['token-0', 'token-1'],
    );
    await client.close();
  });

  describe('publishing', () => {
    it('publishes while connected', async () => {
      const { server, client } = setup();
      await client.connect();
      await client.publish('/queue/a', 'hello', { receipt: true, contentType: 'text/plain' });
      const send = await server.last.waitFor('SEND');
      assert.equal(send.headers['destination'], '/queue/a');
      assert.equal(send.headers['content-type'], 'text/plain');
      await client.close();
    });

    it('rejects while offline by default', async () => {
      const { client } = setup();
      await assert.rejects(client.publish('/queue/a', 'x'), StompNotConnectedError);
    });

    it('queues while offline when configured to', async () => {
      const { server, client } = setup({}, { offline: 'queue', offlineQueueLimit: 2 });
      const first = client.publish('/queue/a', '1');
      const second = client.publish('/queue/a', '2', { receipt: true });
      await assert.rejects(client.publish('/queue/a', '3'), /queue is full/);
      await client.connect();
      await Promise.all([first, second]);
      const sends = server.last.frames.filter((frame) => frame.command === 'SEND');
      assert.deepEqual(
        sends.map((frame) => new TextDecoder().decode(frame.body)),
        ['1', '2'],
      );
      await client.close();
    });

    it('rejects queued publications on close', async () => {
      const { client } = setup({}, { offline: 'queue' });
      const pending = client.publish('/queue/a', '1');
      await client.close();
      await assert.rejects(pending, StompConnectionClosedError);
      await assert.rejects(client.publish('/queue/a', '2'), StompConnectionClosedError);
    });
  });

  describe('subscriptions', () => {
    it('subscribes on connect and delivers messages to the handler', async () => {
      const { server, client } = setup();
      const received: unknown[] = [];
      const sub = client.subscribe('/topic/a', (message) => {
        received.push(message.json());
      });
      assert.equal(sub.established, false);
      await client.connect();
      await sub.ready;
      assert.equal(sub.established, true);
      server.last.message(sub.id, '{"n":1}');
      server.last.message(sub.id, '{"n":2}');
      await until(() => received.length === 2);
      assert.deepEqual(received, [{ n: 1 }, { n: 2 }]);
      assert.deepEqual(client.subscriptions, [sub]);
      await client.close();
    });

    it('subscribes immediately when already connected', async () => {
      const { server, client } = setup();
      await client.connect();
      const sub = client.subscribe('/topic/a', () => {}, { id: 'mine', receipt: true, headers: { selector: 'x' } });
      await sub.ready;
      const subscribe = await server.last.waitFor('SUBSCRIBE');
      assert.equal(subscribe.headers['id'], 'mine');
      assert.equal(subscribe.headers['selector'], 'x');
      assert.throws(() => client.subscribe('/topic/b', () => {}, { id: 'mine' }), /already in use/);
      await client.close();
    });

    it('unsubscribes', async () => {
      const { server, client } = setup();
      await client.connect();
      const received: string[] = [];
      const sub = client.subscribe('/topic/a', (message) => {
        received.push(message.text());
      });
      await sub.ready;
      await sub.unsubscribe();
      assert.equal(sub.active, false);
      assert.deepEqual(client.subscriptions, []);
      const unsubscribe = await server.last.waitFor('UNSUBSCRIBE');
      assert.equal(unsubscribe.headers['id'], sub.id);
      server.last.message(sub.id, 'late');
      await flush();
      assert.deepEqual(received, []);
      await client.close();
    });

    it('supports await using', async () => {
      const { server, client } = setup();
      await client.connect();
      {
        await using sub = client.subscribe('/topic/a', () => {});
        await sub.ready;
      }
      await server.last.waitFor('UNSUBSCRIBE');
      await client.close();
    });

    it('pauses and resumes delivery', async () => {
      const { server, client } = setup();
      await client.connect();
      const received: string[] = [];
      const sub = client.subscribe('/topic/a', (message) => {
        received.push(message.text());
      });
      await sub.ready;
      sub.pause();
      server.last.message(sub.id, '1');
      server.last.message(sub.id, '2');
      await until(() => sub.buffered === 2);
      assert.deepEqual(received, []);
      sub.resume();
      assert.deepEqual(received, ['1', '2']);
      await client.close();
    });

    it('processes asynchronous handlers one at a time, in order', async () => {
      const { server, client } = setup();
      await client.connect();
      const log: string[] = [];
      const sub = client.subscribe('/topic/a', async (message) => {
        log.push(`start ${message.text()}`);
        await delay(5);
        log.push(`end ${message.text()}`);
      });
      await sub.ready;
      server.last.message(sub.id, '1');
      server.last.message(sub.id, '2');
      await until(() => sub.buffered + sub.inFlight === 2);
      await sub.idle();
      assert.deepEqual(log, ['start 1', 'end 1', 'start 2', 'end 2']);
      await client.close();
    });

    it('reports handler errors through onError or the error event', async () => {
      const { server, client, events } = setup();
      await client.connect();
      const boom = new Error('boom');
      const viaOption: unknown[] = [];
      const a = client.subscribe(
        '/topic/a',
        () => {
          throw boom;
        },
        { onError: (error) => viaOption.push(error) },
      );
      const b = client.subscribe('/topic/b', async () => {
        throw boom;
      });
      await Promise.all([a.ready, b.ready]);
      server.last.message(a.id, 'x');
      server.last.message(b.id, 'y');
      await until(() => viaOption.length === 1 && events.error.length === 1);
      assert.equal(viaOption[0], boom);
      assert.equal(events.error[0]?.error, boom);
      assert.equal(events.error[0]?.subscription, b);
      assert.equal(events.error[0]?.message?.text(), 'y');
      assert.equal(a.active, true);
      await client.close();
    });

    it('ends the subscription when the buffer overflows', async () => {
      const { server, client, events } = setup();
      await client.connect();
      const sub = client.subscribe('/topic/a', () => {}, { buffer: { limit: 1 } });
      await sub.ready;
      sub.pause();
      server.last.message(sub.id, '1');
      server.last.message(sub.id, '2');
      await until(() => events.error.length === 1);
      assert.ok(events.error[0]?.error instanceof StompBufferOverflowError);
      assert.equal(sub.active, false);
      await server.last.waitFor('UNSUBSCRIBE');
      await client.close();
    });

    it('acknowledges messages', async () => {
      const { server, client } = setup();
      await client.connect();
      const sub = client.subscribe('/queue/a', (message) => message.ack(), { ack: 'client-individual' });
      await sub.ready;
      const messageId = server.last.message(sub.id, 'x');
      const ack = await server.last.waitFor('ACK');
      assert.equal(ack.headers['id'], `ack-${messageId}`);
      await client.close();
    });
  });

  describe('reconnection', () => {
    it('reconnects after connection loss and re-subscribes', async () => {
      const { server, client, events } = setup();
      const received: string[] = [];
      const sub = client.subscribe('/topic/a', (message) => {
        received.push(message.text());
      });
      await client.connect();
      await server.last.waitFor('SUBSCRIBE');
      server.last.kill();
      await until(() => server.connections.length === 2 && client.connected);
      const resubscribe = await server.last.waitFor('SUBSCRIBE');
      assert.equal(resubscribe.headers['id'], sub.id);
      server.last.message(sub.id, 'after reconnect');
      await until(() => received.length === 1);

      assert.equal(events.connect.length, 2);
      assert.equal(events.disconnect.length, 1);
      assert.equal(events.disconnect[0]?.willReconnect, true);
      assert.ok(events.disconnect[0]?.error instanceof StompConnectionClosedError);
      assert.deepEqual(
        events.reconnecting.map(({ attempt, delay: ms }) => [attempt, ms]),
        [[1, 5]],
      );
      await client.close();
    });

    it('treats ERROR frames as disconnections', async () => {
      const { server, client, events } = setup();
      await client.connect();
      server.last.error('Oops');
      await until(() => events.disconnect.length === 1);
      assert.ok(events.disconnect[0]?.error instanceof StompServerError);
      await until(() => client.connected);
      await client.close();
    });

    it('keeps retrying the first connection until it succeeds', async () => {
      const { server, client, events } = setup({
        rejectConnect: (_frame, index) => (index < 2 ? 'Not yet' : undefined),
      });
      await client.connect();
      assert.equal(server.connections.length, 3);
      assert.equal(events.reconnecting.length, 2);
      assert.deepEqual(
        events.reconnecting.map((event) => event.delay),
        [5, 10],
      );
      assert.equal(events.disconnect.length, 0);
      await client.close();
    });

    it('gives up after maxAttempts', async () => {
      const { client, events } = setup(
        { rejectConnect: () => 'Never' },
        { reconnect: { ...fastReconnect, maxAttempts: 2 } },
      );
      await assert.rejects(client.connect(), StompServerError);
      assert.equal(client.state, 'closed');
      assert.equal(events.reconnecting.length, 2);
      assert.equal(events.close.length, 1);
      assert.ok(events.close[0]?.error instanceof StompServerError);
    });

    it('asks shouldReconnect', async () => {
      const decisions: number[] = [];
      const { client } = setup(
        { rejectConnect: () => 'Bad credentials' },
        {
          reconnect: {
            ...fastReconnect,
            shouldReconnect: (error, attempt) => {
              decisions.push(attempt);
              return !(error instanceof StompServerError && error.message === 'Bad credentials');
            },
          },
        },
      );
      await assert.rejects(client.connect(), /Bad credentials/);
      assert.deepEqual(decisions, [1]);
    });

    it('does not reconnect when disabled', async () => {
      const { server, client, events } = setup({}, { reconnect: false });
      await client.connect();
      server.last.kill();
      await until(() => client.state === 'closed');
      assert.deepEqual(
        events.disconnect.map((event) => event.willReconnect),
        [false],
      );
      assert.equal(events.close.length, 1);
    });

    it('discards buffered client-ack messages when the session ends', async () => {
      const { server, client } = setup();
      await client.connect();
      const sub = client.subscribe('/queue/a', () => {}, { ack: 'client' });
      await sub.ready;
      sub.pause();
      server.last.message(sub.id, '1');
      await until(() => sub.buffered === 1);
      server.last.kill();
      await until(() => !client.connected);
      assert.equal(sub.buffered, 0);
      await client.close();
    });

    it('rejects acknowledgements of messages from a previous session', async () => {
      const { server, client } = setup();
      await client.connect();
      const messages: StompMessage[] = [];
      const sub = client.subscribe('/queue/a', (message) => {
        messages.push(message);
      }, { ack: 'client-individual' });
      await sub.ready;
      server.last.message(sub.id, '1');
      await until(() => messages.length === 1);
      server.last.kill();
      await until(() => server.connections.length === 2 && client.connected);
      await assert.rejects(messages[0]!.ack(), StompNotConnectedError);
      await client.close();
    });
  });

  describe('transactions', () => {
    it('commits when the function succeeds', async () => {
      const { server, client } = setup();
      await client.connect();
      const result = await client.transaction(async (tx) => {
        await tx.send('/queue/a', 'in tx');
        return 42;
      });
      assert.equal(result, 42);
      await server.last.waitFor('COMMIT');
      assert.deepEqual(server.last.commands.slice(1), ['BEGIN', 'SEND', 'COMMIT']);
      await client.close();
    });

    it('aborts and rethrows when the function fails', async () => {
      const { server, client } = setup();
      await client.connect();
      await assert.rejects(
        client.transaction(() => {
          throw new Error('nope');
        }),
        /nope/,
      );
      await server.last.waitFor('ABORT');
      await client.close();
    });

    it('requires a connection', async () => {
      const { client } = setup();
      await assert.rejects(
        client.transaction(() => undefined),
        StompNotConnectedError,
      );
    });
  });

  describe('closing', () => {
    it('disconnects gracefully and ends subscriptions', async () => {
      const { server, client, events } = setup();
      await client.connect();
      const sub = client.subscribe('/topic/a', () => {});
      await sub.ready;
      const closing = client.close();
      assert.equal(client.state, 'closing');
      assert.equal(client.close(), closing);
      await closing;
      assert.equal(client.state, 'closed');
      assert.ok(server.last.commands.includes('DISCONNECT'));
      assert.equal(sub.active, false);
      assert.deepEqual(events.disconnect, [{ error: undefined, willReconnect: false }]);
      assert.deepEqual(events.close, [{ error: undefined }]);
      assert.throws(() => client.subscribe('/topic/b', () => {}), StompConnectionClosedError);
    });

    it('aborts a pending connection attempt', async () => {
      const { server, client } = setup({ silent: true });
      const connecting = client.connect();
      await server.waitForConnections(1);
      await client.close();
      await assert.rejects(connecting, StompConnectionClosedError);
      assert.equal(server.last.transport.closed, true);
    });

    it('stops waiting between reconnection attempts', async () => {
      const { client, events } = setup(
        { rejectConnect: () => 'No' },
        { reconnect: { initialDelay: 60_000, jitter: 0 } },
      );
      const connecting = client.connect();
      await until(() => events.reconnecting.length === 1);
      const started = Date.now();
      await client.close();
      assert.ok(Date.now() - started < 1000);
      await assert.rejects(connecting);
    });

    it('can close a client that never connected', async () => {
      const { client, events } = setup();
      await client.close();
      assert.equal(client.state, 'closed');
      assert.equal(events.close.length, 1);
      await assert.rejects(client.connect(), StompConnectionClosedError);
    });

    it('supports await using', async () => {
      const { server } = setup();
      {
        await using client = new Client({ transport: () => server.transport(), heartbeat: { outgoing: 0, incoming: 0 } });
        await client.connect();
      }
      assert.ok(server.last.commands.includes('DISCONNECT'));
    });
  });
});

describe('backoffDelay', () => {
  it('grows exponentially up to the maximum', () => {
    const options = { initialDelay: 100, factor: 2, maxDelay: 1000, jitter: 0 };
    assert.deepEqual(
      [1, 2, 3, 4, 5, 6].map((attempt) => backoffDelay(attempt, options)),
      [100, 200, 400, 800, 1000, 1000],
    );
  });

  it('applies jitter as a reduction of up to the given fraction', () => {
    assert.equal(backoffDelay(1, { initialDelay: 1000, jitter: 0.5 }, () => 0), 1000);
    assert.equal(backoffDelay(1, { initialDelay: 1000, jitter: 0.5 }, () => 1), 500);
    assert.equal(backoffDelay(1, { initialDelay: 1000, jitter: 0.5 }, () => 0.5), 750);
  });
});
