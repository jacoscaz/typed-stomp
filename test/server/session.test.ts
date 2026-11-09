import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { StompServerError } from '../../src/client/errors.ts';
import type { StompMessage } from '../../src/client/message.ts';
import type { StompVersion } from '../../src/codec/frame.ts';
import { StompProtocolError } from '../../src/connection/errors.ts';
import type { BrokerSession } from '../../src/server/broker.ts';
import type { ServerSession } from '../../src/server/session.ts';
import { MemoryTransport } from '../../src/transport/memory.ts';
import type { TransportData } from '../../src/transport/types.ts';
import { until } from '../client/fake-server.ts';
import { flush } from '../helpers.ts';
import { recordingBroker, serve, session } from './helpers.ts';

/** A client end driven by hand, for sending raw frames. */
function raw(transport: () => MemoryTransport) {
  const end = transport();
  const received: string[] = [];
  let closed = false;
  end.attach({
    onMessage: (data: TransportData) => received.push(typeof data === 'string' ? data : new TextDecoder().decode(data)),
    onClose: () => {
      closed = true;
    },
  });
  return {
    end,
    received,
    get closed() {
      return closed;
    },
  };
}

describe('ServerSession', () => {
  describe('handshake', () => {
    it('negotiates version, heart-beats and identifies the session', async () => {
      const { broker, calls } = recordingBroker();
      const { server, transport } = serve({ broker, heartbeat: { outgoing: 500, incoming: 1000 }, server: 'test/1' });
      const client = await session(transport, { heartbeat: { outgoing: 2000, incoming: 300 } });
      assert.equal(client.version, '1.2');
      assert.equal(client.server, 'test/1');
      const serverSession = server.sessions[0]!;
      assert.equal(client.id, serverSession.id);
      assert.deepEqual(client.heartbeat, { outgoing: 2000, incoming: 500 });
      assert.equal(serverSession.state, 'connected');
      assert.deepEqual(calls.map((call) => call.method), ['connect']);
      await client.disconnect();
    });

    it('picks the highest version the server supports', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker, versions: ['1.0', '1.1'] });
      const client = await session(transport);
      assert.equal(client.version, '1.1');
      await client.disconnect();
    });

    it('lists supported versions when there is no common one', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker, versions: ['1.2'] });
      const peer = raw(transport);
      peer.end.send('CONNECT\naccept-version:1.0,1.1\nhost:/\n\n\0');
      await until(() => peer.closed);
      assert.match(peer.received[0] ?? '', /^ERROR\nversion:1\.2\nmessage:No supported protocol version\n/);
    });

    it('requires CONNECT first', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker });
      const peer = raw(transport);
      peer.end.send('SEND\ndestination:/a\nreceipt:r1\n\n\0');
      await until(() => peer.closed);
      assert.match(peer.received[0] ?? '', /^ERROR\nmessage:Expected a CONNECT frame, got SEND\nreceipt-id:r1\n/);
    });

    it('rejects invalid frames in strict mode', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker });
      const peer = raw(transport);
      // STOMP 1.1+ CONNECT frames require a host header.
      peer.end.send('CONNECT\naccept-version:1.2\n\n\0');
      await until(() => peer.closed);
      assert.match(peer.received[0] ?? '', /host/);
    });

    it('times out clients that never send CONNECT', async () => {
      const { broker } = recordingBroker();
      const { server, transport } = serve({ broker, connectTimeout: 20 });
      const peer = raw(transport);
      await until(() => peer.closed);
      assert.match(peer.received[0] ?? '', /No CONNECT frame received within 20 ms/);
      assert.equal(server.sessions.length, 0);
    });

    it('authenticates, exposing the principal and context', async () => {
      const { broker } = recordingBroker();
      const requests: unknown[] = [];
      const { server, transport } = serve({
        broker,
        authenticate: async (request) => {
          requests.push(request);
          await new Promise((resolve) => setTimeout(resolve, 5));
          if (request.passcode !== 'secret') {
            throw new Error('Bad credentials');
          }
          return { user: request.login };
        },
      });
      const client = await session(() => transport({ ip: '127.0.0.1' }), {
        headers: { login: 'alice', passcode: 'secret' },
      });
      const serverSession = server.sessions[0]!;
      assert.deepEqual(serverSession.principal, { user: 'alice' });
      assert.deepEqual(serverSession.context, { ip: '127.0.0.1' });
      assert.equal(serverSession.host, '/');
      assert.equal((requests[0] as { version: string }).version, '1.2');
      await client.disconnect();

      await assert.rejects(
        session(() => transport(), { headers: { login: 'mallory', passcode: 'guess' } }),
        (error: unknown) => error instanceof StompServerError && error.message === 'Bad credentials',
      );
    });

    it('decodes frames pipelined behind CONNECT with the negotiated version', async () => {
      const { broker, calls } = recordingBroker();
      const { transport } = serve({
        broker,
        authenticate: () => new Promise((resolve) => setTimeout(resolve, 5)),
      });
      const peer = raw(transport);
      peer.end.send('CONNECT\naccept-version:1.2\nhost:/\n\n\0SEND\ndestination:/a\\cb\n\n\0');
      await until(() => calls.some((call) => call.method === 'publish'));
      const publish = calls.find((call) => call.method === 'publish');
      assert.equal(publish?.method === 'publish' && publish.message.destination, '/a:b');
    });

    it('refuses connections the broker refuses', async () => {
      const { broker, calls } = recordingBroker({ fail: { connect: new Error('Too many connections') } });
      const { transport } = serve({ broker });
      await assert.rejects(session(transport), /Too many connections/);
      // disconnect is not called for sessions that never connected.
      await flush();
      assert.deepEqual(calls, []);
    });
  });

  it('passes messages to the broker, in order, with receipts', async () => {
    const { broker, calls } = recordingBroker();
    const { transport } = serve({ broker });
    const client = await session(transport);
    const sends = [
      client.send('/queue/a', '1', { contentType: 'text/plain', headers: { custom: 'x' } }),
      client.send('/queue/a', '2'),
      client.send('/queue/a', '3', { receipt: true }),
    ];
    await Promise.all(sends);
    const published = calls.filter((call) => call.method === 'publish');
    assert.deepEqual(
      published.map((call) => call.method === 'publish' && new TextDecoder().decode(call.message.body)),
      ['1', '2', '3'],
    );
    const first = published[0];
    assert.ok(first?.method === 'publish');
    assert.equal(first.message.destination, '/queue/a');
    assert.deepEqual({ ...first.message.headers }, { 'content-type': 'text/plain', custom: 'x' });
    await client.disconnect();
  });

  it('answers broker failures with ERROR and closes', async () => {
    const { broker } = recordingBroker({ fail: { publish: new Error('Destination is read-only') } });
    const { server, transport } = serve({ broker });
    const client = await session(transport);
    await assert.rejects(
      client.send('/topic/ro', 'x', { receipt: true }),
      (error: unknown) => error instanceof StompServerError && error.message === 'Destination is read-only',
    );
    await client.closed;
    await until(() => server.sessions.length === 0);
  });

  it('authorizes frames', async () => {
    const { broker, calls } = recordingBroker();
    const { transport } = serve({
      broker,
      authorize: (_session, frame) => {
        if (frame.command === 'SUBSCRIBE' && frame.headers.destination.startsWith('/admin/')) {
          throw new Error('Forbidden');
        }
      },
    });
    const client = await session(transport);
    await client.subscribe('/topic/ok', () => {}, { receipt: true }).ready;
    await assert.rejects(client.subscribe('/admin/x', () => {}, { receipt: true }).ready, /Forbidden/);
    assert.deepEqual(
      calls.filter((call) => call.method === 'subscribe').map((call) => call.method === 'subscribe' && call.subscription.destination),
      ['/topic/ok'],
    );
  });

  describe('subscriptions and delivery', () => {
    async function subscribed(version: StompVersion, ack: 'auto' | 'client' | 'client-individual' = 'client-individual') {
      const { broker, calls } = recordingBroker();
      const { server, transport } = serve({ broker });
      const client = await session(transport, { versions: [version] });
      const received: StompMessage[] = [];
      const sub = client.subscribe('/queue/a', (message) => received.push(message), {
        ack,
        receipt: true,
        headers: { 'prefetch-count': '5' },
      });
      await sub.ready;
      const serverSession = server.sessions[0]!;
      return { calls, client, sub, received, serverSession };
    }

    it('registers subscriptions with the broker', async () => {
      const { calls, client, sub, serverSession } = await subscribed('1.2');
      const call = calls.find((c) => c.method === 'subscribe');
      assert.ok(call?.method === 'subscribe');
      assert.equal(call.subscription.id, sub.id);
      assert.equal(call.subscription.ack, 'client-individual');
      assert.equal(call.subscription.headers['prefetch-count'], '5');
      assert.deepEqual(serverSession.subscriptions, [sub.id]);
      await sub.unsubscribe({ receipt: true });
      assert.deepEqual(serverSession.subscriptions, []);
      assert.ok(calls.some((c) => c.method === 'unsubscribe' && c.subscriptionId === sub.id));
      await client.disconnect();
    });

    it('rejects duplicate subscription ids', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker });
      const peer = raw(transport);
      peer.end.send('CONNECT\naccept-version:1.2\nhost:/\n\n\0');
      peer.end.send('SUBSCRIBE\nid:0\ndestination:/a\n\n\0SUBSCRIBE\nid:0\ndestination:/b\n\n\0');
      await until(() => peer.closed);
      assert.match(peer.received.at(-1) ?? '', /Subscription id "0" is already in use/);
    });

    for (const version of ['1.0', '1.1', '1.2'] as const) {
      it(`delivers messages and maps acknowledgements in ${version}`, async () => {
        // STOMP 1.0 has no client-individual mode.
        const { calls, client, sub, received, serverSession } = await subscribed(
          version,
          version === '1.0' ? 'client' : 'client-individual',
        );
        assert.equal(
          serverSession.deliver(sub.id, { messageId: 'm1', destination: '/queue/a', body: 'hello', headers: { x: 'y' } }),
          true,
        );
        await until(() => received.length === 1);
        const message = received[0]!;
        assert.equal(message.text(), 'hello');
        assert.equal(message.messageId, 'm1');
        assert.equal(message.headers['x'], 'y');
        assert.equal(message.subscription, sub.id);
        assert.equal(message.headers.ack !== undefined, version === '1.2');
        await message.ack({ receipt: true });
        const ack = calls.find((call) => call.method === 'ack');
        assert.ok(ack?.method === 'ack');
        assert.deepEqual(ack.ref, { subscriptionId: sub.id, messageId: 'm1' });
        await client.disconnect();
      });
    }

    it('rejects acknowledgements of unknown messages', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker });
      const peer = raw(transport);
      peer.end.send('CONNECT\naccept-version:1.2\nhost:/\n\n\0');
      peer.end.send('ACK\nid:nope\n\n\0');
      await until(() => peer.closed);
      assert.match(peer.received.at(-1) ?? '', /ACK for an unknown or already acknowledged message/);
    });

    it('settles earlier messages cumulatively in client mode', async () => {
      const { client, sub, received, serverSession, calls } = await subscribed('1.2', 'client');
      for (const id of ['m1', 'm2', 'm3']) {
        serverSession.deliver(sub.id, { messageId: id, destination: '/queue/a', body: id });
      }
      await until(() => received.length === 3);
      await received[1]!.ack({ receipt: true });
      // m1 was covered by acknowledging m2; acknowledging it again is an error.
      await assert.rejects(received[0]!.ack({ receipt: true }), StompServerError);
      assert.deepEqual(
        calls.filter((call) => call.method === 'ack').map((call) => call.method === 'ack' && call.ref.messageId),
        ['m2'],
      );
      void client;
    });

    it('does not deliver to unknown subscriptions or closed sessions', async () => {
      const { client, serverSession } = await subscribed('1.2');
      assert.equal(serverSession.deliver('nope', { messageId: 'x', destination: '/a', body: '' }), false);
      await client.disconnect();
      await serverSession.closed;
      assert.equal(serverSession.active, false);
      assert.equal(serverSession.deliver('sub-0', { messageId: 'x', destination: '/a', body: '' }), false);
    });

    it('supports 1.0 subscriptions without ids, unsubscribing by destination', async () => {
      const { broker, calls } = recordingBroker();
      const { server, transport } = serve({ broker });
      const peer = raw(transport);
      peer.end.send('CONNECT\naccept-version:1.0\nhost:/\n\n\0');
      peer.end.send('SUBSCRIBE\ndestination:/topic/a\nreceipt:r1\n\n\0');
      await until(() => peer.received.length === 2);
      const serverSession = server.sessions[0]!;
      serverSession.deliver('/topic/a', { messageId: 'm1', destination: '/topic/a', body: 'x' });
      await until(() => peer.received.length === 3);
      assert.equal(peer.received[2], 'MESSAGE\ndestination:/topic/a\nmessage-id:m1\ncontent-length:1\n\nx\0');
      peer.end.send('UNSUBSCRIBE\ndestination:/topic/a\nreceipt:r2\n\n\0');
      await until(() => peer.received.length === 4);
      assert.ok(calls.some((call) => call.method === 'unsubscribe' && call.subscriptionId === '/topic/a'));
    });
  });

  describe('transactions', () => {
    it('buffers transactional frames when the broker does not support transactions', async () => {
      const { broker, methods, calls } = recordingBroker();
      const { transport } = serve({ broker });
      const client = await session(transport);
      const tx = client.begin();
      await tx.send('/queue/a', '1', { receipt: true });
      assert.deepEqual(methods(), ['connect']);
      await tx.commit({ receipt: true });
      assert.deepEqual(methods(), ['connect', 'publish']);
      const publish = calls[1];
      assert.ok(publish?.method === 'publish');
      assert.equal(publish.transaction, undefined);

      const aborted = client.begin();
      await aborted.send('/queue/a', '2');
      await aborted.abort({ receipt: true });
      assert.deepEqual(methods(), ['connect', 'publish']);
      await client.disconnect();
    });

    it('uses native broker transactions when available', async () => {
      const { broker, methods } = recordingBroker({ transactions: true });
      const { transport } = serve({ broker });
      const client = await session(transport);
      const tx = client.begin();
      await tx.send('/queue/a', '1');
      await tx.commit({ receipt: true });
      assert.deepEqual(methods(), ['connect', 'begin', 'publish', 'commit']);
      client.begin();
      await client.disconnect();
      await until(() => methods().includes('disconnect'));
      // Open transactions are aborted when the session ends.
      assert.deepEqual(methods().slice(-2), ['abort', 'disconnect']);
    });

    it('rejects unknown and duplicate transactions', async () => {
      const { broker } = recordingBroker();
      const { transport } = serve({ broker });
      const peer = raw(transport);
      peer.end.send('CONNECT\naccept-version:1.2\nhost:/\n\n\0');
      peer.end.send('COMMIT\ntransaction:nope\n\n\0');
      await until(() => peer.closed);
      assert.match(peer.received.at(-1) ?? '', /Unknown transaction "nope"/);

      const other = raw(transport);
      other.end.send('CONNECT\naccept-version:1.2\nhost:/\n\n\0');
      other.end.send('BEGIN\ntransaction:t\n\n\0BEGIN\ntransaction:t\n\n\0');
      await until(() => other.closed);
      assert.match(other.received.at(-1) ?? '', /already begun/);
    });
  });

  describe('closing', () => {
    it('acknowledges DISCONNECT and tells the broker', async () => {
      const { broker, methods } = recordingBroker();
      const { server, transport } = serve({ broker });
      const disconnects: unknown[] = [];
      server.on('disconnect', (event) => disconnects.push(event.error));
      const client = await session(transport);
      await client.disconnect();
      await until(() => methods().includes('disconnect'));
      assert.deepEqual(disconnects, [undefined]);
      assert.equal(server.sessions.length, 0);
    });

    it('tells the broker about lost connections', async () => {
      const { broker, calls } = recordingBroker();
      const { server, transport } = serve({ broker });
      const end = transport();
      const client = await session(() => end);
      const serverSession = server.sessions[0]!;
      end.terminate();
      await client.closed;
      await serverSession.closed;
      const disconnect = calls.find((call) => call.method === 'disconnect');
      assert.equal(disconnect?.session, serverSession as BrokerSession);
    });

    it('lets the broker close sessions, optionally with an error', async () => {
      const { broker } = recordingBroker();
      const { server, transport } = serve({ broker });
      const client = await session(transport);
      server.sessions[0]!.close({ message: 'Kicked', headers: { reason: 'idle' }, body: 'details' });
      const info = await client.closed;
      assert.ok(info.error instanceof StompServerError);
      assert.equal(info.error.message, 'Kicked');
      assert.equal(info.error.frame.headers['reason'], 'idle');
      assert.equal(info.error.details, 'details');
    });

    it('closes all sessions and refuses new transports when the server closes', async () => {
      const { broker } = recordingBroker();
      const { server, transport } = serve({ broker });
      const a = await session(transport);
      const b = await session(transport);
      await server.close();
      await a.closed;
      await b.closed;
      assert.equal(server.sessions.length, 0);
      assert.throws(() => transport(), /Server is closed/);
    });

    it('reports broker errors on disconnect through the error event', async () => {
      const failure = new Error('cleanup failed');
      const { broker } = recordingBroker();
      broker.disconnect = () => {
        throw failure;
      };
      const { server, transport } = serve({ broker });
      const errors: { error: unknown; session?: ServerSession }[] = [];
      server.on('error', (event) => errors.push(event));
      const client = await session(transport);
      await client.disconnect();
      await until(() => errors.length === 1);
      assert.equal(errors[0]?.error, failure);
    });
  });

  it('turns client protocol errors into ERROR frames', async () => {
    const { broker } = recordingBroker();
    const { transport } = serve({ broker });
    const peer = raw(transport);
    peer.end.send('CONNECT\naccept-version:1.2\nhost:/\n\n\0');
    peer.end.send('SEND\n\n\0');
    await until(() => peer.closed);
    assert.match(peer.received.at(-1) ?? '', /^ERROR\nmessage:SEND frames require a "destination" header/);
    void StompProtocolError;
  });
});
