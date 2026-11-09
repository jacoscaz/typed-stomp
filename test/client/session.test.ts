import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { StompConnectTimeoutError, StompNotConnectedError, StompServerError } from '../../src/client/errors.ts';
import type { StompMessage } from '../../src/client/message.ts';
import { ClientSession, type ClientSessionOptions } from '../../src/client/session.ts';
import type { StompVersion } from '../../src/codec/frame.ts';
import { StompConnectionClosedError, StompProtocolError } from '../../src/connection/errors.ts';
import { MemoryTransport } from '../../src/transport/memory.ts';
import { flush } from '../helpers.ts';
import { FakeServer, delay, type FakeServerOptions } from './fake-server.ts';

async function connect(
  serverOptions: FakeServerOptions = {},
  sessionOptions: Partial<ClientSessionOptions> = {},
): Promise<{ server: FakeServer; session: ClientSession }> {
  const server = new FakeServer(serverOptions);
  const session = await ClientSession.connect({
    transport: server.transport(),
    heartbeat: { outgoing: 0, incoming: 0 },
    ...sessionOptions,
  });
  return { server, session };
}

describe('ClientSession', () => {
  describe('handshake', () => {
    it('sends CONNECT and negotiates the highest common version', async () => {
      const { server, session } = await connect(
        { versions: ['1.0', '1.1'] },
        { headers: { login: 'guest', passcode: 'secret' }, host: 'vhost', heartbeat: { outgoing: 100, incoming: 200 } },
      );
      const connectFrame = server.last.frames[0]!;
      assert.equal(connectFrame.command, 'CONNECT');
      assert.deepEqual(
        { ...connectFrame.headers },
        {
          login: 'guest',
          passcode: 'secret',
          'accept-version': '1.0,1.1,1.2',
          host: 'vhost',
          'heart-beat': '100,200',
        },
      );
      assert.equal(session.state, 'connected');
      assert.equal(session.version, '1.1');
      assert.equal(session.server, 'fake/1.0');
      assert.equal(session.id, 'session-0');
      await session.disconnect();
    });

    it('can connect with the STOMP command', async () => {
      const { server, session } = await connect({}, { command: 'STOMP' });
      assert.equal(server.last.frames[0]?.command, 'STOMP');
      await session.disconnect();
    });

    it('negotiates heart-beats with the server', async () => {
      const { session } = await connect({ heartBeat: '50,1000' }, { heartbeat: { outgoing: 200, incoming: 100 } });
      assert.deepEqual(session.heartbeat, { outgoing: 1000, incoming: 100 });
      session.abort();
    });

    it('rejects with the server error when CONNECT is refused', async () => {
      const server = new FakeServer({ rejectConnect: () => 'Bad credentials' });
      const transport = server.transport();
      await assert.rejects(
        ClientSession.connect({ transport }),
        (error: unknown) => error instanceof StompServerError && error.message === 'Bad credentials',
      );
      assert.equal(transport.closed, true);
    });

    it('rejects when the server picks a version that was not offered', async () => {
      await assert.rejects(
        connect({ forceVersion: '1.2' }, { versions: ['1.0', '1.1'] }),
        (error: unknown) => error instanceof StompProtocolError && /not offered/.test(error.message),
      );
    });

    it('times out waiting for CONNECTED', async () => {
      await assert.rejects(connect({ silent: true }, { connectTimeout: 20 }), StompConnectTimeoutError);
    });

    it('can be aborted', async () => {
      const controller = new AbortController();
      const server = new FakeServer({ silent: true });
      const pending = ClientSession.connect({ transport: server.transport(), signal: controller.signal });
      controller.abort(new Error('stop'));
      await assert.rejects(pending, /stop/);
      await assert.rejects(
        ClientSession.connect({ transport: server.transport(), signal: AbortSignal.abort(new Error('early')) }),
        /early/,
      );
    });

    it('rejects when the transport closes during the handshake', async () => {
      const server = new FakeServer({ silent: true });
      const pending = ClientSession.connect({ transport: server.transport() });
      await flush();
      server.last.connection.close();
      await assert.rejects(pending, StompConnectionClosedError);
    });

    it('fails on transports that are already closed', async () => {
      const [a] = MemoryTransport.pair();
      a.close();
      await flush();
      await assert.rejects(ClientSession.connect({ transport: a }), StompConnectionClosedError);
    });
  });

  it('sends messages, with and without receipts', async () => {
    const { server, session } = await connect();
    await session.send('/queue/a', 'one', { contentType: 'text/plain', headers: { priority: '9' } });
    await session.send('/queue/a', Uint8Array.of(1, 2), { receipt: true });
    const sends = server.last.frames.filter((frame) => frame.command === 'SEND');
    assert.equal(sends.length, 2);
    assert.equal(sends[0]?.headers['content-type'], 'text/plain');
    assert.equal(sends[0]?.headers['priority'], '9');
    assert.equal(sends[0]?.headers['receipt'], undefined);
    assert.equal(sends[1]?.headers['receipt'], 'receipt-0');
    assert.deepEqual(sends[1]?.body, Uint8Array.of(1, 2));
    await session.disconnect();
  });

  describe('subscriptions', () => {
    it('routes messages to subscriptions by id', async () => {
      const { server, session } = await connect();
      const a: string[] = [];
      const b: string[] = [];
      const subA = session.subscribe('/topic/a', (message) => a.push(message.text()));
      session.subscribe('/topic/b', (message) => b.push(message.text()), { id: 'custom' });
      await server.last.waitFor('SUBSCRIBE', (frame) => frame.headers['id'] === 'custom');
      server.last.message(subA.id, 'for a');
      server.last.message('custom', 'for b');
      server.last.message('unknown', 'ignored');
      await flush();
      assert.deepEqual(a, ['for a']);
      assert.deepEqual(b, ['for b']);
      await session.disconnect();
    });

    it('routes 1.0 messages without a subscription header by destination', async () => {
      const { server, session } = await connect({ versions: ['1.0'] });
      const received: string[] = [];
      session.subscribe('/topic/a', (message) => received.push(message.text()));
      await server.last.waitFor('SUBSCRIBE');
      server.last.message('sub-0', 'hello', { subscription: undefined });
      await flush();
      assert.deepEqual(received, ['hello']);
      await session.disconnect();
    });

    it('confirms subscriptions with receipts and unsubscribes', async () => {
      const { server, session } = await connect();
      const received: string[] = [];
      const sub = session.subscribe('/topic/a', (message) => received.push(message.text()), { receipt: true });
      await sub.ready;
      await sub.unsubscribe({ receipt: true });
      const unsubscribe = await server.last.waitFor('UNSUBSCRIBE');
      assert.equal(unsubscribe.headers['id'], sub.id);
      server.last.message(sub.id, 'late');
      await flush();
      assert.deepEqual(received, []);
      await session.disconnect();
    });

    it('rejects duplicate ids and unsupported ack modes', async () => {
      const { session } = await connect({ versions: ['1.0'] });
      session.subscribe('/a', () => {}, { id: 'x' });
      assert.throws(() => session.subscribe('/b', () => {}, { id: 'x' }), /already in use/);
      assert.throws(() => session.subscribe('/b', () => {}, { ack: 'client-individual' }), /1\.1/);
      await session.disconnect();
    });
  });

  describe('acknowledgements', () => {
    async function receive(version: StompVersion) {
      const { server, session } = await connect({ versions: [version] });
      const messages: StompMessage[] = [];
      const sub = session.subscribe('/queue/a', (message) => messages.push(message), { ack: 'client' });
      await server.last.waitFor('SUBSCRIBE');
      server.last.message(sub.id, 'payload');
      await flush();
      return { server, session, message: messages[0]! };
    }

    it('acknowledges by ack id in 1.2', async () => {
      const { server, session, message } = await receive('1.2');
      assert.equal(message.ackRequired, true);
      await message.ack({ receipt: true });
      const ack = await server.last.waitFor('ACK');
      assert.deepEqual({ ...ack.headers }, { id: `ack-${message.messageId}`, receipt: 'receipt-0' });
      await session.disconnect();
    });

    it('acknowledges by message id and subscription in 1.1', async () => {
      const { server, session, message } = await receive('1.1');
      await message.nack();
      const nack = await server.last.waitFor('NACK');
      assert.deepEqual({ ...nack.headers }, { 'message-id': message.messageId, subscription: 'sub-0' });
      await session.disconnect();
    });

    it('acknowledges by message id in 1.0, where NACK does not exist', async () => {
      const first = await receive('1.0');
      await assert.rejects(first.message.nack(), /1\.1/);
      await first.session.disconnect();
      const { server, session, message } = await receive('1.0');
      await message.ack();
      assert.deepEqual({ ...(await server.last.waitFor('ACK')).headers }, { 'message-id': message.messageId });
      await session.disconnect();
    });

    it('only acknowledges once, and is a no-op in auto mode', async () => {
      const { server, session } = await connect();
      const messages: StompMessage[] = [];
      const auto = session.subscribe('/a', (message) => messages.push(message));
      const manual = session.subscribe('/b', (message) => messages.push(message), { ack: 'client-individual' });
      await server.last.waitFor('SUBSCRIBE', (frame) => frame.headers['id'] === manual.id);
      server.last.message(auto.id, '1');
      server.last.message(manual.id, '2');
      await flush();
      await messages[0]!.ack();
      await messages[1]!.ack();
      await messages[1]!.nack();
      await flush();
      assert.deepEqual(
        server.last.commands.filter((command) => command === 'ACK' || command === 'NACK'),
        ['ACK'],
      );
      await session.disconnect();
    });

    it('rejects acknowledgements after the session closed', async () => {
      const { session, message } = await receive('1.2');
      session.abort();
      await assert.rejects(message.ack(), StompNotConnectedError);
    });
  });

  it('runs transactions', async () => {
    const { server, session } = await connect();
    const tx = session.begin({ receipt: true });
    await tx.ready;
    await tx.send('/queue/a', 'in tx');
    await tx.commit({ receipt: true });
    const tx2 = session.begin();
    await tx2.abort();
    await flush();
    assert.deepEqual(server.last.commands.slice(1), ['BEGIN', 'SEND', 'COMMIT', 'BEGIN', 'ABORT']);
    const send = server.last.frames.find((frame) => frame.command === 'SEND');
    assert.equal(send?.headers['transaction'], tx.id);
    await session.disconnect();
  });

  describe('closing', () => {
    it('disconnects gracefully with a receipt', async () => {
      const { server, session } = await connect();
      await session.disconnect();
      assert.equal(session.state, 'closed');
      const disconnect = await server.last.waitFor('DISCONNECT');
      assert.ok(disconnect.headers['receipt']);
      assert.deepEqual(await session.closed, { code: 1000, reason: '' });
      await assert.rejects(session.send('/a'), StompNotConnectedError);
      await session.disconnect();
    });

    it('gives up waiting for the DISCONNECT receipt after a timeout', async () => {
      const server = new FakeServer();
      const transport = server.transport();
      const session = await ClientSession.connect({ transport, heartbeat: { outgoing: 0, incoming: 0 } });
      // Make the server ignore DISCONNECT.
      server.last.connection.pause();
      const started = Date.now();
      await session.disconnect({ timeout: 30 });
      assert.ok(Date.now() - started >= 25);
      assert.equal(session.state, 'closed');
    });

    it('fails on ERROR frames, rejecting the matching receipt', async () => {
      const { server, session } = await connect({ autoReceipt: false });
      const sending = session.send('/a', 'x', { receipt: true });
      const other = session.send('/b', 'y', { receipt: true });
      await server.last.waitFor('SEND', (frame) => frame.headers['destination'] === '/b');
      // Answer the first receipt with an error.
      server.last.error('Not allowed', { 'receipt-id': 'receipt-0' });
      await assert.rejects(sending, (error: unknown) => error instanceof StompServerError);
      await assert.rejects(other, StompConnectionClosedError);
      const info = await session.closed;
      assert.ok(info.error instanceof StompServerError);
      assert.equal(session.state, 'closed');
    });

    it('reports connection loss', async () => {
      const { server, session } = await connect();
      server.last.kill();
      const info = await session.closed;
      assert.ok(info.error instanceof StompConnectionClosedError);
    });

    it('detects dead peers through heart-beats', async () => {
      // The server promises heart-beats every 20 ms but never sends any.
      const { session } = await connect(
        { heartBeat: '20,0' },
        { heartbeat: { outgoing: 0, incoming: 20 }, heartbeatTolerance: 1 },
      );
      const info = await Promise.race([session.closed, delay(1000).then(() => undefined)]);
      assert.equal(info?.error?.constructor.name, 'StompHeartbeatTimeoutError');
    });
  });
});
