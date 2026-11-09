import assert from 'node:assert/strict';
import { describe, it, type TestContext } from 'node:test';
import { StompSerializeError } from '../../src/codec/errors.ts';
import {
  Connection,
  type ConnectionCloseInfo,
  type ConnectionOptions,
  type ConnectionRole,
  type IncomingFrame,
} from '../../src/connection/connection.ts';
import {
  StompConnectionClosedError,
  StompHeartbeatTimeoutError,
  StompProtocolError,
} from '../../src/connection/errors.ts';
import type { ValidationIssue } from '../../src/frames/validate.ts';
import { MemoryTransport } from '../../src/transport/memory.ts';
import type { TransportData } from '../../src/transport/types.ts';
import { flush, recordSends, text } from '../helpers.ts';

interface Side<R extends ConnectionRole> {
  readonly connection: Connection<R>;
  readonly transport: MemoryTransport;
  readonly frames: IncomingFrame<R>[];
  readonly warnings: (readonly ValidationIssue[])[];
  readonly closes: ConnectionCloseInfo[];
  readonly sent: TransportData[];
}

type Overrides<R extends ConnectionRole> = Partial<Omit<ConnectionOptions<R>, 'role' | 'transport'>>;

function side<R extends ConnectionRole>(role: R, transport: MemoryTransport, overrides: Overrides<R> = {}): Side<R> {
  const frames: IncomingFrame<R>[] = [];
  const warnings: (readonly ValidationIssue[])[] = [];
  const closes: ConnectionCloseInfo[] = [];
  const sent = recordSends(transport);
  const connection = new Connection<R>({
    role,
    transport,
    onFrame: (frame, issues) => {
      frames.push(frame);
      warnings.push(issues);
    },
    onClose: (info) => closes.push(info),
    ...overrides,
  });
  return { connection, transport, frames, warnings, closes, sent };
}

function connect(options: { client?: Overrides<'client'>; server?: Overrides<'server'> } = {}) {
  const [clientTransport, serverTransport] = MemoryTransport.pair();
  return {
    client: side('client', clientTransport, options.client),
    server: side('server', serverTransport, options.server),
  };
}

/** A server-side transport driven by hand, for sending arbitrary bytes. */
function rawPeer() {
  const [clientTransport, rawTransport] = MemoryTransport.pair();
  const received: TransportData[] = [];
  rawTransport.attach({ onMessage: (data) => received.push(data), onClose: () => {} });
  return { client: side('client', clientTransport), raw: rawTransport, received };
}

function enableMockTimers(t: TestContext): void {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
}

describe('Connection', () => {
  it('exchanges validated frames in both directions', async () => {
    const { client, server } = connect();
    client.connection.send({ command: 'SEND', headers: { destination: '/queue/a' }, body: 'hi' });
    await flush();
    assert.equal(server.frames.length, 1);
    const frame = server.frames[0]!;
    assert.equal(frame.command, 'SEND');
    assert.equal(frame.headers['destination'], '/queue/a');
    assert.equal(text(frame.body), 'hi');

    server.connection.send({ command: 'RECEIPT', headers: { 'receipt-id': '1' } });
    await flush();
    assert.equal(client.frames[0]?.command, 'RECEIPT');
  });

  it('escapes and unescapes headers according to the version', async () => {
    const { client, server } = connect();
    client.connection.version = '1.2';
    server.connection.version = '1.2';
    client.connection.send({ command: 'SEND', headers: { destination: '/a:b\n' } });
    await flush();
    assert.equal(client.sent[0], 'SEND\ndestination:/a\\cb\\n\n\n\0');
    assert.equal(server.frames[0]?.headers['destination'], '/a:b\n');
  });

  describe('message types', () => {
    it('sends text unless the body is binary in auto mode', async () => {
      const { client } = connect();
      client.connection.send({ command: 'SEND', headers: { destination: '/a' }, body: 'text' });
      client.connection.send({ command: 'SEND', headers: { destination: '/a' }, body: new TextEncoder().encode('utf8') });
      client.connection.send({ command: 'SEND', headers: { destination: '/a' }, body: Uint8Array.of(0xff) });
      assert.equal(typeof client.sent[0], 'string');
      assert.equal(typeof client.sent[1], 'string');
      assert.ok(client.sent[2] instanceof Uint8Array);
    });

    it('always sends binary in binary mode', () => {
      const { client } = connect({ client: { messageType: 'binary' } });
      client.connection.send({ command: 'SEND', headers: { destination: '/a' }, body: 'text' });
      assert.ok(client.sent[0] instanceof Uint8Array);
    });

    it('refuses binary bodies in text mode', () => {
      const { client } = connect({ client: { messageType: 'text' } });
      assert.throws(
        () => client.connection.send({ command: 'SEND', headers: { destination: '/a' }, body: Uint8Array.of(0xff) }),
        StompSerializeError,
      );
    });
  });

  it('passes validation warnings along with frames in lenient mode', async () => {
    const { client, server } = connect();
    server.connection.send({ command: 'RECEIPT', headers: { 'receipt-id': '1' }, body: 'unexpected' });
    await flush();
    assert.equal(client.frames.length, 1);
    assert.deepEqual(
      client.warnings[0]?.map((issue) => [issue.severity, issue.code]),
      [['warning', 'unexpected-body']],
    );
  });

  describe('protocol errors', () => {
    it('makes a server reply with an ERROR frame and fail', async () => {
      const { client, server } = connect();
      client.connection.send({ command: 'SEND', headers: { receipt: 'r-1' } });
      await flush();
      assert.equal(server.frames.length, 0);
      assert.equal(server.closes.length, 1);
      const error = server.closes[0]?.error;
      assert.ok(error instanceof StompProtocolError);
      assert.equal(error.issues[0]?.code, 'missing-header');

      const reply = client.frames[0];
      assert.equal(reply?.command, 'ERROR');
      assert.equal(reply?.headers['receipt-id'], 'r-1');
      assert.match(reply?.headers['message'] ?? '', /destination/);
      assert.match(text(reply!.body), /error: SEND frames require a "destination" header/);
      assert.equal(client.closes.length, 1);
      assert.equal(client.closes[0]?.error, undefined);
    });

    it('makes a server reply to malformed data', async () => {
      const [clientTransport, serverTransport] = MemoryTransport.pair();
      const replies: TransportData[] = [];
      clientTransport.attach({ onMessage: (data) => replies.push(data), onClose: () => {} });
      const server = side('server', serverTransport);
      clientTransport.send('SEND\nno colon here\n\n\0');
      await flush();
      assert.ok(server.closes[0]?.error instanceof StompProtocolError);
      assert.match(String(replies[0]), /^ERROR\nmessage:Malformed frame: Header line without a colon\n/);
    });

    it('makes a client fail without replying', async () => {
      const { client, raw, received } = rawPeer();
      raw.send('MESSAGE\ndestination:/a\n\n\0');
      await flush();
      const error = client.closes[0]?.error;
      assert.ok(error instanceof StompProtocolError);
      assert.match(error.message, /message-id/);
      assert.deepEqual(received, []);
      assert.equal(client.connection.state, 'closed');
    });

    it('stops processing frames after a failure', async () => {
      const { client, raw } = rawPeer();
      raw.send('RECEIPT\n\n\0RECEIPT\nreceipt-id:2\n\n\0');
      await flush();
      assert.equal(client.frames.length, 0);
      assert.equal(client.closes.length, 1);
    });
  });

  it('fails when onFrame throws, without sending ERROR frames', async () => {
    const boom = new Error('boom');
    const { client, server } = connect({
      server: {
        onFrame: () => {
          throw boom;
        },
      },
    });
    client.connection.send({ command: 'SEND', headers: { destination: '/a' } });
    await flush();
    assert.equal(server.closes[0]?.error, boom);
    assert.equal(client.frames.length, 0);
  });

  it('can fail with a custom final frame', async () => {
    const { client, server } = connect();
    const error = new Error('access denied');
    server.connection.fail(error, { command: 'ERROR', headers: { message: 'Access denied' } });
    assert.equal(server.connection.state, 'closed');
    assert.deepEqual(server.closes, [{ error }]);
    await flush();
    assert.equal(client.frames[0]?.headers['message'], 'Access denied');
    server.connection.fail(new Error('again'));
    assert.equal(server.closes.length, 1);
  });

  describe('closing', () => {
    it('closes in an orderly fashion', async () => {
      const { client, server } = connect();
      client.connection.close();
      assert.equal(client.connection.state, 'closing');
      assert.throws(() => client.connection.send({ command: 'DISCONNECT' }), StompConnectionClosedError);
      await flush();
      assert.equal(client.connection.state, 'closed');
      assert.deepEqual(client.closes, [{ code: 1000, reason: '' }]);
      assert.deepEqual(server.closes, [{ code: 1000, reason: '' }]);
      client.connection.close();
      assert.equal(client.closes.length, 1);
    });

    it('reports unexpected transport loss as an error', async () => {
      const { client, server } = connect();
      client.transport.terminate();
      await flush();
      assert.ok(client.closes[0]?.error instanceof StompConnectionClosedError);
      assert.ok(server.closes[0]?.error instanceof StompConnectionClosedError);
    });

    it('reports transport errors', async () => {
      const { client } = connect();
      const error = new Error('socket error');
      client.transport.terminate(error);
      await flush();
      assert.equal(client.closes[0]?.error, error);
    });
  });

  describe('pausing', () => {
    it('holds back pipelined frames until resumed, so the version can change', async () => {
      const [clientTransport, serverTransport] = MemoryTransport.pair();
      clientTransport.attach({ onMessage: () => {}, onClose: () => {} });
      const server = side('server', serverTransport, {
        onFrame: (frame) => {
          server.frames.push(frame);
          if (frame.command === 'CONNECT') {
            server.connection.pause();
          }
        },
      });
      clientTransport.send('CONNECT\naccept-version:1.2\nhost:h\n\n\0SEND\ndestination:/a\\cb\n\n\0');
      await flush();
      assert.deepEqual(
        server.frames.map((frame) => frame.command),
        ['CONNECT'],
      );
      assert.equal(server.connection.paused, true);

      // Simulate asynchronous authentication, then negotiate.
      await new Promise((resolve) => setTimeout(resolve, 5));
      server.connection.version = '1.2';
      server.connection.resume();
      assert.equal(server.frames[1]?.headers['destination'], '/a:b');
    });

    it('turns errors in buffered frames into failures on resume', async () => {
      const { client, raw } = rawPeer();
      client.connection.pause();
      raw.send('RECEIPT\n\n\0');
      await flush();
      assert.equal(client.closes.length, 0);
      client.connection.resume();
      assert.ok(client.closes[0]?.error instanceof StompProtocolError);
    });
  });

  describe('heart-beats', () => {
    it('sends EOLs when nothing else was sent in the interval', async (t) => {
      enableMockTimers(t);
      const { client } = connect();
      client.connection.startHeartbeat({ outgoing: 1000, incoming: 0 });
      t.mock.timers.tick(999);
      assert.deepEqual(client.sent, []);
      t.mock.timers.tick(1);
      assert.deepEqual(client.sent, ['\n']);

      // A frame at t=1500 postpones the next heart-beat to t=2500.
      t.mock.timers.tick(500);
      client.connection.send({ command: 'SEND', headers: { destination: '/a' } });
      t.mock.timers.tick(999);
      assert.equal(client.sent.length, 2);
      t.mock.timers.tick(1);
      assert.equal(client.sent.length, 3);
      assert.equal(client.sent[2], '\n');
      client.connection.close();
    });

    it('sends binary EOLs in binary mode', (t) => {
      enableMockTimers(t);
      const { client } = connect({ client: { messageType: 'binary' } });
      client.connection.startHeartbeat({ outgoing: 100, incoming: 0 });
      t.mock.timers.tick(100);
      assert.deepEqual(client.sent, [Uint8Array.of(0x0a)]);
      client.connection.close();
    });

    it('stays alive while the peer sends heart-beats', async (t) => {
      enableMockTimers(t);
      const { client, server } = connect();
      client.connection.startHeartbeat({ outgoing: 1000, incoming: 0 });
      server.connection.startHeartbeat({ outgoing: 0, incoming: 1000 });
      for (let i = 0; i < 40; i += 1) {
        t.mock.timers.tick(250);
        await flush();
      }
      assert.equal(server.connection.state, 'open');
      assert.deepEqual(server.closes, []);
      client.connection.close();
      await flush();
    });

    it('fails the connection when the peer goes silent', async (t) => {
      enableMockTimers(t);
      const { client, server } = connect({ server: { heartbeatTolerance: 1.5 } });
      server.connection.startHeartbeat({ outgoing: 0, incoming: 1000 });
      t.mock.timers.tick(1499);
      assert.equal(server.connection.state, 'open');
      t.mock.timers.tick(1);
      assert.equal(server.connection.state, 'closed');
      const error = server.closes[0]?.error;
      assert.ok(error instanceof StompHeartbeatTimeoutError);
      assert.equal(error.elapsed, 1500);
      await flush();
      assert.equal(client.connection.state, 'closed');
    });

    it('stops heart-beating when closed', async (t) => {
      enableMockTimers(t);
      const { client } = connect();
      client.connection.startHeartbeat({ outgoing: 100, incoming: 100 });
      client.connection.close();
      t.mock.timers.tick(10_000);
      assert.deepEqual(client.sent, []);
      await flush();
      assert.equal(client.closes[0]?.error, undefined);
    });
  });
});
