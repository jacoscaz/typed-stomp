import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { createServer, type Socket } from 'node:net';
import { after, before, describe, it } from 'node:test';
import { WebSocketServer, WebSocket as WsWebSocket } from 'ws';
import { Connection } from '../../src/connection/connection.ts';
import type { IncomingFrame } from '../../src/connection/connection.ts';
import { TransportClosedError } from '../../src/transport/errors.ts';
import type { TransportCloseInfo, TransportData } from '../../src/transport/types.ts';
import {
  STOMP_SUBPROTOCOLS,
  WebSocketTransport,
  type WebSocketConstructor,
} from '../../src/transport/websocket.ts';
import { text } from '../helpers.ts';

interface Endpoint {
  readonly messages: TransportData[];
  readonly closes: TransportCloseInfo[];
  readonly closed: Promise<TransportCloseInfo>;
}

function observe(transport: WebSocketTransport): Endpoint {
  const messages: TransportData[] = [];
  const closes: TransportCloseInfo[] = [];
  const { promise: closed, resolve } = Promise.withResolvers<TransportCloseInfo>();
  transport.attach({
    onMessage: (data) => messages.push(data),
    onClose: (info) => {
      closes.push(info);
      resolve(info);
    },
  });
  return { messages, closes, closed };
}

async function until(condition: () => boolean, timeout = 2000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('Timed out waiting for condition');
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

const implementations: [string, WebSocketConstructor<never>][] = [
  ['global WebSocket', globalThis.WebSocket],
  ['ws WebSocket', WsWebSocket],
];

describe('WebSocketTransport', () => {
  let server: WebSocketServer;
  let url: string;
  let next: ((transport: WebSocketTransport) => void) | undefined;

  before(async () => {
    server = new WebSocketServer({
      port: 0,
      host: '127.0.0.1',
      handleProtocols: (protocols) => (protocols.has('v12.stomp') ? 'v12.stomp' : false),
    });
    server.on('connection', (socket) => {
      const transport = new WebSocketTransport(socket);
      const handler = next;
      next = undefined;
      handler?.(transport);
    });
    await once(server, 'listening');
    url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  after(async () => {
    for (const client of server.clients) {
      client.terminate();
    }
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  /** Connects a client and returns both ends. */
  async function connect(WebSocket: WebSocketConstructor<never>) {
    const { promise: serverSide, resolve } = Promise.withResolvers<WebSocketTransport>();
    next = resolve;
    const client = await WebSocketTransport.connect(url, { WebSocket });
    return { client, server: await serverSide };
  }

  for (const [name, WebSocket] of implementations) {
    describe(`with the ${name} client`, () => {
      it('negotiates a STOMP subprotocol', async () => {
        const { client } = await connect(WebSocket);
        assert.equal(client.protocol, 'v12.stomp');
        client.close();
      });

      it('exchanges text and binary messages', async () => {
        const { client, server } = await connect(WebSocket);
        const clientSide = observe(client);
        const serverSide = observe(server);
        client.send('hello');
        client.send(Uint8Array.of(1, 2, 3));
        await until(() => serverSide.messages.length === 2);
        assert.deepEqual(serverSide.messages, ['hello', Uint8Array.of(1, 2, 3)]);
        server.send('world');
        server.send(Uint8Array.of(4, 5));
        await until(() => clientSide.messages.length === 2);
        assert.deepEqual(clientSide.messages, ['world', Uint8Array.of(4, 5)]);
        client.close();
        await clientSide.closed;
      });

      it('sends views over larger buffers correctly', async () => {
        const { client, server } = await connect(WebSocket);
        const serverSide = observe(server);
        client.send(Uint8Array.of(0, 1, 2, 3, 4).subarray(1, 3));
        await until(() => serverSide.messages.length === 1);
        assert.deepEqual(serverSide.messages, [Uint8Array.of(1, 2)]);
        client.close();
      });

      it('reports orderly closes initiated by the client', async () => {
        const { client, server } = await connect(WebSocket);
        const clientSide = observe(client);
        const serverSide = observe(server);
        client.close(1000, 'done');
        assert.throws(() => client.send('late'), TransportClosedError);
        const [clientInfo, serverInfo] = await Promise.all([clientSide.closed, serverSide.closed]);
        assert.equal(clientInfo.code, 1000);
        assert.equal(clientInfo.clean, true);
        assert.equal(serverInfo.code, 1000);
        assert.equal(serverInfo.reason, 'done');
      });

      it('reports closes initiated by the server, with code and reason', async () => {
        const { client, server } = await connect(WebSocket);
        const clientSide = observe(client);
        observe(server);
        server.close(4000, 'bye');
        const info = await clientSide.closed;
        assert.equal(info.code, 4000);
        assert.equal(info.reason, 'bye');
        assert.equal(info.clean, true);
        assert.equal(clientSide.closes.length, 1);
      });

      it('reports abrupt connection loss as unclean', async () => {
        const { client } = await connect(WebSocket);
        const clientSide = observe(client);
        for (const socket of server.clients) {
          socket.terminate();
        }
        const info = await clientSide.closed;
        assert.equal(info.clean, false);
      });

      it('rejects when the connection cannot be established', async () => {
        const closedPort = await new Promise<number>((resolve) => {
          const probe = createServer().listen(0, '127.0.0.1', () => {
            const { port } = probe.address() as AddressInfo;
            probe.close(() => resolve(port));
          });
        });
        await assert.rejects(
          WebSocketTransport.connect(`ws://127.0.0.1:${closedPort}`, { WebSocket }),
          TransportClosedError,
        );
      });

      it('can be aborted while connecting', async () => {
        // A TCP server that accepts connections but never completes the
        // WebSocket handshake.
        const sockets: Socket[] = [];
        const silent = createServer((socket) => sockets.push(socket)).listen(0, '127.0.0.1');
        await once(silent, 'listening');
        try {
          const controller = new AbortController();
          const pending = WebSocketTransport.connect(`ws://127.0.0.1:${(silent.address() as AddressInfo).port}`, {
            WebSocket,
            signal: controller.signal,
          });
          setTimeout(() => controller.abort(new Error('gave up')), 50);
          await assert.rejects(pending, /gave up/);
        } finally {
          for (const socket of sockets) {
            socket.destroy();
          }
          silent.close();
        }
      });

      it('carries STOMP frames between connections', async () => {
        const { client, server } = await connect(WebSocket);
        const received: IncomingFrame<'server'>[] = [];
        const serverConnection = new Connection({
          role: 'server',
          transport: server,
          onFrame: (frame) => received.push(frame),
          onClose: () => {},
        });
        const clientConnection = new Connection({
          role: 'client',
          transport: client,
          onFrame: () => {},
          onClose: () => {},
        });
        clientConnection.send({ command: 'SEND', headers: { destination: '/queue/a' }, body: 'payload' });
        clientConnection.send({ command: 'SEND', headers: { destination: '/queue/b' }, body: Uint8Array.of(0xff, 0) });
        await until(() => received.length === 2);
        assert.equal(received[0]?.headers['destination'], '/queue/a');
        assert.equal(text(received[0]!.body), 'payload');
        assert.deepEqual(received[1]?.body, Uint8Array.of(0xff, 0));
        clientConnection.close();
        serverConnection.close();
      });
    });
  }

  it('rejects immediately when the signal is already aborted', async () => {
    await assert.rejects(
      WebSocketTransport.connect(url, { WebSocket: WsWebSocket, signal: AbortSignal.abort(new Error('nope')) }),
      /nope/,
    );
  });

  it('requests the STOMP subprotocols by default', async () => {
    let requested: string | undefined;
    const { promise, resolve } = Promise.withResolvers<WebSocketTransport>();
    next = resolve;
    server.once('headers', (_headers, request) => {
      requested = request.headers['sec-websocket-protocol'];
    });
    const client = await WebSocketTransport.connect(url, { WebSocket: WsWebSocket });
    await promise;
    assert.deepEqual(requested?.split(/,\s*/), STOMP_SUBPROTOCOLS);
    client.close();
  });

  it('passes implementation-specific options to the constructor', async () => {
    let header: string | string[] | undefined;
    const { promise, resolve } = Promise.withResolvers<WebSocketTransport>();
    next = resolve;
    server.once('headers', (_headers, request) => {
      header = request.headers['x-custom'];
    });
    const client = await WebSocketTransport.connect(url, {
      WebSocket: WsWebSocket,
      webSocketOptions: { headers: { 'x-custom': 'yes' } },
    });
    await promise;
    assert.equal(header, 'yes');
    client.close();
  });

  it('refuses to wrap a socket that is not open', () => {
    const socket = new WsWebSocket(url);
    socket.on('error', () => {});
    assert.throws(() => new WebSocketTransport(socket), TransportClosedError);
    socket.terminate();
  });
});
