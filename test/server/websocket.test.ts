import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer, type IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, it } from 'node:test';
import { WebSocketServer, WebSocket as WsWebSocket } from 'ws';
import { Client } from '../../src/client/client.ts';
import { InMemoryBroker } from '../../src/server/memory.ts';
import { WebSocketBroker, selectStompSubprotocol } from '../../src/server/websocket.ts';
import type { WebSocketConstructor } from '../../src/transport/websocket.ts';
import { until } from '../client/fake-server.ts';

const implementations: [string, WebSocketConstructor<never> | undefined][] = [
  ['the global WebSocket', undefined],
  ['the ws WebSocket', WsWebSocket],
];

function urlOf(broker: WebSocketBroker): string {
  const address = broker.address() as AddressInfo;
  return `ws://127.0.0.1:${address.port}`;
}

describe('WebSocketBroker', () => {
  it('selects STOMP subprotocols', () => {
    assert.equal(selectStompSubprotocol(new Set(['v11.stomp', 'v12.stomp'])), 'v12.stomp');
    assert.equal(selectStompSubprotocol(new Set(['v10.stomp'])), 'v10.stomp');
    assert.equal(selectStompSubprotocol(new Set(['mqtt'])), false);
  });

  for (const [name, WebSocket] of implementations) {
    it(`serves clients using ${name}`, async () => {
      await using broker = await WebSocketBroker.listen({
        WebSocketServer,
        webSocketServerOptions: { port: 0, host: '127.0.0.1' },
      });
      assert.ok(broker.broker instanceof InMemoryBroker);
      const options = { url: urlOf(broker), reconnect: false as const, ...(WebSocket === undefined ? {} : { WebSocket }) };
      await using publisher = new Client(options);
      await using subscriber = new Client(options);
      await Promise.all([publisher.connect(), subscriber.connect()]);
      const received: string[] = [];
      const sub = subscriber.subscribe(
        '/topic/greetings',
        (message) => {
          received.push(message.text());
        },
        { receipt: true },
      );
      await sub.ready;
      await publisher.publish('/topic/greetings', 'hello over ws', { receipt: true });
      await until(() => received.length === 1);
      assert.deepEqual(received, ['hello over ws']);
      assert.equal(broker.server.sessions.length, 2);
    });
  }

  it('negotiates the STOMP subprotocol', async () => {
    await using broker = await WebSocketBroker.listen({
      WebSocketServer,
      webSocketServerOptions: { port: 0, host: '127.0.0.1' },
    });
    const socket = new WsWebSocket(urlOf(broker), ['v11.stomp', 'v12.stomp']);
    await once(socket, 'open');
    assert.equal(socket.protocol, 'v12.stomp');
    socket.close();
  });

  it('passes the upgrade request to hooks as context, and uses the given broker', async () => {
    const broker = new InMemoryBroker({ queuePrefixes: ['/q/'] });
    const contexts: unknown[] = [];
    await using wsBroker = await WebSocketBroker.listen({
      WebSocketServer,
      webSocketServerOptions: { port: 0, host: '127.0.0.1' },
      broker,
      authenticate: (request) => {
        contexts.push(request.context);
        const header = (request.context as IncomingMessage).headers['x-token'];
        if (header !== 'let-me-in') {
          throw new Error('Missing token');
        }
        return 'user';
      },
    });
    assert.equal(wsBroker.broker, broker);
    await using client = new Client({
      url: urlOf(wsBroker),
      WebSocket: WsWebSocket,
      webSocketOptions: { headers: { 'x-token': 'let-me-in' } },
      reconnect: false,
    });
    await client.connect();
    assert.equal(wsBroker.server.sessions[0]?.principal, 'user');
    assert.ok((contexts[0] as IncomingMessage).headers !== undefined);

    const refused = new Client({ url: urlOf(wsBroker), WebSocket: WsWebSocket, reconnect: false });
    await assert.rejects(refused.connect(), /Missing token/);
  });

  it('attaches to an existing HTTP server', async () => {
    const http = createServer();
    http.listen(0, '127.0.0.1');
    await once(http, 'listening');
    try {
      await using broker = await WebSocketBroker.listen({
        WebSocketServer,
        webSocketServerOptions: { server: http, path: '/stomp' },
      });
      const { port } = http.address() as AddressInfo;
      await using client = new Client({ url: `ws://127.0.0.1:${port}/stomp`, reconnect: false });
      await client.connect();
      assert.equal(broker.server.sessions.length, 1);
    } finally {
      http.close();
    }
  });

  it('closes client connections when closed', async () => {
    const broker = await WebSocketBroker.listen({
      WebSocketServer,
      webSocketServerOptions: { port: 0, host: '127.0.0.1' },
    });
    const url = urlOf(broker);
    const client = new Client({ url, reconnect: false });
    const disconnects: unknown[] = [];
    client.on('disconnect', (event) => disconnects.push(event));
    await client.connect();
    await broker.close();
    await until(() => client.state === 'closed');
    assert.equal(disconnects.length, 1);
    await assert.rejects(new Client({ url, reconnect: false }).connect());
  });

  it('rejects listen() when the port is taken', async () => {
    await using first = await WebSocketBroker.listen({
      WebSocketServer,
      webSocketServerOptions: { port: 0, host: '127.0.0.1' },
    });
    const { port } = first.address() as AddressInfo;
    await assert.rejects(
      WebSocketBroker.listen({ WebSocketServer, webSocketServerOptions: { port, host: '127.0.0.1' } }),
      /EADDRINUSE/,
    );
  });
});
