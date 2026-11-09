import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { after, before, describe, it } from 'node:test';
import { WebSocketServer, WebSocket as WsWebSocket } from 'ws';
import { Client } from '../../src/client/client.ts';
import { WebSocketTransport, type WebSocketConstructor } from '../../src/transport/websocket.ts';
import { FakeServer, until } from './fake-server.ts';

describe('Client over WebSocket', () => {
  let wss: WebSocketServer;
  let url: string;
  const server = new FakeServer();

  before(async () => {
    wss = new WebSocketServer({ port: 0, host: '127.0.0.1', handleProtocols: () => 'v12.stomp' });
    wss.on('connection', (socket) => server.accept(new WebSocketTransport(socket)));
    await once(wss, 'listening');
    url = `ws://127.0.0.1:${(wss.address() as AddressInfo).port}`;
  });

  after(async () => {
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  const implementations: [string, WebSocketConstructor<never> | undefined][] = [
    ['the global WebSocket', undefined],
    ['the ws WebSocket', WsWebSocket],
  ];

  for (const [name, WebSocket] of implementations) {
    it(`connects by url with ${name}, subscribes and publishes`, async () => {
      const client = new Client({ url, ...(WebSocket === undefined ? {} : { WebSocket }), reconnect: false });
      const received: string[] = [];
      const sub = client.subscribe('/topic/a', (message) => {
        received.push(message.text());
      });
      await client.connect();
      await sub.ready;
      const connection = server.last;
      await connection.waitFor('SUBSCRIBE');
      connection.message(sub.id, 'from server');
      await until(() => received.length === 1);
      await client.publish('/queue/b', 'from client', { receipt: true });
      const send = await connection.waitFor('SEND');
      assert.equal(new TextDecoder().decode(send.body), 'from client');
      assert.deepEqual(received, ['from server']);
      await client.close();
      await until(() => connection.closed);
    });
  }
});
