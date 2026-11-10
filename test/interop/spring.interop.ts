/**
 * Interoperability tests against Spring's STOMP implementation, running in a
 * Docker container (see test/fixtures/spring-broker). Run with
 * `npm run test:interop`; skipped when Docker is not available.
 */
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import type { AddressInfo } from 'node:net';
import { after, before, describe, it } from 'node:test';
import { WebSocketServer, WebSocket as WsWebSocket } from 'ws';
import { Client } from '../../src/client/client.ts';
import type { StompMessage } from '../../src/client/message.ts';
import { WebSocketBroker } from '../../src/server/websocket.ts';
import type { WebSocketConstructor } from '../../src/transport/websocket.ts';
import { until } from '../client/fake-server.ts';
import { buildImage, dockerAvailable, runDetached, runToCompletion, waitForHttp, type Container } from './docker.ts';

const IMAGE = 'typed-stomp-spring-interop';
const FIXTURE = fileURLToPath(new URL('../fixtures/spring-broker', import.meta.url));

const available = await dockerAvailable();
const skip = available ? false : 'Docker is not available';

const implementations: [string, WebSocketConstructor<never> | undefined][] = [
  ['the global WebSocket', undefined],
  ['the ws WebSocket', WsWebSocket],
];

/**
 * Spring's simple broker does not answer receipts for SUBSCRIBE, and handles
 * inbound frames on a thread pool, so a SEND right after a SUBSCRIBE can
 * overtake it. Give Spring a moment to register subscriptions.
 */
const subscriptionSettled = () => new Promise((resolve) => setTimeout(resolve, 200));

describe('Spring interoperability', { skip }, () => {
  // The first build downloads Maven dependencies and can take minutes.
  before(async () => {
    await buildImage(IMAGE, FIXTURE);
  }, { timeout: 15 * 60_000 });

  describe('typed-stomp client against the Spring broker', () => {
    let container: Container;
    let url: string;

    before(async () => {
      container = await runDetached(IMAGE, [8080]);
      const port = await container.port(8080);
      await waitForHttp(`http://127.0.0.1:${port}/`);
      url = `ws://127.0.0.1:${port}/ws`;
    });

    after(async () => {
      await container?.remove();
    });

    for (const [name, WebSocket] of implementations) {
      it(`connects and round-trips through an @MessageMapping with ${name}`, async () => {
        await using client = new Client({
          url,
          reconnect: false,
          heartbeat: { outgoing: 0, incoming: 0 },
          ...(WebSocket === undefined ? {} : { WebSocket }),
        });
        await client.connect();
        assert.equal(client.session?.version, '1.2');

        const received: string[] = [];
        const sub = client.subscribe('/topic/echo', (message) => {
          received.push(message.text());
        });
        await sub.ready;
        await subscriptionSettled();
        await client.publish('/app/echo', 'héllo, Spring');
        await until(() => received.length === 1);
        assert.deepEqual(received, ['héllo, Spring']);
      });
    }

    it('exchanges messages between two clients through the simple broker', async () => {
      await using a = new Client({ url, reconnect: false });
      await using b = new Client({ url, reconnect: false });
      await Promise.all([a.connect(), b.connect()]);
      const messages: StompMessage[] = [];
      const sub = b.subscribe('/queue/inbox', (message) => {
        messages.push(message);
      });
      await sub.ready;
      await subscriptionSettled();
      await a.publish('/queue/inbox', JSON.stringify({ n: 42 }), {
        contentType: 'application/json',
        headers: { custom: 'header' },
      });
      await until(() => messages.length === 1);
      const message = messages[0]!;
      assert.deepEqual(message.json(), { n: 42 });
      assert.match(message.contentType ?? '', /^application\/json/);
      assert.equal(message.headers['custom'], 'header');
      assert.equal(message.subscription, sub.id);
    });

    it('uses an application destination that transforms payloads', async () => {
      await using client = new Client({ url, reconnect: false });
      await client.connect();
      const received: string[] = [];
      const sub = client.subscribe('/topic/upper', (message) => {
        received.push(message.text());
      });
      await sub.ready;
      await subscriptionSettled();
      await client.publish('/app/upper', 'shout');
      await until(() => received.length === 1);
      assert.deepEqual(received, ['SHOUT']);
    });

    it('negotiates older protocol versions', async () => {
      await using client = new Client({ url, reconnect: false, versions: ['1.1'] });
      await client.connect();
      assert.equal(client.session?.version, '1.1');
    });

    it('disconnects gracefully', async () => {
      const client = new Client({ url, reconnect: false });
      await client.connect();
      const started = Date.now();
      await client.close();
      assert.equal(client.state, 'closed');
      // Spring answers DISCONNECT promptly; we should not hit the 5 s timeout.
      assert.ok(Date.now() - started < 4000, `close took ${Date.now() - started} ms`);
    });
  });

  describe("Spring's STOMP client against typed-stomp's server", () => {
    let broker: WebSocketBroker;
    let port: number;

    before(async () => {
      broker = await WebSocketBroker.listen({
        WebSocketServer,
        // The container reaches the host through host.docker.internal.
        webSocketServerOptions: { port: 0, host: '0.0.0.0' },
      });
      port = (broker.address() as AddressInfo).port;
    });

    after(async () => {
      await broker?.close();
    });

    it('connects, subscribes, sends with a receipt, receives and disconnects', async () => {
      // A typed-stomp client answers Spring's ping with a pong.
      await using node = new Client({ url: `ws://127.0.0.1:${port}`, reconnect: false });
      await node.connect();
      const sub = node.subscribe('/topic/to-node', async (message) => {
        await node.publish('/topic/to-spring', message.text() === 'ping' ? 'pong' : 'unexpected');
      });
      await sub.ready;

      const { code, output } = await runToCompletion(IMAGE, {
        MODE: 'client',
        TARGET_URL: `ws://host.docker.internal:${port}/`,
      });
      assert.equal(code, 0, output.split('\n').filter((line) => /INTEROP|CONNECTED|RECEIVED|RECEIPT|Exception/.test(line)).join('\n'));
      assert.match(output, /INTEROP OK/);
      await until(() => broker.server.sessions.length === 1);
    });
  });
});
