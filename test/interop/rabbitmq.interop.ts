/**
 * Interoperability tests against RabbitMQ's Web STOMP plugin, running in a
 * Docker container from the official image. Run with
 * `npm run test:interop`; skipped when Docker is not available.
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { WebSocket as WsWebSocket } from 'ws';
import { Client, type ClientOptions } from '../../src/client/client.ts';
import type { StompMessage } from '../../src/client/message.ts';
import type { WebSocketConstructor } from '../../src/transport/websocket.ts';
import { until } from '../client/fake-server.ts';
import { docker, dockerAvailable, retry, runDetached, type Container } from './docker.ts';

const IMAGE = 'rabbitmq:4';
const WEB_STOMP_PORT = 15674;

const available = await dockerAvailable();
const skip = available ? false : 'Docker is not available';

const implementations: [string, WebSocketConstructor<never> | undefined][] = [
  ['the global WebSocket', undefined],
  ['the ws WebSocket', WsWebSocket],
];

/** A queue name no other test uses. */
const uniqueQueue = () => `/queue/typed-stomp-${randomUUID()}`;

/** Waits briefly, to check that something does *not* happen. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 300));

describe('RabbitMQ interoperability', { skip }, () => {
  let container: Container;
  let url: string;

  const connect = async (options: Partial<ClientOptions> = {}): Promise<Client> => {
    const client = new Client({
      url,
      connectHeaders: { login: 'guest', passcode: 'guest' },
      reconnect: false,
      ...options,
    });
    await client.connect();
    return client;
  };

  /** Subscribes, waiting for RabbitMQ to confirm with a receipt. */
  const collect = async (
    client: Client,
    destination: string,
    options: Parameters<Client['subscribe']>[2] = {},
    onMessage: (message: StompMessage) => unknown = () => {},
  ) => {
    const messages: StompMessage[] = [];
    const subscription = client.subscribe(
      destination,
      async (message) => {
        messages.push(message);
        await onMessage(message);
      },
      { receipt: true, ...options },
    );
    await subscription.ready;
    return { subscription, messages, texts: () => messages.map((message) => message.text()) };
  };

  before(async () => {
    await docker(['pull', '--quiet', IMAGE], 10 * 60_000);
    container = await runDetached(IMAGE, [WEB_STOMP_PORT], [
      'sh',
      '-c',
      'rabbitmq-plugins enable --offline rabbitmq_web_stomp && exec docker-entrypoint.sh rabbitmq-server',
    ]);
    url = `ws://127.0.0.1:${await container.port(WEB_STOMP_PORT)}/ws`;
    // RabbitMQ takes a few seconds to start: retry until a session succeeds.
    await retry(async () => {
      const client = await connect();
      await client.close();
    });
    // The first run pulls the image.
  }, { timeout: 15 * 60_000 });

  after(async () => {
    await container?.remove();
  });

  for (const [name, WebSocket] of implementations) {
    it(`connects and publishes to a topic with receipts, using ${name}`, async () => {
      await using client = await connect(WebSocket === undefined ? {} : { WebSocket });
      assert.equal(client.session?.version, '1.2');
      assert.match(client.session?.server ?? '', /^RabbitMQ\//);
      const topic = `/topic/typed-stomp.${randomUUID()}`;
      const sub = await collect(client, topic);
      await client.publish(topic, 'héllo, RabbitMQ', { receipt: true, headers: { custom: 'header' } });
      await until(() => sub.messages.length === 1);
      assert.deepEqual(sub.texts(), ['héllo, RabbitMQ']);
      assert.equal(sub.messages[0]?.headers['custom'], 'header');
    });
  }

  it('keeps queue messages until a consumer subscribes', async () => {
    await using producer = await connect();
    await using consumer = await connect();
    const queue = uniqueQueue();
    // Declaring the queue requires a subscription; then the queue persists.
    await (await collect(consumer, queue)).subscription.unsubscribe({ receipt: true });
    await producer.publish(queue, JSON.stringify({ n: 1 }), { receipt: true, contentType: 'application/json' });
    await producer.publish(queue, JSON.stringify({ n: 2 }), { receipt: true, contentType: 'application/json' });
    const sub = await collect(consumer, queue);
    await until(() => sub.messages.length === 2);
    assert.deepEqual(
      sub.messages.map((message) => message.json()),
      [{ n: 1 }, { n: 2 }],
    );
    assert.equal(sub.messages[0]?.contentType, 'application/json');
  });

  it('holds back deliveries beyond prefetch-count until messages are acknowledged', async () => {
    await using client = await connect();
    const queue = uniqueQueue();
    const sub = await collect(client, queue, { ack: 'client-individual', headers: { 'prefetch-count': '1' } });
    await client.publish(queue, '1', { receipt: true });
    await client.publish(queue, '2', { receipt: true });
    await until(() => sub.messages.length === 1);
    await settle();
    assert.deepEqual(sub.texts(), ['1']);
    await sub.messages[0]!.ack({ receipt: true });
    await until(() => sub.messages.length === 2);
    assert.deepEqual(sub.texts(), ['1', '2']);
    await sub.messages[1]!.ack({ receipt: true });
  });

  it('redelivers nacked messages', async () => {
    await using client = await connect();
    const queue = uniqueQueue();
    const sub = await collect(client, queue, { ack: 'client-individual' }, (message) =>
      message.headers['redelivered'] === 'true' ? message.ack() : message.nack(),
    );
    await client.publish(queue, 'retry me', { receipt: true });
    await until(() => sub.messages.length === 2);
    assert.deepEqual(sub.texts(), ['retry me', 'retry me']);
    assert.deepEqual(
      sub.messages.map((message) => message.headers['redelivered']),
      ['false', 'true'],
    );
  });

  it('redelivers unacknowledged messages to another consumer when a session ends', async () => {
    const queue = uniqueQueue();
    const first = await connect();
    const a = await collect(first, queue, { ack: 'client' });
    await first.publish(queue, 'unacked', { receipt: true });
    await until(() => a.messages.length === 1);
    await first.close();
    await using second = await connect();
    const b = await collect(second, queue, { ack: 'client' });
    await until(() => b.messages.length === 1);
    assert.equal(b.messages[0]?.text(), 'unacked');
    assert.equal(b.messages[0]?.headers['redelivered'], 'true');
    await b.messages[0]!.ack({ receipt: true });
  });

  it('applies committed transactions and discards aborted ones', async () => {
    await using client = await connect();
    const queue = uniqueQueue();
    const sub = await collect(client, queue);
    // RabbitMQ only sends RECEIPTs for transactional frames once the
    // transaction commits, so awaiting them inside the transaction would
    // deadlock: send them without receipts.
    await client.transaction(async (tx) => {
      await tx.send(queue, 'committed 1');
      await tx.send(queue, 'committed 2');
      await settle();
      assert.deepEqual(sub.texts(), []);
    });
    await until(() => sub.messages.length === 2);
    await assert.rejects(
      client.transaction(async (tx) => {
        await tx.send(queue, 'aborted');
        throw new Error('roll back');
      }),
      /roll back/,
    );
    await client.publish(queue, 'after', { receipt: true });
    await until(() => sub.messages.length === 3);
    assert.deepEqual(sub.texts(), ['committed 1', 'committed 2', 'after']);
  });

  it('acknowledges with STOMP 1.1 headers', async () => {
    await using client = await connect({ versions: ['1.1'] });
    assert.equal(client.session?.version, '1.1');
    const queue = uniqueQueue();
    const sub = await collect(client, queue, { ack: 'client-individual', headers: { 'prefetch-count': '1' } });
    await client.publish(queue, '1', { receipt: true });
    await client.publish(queue, '2', { receipt: true });
    await until(() => sub.messages.length === 1);
    // In 1.1, ACK carries message-id and subscription instead of an ack id.
    await sub.messages[0]!.ack({ receipt: true });
    await until(() => sub.messages.length === 2);
    assert.deepEqual(sub.texts(), ['1', '2']);
  });

  it('negotiates heart-beats and stays connected', async () => {
    await using client = await connect({ heartbeat: { outgoing: 200, incoming: 200 } });
    const intervals = client.session?.heartbeat;
    assert.ok((intervals?.outgoing ?? 0) > 0, 'outgoing heart-beats negotiated');
    assert.ok((intervals?.incoming ?? 0) > 0, 'incoming heart-beats negotiated');
    await new Promise((resolve) => setTimeout(resolve, 1500));
    assert.equal(client.state, 'connected');
  });

  it('reports server errors', async () => {
    const client = new Client({
      url,
      connectHeaders: { login: 'guest', passcode: 'wrong' },
      reconnect: false,
    });
    await assert.rejects(client.connect(), (error: unknown) => {
      return error instanceof Error && error.name === 'StompServerError';
    });
  });

  it('disconnects gracefully', async () => {
    const client = await connect();
    const started = Date.now();
    await client.close();
    assert.equal(client.state, 'closed');
    assert.ok(Date.now() - started < 4000, `close took ${Date.now() - started} ms`);
  });
});
