import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import type { Client } from '../../src/client/client.ts';
import type { StompMessage } from '../../src/client/message.ts';
import type { SubscribeOptions } from '../../src/client/subscription.ts';
import { InMemoryBroker, type InMemoryBrokerOptions } from '../../src/server/memory.ts';
import { until } from '../client/fake-server.ts';
import { flush } from '../helpers.ts';
import { client as createClient, serve } from './helpers.ts';

async function setup(options: InMemoryBrokerOptions = {}) {
  const broker = new InMemoryBroker(options);
  const { server, transport } = serve({ broker });
  const clients: Client[] = [];
  const connect = async () => {
    const c = createClient(transport);
    clients.push(c);
    await c.connect();
    return c;
  };
  const close = () => Promise.all(clients.map((c) => c.close()));
  return { broker, server, connect, close };
}

/** Subscribes and collects messages; `onMessage` defaults to collecting only. */
async function collect(
  client: Client,
  destination: string,
  options: SubscribeOptions = {},
  onMessage: (message: StompMessage) => void | Promise<void> = () => {},
) {
  const messages: StompMessage[] = [];
  const subscription = client.subscribe(
    destination,
    (message) => {
      messages.push(message);
      return onMessage(message);
    },
    { receipt: true, ...options },
  );
  await subscription.ready;
  const texts = () => messages.map((message) => message.text());
  return { subscription, messages, texts };
}

describe('InMemoryBroker', () => {
  it('fans topic messages out to all subscribers', async () => {
    const { connect, close } = await setup();
    const publisher = await connect();
    const a = await collect(await connect(), '/topic/news');
    const b = await collect(await connect(), '/topic/news');
    const other = await collect(await connect(), '/topic/other');
    await publisher.publish('/topic/news', 'hello', { receipt: true });
    await until(() => a.messages.length === 1 && b.messages.length === 1);
    assert.deepEqual(a.texts(), ['hello']);
    assert.deepEqual(b.texts(), ['hello']);
    assert.deepEqual(other.texts(), []);
    await close();
  });

  it('drops topic messages nobody is subscribed to', async () => {
    const { broker, connect, close } = await setup();
    const publisher = await connect();
    await publisher.publish('/topic/news', 'lost', { receipt: true });
    const late = await collect(await connect(), '/topic/news');
    await flush();
    assert.deepEqual(late.texts(), []);
    assert.equal(broker.subscribers('/topic/news'), 1);
    await close();
  });

  it('keeps queue messages until a consumer arrives', async () => {
    const { broker, connect, close } = await setup();
    const publisher = await connect();
    await publisher.publish('/queue/jobs', '1', { receipt: true });
    await publisher.publish('/queue/jobs', '2', { receipt: true });
    assert.equal(broker.pending('/queue/jobs'), 2);
    const consumer = await collect(await connect(), '/queue/jobs');
    await until(() => consumer.messages.length === 2);
    assert.deepEqual(consumer.texts(), ['1', '2']);
    assert.equal(broker.pending('/queue/jobs'), 0);
    await close();
  });

  it('distributes queue messages round-robin', async () => {
    const { connect, close } = await setup();
    const publisher = await connect();
    const a = await collect(await connect(), '/queue/jobs');
    const b = await collect(await connect(), '/queue/jobs');
    for (const n of ['1', '2', '3', '4']) {
      await publisher.publish('/queue/jobs', n, { receipt: true });
    }
    await until(() => a.messages.length + b.messages.length === 4);
    assert.deepEqual(a.texts(), ['1', '3']);
    assert.deepEqual(b.texts(), ['2', '4']);
    await close();
  });

  it('supports custom queue prefixes', async () => {
    const { broker, close } = await setup({ queuePrefixes: ['/q/'] });
    assert.equal(broker.isQueue('/q/a'), true);
    assert.equal(broker.isQueue('/queue/a'), false);
    await close();
  });

  it('lets the application publish messages', async () => {
    const { broker, connect, close } = await setup();
    const sub = await collect(await connect(), '/topic/ticks');
    broker.send('/topic/ticks', '{"t":1}', { 'content-type': 'application/json' });
    await until(() => sub.messages.length === 1);
    assert.deepEqual(sub.messages[0]?.json(), { t: 1 });
    assert.equal(sub.messages[0]?.contentType, 'application/json');
    await close();
  });

  describe('acknowledgements', () => {
    it('redelivers nacked queue messages to the next consumer', async () => {
      const { broker, connect, close } = await setup();
      const a = await collect(await connect(), '/queue/jobs', { ack: 'client-individual' }, (message) => message.nack());
      const b = await collect(await connect(), '/queue/jobs', { ack: 'client-individual' }, (message) => message.ack());
      broker.send('/queue/jobs', 'job');
      await until(() => b.messages.length === 1);
      assert.deepEqual(a.texts(), ['job']);
      assert.equal(a.messages[0]?.headers['redelivered'], undefined);
      assert.equal(b.messages[0]?.headers['redelivered'], 'true');
      await close();
    });

    it('redelivers unacknowledged queue messages when the consumer disconnects', async () => {
      const { broker, connect, close } = await setup();
      const first = await connect();
      const a = await collect(first, '/queue/jobs', { ack: 'client' });
      broker.send('/queue/jobs', '1');
      broker.send('/queue/jobs', '2');
      await until(() => a.messages.length === 2);
      await first.close();
      assert.equal(broker.pending('/queue/jobs'), 2);
      const b = await collect(await connect(), '/queue/jobs', { ack: 'client' });
      await until(() => b.messages.length === 2);
      assert.deepEqual(b.texts(), ['1', '2']);
      await close();
    });

    it('redelivers unacknowledged queue messages on unsubscribe', async () => {
      const { broker, connect, close } = await setup();
      const c = await connect();
      const a = await collect(c, '/queue/jobs', { ack: 'client-individual' });
      broker.send('/queue/jobs', '1');
      await until(() => a.messages.length === 1);
      await a.subscription.unsubscribe({ receipt: true });
      assert.equal(broker.pending('/queue/jobs'), 1);
      await close();
    });

    it('limits unacknowledged messages with prefetch', async () => {
      const { broker, connect, close } = await setup({ prefetch: 2 });
      const a = await collect(await connect(), '/queue/jobs', { ack: 'client-individual' });
      for (const n of ['1', '2', '3']) {
        broker.send('/queue/jobs', n);
      }
      await until(() => a.messages.length === 2);
      await flush();
      assert.equal(a.messages.length, 2);
      assert.equal(broker.pending('/queue/jobs'), 1);
      await a.messages[0]!.ack();
      await until(() => a.messages.length === 3);
      assert.deepEqual(a.texts(), ['1', '2', '3']);
      await close();
    });

    it('honours the prefetch-count header, also for topics', async () => {
      const { broker, connect, close } = await setup();
      const a = await collect(await connect(), '/topic/t', { ack: 'client', headers: { 'prefetch-count': '1' } });
      broker.send('/topic/t', '1');
      broker.send('/topic/t', '2');
      await until(() => a.messages.length === 1);
      await flush();
      assert.equal(a.messages.length, 1);
      await a.messages[0]!.ack();
      await until(() => a.messages.length === 2);
      await close();
    });

    it('acknowledges cumulatively in client mode', async () => {
      const { broker, connect, close } = await setup({ prefetch: 3 });
      const a = await collect(await connect(), '/queue/jobs', { ack: 'client' });
      for (const n of ['1', '2', '3', '4']) {
        broker.send('/queue/jobs', n);
      }
      await until(() => a.messages.length === 3);
      // Acknowledging the third message frees all three slots.
      await a.messages[2]!.ack();
      await until(() => a.messages.length === 4);
      await close();
    });

    it('does not track auto-mode deliveries', async () => {
      const { broker, connect, close } = await setup({ prefetch: 1 });
      const a = await collect(await connect(), '/queue/jobs');
      for (const n of ['1', '2', '3']) {
        broker.send('/queue/jobs', n);
      }
      await until(() => a.messages.length === 3);
      await close();
    });
  });

  describe('transactions', () => {
    it('applies operations atomically on commit', async () => {
      const { connect, close } = await setup();
      const publisher = await connect();
      const a = await collect(await connect(), '/topic/t');
      await publisher.transaction(async (tx) => {
        await tx.send('/topic/t', '1', { receipt: true });
        await tx.send('/topic/t', '2', { receipt: true });
        assert.deepEqual(a.texts(), []);
      });
      await until(() => a.messages.length === 2);
      assert.deepEqual(a.texts(), ['1', '2']);
      await close();
    });

    it('discards operations on abort', async () => {
      const { connect, close } = await setup();
      const publisher = await connect();
      const a = await collect(await connect(), '/topic/t');
      await assert.rejects(
        publisher.transaction(async (tx) => {
          await tx.send('/topic/t', 'never', { receipt: true });
          throw new Error('rollback');
        }),
        /rollback/,
      );
      await publisher.publish('/topic/t', 'after', { receipt: true });
      await until(() => a.messages.length === 1);
      assert.deepEqual(a.texts(), ['after']);
      await close();
    });

    it('includes acknowledgements', async () => {
      const { broker, connect, close } = await setup({ prefetch: 1 });
      const c = await connect();
      const a = await collect(c, '/queue/jobs', { ack: 'client-individual' });
      broker.send('/queue/jobs', '1');
      broker.send('/queue/jobs', '2');
      await until(() => a.messages.length === 1);
      await c.transaction(async (tx) => {
        await tx.ack(a.messages[0]!, { receipt: true });
        await flush();
        // Not applied until commit: no capacity freed yet.
        assert.equal(a.messages.length, 1);
      });
      await until(() => a.messages.length === 2);
      await close();
    });
  });
});
