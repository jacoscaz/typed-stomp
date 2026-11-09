// Compile-time checks for the main README examples. Not executed.
import { WebSocketServer, WebSocket } from 'ws';
import {
  Client,
  ClientSession,
  StompServerError,
  type StompMessage,
} from '../../src/client/index.ts';
import { InMemoryBroker, StompServer, WebSocketBroker, type Broker } from '../../src/server/index.ts';
import { WebSocketTransport } from '../../src/transport/index.ts';

interface Job {
  readonly id: string;
}

declare const token: string;
declare const job: Job;
declare const message: StompMessage;
declare function process(job: Job): Promise<void>;
declare function getToken(): Promise<string>;
declare function isValid(login: string | undefined, passcode: string | undefined): boolean;
declare const wss: WebSocketServer;

export async function clientExamples(): Promise<void> {
  const client = new Client({
    url: 'wss://broker.example.com/ws',
    connectHeaders: { login: 'guest', passcode: 'guest' },
  });
  client.subscribe('/topic/news', (m) => {
    console.log(m.destination, m.text());
  });
  await client.connect();
  await client.publish('/topic/news', 'Hello!');
  await client.close();

  const withWs = new Client({
    url: 'wss://broker.example.com/ws',
    WebSocket,
    webSocketOptions: { headers: { authorization: `Bearer ${token}` } },
  });
  await withWs.publish('/queue/jobs', JSON.stringify(job), {
    contentType: 'application/json',
    headers: { priority: '9' },
    receipt: true,
  });

  const subscription = client.subscribe(
    '/queue/jobs',
    async (m) => {
      await process(m.json<Job>());
      await m.ack();
    },
    {
      ack: 'client-individual',
      concurrency: 1,
      buffer: { limit: 1000, overflow: 'error' },
      headers: { 'prefetch-count': '10' },
      onError: (error) => console.error(error),
    },
  );
  subscription.pause();
  subscription.resume();
  await subscription.idle();
  await subscription.unsubscribe();

  const reconnecting = new Client({
    url: 'wss://broker.example.com/ws',
    reconnect: {
      initialDelay: 1000,
      maxDelay: 30_000,
      factor: 2,
      jitter: 0.5,
      maxAttempts: Number.POSITIVE_INFINITY,
      shouldReconnect: (error) => !(error instanceof StompServerError),
    },
    connectHeaders: async () => ({ passcode: await getToken() }),
    heartbeat: { outgoing: 10_000, incoming: 10_000 },
    connectTimeout: 10_000,
    disconnectTimeout: 5_000,
  });
  reconnecting.on('connect', ({ version, server }) => console.log(version, server));
  reconnecting.on('disconnect', ({ error, willReconnect }) => console.log(error, willReconnect));
  reconnecting.on('reconnecting', ({ attempt, delay, error }) => console.log(attempt, delay, error));
  reconnecting.on('error', ({ error, subscription: sub, message: m }) => console.log(error, sub, m));
  reconnecting.on('close', ({ error }) => console.log(error));

  await client.transaction(async (tx) => {
    await tx.send('/queue/a', 'first');
    await tx.send('/queue/b', 'second');
    await tx.ack(message);
  });

  {
    await using scoped = new Client({ url: 'wss://broker.example.com/ws' });
    await scoped.connect();
  }

  const transport = await WebSocketTransport.connect('wss://broker.example.com/ws');
  const session = await ClientSession.connect({ transport, headers: { login: 'a', passcode: 'b' } });
  session.subscribe('/topic/news', (m) => console.log(m.text()));
  await session.send('/topic/news', 'Hello!', { receipt: true });
  await session.disconnect();
}

export async function serverExamples(): Promise<void> {
  const memory = new InMemoryBroker();
  const server = await WebSocketBroker.listen({
    WebSocketServer,
    webSocketServerOptions: { port: 61614 },
    broker: memory,
    authenticate: ({ login, passcode, context }) => {
      void context;
      if (!isValid(login, passcode)) {
        throw new Error('Bad credentials');
      }
      return { user: login };
    },
    authorize: (_session, frame) => {
      if (frame.command === 'SUBSCRIBE' && frame.headers.destination.startsWith('/admin/')) {
        throw new Error('Forbidden');
      }
    },
  });
  memory.send('/topic/ticks', JSON.stringify({ now: Date.now() }));
  await server.close();

  const broker: Broker = {
    subscribe() {},
    unsubscribe() {},
    publish() {},
    ack() {},
    nack() {},
    disconnect() {},
  };
  const stompServer = new StompServer({ broker });
  wss.on('connection', (socket, request) => stompServer.accept(new WebSocketTransport(socket), request));
}
