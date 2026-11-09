# typed-stomp

A modern [STOMP](https://stomp.github.io/) over WebSocket library for
browser-side and server-side JavaScript runtimes, written in TypeScript.
It provides both a **client** and a **server**.

- **Client and server**, sharing one protocol core.
- **STOMP 1.2, 1.1 and 1.0**, with version and heart-beat negotiation.
- **Zero runtime dependencies.** Web-standard APIs only; WebSocket
  implementations such as [`ws`](https://github.com/websockets/ws) are passed
  in, never imported.
- **Typed frames and runtime frame validation** against the specification,
  with strict and lenient modes.
- **Reconnecting client** with exponential backoff, automatic
  resubscription and an optional offline queue.
- **Subscriptions with flow control**: async handlers, bounded concurrency,
  pause/resume and bounded buffers.
- **Pluggable brokers** on the server side, with a ready-to-run in-memory
  broker and WebSocket server.
- Tested for interoperability with **Spring**'s STOMP client and broker.

Payloads are opaque bytes: validating them is left to your schema library of
choice.

## Installation

```sh
npm install typed-stomp
```

Requires Node.js 24 or later, or any modern browser, Deno or Bun. The package
is ESM-only.

To run a server on Node.js, also install `ws` (Node.js has no built-in
WebSocket server):

```sh
npm install ws
```

## Client

```ts
import { Client } from 'typed-stomp/client';

const client = new Client({
  url: 'wss://broker.example.com/ws',
  connectHeaders: { login: 'guest', passcode: 'guest' },
});

client.subscribe('/topic/news', (message) => {
  console.log(message.destination, message.text());
});

await client.connect();
await client.publish('/topic/news', 'Hello!');
await client.close();
```

`Client` uses `globalThis.WebSocket` by default. To use another
implementation, such as `ws` with custom headers, pass its constructor:

```ts
import { WebSocket } from 'ws';

const client = new Client({
  url: 'wss://broker.example.com/ws',
  WebSocket,
  webSocketOptions: { headers: { authorization: `Bearer ${token}` } },
});
```

### Publishing

```ts
await client.publish('/queue/jobs', JSON.stringify(job), {
  contentType: 'application/json',
  headers: { priority: '9' },
  receipt: true, // resolve only once the broker confirms with a RECEIPT
});
```

Bodies can be strings or `Uint8Array`s. While disconnected, `publish` rejects
by default. With `offline: 'queue'`, messages are queued (up to
`offlineQueueLimit`) and sent on reconnection.

### Subscriptions

```ts
const subscription = client.subscribe(
  '/queue/jobs',
  async (message) => {
    await process(message.json<Job>()); // a cast, not validation
    await message.ack();
  },
  {
    ack: 'client-individual', // 'auto' (default), 'client' or 'client-individual'
    concurrency: 1, // handler calls in flight; 1 (default) keeps messages in order
    buffer: { limit: 1000, overflow: 'error' }, // or 'drop-oldest' / 'drop-newest'
    headers: { 'prefetch-count': '10' }, // broker-specific SUBSCRIBE headers
    onError: (error, message) => console.error(error),
  },
);

subscription.pause(); // stop calling the handler; messages are buffered
subscription.resume(); // handle buffered messages, then new ones
await subscription.idle(); // wait until nothing is buffered or in flight
await subscription.unsubscribe();
```

- Handlers may return promises. The subscription waits for them, keeping at
  most `concurrency` calls in flight.
- While paused or busy, messages are buffered. When the buffer is full, the
  `error` policy ends the subscription with a `StompBufferOverflowError`.
- Subscriptions survive reconnections: they are re-established, with the
  same id, every time the client reconnects.
- `message.ack()` and `message.nack()` send the right headers for the
  negotiated version. They are no-ops in `auto` mode.
- Handler errors go to `onError`, or to the client's `error` event.

### Reconnection and events

```ts
const client = new Client({
  url,
  reconnect: {
    initialDelay: 1000,
    maxDelay: 30_000,
    factor: 2,
    jitter: 0.5,
    maxAttempts: Infinity,
    shouldReconnect: (error, attempt) => !(error instanceof StompServerError),
  },
  // Called before every attempt, e.g. to refresh tokens.
  connectHeaders: async () => ({ passcode: await getToken() }),
});

client.on('connect', ({ version, server }) => {});
client.on('disconnect', ({ error, willReconnect }) => {});
client.on('reconnecting', ({ attempt, delay, error }) => {});
client.on('error', ({ error, subscription, message }) => {});
client.on('close', ({ error }) => {});
```

`connect()` resolves on the first successful connection; failed attempts are
retried. Pass `reconnect: false` to disable reconnection.

### Transactions

```ts
await client.transaction(async (tx) => {
  await tx.send('/queue/a', 'first');
  await tx.send('/queue/b', 'second');
  await tx.ack(message);
}); // committed if the function succeeds, aborted if it throws
```

### Heart-beats, timeouts and cleanup

```ts
const client = new Client({
  url,
  heartbeat: { outgoing: 10_000, incoming: 10_000 }, // the default
  connectTimeout: 10_000,
  disconnectTimeout: 5_000,
});

{
  await using scoped = new Client({ url });
  await scoped.connect();
} // closed here
```

### Single sessions

`Client` is built on `ClientSession`: one session over one transport, with
no reconnection. It is useful when you manage connections yourself:

```ts
import { ClientSession } from 'typed-stomp/client';
import { WebSocketTransport } from 'typed-stomp/transport';

const transport = await WebSocketTransport.connect('wss://broker.example.com/ws');
const session = await ClientSession.connect({ transport, headers: { login, passcode } });
session.subscribe('/topic/news', (message) => console.log(message.text()));
await session.send('/topic/news', 'Hello!', { receipt: true });
await session.disconnect();
```

## Server

### Ready-to-run WebSocket broker

```ts
import { WebSocketServer } from 'ws';
import { InMemoryBroker, WebSocketBroker } from 'typed-stomp/server';

const memory = new InMemoryBroker();
const server = await WebSocketBroker.listen({
  WebSocketServer,
  webSocketServerOptions: { port: 61614 }, // or { server: httpServer, path: '/stomp' }
  broker: memory, // optional: an InMemoryBroker is created by default
  authenticate: ({ login, passcode, context }) => {
    // `context` is the HTTP upgrade request.
    if (!isValid(login, passcode)) throw new Error('Bad credentials');
    return { user: login }; // becomes session.principal
  },
  authorize: (session, frame) => {
    if (frame.command === 'SUBSCRIBE' && frame.headers.destination.startsWith('/admin/')) {
      throw new Error('Forbidden');
    }
  },
});

// Publish from the application:
memory.send('/topic/ticks', JSON.stringify({ now: Date.now() }));

await server.close();
```

`WebSocketBroker` accepts any `Broker`.

### `InMemoryBroker`

A minimal single-process broker for development, tests and small
deployments. Nothing is persisted.

- **Topics** (the default) fan out each message to all current subscribers.
- **Queues** (destinations starting with `/queue/`, configurable with
  `queuePrefixes`) keep messages until a consumer is available, then
  distribute them round-robin.
- **Acks:** all three ack modes are supported.
- **Redelivery:** unacknowledged queue messages are redelivered (with a
  `redelivered: true` header) on `NACK`, unsubscribe or disconnect.
- **Prefetch:** limits unacknowledged messages per subscription. Set it with
  the `prefetch` option or a `prefetch-count` subscription header.
- **Transactions** are applied atomically on commit.

### Custom brokers

`StompServer` handles the protocol and delegates routing to a `Broker`. Its
methods may be synchronous or asynchronous. Throwing rejects the frame: the
client receives an `ERROR` frame and the session closes.

```ts
import { StompServer, type Broker } from 'typed-stomp/server';
import { WebSocketTransport } from 'typed-stomp/transport';

const broker: Broker = {
  subscribe(session, { id, destination, ack, headers }) {},
  unsubscribe(session, subscriptionId) {},
  publish(session, { destination, headers, body }, transaction) {
    // deliver to subscribers with:
    // otherSession.deliver(subscriptionId, { messageId, destination, headers, body });
  },
  ack(session, { subscriptionId, messageId }, transaction) {},
  nack(session, { subscriptionId, messageId }, transaction) {},
  disconnect(session) {},
  // Optional: connect, begin, commit, abort.
};

const server = new StompServer({ broker });
wss.on('connection', (socket, request) => server.accept(new WebSocketTransport(socket), request));
```

- **Ordering:** frames of a session are processed one at a time, in order.
- **Receipts** are sent once the broker is done with a frame.
- **Acks** reach the broker as `{ subscriptionId, messageId }`, whatever the
  STOMP version.
- **Transactions:** if the broker does not implement `begin`, `commit` and
  `abort`, the server buffers transactional frames and replays them on
  commit.

## Lower-level building blocks

- `typed-stomp/codec`: an incremental frame parser (with size limits) and a
  serializer, with header escaping for each version.
- `typed-stomp/frames`: frame types (`ClientFrame<'1.2'>`,
  `ServerFrame<V>`, ...) and validators (`validateClientFrame`,
  `validateServerFrame`).
- `typed-stomp/transport`: the `Transport` interface, `WebSocketTransport`
  (any `WebSocket`-compatible implementation) and in-memory transport pairs
  for tests.

Everything is also exported from `typed-stomp`.

## Interoperability notes

- **`host` header:** STOMP 1.1+ clients should send one. The client sends
  `host: /` by default (RabbitMQ's default virtual host; Spring and ActiveMQ
  ignore it). Set the `host` option to change it. The server accepts
  `CONNECT` frames without `host`, as sent by Spring's client and stomp.js.
- **Spring's simple broker** does not answer receipts for `SUBSCRIBE` and
  `SEND`; don't use `receipt: true` with it except for `DISCONNECT`.
- **Spring's simple broker** has no acknowledgement or prefetch support, so
  flow control with `client` ack modes requires a full broker (e.g.
  RabbitMQ, ActiveMQ, or Spring's broker relay to one).

## Development

```sh
npm run typecheck     # TypeScript 7, including tests
npm test              # unit and WebSocket tests (node:test, no Docker needed)
npm run test:interop  # Spring interoperability tests; requires Docker
npm run build         # emits dist/
```

The interop tests build a small Spring Boot application
(`test/fixtures/spring-broker`) into a Docker image. They test this
library's client against Spring's broker, and Spring's client against this
library's server.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

## License

MIT
