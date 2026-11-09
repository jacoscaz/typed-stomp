# typed-stomp — Architecture

A modern STOMP over WebSocket library, written in TypeScript, providing both a
**client** and a **server** for browser-side and server-side JavaScript
runtimes.

## Goals

- **Client and server** from the start, sharing a single protocol core.
- **Universal runtime support**: browsers, Node.js ≥ 24, Deno, Bun, workers.
  Only web-standard APIs are used in the core (`WebSocket`, `TextEncoder`,
  `TextDecoder`, `AbortSignal`, `Uint8Array`, `EventTarget`).
- **Zero runtime dependencies.** Implementation-specific `WebSocket` classes
  (e.g. from `ws`) are passed in by the application, not imported.
- **STOMP 1.2 first**, with negotiated fallback to 1.1 and 1.0.
- **Typed frames**: frames are a discriminated union over `command`, with
  per-command (and, where the spec differs, per-version) header shapes.
- **Runtime frame validation** against the STOMP specification, hand-written
  and driven by a spec table.

## Non-goals

- **Payload validation.** Bodies are opaque bytes. Convenience accessors
  (`text()`, `json<T>()`) exist, but `json<T>()` is a cast — validating
  payloads is left to consumers and their schema library of choice.
- **A production broker.** The server ships a pluggable `Broker` interface
  plus a minimal `InMemoryBroker` intended for development, testing and
  single-process use. Persistence, clustering, TTLs, dead-lettering and
  wildcard routing are out of scope.
- **SockJS** or other WebSocket emulation transports.

## Layers

```
┌───────────────────────────┐   ┌──────────────────────────────────────┐
│ client/                   │   │ server/                              │
│  Client (reconnect)       │   │  StompServer ── Broker (interface)   │
│  ClientSession            │   │  ServerSession   └ InMemoryBroker    │
│  Subscription dispatcher  │   │                                      │
└─────────────┬─────────────┘   └──────────────────┬───────────────────┘
              └──────────────┬─────────────────────┘
                 ┌───────────┴────────────┐
                 │ connection/            │  frame I/O, heart-beats,
                 │  Connection (shared)   │  negotiation helpers, teardown
                 └───────────┬────────────┘
        ┌────────────────────┼─────────────────────┐
┌───────┴────────┐  ┌────────┴────────┐  ┌─────────┴─────────┐
│ codec/         │  │ frames/         │  │ transport/        │
│ parser,        │  │ typed frames,   │  │ Transport iface,  │
│ serializer,    │  │ spec table,     │  │ WebSocket, ws,    │
│ escaping       │  │ validator       │  │ Deno, Bun, memory │
└────────────────┘  └─────────────────┘  └───────────────────┘
```

### `codec/` — frame parser and serializer

Pure and synchronous, with no I/O and no knowledge of sessions.

- **`FrameParser`** is incremental and byte-oriented. Input chunks may be
  `string`, `ArrayBuffer` or any `ArrayBufferView`; strings are UTF-8 encoded
  first so that `content-length` is always counted in bytes. It handles:
  - several frames in one chunk, and frames split across chunks;
  - heart-beat EOLs (`\n` and `\r\n`) between frames, reported via
    `onHeartbeat`;
  - `\r\n` line endings;
  - `content-length`-delimited bodies (which may contain NUL), and
    NUL-terminated bodies otherwise;
  - header unescaping according to the negotiated version (none in 1.0;
    `\n`, `\c`, `\\` in 1.1; plus `\r` in 1.2; never for `CONNECT` and
    `CONNECTED`). Undefined escape sequences are fatal, per spec;
  - repeated headers (first occurrence wins, per spec);
  - configurable limits (`maxHeaderSize`, `maxHeaders`, `maxBodySize`) to
    protect servers from hostile peers.

  Parsed headers live in null-prototype objects, so that header names such as
  `__proto__` cannot pollute prototypes.

  Frames are emitted through an `onFrame` callback rather than returned, so
  that the consumer can switch `parser.version` synchronously when it sees
  `CONNECT`/`CONNECTED`, before the next frame in the same chunk is decoded.

- **`serializeFrame`** produces a `Uint8Array`; **`serializeFrameToString`**
  produces a string (for WebSocket text messages) and throws if the body is
  not valid UTF-8. Both escape headers for the target version, add
  `content-length` automatically for non-empty bodies, and reject frames that
  cannot be represented (e.g. a newline in a header value in 1.0).

### `frames/` — typed frames and validation

- `ClientFrame<V>` and `ServerFrame<V>` are discriminated unions over
  `command`. `V` is the negotiated `StompVersion`; it drives header shapes
  that differ between versions (e.g. `ACK` uses `id` in 1.2 but
  `message-id` + `subscription` in 1.1, and `NACK` does not exist in 1.0).
- `validateClientFrame()` / `validateServerFrame()` check a raw frame against
  a spec table (version × command):
  - known command, sent in the right direction, available in the version;
  - required headers per version;
  - header value formats (`ack` modes, `heart-beat`, `accept-version`,
    `content-length` matching the body);
  - bodies only on `SEND`, `MESSAGE` and `ERROR`.

  Each rule is either always an error, or an error only in **strict** mode
  and a warning in **lenient** mode. Defaults: lenient on the client (to
  tolerate broker quirks), strict on the server. Results are returned as
  values (`{ valid, frame, issues }`); `assertValid()` throws a
  `StompValidationError`.

  Negotiation frames (`CONNECT`, `STOMP`, `CONNECTED`) are validated by their
  own rules, independently of the version passed in.

### `transport/` — the Transport interface

```ts
interface Transport {
  readonly closed: boolean;
  attach(handlers: { onMessage(data: string | Uint8Array): void; onClose(info: TransportCloseInfo): void }): void;
  send(data: string | Uint8Array): void;
  close(code?: number, reason?: string): void;
}
```

A transport is **open when constructed**; establishing the connection happens
before. Events that occur before `attach()` are buffered, `onClose` fires
exactly once, and `send()` throws once closed. `TransportBase` implements this
bookkeeping for concrete transports.

- **`WebSocketTransport`** wraps any object matching `WebSocketLike`, the
  subset of the standard `WebSocket` API we use. This covers browser,
  Node.js, Deno and Bun `WebSocket`s **and** the `WebSocket` class of the
  `ws` package, on both the client and the server side (`ws` server sockets
  can be wrapped directly).
  - `WebSocketTransport.connect(url, { WebSocket, webSocketOptions, protocols,
    signal })` instantiates the given constructor (default:
    `globalThis.WebSocket`), passing `webSocketOptions` as the third,
    implementation-specific argument (e.g. `ws` client options such as custom
    headers), and resolves once the socket is open. STOMP subprotocols
    (`v12.stomp`, `v11.stomp`, `v10.stomp`) are requested by default.
  - `WebSocketTransport.open(socket)` waits for an existing socket to open
    (e.g. Deno's `upgradeWebSocket`).
- Server: Node.js has no built-in WebSocket **server**. HTTP and upgrade
  handling stay with the host application, which hands open sockets to the
  server. `ws` sockets need no adapter; Bun's `ServerWebSocket` (handler-based)
  would need a thin adapter (deferred).
- **`MemoryTransport.pair()`** creates two connected in-memory transports
  (asynchronous, ordered delivery; binary data copied; `terminate()` simulates
  connection loss) for tests.

### `connection/` — shared protocol core

**`Connection`** (internal, not part of the public exports yet) is common to
both sides:

- wires a `FrameParser` and the serializer to a transport;
- validates incoming frames for its role (lenient by default for clients,
  strict for servers) and hands typed frames plus warnings to `onFrame`;
- chooses text or binary WebSocket messages (`messageType: 'auto' | 'text' |
  'binary'`; `auto` uses text unless the body is not valid UTF-8);
- `pause()`/`resume()` hold back incoming frames (including ones already
  received in the same transport message), so a server can authenticate
  asynchronously and switch `version` before pipelined frames are decoded;
- drives heart-beats with one timer per direction, re-armed from last-activity
  timestamps, so sending and receiving cost nothing extra. The incoming check
  allows `heartbeatTolerance` (default 2) times the negotiated interval;
- turns malformed or invalid incoming data into a `StompProtocolError`. A
  server replies with an `ERROR` frame (with `receipt-id` when available, and
  the validation issues in the body), then the connection fails;
- `close()` is orderly (waits for the transport); `fail(error, finalFrame?)`
  calls `onClose` immediately, without waiting for a closing handshake that
  may never complete with an unresponsive peer.

Helpers: `negotiateVersion`, `parseAcceptVersion`, `parseHeartBeat`,
`formatHeartBeat`, `negotiateHeartBeat` (symmetric, used by both sides), and
`ReceiptTracker` (receipt promises that never cause unhandled rejections).

### `client/`

- **`ClientSession`**: a single session over a single transport, with no
  reconnection (`connecting → connected → disconnecting → closed`).
  - `ClientSession.connect({ transport, headers, host, versions, heartbeat,
    connectTimeout, signal, ... })` sends `CONNECT` (or `STOMP`) and resolves
    on `CONNECTED`, having negotiated the version (it must be one we offered)
    and heart-beats. `host` defaults to `/`, the default virtual host of
    brokers that use it. An `ERROR` reply rejects with `StompServerError`.
  - `send`, `subscribe`, `begin` (transactions), message `ack`/`nack`.
    `{ receipt: true }` on any of them makes the returned promise wait for
    the `RECEIPT`. Session subscription handlers are synchronous.
  - Incoming `MESSAGE` frames are routed by `subscription` header (by
    destination for 1.0 servers that omit it); messages for unknown
    subscriptions are ignored.
  - An `ERROR` frame after connecting rejects the matching receipt, if any,
    and fails the session; all pending receipts are rejected on close.
  - `disconnect({ timeout })` sends `DISCONNECT`, waits for its receipt (or
    the timeout), then closes. `abort()` closes immediately. `closed`
    resolves with the close info.
- **`StompMessage`**: headers, body, `text()`, `json<T>()` (a cast, not
  validation), and `ack()`/`nack()` with the right headers for the version
  (`id` from the `ack` header in 1.2; `message-id` + `subscription` in 1.1;
  `message-id` in 1.0, where `NACK` is rejected). Acknowledging is a no-op in
  `auto` mode, only the first call counts, and acknowledging a message from
  a session that has ended rejects with `StompNotConnectedError`.
- **`Client`**: keeps a session alive on top of `ClientSession`.
  - Connects via `url` (+ optional `WebSocket` constructor and
    `webSocketOptions`) or a custom `transport` factory.
  - `connect()` resolves on the first successful session; failed attempts are
    retried per the reconnection policy. It rejects if the client gives up
    or is closed first.
  - Reconnection: exponential backoff (`initialDelay` 1 s, `factor` 2,
    `maxDelay` 30 s, `jitter` 0.5 as a random reduction of up to 50%),
    `maxAttempts` (consecutive failures) and a `shouldReconnect(error,
    attempt)` hook. `reconnect: false` disables it. `connectHeaders` may be a
    function, called before every attempt (e.g. to refresh tokens).
  - `publish()` rejects while offline, or queues (bounded) with
    `offline: 'queue'`. `transaction(fn)` commits if `fn` succeeds and
    aborts otherwise. `close()` disconnects gracefully and is idempotent;
    `await using` is supported.
  - Events (`client.on(...)`): `connect`, `disconnect` (with `willReconnect`),
    `reconnecting` (with `attempt` and `delay`), `warning` (validation
    warnings), `error` (subscription errors; reported as uncaught if nobody
    listens), `close`.
- **`Subscription`** (from `client.subscribe`) survives reconnections: it is
  re-established with the same id on every new session.

  ```ts
  const sub = client.subscribe('/queue/jobs', async (msg) => {
    await work(msg.json<Job>());
    await msg.ack();
  }, { ack: 'client-individual', concurrency: 1, buffer: { limit: 1000, overflow: 'error' } });

  sub.pause(); sub.resume(); sub.paused; sub.buffered; await sub.idle(); await sub.unsubscribe();
  ```

  - Messages go through a **`Dispatcher`**: if the handler returns a
    promise, the dispatcher waits for it, keeping at most `concurrency`
    (default 1, i.e. strictly in order) calls in flight.
  - While paused or saturated, messages accumulate in a bounded buffer
    (default 10000). `overflow`: `error` (default) ends the subscription and
    reports a `StompBufferOverflowError`; `drop-oldest` and `drop-newest`
    discard messages.
  - Handler errors go to the subscription's `onError`, or the client's
    `error` event; the subscription continues.
  - When a session ends, buffered messages received in a `client` ack mode
    are discarded (they can no longer be acknowledged, and the broker will
    redeliver them); `auto` mode messages are still handled.
  - Real broker-side flow control requires `client` ack modes plus broker
    prefetch (Spring's SimpleBroker does not support this; its broker relay
    and `InMemoryBroker` do).
  - Async iterators are deferred; they will wrap this dispatcher.

### `server/`

- **`ServerSession`**: the server side of one session; implements
  `BrokerSession`, the broker's view of it.
  - Handshake: the first frame must be `CONNECT`/`STOMP` (within
    `connectTimeout`, default 10 s). The highest common version is picked;
    with none, the `ERROR` frame lists the supported versions in a `version`
    header. The `authenticate(request)` hook returns a principal or throws to
    refuse; then `broker.connect` runs, `CONNECTED` is sent (`version`,
    `session`, `server`, `heart-beat`) and heart-beats start.
  - Frames are processed **one at a time, in order**: incoming frames are held
    back (via `Connection.pause()`) while the broker handles the current one,
    which also means frames pipelined behind `CONNECT` are decoded with the
    negotiated version, and slow brokers apply backpressure.
  - The `authorize(session, frame)` hook runs before every frame after the
    handshake (except `DISCONNECT`); throwing refuses it.
  - `RECEIPT`s are sent once the broker has processed the frame. Any failure
    (protocol error, hook or broker error) is answered with an `ERROR` frame
    carrying the message and the `receipt-id`, and the session closes.
  - `deliver()` fills in `message-id`, `subscription` (omitted for 1.0
    subscriptions without an id) and, for client ack modes in 1.2, a
    generated `ack` id. Pending acknowledgements are tracked so that
    `ACK`/`NACK` frames are mapped back to `{ subscriptionId, messageId }`
    whatever the version, unknown ones are rejected, and `client` mode
    settles earlier messages cumulatively.
  - On close, open native transactions are aborted and `broker.disconnect`
    is called (only for sessions the broker saw connect).
- **`StompServer`**: accepts open transports (`accept(transport,
  context?)`), runs a `ServerSession` for each, and emits `connect`,
  `disconnect` and `error` events. It does not listen on any port. `close()`
  closes all sessions and waits for the broker.
- **`Broker` interface**:

  ```ts
  interface Broker {
    connect?(session: BrokerSession): void | Promise<void>;
    disconnect(session: BrokerSession): void | Promise<void>;
    subscribe(session: BrokerSession, sub: SubscriptionInfo): void | Promise<void>;
    unsubscribe(session: BrokerSession, subscriptionId: string): void | Promise<void>;
    publish(session: BrokerSession, message: InboundMessage, tx?: string): void | Promise<void>;
    ack(session: BrokerSession, ref: AckRef, tx?: string): void | Promise<void>;
    nack(session: BrokerSession, ref: AckRef, tx?: string): void | Promise<void>;
    begin?(session: BrokerSession, tx: string): void | Promise<void>;
    commit?(session: BrokerSession, tx: string): void | Promise<void>;
    abort?(session: BrokerSession, tx: string): void | Promise<void>;
  }

  interface BrokerSession {
    readonly id: string;
    readonly version: StompVersion;
    readonly principal: unknown;
    readonly host: string | undefined;
    readonly context: unknown;
    readonly active: boolean;
    deliver(subscriptionId: string, message: OutboundMessage): boolean;
    close(error?: { message: string; headers?: FrameHeaders; body?: string | Uint8Array }): void;
  }
  ```

  `AckRef` is always `{ subscriptionId, messageId }`, whatever the version.
  Throwing from a broker method rejects the frame being processed.

- **Transactions**: if the broker implements `begin`/`commit`/`abort` they
  are used; otherwise the session buffers `SEND`/`ACK`/`NACK` between `BEGIN`
  and `COMMIT` and replays them on commit (not atomic). Either way, the
  session rejects unknown and duplicate transaction ids.
- **`InMemoryBroker`**: written purely against the public `Broker`
  interface.
  - Topics (default) fan out to current subscribers; queues (destinations
    with a queue prefix, `/queue/` by default) keep messages until a
    consumer is available and distribute them round-robin.
  - All three ack modes; `client` mode settles cumulatively.
  - Unacknowledged queue messages are redelivered (with `redelivered: true`)
    on `NACK`, unsubscribe or disconnect; unacknowledged topic messages are
    dropped in those cases.
  - Prefetch (broker default, or a `prefetch-count` subscription header)
    limits unacknowledged messages per subscription; topic messages beyond
    it wait in a per-subscription backlog.
  - Native transactions, applied atomically on commit.
  - `send(destination, body, headers)` publishes from the application;
    `pending()` and `subscribers()` help with introspection.
- **`WebSocketBroker`**: a ready-to-run server: a WebSocket server wired to a
  `StompServer` and a broker (`InMemoryBroker` by default). The
  `WebSocketServer` class is injected (`WebSocketBroker.listen({
  WebSocketServer, webSocketServerOptions: { port } })`, or `{ server,
  path }` to share an HTTP server), so the library never imports `ws`.
  `handleProtocols` selects a STOMP subprotocol by default, and each
  connection's upgrade request becomes `session.context`.

## Packaging

- ESM only, `"type": "module"`, Node.js ≥ 24.
- Built with TypeScript 7 (`tsc`).
- Single package with subpath exports: `typed-stomp`, `typed-stomp/codec`,
  `typed-stomp/frames`, `typed-stomp/transport`, `typed-stomp/client` and
  `typed-stomp/server` (including `InMemoryBroker` and `WebSocketBroker`).

## Testing

Build- and test-time dependencies are kept to a minimum: `typescript` and
`@types/node`, `ws` and `@types/ws`.

- **Node's native test runner** (`node:test`, `node:assert`). Tests are
  written in TypeScript and run directly from source via Node's built-in type
  stripping, so sources use erasable syntax only (`erasableSyntaxOnly`) and
  relative imports carry `.ts` extensions, rewritten to `.js` by `tsc` at
  build time (`rewriteRelativeImportExtensions`).
- Unit tests: codec (including randomized split-point round-trips),
  validator, sessions over in-memory transports, and client ↔ server tests
  against `InMemoryBroker`.
- WebSocket tests run on Node only, against a `ws` server, with both Node's
  native `WebSocket` and `ws`'s `WebSocket` as clients.
- Compile-only checks in `test/types/` cover WebSocket implementations'
  assignability and the README examples.
- **Spring interop** (`npm run test:interop`, `test/interop/*.interop.ts`,
  kept out of `npm test`; skipped without Docker) uses the `docker` CLI
  directly (build, run, inspect the mapped port, remove) from a small helper
  rather than a container library, with one Spring Boot 4 fixture image
  (`test/fixtures/spring-broker/`):
  - our client against Spring's broker (`@EnableWebSocketMessageBroker`,
    simple broker, raw WebSocket endpoint at `/ws`, `@MessageMapping`
    handlers at `/app/echo` and `/app/upper`), with both `WebSocket`
    implementations, versions 1.2 and 1.1;
  - our server (`WebSocketBroker`) against Spring's `WebSocketStompClient`
    (`MODE=client`), which connects with heart-beats, subscribes, sends with
    a receipt, expects a reply from a typed-stomp client, disconnects, and
    reports the result through its exit code.
  - Findings so far: Spring's client omits the `host` header (now only a
    validation warning), and Spring's simple broker does not answer receipts
    for `SUBSCRIBE` and `SEND`.

## Roadmap

1. Codec and frame validation. ✓
2. Transport interface, in-memory transport, shared connection core. ✓
3. Client session, client and subscription dispatcher. ✓
4. Server session, `StompServer`, `Broker` interface, `InMemoryBroker`,
   `WebSocketBroker`. ✓
5. Spring interop fixtures and tests. ✓
6. Later: server adapters for runtimes whose sockets are not `WebSocketLike`
   (Bun), async-iterator subscriptions, raw TCP transport.
