# typed-stomp — Architecture

A modern STOMP over WebSocket library, written in TypeScript, providing both a
**client** and a **server** for browser-side and server-side JavaScript
runtimes.

## Goals

- **Client and server** from the start, sharing a single protocol core.
- **Universal runtime support**: browsers, Node.js ≥ 24, Deno, Bun, workers.
  Only web-standard APIs are used in the core (`WebSocket`, `TextEncoder`,
  `TextDecoder`, `AbortSignal`, `Uint8Array`, `EventTarget`).
- **Zero runtime dependencies** in the core. Optional peer dependencies only
  for runtime-specific adapters (e.g. `ws` for Node.js servers).
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
  send(data: string | Uint8Array): void;
  close(code?: number, reason?: string): void;
  // events: open, message (string | Uint8Array), close, error
}
```

- Client: native `WebSocket` (available in all supported runtimes).
- Server: Node.js has no built-in WebSocket **server**, so the server accepts
  already-upgraded sockets through thin adapters: `ws` (optional peer
  dependency), Deno (`Deno.upgradeWebSocket`), Bun (`Bun.serve` websocket
  handlers). HTTP and upgrade handling stay with the host application.
- An in-memory transport pair lets client and server sessions talk to each
  other directly in tests.

### `connection/` — shared protocol core

Common to both sides: wiring a parser and serializer to a transport, choosing
text vs binary WebSocket messages, outgoing and incoming heart-beats (with
tolerance for background-tab timer throttling in browsers), version and
heart-beat negotiation helpers, receipt bookkeeping, and ERROR → close
semantics.

### `client/`

- **`ClientSession`**: the protocol state machine for a single connection
  (`connecting → connected → disconnecting → closed`). Receipts are Promises
  (`{ receipt: true }` on any frame; `DISCONNECT` always awaits one). `ERROR`
  frames become typed errors that reject pending operations.
- **`Client`**: reconnect policy on top of sessions — exponential backoff with
  jitter, an async `connectHeaders()` hook (e.g. for refreshing tokens),
  automatic resubscription, configurable offline behaviour (reject by default).
- **Subscriptions** are callback-based:

  ```ts
  const sub = client.subscribe('/queue/jobs', async (msg) => {
    await work(msg.json<Job>());
    await msg.ack();
  }, { ack: 'client-individual', concurrency: 1, buffer: { limit: 1000, overflow: 'error' } });

  sub.pause(); sub.resume(); sub.paused; await sub.unsubscribe();
  ```

  - If a callback returns a Promise, the dispatcher waits for it, keeping at
    most `concurrency` callbacks in flight.
  - While paused or saturated, messages accumulate in a bounded buffer;
    `overflow` selects `'error' | 'drop-oldest' | 'drop-newest'`.
  - Real broker-side flow control requires `client` ack modes plus broker
    prefetch (Spring's SimpleBroker does not support this; its broker relay
    and `InMemoryBroker` do).
  - Async iterators are deferred; they will wrap this dispatcher.

### `server/`

- **`StompServer`** accepts transports and creates one `ServerSession` per
  connection.
- **`ServerSession`** handles `CONNECT`/`STOMP` negotiation (version,
  heart-beats, `host`), validates every frame, manages receipts, normalises
  ack identifiers across versions, and forwards to the broker. Parsing is
  paused while the asynchronous `authenticate` hook runs, so pipelined frames
  are decoded with the negotiated version.
- **Hooks**: `authenticate(connectFrame) → principal | reject`, and frame
  interceptors for authorization or rewriting before routing.
- **`Broker` interface** (sketch):

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
    deliver(subscriptionId: string, message: OutboundMessage): void;
    close(error?: { message: string; body?: string | Uint8Array }): void;
  }
  ```

  `AckRef` is always `{ subscriptionId, messageId }`, whatever the version;
  the session maps 1.2 `ack` ids back to it. `deliver()` fills in the
  `subscription`, `message-id` and `ack` headers for the negotiated version.

- **Transactions**: if the broker implements `begin`/`commit`/`abort` they
  are used; otherwise the session buffers `SEND`/`ACK`/`NACK` between `BEGIN`
  and `COMMIT` and replays them on commit (not atomic).
- **`InMemoryBroker`** (`typed-stomp/server/memory`): topic fan-out and queue
  round-robin (configurable destination prefixes), all three ack modes,
  redelivery on `NACK` or disconnect, optional per-subscription prefetch, and
  native transactions. It is written purely against the public `Broker`
  interface.

## Packaging

- ESM only, `"type": "module"`, Node.js ≥ 24.
- Built with TypeScript 7 (`tsc`).
- Single package with subpath exports: `typed-stomp`, `typed-stomp/codec`,
  `typed-stomp/client`, `typed-stomp/server`, `typed-stomp/server/ws`,
  `typed-stomp/server/memory`.

## Testing

Build- and test-time dependencies are kept to a minimum: `typescript` and
`@types/node`, plus `ws` once the server adapters land.

- **Node's native test runner** (`node:test`, `node:assert`). Tests are
  written in TypeScript and run directly from source via Node's built-in type
  stripping, so sources use erasable syntax only (`erasableSyntaxOnly`) and
  relative imports carry `.ts` extensions, rewritten to `.js` by `tsc` at
  build time (`rewriteRelativeImportExtensions`).
- Unit tests: codec (including randomized split-point round-trips),
  validator, sessions over in-memory transports, and client ↔ server tests
  against `InMemoryBroker`.
- WebSocket tests run on Node only: the client uses Node's native
  `WebSocket`, the server uses the `ws` adapter.
- **Spring interop** uses the `docker` CLI directly (build, run, inspect the
  mapped port, stop) from a small test helper rather than a container
  library, with one Spring Boot fixture image (`test/fixtures/spring-broker/`):
  - our client against Spring's broker (`@EnableWebSocketMessageBroker`,
    SimpleBroker, raw WebSocket endpoint, a few `@MessageMapping` handlers);
  - our server against Spring's `WebSocketStompClient`, which runs a scripted
    scenario against our server on the host and reports pass/fail.

## Roadmap

1. Codec and frame validation. ← *current*
2. Transport interface, in-memory transport, shared connection core.
3. Client session, client and subscription dispatcher.
4. Server session, `StompServer`, `Broker` interface, `InMemoryBroker`.
5. Server adapters (`ws`, Deno, Bun).
6. Spring interop fixtures and tests.
7. Later: async-iterator subscriptions, raw TCP transport.
