import { STOMP_SUBPROTOCOLS, WebSocketTransport, type WebSocketLike } from '../transport/websocket.ts';
import type { Broker } from './broker.ts';
import { InMemoryBroker } from './memory.ts';
import { StompServer, type StompServerOptions } from './server.ts';

/**
 * The subset of the `WebSocketServer` API of the `ws` package used here.
 */
export interface WebSocketServerLike {
  // Loose listener types, so that implementations with stricter, overloaded
  // signatures remain assignable.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, listener: (...args: any[]) => void): unknown;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  off(event: string, listener: (...args: any[]) => void): unknown;
  close(callback?: (error?: Error) => void): void;
  address(): { readonly port: number; readonly address: string; readonly family: string } | string | null;
}

/**
 * A `WebSocketServer` constructor, such as the one of the `ws` package. `O`
 * is the type of its options (e.g. `{ port }` or `{ server, path }`).
 */
export type WebSocketServerConstructor<O> = new (options: O) => WebSocketServerLike;

export interface WebSocketBrokerOptions<O extends object> extends Omit<StompServerOptions, 'broker'> {
  /** The `WebSocketServer` class, e.g. from the `ws` package. */
  readonly WebSocketServer: WebSocketServerConstructor<O>;
  /**
   * Options for the WebSocket server, e.g. `{ port: 61614 }` or
   * `{ server: httpServer, path: '/stomp' }`. Unless overridden here,
   * `handleProtocols` selects a STOMP subprotocol when the client offers
   * one.
   */
  readonly webSocketServerOptions: O;
  /** Defaults to a new {@link InMemoryBroker}. */
  readonly broker?: Broker;
}

/**
 * Picks the preferred STOMP subprotocol offered by a client, or none. Use
 * as the `handleProtocols` option of a `ws` server.
 */
export function selectStompSubprotocol(protocols: ReadonlySet<string>): string | false {
  return STOMP_SUBPROTOCOLS.find((protocol) => protocols.has(protocol)) ?? false;
}

/**
 * A ready-to-run STOMP over WebSocket server: a WebSocket server (typically
 * from the `ws` package, which this library does not depend on: pass its
 * class in) wired to a {@link StompServer} and a broker, an
 * {@link InMemoryBroker} by default.
 *
 * ```ts
 * import { WebSocketServer } from 'ws';
 *
 * const broker = await WebSocketBroker.listen({ WebSocketServer, webSocketServerOptions: { port: 61614 } });
 * // ...
 * await broker.close();
 * ```
 *
 * Each connection's HTTP upgrade request is passed to hooks and the broker
 * as `session.context`.
 */
export class WebSocketBroker<O extends object = object> {
  readonly broker: Broker;
  readonly server: StompServer;
  readonly webSocketServer: WebSocketServerLike;
  #closing: Promise<void> | undefined;

  constructor(options: WebSocketBrokerOptions<O>) {
    const { WebSocketServer, webSocketServerOptions, broker, ...serverOptions } = options;
    this.broker = broker ?? new InMemoryBroker();
    this.server = new StompServer({ ...serverOptions, broker: this.broker });
    this.webSocketServer = new WebSocketServer({
      handleProtocols: selectStompSubprotocol,
      ...webSocketServerOptions,
    });
    this.webSocketServer.on('connection', (socket: WebSocketLike, request: unknown) => {
      try {
        this.server.accept(new WebSocketTransport(socket), request);
      } catch {
        socket.close(1001, 'Server shutting down');
      }
    });
  }

  /**
   * Creates a broker and waits until its WebSocket server is listening (or
   * resolves right away when attached to an HTTP server that already is).
   */
  static async listen<O extends object>(options: WebSocketBrokerOptions<O>): Promise<WebSocketBroker<O>> {
    const broker = new WebSocketBroker(options);
    await broker.#listening();
    return broker;
  }

  /** The address the WebSocket server is bound to, if any. */
  address(): ReturnType<WebSocketServerLike['address']> {
    return this.webSocketServer.address();
  }

  /** Closes all sessions, then the WebSocket server. Idempotent. */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(): Promise<void> {
    await this.server.close();
    await new Promise<void>((resolve, reject) => {
      this.webSocketServer.close((error) => (error === undefined ? resolve() : reject(error)));
    });
  }

  #listening(): Promise<void> {
    const wss = this.webSocketServer;
    if (wss.address() !== null) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const onListening = () => {
        wss.off('error', onError);
        resolve();
      };
      const onError = (error: unknown) => {
        wss.off('listening', onListening);
        reject(error);
      };
      wss.on('listening', onListening);
      wss.on('error', onError);
    });
  }
}
