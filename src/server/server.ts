import type { ConnectionCloseInfo } from '../connection/connection.ts';
import { StompConnectionClosedError } from '../connection/errors.ts';
import type { Transport } from '../transport/types.ts';
import { Emitter, reportError } from '../util/emitter.ts';
import type { Broker } from './broker.ts';
import { ServerSession, type ServerSessionOptions } from './session.ts';

export type StompServerOptions = Omit<
  ServerSessionOptions,
  'transport' | 'id' | 'context' | 'onConnect' | 'onClose' | 'onError'
>;

export interface StompServerEvents {
  /** A session completed its handshake. */
  connect: { readonly session: ServerSession };
  /** A session has closed. */
  disconnect: { readonly session: ServerSession; readonly error: unknown };
  /**
   * Errors that cannot be reported to a client, e.g. failures of
   * `broker.disconnect`. If there are no listeners, they are reported as
   * uncaught.
   */
  error: { readonly error: unknown; readonly session?: ServerSession };
}

/**
 * A STOMP server: accepts transports (typically server-side WebSockets),
 * runs a {@link ServerSession} for each, and routes through a
 * {@link Broker}. It does not listen on any port itself; see
 * `WebSocketBroker` for a ready-to-run server.
 *
 * ```ts
 * const server = new StompServer({ broker: new InMemoryBroker() });
 * wss.on('connection', (socket, request) => server.accept(new WebSocketTransport(socket), request));
 * ```
 */
export class StompServer {
  readonly broker: Broker;
  readonly #options: StompServerOptions;
  readonly #events = new Emitter<StompServerEvents>();
  readonly #sessions = new Set<ServerSession>();
  #closing: Promise<void> | undefined;

  constructor(options: StompServerOptions) {
    this.#options = options;
    this.broker = options.broker;
  }

  /** All open sessions, including those still in their handshake. */
  get sessions(): readonly ServerSession[] {
    return [...this.#sessions];
  }

  get closed(): boolean {
    return this.#closing !== undefined;
  }

  on<K extends keyof StompServerEvents>(type: K, listener: (event: StompServerEvents[K]) => void): () => void {
    return this.#events.on(type, listener);
  }

  once<K extends keyof StompServerEvents>(type: K, listener: (event: StompServerEvents[K]) => void): () => void {
    return this.#events.once(type, listener);
  }

  off<K extends keyof StompServerEvents>(type: K, listener: (event: StompServerEvents[K]) => void): void {
    this.#events.off(type, listener);
  }

  /**
   * Serves a client over an open transport. `context` is exposed to hooks
   * and the broker as `session.context` (e.g. the HTTP upgrade request).
   */
  accept(transport: Transport, context?: unknown): ServerSession {
    if (this.#closing !== undefined) {
      transport.close(1001, 'Server shutting down');
      throw new StompConnectionClosedError('Server is closed');
    }
    const session = new ServerSession({
      ...this.#options,
      transport,
      context,
      onConnect: (s) => this.#events.emit('connect', { session: s }),
      onClose: (s, info: ConnectionCloseInfo) => {
        this.#sessions.delete(s);
        this.#events.emit('disconnect', { session: s, error: info.error });
      },
      onError: (error, s) => this.#reportError(error, s),
    });
    if (session.state !== 'closed') {
      this.#sessions.add(session);
    }
    return session;
  }

  /**
   * Stops accepting transports and closes all sessions, waiting for brokers
   * to process their disconnection. Idempotent.
   */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(): Promise<void> {
    const sessions = [...this.#sessions];
    for (const session of sessions) {
      session.close();
    }
    await Promise.all(sessions.map((session) => session.closed));
  }

  #reportError(error: unknown, session: ServerSession): void {
    if (!this.#events.emit('error', { error, session })) {
      reportError(error);
    }
  }
}
