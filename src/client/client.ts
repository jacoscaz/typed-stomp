import type { FrameHeaders, StompVersion } from '../codec/frame.ts';
import type { ParserLimits } from '../codec/parser.ts';
import type { MessageType } from '../connection/connection.ts';
import { StompConnectionClosedError } from '../connection/errors.ts';
import type { ServerFrame } from '../frames/types.ts';
import type { ValidationIssue, ValidationMode } from '../frames/validate.ts';
import type { Transport } from '../transport/types.ts';
import { WebSocketTransport, type WebSocketConstructor, type WebSocketTransportOptions } from '../transport/websocket.ts';
import { Emitter, reportError } from '../util/emitter.ts';
import { backoffDelay, type ReconnectOptions } from './backoff.ts';
import { StompNotConnectedError } from './errors.ts';
import type { StompMessage } from './message.ts';
import {
  ClientSession,
  type ClientSessionOptions,
  type HeartbeatOptions,
  type SendOptions,
  type SessionTransaction,
} from './session.ts';
import { ATTACH, CLOSE, DETACH, Subscription, type MessageHandler, type SubscribeOptions } from './subscription.ts';

export type ClientState = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'closing' | 'closed';

export interface ClientEvents {
  /** A session has been established. */
  connect: { readonly version: StompVersion; readonly server: string | undefined; readonly session: string | undefined };
  /** An established session has ended. */
  disconnect: { readonly error: unknown; readonly willReconnect: boolean };
  /** A reconnection attempt will be made after `delay` ms. */
  reconnecting: { readonly attempt: number; readonly delay: number; readonly error: unknown };
  /**
   * Errors without a caller to report them to, such as subscription handler
   * errors. If there are no listeners, they are reported as uncaught.
   */
  error: { readonly error: unknown; readonly subscription?: Subscription; readonly message?: StompMessage };
  /** Validation warnings about incoming frames. */
  warning: { readonly issues: readonly ValidationIssue[]; readonly frame: ServerFrame };
  /** The client has stopped for good, either closed or after giving up. */
  close: { readonly error: unknown };
}

export interface ClientOptions<O = never> extends ParserLimits {
  /** WebSocket URL of the broker. Either this or `transport` is required. */
  readonly url?: string | URL;
  /** `WebSocket` implementation; defaults to `globalThis.WebSocket`. */
  readonly WebSocket?: WebSocketConstructor<O>;
  /** Implementation-specific WebSocket options (e.g. `ws` client options). */
  readonly webSocketOptions?: O;
  /** WebSocket subprotocols; defaults to the STOMP ones. */
  readonly protocols?: string | readonly string[];
  /** Custom transport factory, used instead of `url`. */
  readonly transport?: (signal: AbortSignal) => Transport | Promise<Transport>;
  /**
   * Additional `CONNECT` headers (e.g. `login` and `passcode`), or a
   * function returning them, called before every connection attempt (e.g.
   * to refresh tokens).
   */
  readonly connectHeaders?: FrameHeaders | (() => FrameHeaders | Promise<FrameHeaders>);
  /** Value of the `host` header. Defaults to `/`. */
  readonly host?: string;
  /** Versions to offer; defaults to all supported versions. */
  readonly versions?: readonly StompVersion[];
  /** Command used to connect. Defaults to `CONNECT`. */
  readonly command?: 'CONNECT' | 'STOMP';
  /** Defaults to 10 s in both directions. */
  readonly heartbeat?: HeartbeatOptions;
  /** Multiplier for the incoming heart-beat interval. Defaults to 2. */
  readonly heartbeatTolerance?: number;
  /** Time to wait for `CONNECTED`, in ms. Defaults to 10000. */
  readonly connectTimeout?: number;
  /** Time to wait for the `DISCONNECT` receipt when closing, in ms. Defaults to 5000. */
  readonly disconnectTimeout?: number;
  /** Reconnection policy, or `false` to disable reconnection. */
  readonly reconnect?: ReconnectOptions | false;
  /**
   * What `publish` does while not connected: `reject` (the default) or
   * `queue` until connected.
   */
  readonly offline?: 'reject' | 'queue';
  /** Maximum number of queued publications while offline. Defaults to 1000. */
  readonly offlineQueueLimit?: number;
  /** Defaults to `lenient`. */
  readonly validation?: ValidationMode;
  /** Defaults to `auto`. */
  readonly messageType?: MessageType;
}

export type Transaction = SessionTransaction;

interface QueuedPublication {
  readonly run: (session: ClientSession) => Promise<void>;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
}

/**
 * A STOMP client that keeps a session alive: it reconnects with
 * exponential backoff, re-establishes subscriptions and optionally queues
 * publications while offline.
 *
 * ```ts
 * const client = new Client({ url: 'wss://broker/ws', connectHeaders: { login, passcode } });
 * client.subscribe('/topic/news', (message) => console.log(message.text()));
 * await client.connect();
 * await client.publish('/queue/jobs', JSON.stringify(job), { receipt: true });
 * await client.close();
 * ```
 */
export class Client<O = never> {
  readonly #options: ClientOptions<O>;
  readonly #events = new Emitter<ClientEvents>();
  readonly #subscriptions = new Map<string, Subscription>();
  readonly #controller = new AbortController();
  readonly #connected = Promise.withResolvers<void>();
  #state: ClientState = 'idle';
  #session: ClientSession | undefined;
  #running: Promise<void> | undefined;
  #closing: Promise<void> | undefined;
  #offlineQueue: QueuedPublication[] = [];
  #nextSubscription = 0;
  #everConnected = false;
  #finished = false;

  constructor(options: ClientOptions<O>) {
    if ((options.url === undefined) === (options.transport === undefined)) {
      throw new TypeError('Exactly one of the url and transport options is required');
    }
    this.#options = options;
    this.#connected.promise.catch(() => {});
  }

  get state(): ClientState {
    return this.#state;
  }

  /** The current session, while connected. */
  get session(): ClientSession | undefined {
    return this.#session;
  }

  get connected(): boolean {
    return this.#state === 'connected';
  }

  /** Active subscriptions. */
  get subscriptions(): readonly Subscription[] {
    return [...this.#subscriptions.values()];
  }

  on<K extends keyof ClientEvents>(type: K, listener: (event: ClientEvents[K]) => void): () => void {
    return this.#events.on(type, listener);
  }

  once<K extends keyof ClientEvents>(type: K, listener: (event: ClientEvents[K]) => void): () => void {
    return this.#events.once(type, listener);
  }

  off<K extends keyof ClientEvents>(type: K, listener: (event: ClientEvents[K]) => void): void {
    this.#events.off(type, listener);
  }

  /**
   * Starts the client. Resolves once connected for the first time; failed
   * attempts are retried according to the reconnection policy. Rejects if
   * the client gives up or is closed before connecting.
   */
  connect(): Promise<void> {
    if (this.#state === 'idle') {
      this.#running = this.#run();
    }
    return this.#connected.promise;
  }

  /**
   * Sends a message. While not connected, rejects with
   * {@link StompNotConnectedError}, or queues the message if the `offline`
   * option is `queue`.
   */
  publish(destination: string, body?: string | Uint8Array, options: Omit<SendOptions, 'transaction'> = {}): Promise<void> {
    const session = this.#session;
    if (session !== undefined && session.state === 'connected') {
      return session.send(destination, body, options);
    }
    if (this.#finished || this.#state === 'closing') {
      return Promise.reject(new StompConnectionClosedError('Client is closed'));
    }
    if (this.#options.offline !== 'queue') {
      return Promise.reject(new StompNotConnectedError());
    }
    if (this.#offlineQueue.length >= (this.#options.offlineQueueLimit ?? 1000)) {
      return Promise.reject(new StompNotConnectedError('Client is not connected and the offline queue is full'));
    }
    return new Promise<void>((resolve, reject) => {
      this.#offlineQueue.push({ run: (s) => s.send(destination, body, options), resolve, reject });
    });
  }

  /**
   * Subscribes to a destination. The subscription is established as soon as
   * the client is connected, and again after every reconnection.
   */
  subscribe(destination: string, handler: MessageHandler, options: SubscribeOptions = {}): Subscription {
    if (this.#finished || this.#state === 'closing') {
      throw new StompConnectionClosedError('Client is closed');
    }
    const id = options.id ?? `sub-${this.#nextSubscription++}`;
    if (this.#subscriptions.has(id)) {
      throw new TypeError(`Subscription id "${id}" is already in use`);
    }
    const subscription = new Subscription(
      {
        remove: (sub) => {
          if (this.#subscriptions.get(sub.id) === sub) {
            this.#subscriptions.delete(sub.id);
          }
        },
        reportError: (error, sub, message) => this.#reportError(error, sub, message),
      },
      id,
      destination,
      handler,
      options,
    );
    this.#subscriptions.set(id, subscription);
    const session = this.#session;
    if (session !== undefined && session.state === 'connected') {
      subscription[ATTACH](session);
    }
    return subscription;
  }

  /**
   * Runs `fn` within a transaction on the current session: commits if it
   * succeeds, aborts and rethrows if it fails.
   */
  async transaction<T>(fn: (transaction: Transaction) => T | Promise<T>): Promise<T> {
    const session = this.#session;
    if (session === undefined || session.state !== 'connected') {
      throw new StompNotConnectedError();
    }
    const transaction = session.begin();
    let result: T;
    try {
      result = await fn(transaction);
    } catch (err) {
      await transaction.abort().catch(() => {});
      throw err;
    }
    await transaction.commit();
    return result;
  }

  /**
   * Stops the client: disconnects gracefully if connected, stops
   * reconnecting, ends all subscriptions and rejects queued publications.
   */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(): Promise<void> {
    if (this.#finished) {
      return;
    }
    this.#state = 'closing';
    this.#controller.abort(new StompConnectionClosedError('Client closed'));
    const session = this.#session;
    if (session !== undefined) {
      await session.disconnect({ timeout: this.#options.disconnectTimeout ?? 5000 });
    }
    await this.#running;
    this.#finish(undefined);
  }

  async #run(): Promise<void> {
    const signal = this.#controller.signal;
    let failures = 0;
    while (!signal.aborted) {
      this.#state = 'connecting';
      let error: unknown;
      let session: ClientSession | undefined;
      try {
        session = await this.#openSession(signal);
      } catch (err) {
        error = err;
      }
      if (signal.aborted) {
        await session?.disconnect({ timeout: this.#options.disconnectTimeout ?? 5000 });
        return;
      }
      if (session !== undefined) {
        failures = 0;
        this.#established(session);
        const info = await session.closed;
        this.#lost();
        if (signal.aborted) {
          this.#events.emit('disconnect', { error: info.error, willReconnect: false });
          return;
        }
        error = info.error ?? new StompConnectionClosedError('Connection closed by the server');
      }
      failures += 1;
      const willReconnect = this.#shouldReconnect(error, failures);
      if (session !== undefined) {
        this.#events.emit('disconnect', { error, willReconnect });
      }
      if (!willReconnect) {
        this.#finish(error);
        return;
      }
      const delay = backoffDelay(failures, this.#options.reconnect || {});
      this.#state = 'reconnecting';
      this.#events.emit('reconnecting', { attempt: failures, delay, error });
      await sleep(delay, signal);
    }
  }

  async #openSession(signal: AbortSignal): Promise<ClientSession> {
    const transport = await this.#openTransport(signal);
    try {
      const { connectHeaders } = this.#options;
      const headers = typeof connectHeaders === 'function' ? await connectHeaders() : connectHeaders;
      signal.throwIfAborted();
      const o = this.#options;
      const sessionOptions: ClientSessionOptions = {
        transport,
        signal,
        onWarning: (issues, frame) => this.#events.emit('warning', { issues, frame }),
        ...(headers === undefined ? {} : { headers }),
        ...(o.host === undefined ? {} : { host: o.host }),
        ...(o.versions === undefined ? {} : { versions: o.versions }),
        ...(o.command === undefined ? {} : { command: o.command }),
        ...(o.heartbeat === undefined ? {} : { heartbeat: o.heartbeat }),
        ...(o.heartbeatTolerance === undefined ? {} : { heartbeatTolerance: o.heartbeatTolerance }),
        ...(o.connectTimeout === undefined ? {} : { connectTimeout: o.connectTimeout }),
        ...(o.validation === undefined ? {} : { validation: o.validation }),
        ...(o.messageType === undefined ? {} : { messageType: o.messageType }),
        ...(o.maxHeaderSize === undefined ? {} : { maxHeaderSize: o.maxHeaderSize }),
        ...(o.maxHeaders === undefined ? {} : { maxHeaders: o.maxHeaders }),
        ...(o.maxBodySize === undefined ? {} : { maxBodySize: o.maxBodySize }),
      };
      return await ClientSession.connect(sessionOptions);
    } catch (err) {
      transport.close();
      throw err;
    }
  }

  async #openTransport(signal: AbortSignal): Promise<Transport> {
    const o = this.#options;
    if (o.transport !== undefined) {
      return o.transport(signal);
    }
    const options: WebSocketTransportOptions<O> = {
      signal,
      ...(o.WebSocket === undefined ? {} : { WebSocket: o.WebSocket }),
      ...(o.webSocketOptions === undefined ? {} : { webSocketOptions: o.webSocketOptions }),
      ...(o.protocols === undefined ? {} : { protocols: o.protocols }),
    };
    return WebSocketTransport.connect(o.url as string | URL, options);
  }

  #established(session: ClientSession): void {
    this.#session = session;
    this.#state = 'connected';
    this.#everConnected = true;
    this.#events.emit('connect', { version: session.version, server: session.server, session: session.id });
    this.#connected.resolve();
    for (const subscription of this.#subscriptions.values()) {
      subscription[ATTACH](session);
    }
    const queue = this.#offlineQueue;
    this.#offlineQueue = [];
    for (const publication of queue) {
      publication.run(session).then(publication.resolve, publication.reject);
    }
  }

  #lost(): void {
    this.#session = undefined;
    for (const subscription of this.#subscriptions.values()) {
      subscription[DETACH]();
    }
  }

  #shouldReconnect(error: unknown, failures: number): boolean {
    const reconnect = this.#options.reconnect;
    if (reconnect === false) {
      return false;
    }
    const options = reconnect ?? {};
    if (failures > (options.maxAttempts ?? Number.POSITIVE_INFINITY)) {
      return false;
    }
    return options.shouldReconnect?.(error, failures) ?? true;
  }

  #finish(error: unknown): void {
    if (this.#finished) {
      return;
    }
    this.#finished = true;
    this.#state = 'closed';
    const reason = error ?? new StompConnectionClosedError('Client closed');
    if (!this.#everConnected) {
      this.#connected.reject(reason);
    }
    const queue = this.#offlineQueue;
    this.#offlineQueue = [];
    for (const publication of queue) {
      publication.reject(reason);
    }
    for (const subscription of [...this.#subscriptions.values()]) {
      subscription[CLOSE](reason);
    }
    this.#events.emit('close', { error });
  }

  #reportError(error: unknown, subscription?: Subscription, message?: StompMessage): void {
    const event = {
      error,
      ...(subscription === undefined ? {} : { subscription }),
      ...(message === undefined ? {} : { message }),
    };
    if (!this.#events.emit('error', event)) {
      reportError(error);
    }
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
  });
}
