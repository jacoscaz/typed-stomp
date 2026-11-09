import { StompError } from '../codec/errors.ts';
import { STOMP_VERSIONS, isStompVersion, type FrameHeaders, type StompVersion } from '../codec/frame.ts';
import type { ParserLimits } from '../codec/parser.ts';
import {
  Connection,
  type ConnectionCloseInfo,
  type MessageType,
} from '../connection/connection.ts';
import { StompConnectionClosedError, StompProtocolError } from '../connection/errors.ts';
import {
  formatHeartBeat,
  negotiateHeartBeat,
  parseHeartBeat,
  type HeartBeatIntervals,
} from '../connection/negotiate.ts';
import { ReceiptTracker } from '../connection/receipts.ts';
import type { AckMode, ErrorFrame, MessageFrame, ServerFrame } from '../frames/types.ts';
import type { ValidationIssue, ValidationMode } from '../frames/validate.ts';
import type { Transport } from '../transport/types.ts';
import { StompConnectTimeoutError, StompNotConnectedError, StompServerError } from './errors.ts';
import { StompMessage, type AckOptions } from './message.ts';

export interface HeartbeatOptions {
  /** How often we can send heart-beats, in ms; 0 for never. */
  readonly outgoing: number;
  /** How often we want to receive heart-beats, in ms; 0 for never. */
  readonly incoming: number;
}

export const DEFAULT_HEARTBEAT: HeartbeatOptions = Object.freeze({ outgoing: 10_000, incoming: 10_000 });

export interface ClientSessionOptions extends ParserLimits {
  readonly transport: Transport;
  /** Versions to offer, defaults to all supported versions. */
  readonly versions?: readonly StompVersion[];
  /**
   * Value of the `host` header (the virtual host, for brokers like RabbitMQ).
   * Defaults to `/`.
   */
  readonly host?: string;
  /** Additional `CONNECT` headers, such as `login` and `passcode`. */
  readonly headers?: FrameHeaders;
  /** Command used to connect. Defaults to `CONNECT`. */
  readonly command?: 'CONNECT' | 'STOMP';
  /** Defaults to {@link DEFAULT_HEARTBEAT}. */
  readonly heartbeat?: HeartbeatOptions;
  /** Multiplier for the incoming heart-beat interval. Defaults to 2. */
  readonly heartbeatTolerance?: number;
  /** Time to wait for `CONNECTED`, in ms. Defaults to 10000; 0 disables. */
  readonly connectTimeout?: number;
  /** Aborts the connection attempt (not the session once connected). */
  readonly signal?: AbortSignal;
  /** Defaults to `lenient`. */
  readonly validation?: ValidationMode;
  /** Defaults to `auto`. */
  readonly messageType?: MessageType;
  /** Called with validation warnings about incoming frames. */
  readonly onWarning?: (issues: readonly ValidationIssue[], frame: ServerFrame) => void;
}

export type ClientSessionState = 'connecting' | 'connected' | 'disconnecting' | 'closed';

export type SessionCloseInfo = ConnectionCloseInfo;

export interface SendOptions {
  /** Additional headers. */
  readonly headers?: FrameHeaders;
  readonly contentType?: string;
  /** Wait for the server to confirm with a `RECEIPT`. */
  readonly receipt?: boolean;
  readonly transaction?: string;
}

export interface SessionSubscribeOptions {
  /** Subscription id; generated if omitted. */
  readonly id?: string;
  /** Defaults to `auto`. */
  readonly ack?: AckMode;
  /** Additional headers (e.g. broker-specific ones such as `prefetch-count`). */
  readonly headers?: FrameHeaders;
  /** Wait for the server to confirm with a `RECEIPT`. */
  readonly receipt?: boolean;
}

export interface ReceiptOptions {
  readonly receipt?: boolean;
}

/** A subscription within a single session. */
export interface SessionSubscription {
  readonly id: string;
  readonly destination: string;
  readonly ack: AckMode;
  /** Settles once the `SUBSCRIBE` frame is sent, or confirmed if a receipt was requested. */
  readonly ready: Promise<void>;
  unsubscribe(options?: ReceiptOptions): Promise<void>;
}

/** A transaction within a single session. */
export interface SessionTransaction {
  readonly id: string;
  /** Settles once `BEGIN` is sent, or confirmed if a receipt was requested. */
  readonly ready: Promise<void>;
  send(destination: string, body?: string | Uint8Array, options?: Omit<SendOptions, 'transaction'>): Promise<void>;
  ack(message: StompMessage, options?: ReceiptOptions): Promise<void>;
  nack(message: StompMessage, options?: ReceiptOptions): Promise<void>;
  commit(options?: ReceiptOptions): Promise<void>;
  abort(options?: ReceiptOptions): Promise<void>;
}

interface SubscriptionEntry {
  readonly id: string;
  readonly destination: string;
  readonly ack: AckMode;
  readonly onMessage: (message: StompMessage) => void;
}

interface FrameToSend {
  readonly command: string;
  readonly headers: FrameHeaders;
  readonly body?: string | Uint8Array;
}

/**
 * A single STOMP session over a single transport: the `CONNECT` handshake,
 * then sending, subscribing, acknowledging and transactions, until the
 * connection closes. Sessions do not reconnect; see `Client` for that.
 *
 * ```ts
 * const session = await ClientSession.connect({ transport, headers: { login, passcode } });
 * session.subscribe('/topic/news', (message) => console.log(message.text()));
 * await session.send('/queue/jobs', JSON.stringify(job), { receipt: true });
 * await session.disconnect();
 * ```
 */
export class ClientSession {
  readonly #connection: Connection<'client'>;
  readonly #offered: readonly StompVersion[];
  readonly #heartbeatOptions: HeartbeatOptions;
  readonly #onWarning: ClientSessionOptions['onWarning'];
  readonly #receipts = new ReceiptTracker('receipt-');
  readonly #subscriptions = new Map<string, SubscriptionEntry>();
  readonly #handshake = Promise.withResolvers<void>();
  readonly #closed = Promise.withResolvers<SessionCloseInfo>();

  #state: ClientSessionState = 'connecting';
  #version: StompVersion = '1.0';
  #server: string | undefined;
  #sessionId: string | undefined;
  #heartbeat: HeartBeatIntervals = { outgoing: 0, incoming: 0 };
  #nextSubscription = 0;
  #nextTransaction = 0;
  #connectTimer: ReturnType<typeof setTimeout> | undefined;
  #signal: AbortSignal | undefined;
  #onAbort: (() => void) | undefined;

  private constructor(options: ClientSessionOptions) {
    this.#offered = options.versions ?? STOMP_VERSIONS;
    this.#heartbeatOptions = options.heartbeat ?? DEFAULT_HEARTBEAT;
    this.#onWarning = options.onWarning;
    this.#handshake.promise.catch(() => {});

    this.#connection = new Connection({
      role: 'client',
      transport: options.transport,
      ...(options.validation === undefined ? {} : { validation: options.validation }),
      ...(options.messageType === undefined ? {} : { messageType: options.messageType }),
      ...(options.heartbeatTolerance === undefined ? {} : { heartbeatTolerance: options.heartbeatTolerance }),
      ...(options.maxHeaderSize === undefined ? {} : { maxHeaderSize: options.maxHeaderSize }),
      ...(options.maxHeaders === undefined ? {} : { maxHeaders: options.maxHeaders }),
      ...(options.maxBodySize === undefined ? {} : { maxBodySize: options.maxBodySize }),
      onFrame: (frame, warnings) => this.#receive(frame, warnings),
      onClose: (info) => this.#onClose(info),
    });
    if (this.#state !== 'connecting') {
      // The transport was already closed.
      return;
    }

    const connectTimeout = options.connectTimeout ?? 10_000;
    if (connectTimeout > 0) {
      this.#connectTimer = setTimeout(() => {
        this.#connection.fail(new StompConnectTimeoutError(connectTimeout));
      }, connectTimeout);
    }
    if (options.signal !== undefined) {
      const signal = options.signal;
      this.#signal = signal;
      this.#onAbort = () => this.#connection.fail(signal.reason);
      signal.addEventListener('abort', this.#onAbort, { once: true });
    }

    const { outgoing, incoming } = this.#heartbeatOptions;
    try {
      this.#connection.send({
        command: options.command ?? 'CONNECT',
        headers: {
          ...options.headers,
          'accept-version': this.#offered.join(','),
          host: options.host ?? '/',
          'heart-beat': formatHeartBeat([outgoing, incoming]),
        },
      });
    } catch (err) {
      this.#connection.fail(err);
    }
  }

  /**
   * Connects over an open transport. Resolves once the server has answered
   * with `CONNECTED`; rejects with a {@link StompServerError} if it answered
   * with `ERROR`, or with the cause of the failure otherwise. The transport
   * is closed on failure.
   */
  static async connect(options: ClientSessionOptions): Promise<ClientSession> {
    if (options.signal?.aborted) {
      options.transport.close();
      throw options.signal.reason;
    }
    const session = new ClientSession(options);
    await session.#handshake.promise;
    return session;
  }

  get state(): ClientSessionState {
    return this.#state;
  }

  /** The negotiated STOMP version. */
  get version(): StompVersion {
    return this.#version;
  }

  /** The `server` header of the `CONNECTED` frame. */
  get server(): string | undefined {
    return this.#server;
  }

  /** The `session` header of the `CONNECTED` frame. */
  get id(): string | undefined {
    return this.#sessionId;
  }

  /** The negotiated heart-beat intervals. */
  get heartbeat(): HeartBeatIntervals {
    return this.#heartbeat;
  }

  /** Resolves (never rejects) once the session is closed. */
  get closed(): Promise<SessionCloseInfo> {
    return this.#closed.promise;
  }

  /** Sends a message to a destination. */
  send(destination: string, body: string | Uint8Array = '', options: SendOptions = {}): Promise<void> {
    return this.#request(
      {
        command: 'SEND',
        headers: {
          ...options.headers,
          destination,
          'content-type': options.contentType ?? options.headers?.['content-type'],
          transaction: options.transaction,
        },
        body,
      },
      options.receipt,
    );
  }

  /**
   * Subscribes to a destination. `onMessage` is called synchronously for
   * each message and must not throw: errors close the session.
   */
  subscribe(
    destination: string,
    onMessage: (message: StompMessage) => void,
    options: SessionSubscribeOptions = {},
  ): SessionSubscription {
    this.#assertConnected();
    const id = options.id ?? `sub-${this.#nextSubscription++}`;
    if (this.#subscriptions.has(id)) {
      throw new StompError(`Subscription id "${id}" is already in use`);
    }
    const ack = options.ack ?? 'auto';
    if (ack === 'client-individual' && this.#version === '1.0') {
      throw new StompError('The client-individual ack mode requires STOMP 1.1 or later');
    }
    this.#subscriptions.set(id, { id, destination, ack, onMessage });
    let ready: Promise<void>;
    try {
      ready = this.#request(
        { command: 'SUBSCRIBE', headers: { ...options.headers, destination, id, ack } },
        options.receipt,
        true,
      );
    } catch (err) {
      this.#subscriptions.delete(id);
      throw err;
    }
    ready.catch(() => {});
    return {
      id,
      destination,
      ack,
      ready,
      unsubscribe: (unsubscribeOptions = {}) => this.#unsubscribe(id, unsubscribeOptions),
    };
  }

  /**
   * Starts a transaction. Frames sent through the returned object are part
   * of it until it is committed or aborted.
   */
  begin(options: ReceiptOptions = {}): SessionTransaction {
    this.#assertConnected();
    const id = `tx-${this.#nextTransaction++}`;
    const ready = this.#request({ command: 'BEGIN', headers: { transaction: id } }, options.receipt, true);
    ready.catch(() => {});
    return {
      id,
      ready,
      send: (destination, body, sendOptions = {}) => this.send(destination, body, { ...sendOptions, transaction: id }),
      ack: (message, ackOptions = {}) => message.ack({ ...ackOptions, transaction: id }),
      nack: (message, ackOptions = {}) => message.nack({ ...ackOptions, transaction: id }),
      commit: (commitOptions = {}) =>
        this.#request({ command: 'COMMIT', headers: { transaction: id } }, commitOptions.receipt),
      abort: (abortOptions = {}) => this.#request({ command: 'ABORT', headers: { transaction: id } }, abortOptions.receipt),
    };
  }

  /**
   * Disconnects gracefully: sends `DISCONNECT`, waits for its receipt (at
   * most `timeout` ms, default 5000), then closes the transport. During the
   * handshake, aborts it instead.
   */
  async disconnect(options: { readonly timeout?: number } = {}): Promise<void> {
    if (this.#state === 'connecting') {
      this.#connection.fail(new StompConnectionClosedError('Disconnected during the handshake'));
    }
    if (this.#state !== 'connected') {
      await this.#closed.promise;
      return;
    }
    this.#state = 'disconnecting';
    const request = this.#receipts.create();
    try {
      this.#connection.send({ command: 'DISCONNECT', headers: { receipt: request.id } });
      const timeout = options.timeout ?? 5000;
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([
        request.promise.catch(() => {}),
        this.#closed.promise,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, timeout);
        }),
      ]);
      clearTimeout(timer);
    } catch {
      // The connection is going away regardless.
    }
    this.#connection.close();
    await this.#closed.promise;
  }

  /** Closes the session immediately, without `DISCONNECT`. */
  abort(reason: unknown = new StompConnectionClosedError('Session aborted')): void {
    this.#connection.fail(reason);
  }

  #assertConnected(): void {
    if (this.#state !== 'connected') {
      throw new StompNotConnectedError(`Session is ${this.#state}`);
    }
  }

  /**
   * Sends a frame, optionally requesting a receipt. Errors are returned as
   * rejected promises, unless `throwSync` is set.
   */
  #request(frame: FrameToSend, receipt: boolean | undefined, throwSync = false): Promise<void> {
    try {
      this.#assertConnected();
      if (receipt !== true) {
        this.#connection.send(frame);
        return Promise.resolve();
      }
      const request = this.#receipts.create();
      try {
        this.#connection.send({ ...frame, headers: { ...frame.headers, receipt: request.id } });
      } catch (err) {
        this.#receipts.reject(request.id, err);
        throw err;
      }
      return request.promise.then(() => undefined);
    } catch (err) {
      if (throwSync) {
        throw err;
      }
      return Promise.reject(err);
    }
  }

  #unsubscribe(id: string, options: ReceiptOptions): Promise<void> {
    if (!this.#subscriptions.delete(id) || this.#state !== 'connected') {
      return Promise.resolve();
    }
    return this.#request({ command: 'UNSUBSCRIBE', headers: { id } }, options.receipt);
  }

  #acknowledge = (kind: 'ACK' | 'NACK', message: StompMessage, options: AckOptions): Promise<void> => {
    if (this.#state !== 'connected') {
      return Promise.reject(
        new StompNotConnectedError(`Cannot ${kind} a message: the session it was received in is ${this.#state}`),
      );
    }
    if (kind === 'NACK' && this.#version === '1.0') {
      return Promise.reject(new StompError('NACK requires STOMP 1.1 or later'));
    }
    const { headers } = message.frame;
    let ackHeaders: FrameHeaders;
    switch (this.#version) {
      case '1.2':
        ackHeaders = { id: headers.ack ?? headers['message-id'] };
        break;
      case '1.1':
        ackHeaders = { 'message-id': headers['message-id'], subscription: headers.subscription };
        break;
      case '1.0':
        ackHeaders = { 'message-id': headers['message-id'] };
        break;
    }
    return this.#request(
      { command: kind, headers: { ...ackHeaders, transaction: options.transaction } },
      options.receipt,
    );
  };

  #receive(frame: ServerFrame, warnings: readonly ValidationIssue[]): void {
    if (warnings.length > 0) {
      this.#onWarning?.(warnings, frame);
    }
    if (this.#state === 'connecting') {
      this.#completeHandshake(frame);
      return;
    }
    switch (frame.command) {
      case 'MESSAGE':
        this.#dispatch(frame);
        break;
      case 'RECEIPT':
        this.#receipts.resolve(frame.headers['receipt-id'], frame);
        break;
      case 'ERROR':
        this.#serverError(frame);
        break;
      case 'CONNECTED':
        this.#connection.fail(new StompProtocolError('Unexpected CONNECTED frame', { frame }));
        break;
    }
  }

  #completeHandshake(frame: ServerFrame): void {
    if (frame.command === 'ERROR') {
      this.#serverError(frame);
      return;
    }
    if (frame.command !== 'CONNECTED') {
      this.#connection.fail(new StompProtocolError(`Expected a CONNECTED frame, got ${frame.command}`, { frame }));
      return;
    }
    const version = frame.headers.version ?? '1.0';
    if (!isStompVersion(version) || !this.#offered.includes(version)) {
      this.#connection.fail(
        new StompProtocolError(`Server selected version ${version}, which was not offered`, { frame }),
      );
      return;
    }
    this.#version = version;
    this.#connection.version = version;
    this.#server = frame.headers.server;
    this.#sessionId = frame.headers.session;
    const { outgoing, incoming } = this.#heartbeatOptions;
    this.#heartbeat = negotiateHeartBeat([outgoing, incoming], parseHeartBeat(frame.headers['heart-beat']));
    this.#connection.startHeartbeat(this.#heartbeat);
    this.#state = 'connected';
    this.#cleanupHandshake();
    this.#handshake.resolve();
  }

  #dispatch(frame: MessageFrame): void {
    const subscriptionId = frame.headers.subscription;
    let entry = subscriptionId === undefined ? undefined : this.#subscriptions.get(subscriptionId);
    if (entry === undefined && subscriptionId === undefined) {
      // STOMP 1.0 servers may omit the subscription header.
      for (const candidate of this.#subscriptions.values()) {
        if (candidate.destination === frame.headers.destination) {
          entry = candidate;
          break;
        }
      }
    }
    // Messages for unknown subscriptions (e.g. in flight while unsubscribing) are ignored.
    entry?.onMessage(new StompMessage(frame, entry.ack, this.#acknowledge));
  }

  #serverError(frame: ErrorFrame): void {
    const error = new StompServerError(frame);
    const receiptId = frame.headers['receipt-id'];
    if (receiptId !== undefined) {
      this.#receipts.reject(receiptId, error);
    }
    this.#connection.fail(error);
  }

  #cleanupHandshake(): void {
    clearTimeout(this.#connectTimer);
    this.#connectTimer = undefined;
    if (this.#onAbort !== undefined) {
      this.#signal?.removeEventListener('abort', this.#onAbort);
      this.#onAbort = undefined;
    }
  }

  #onClose(info: ConnectionCloseInfo): void {
    const wasConnecting = this.#state === 'connecting';
    this.#state = 'closed';
    this.#cleanupHandshake();
    this.#subscriptions.clear();
    this.#receipts.rejectAll(
      new StompConnectionClosedError('Connection closed before the receipt arrived', { cause: info.error }),
    );
    if (wasConnecting) {
      this.#handshake.reject(
        info.error ?? new StompConnectionClosedError('Connection closed during the handshake'),
      );
    }
    this.#closed.resolve(info);
  }
}
