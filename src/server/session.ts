import { StompError } from '../codec/errors.ts';
import { STOMP_VERSIONS, type FrameHeaders, type FrameInit, type StompVersion } from '../codec/frame.ts';
import type { ParserLimits } from '../codec/parser.ts';
import { Connection, type ConnectionCloseInfo, type MessageType } from '../connection/connection.ts';
import { StompProtocolError } from '../connection/errors.ts';
import {
  formatHeartBeat,
  negotiateHeartBeat,
  negotiateVersion,
  parseHeartBeat,
  type HeartbeatOptions,
} from '../connection/negotiate.ts';
import type {
  AckFrame,
  AckMode,
  ClientFrame,
  ConnectFrame,
  NackFrame,
  SendFrame,
  StompConnectFrame,
  SubscribeFrame,
  UnsubscribeFrame,
} from '../frames/types.ts';
import type { ValidationMode } from '../frames/validate.ts';
import type { Transport } from '../transport/types.ts';
import type { AckRef, Broker, BrokerSession, InboundMessage, OutboundMessage, SessionError } from './broker.ts';

/** What the `authenticate` hook receives. */
export interface ConnectRequest {
  readonly frame: ConnectFrame | StompConnectFrame;
  /** The negotiated version. */
  readonly version: StompVersion;
  readonly login: string | undefined;
  readonly passcode: string | undefined;
  readonly host: string | undefined;
  /** Application-provided context, e.g. the HTTP upgrade request. */
  readonly context: unknown;
}

export interface ServerSessionOptions extends ParserLimits {
  readonly transport: Transport;
  readonly broker: Broker;
  /** Session id; a random UUID by default. */
  readonly id?: string;
  /** Application-provided context, exposed to hooks and the broker. */
  readonly context?: unknown;
  /** Versions to accept; defaults to all supported versions. */
  readonly versions?: readonly StompVersion[];
  /** Server heart-beat preferences. Defaults to 10 s in both directions. */
  readonly heartbeat?: HeartbeatOptions;
  /** Multiplier for the incoming heart-beat interval. Defaults to 2. */
  readonly heartbeatTolerance?: number;
  /** Value of the `server` header of `CONNECTED`, or false to omit it. */
  readonly server?: string | false;
  /** Time to wait for `CONNECT`, in ms. Defaults to 10000; 0 disables. */
  readonly connectTimeout?: number;
  /**
   * Authenticates a client. Returns (or resolves to) a principal, exposed
   * as `session.principal`; throws (or rejects) to refuse the connection,
   * in which case the client receives an `ERROR` frame with the error's
   * message.
   */
  readonly authenticate?: (request: ConnectRequest) => unknown;
  /**
   * Called for every frame after the handshake (except `DISCONNECT`),
   * before it is processed. Throw (or reject) to refuse the frame: the
   * client receives an `ERROR` frame and the session is closed.
   */
  readonly authorize?: (session: ServerSession, frame: ClientFrame) => void | Promise<void>;
  /** Defaults to `strict`. */
  readonly validation?: ValidationMode;
  /** Defaults to `auto`. */
  readonly messageType?: MessageType;
  /** Called once the handshake is complete. */
  readonly onConnect?: (session: ServerSession) => void;
  /** Called once the session is closed. */
  readonly onClose?: (session: ServerSession, info: ConnectionCloseInfo) => void;
  /** Called with errors that cannot be reported to the client. */
  readonly onError?: (error: unknown, session: ServerSession) => void;
}

export type ServerSessionState = 'connecting' | 'connected' | 'closed';

export const DEFAULT_SERVER_HEARTBEAT: HeartbeatOptions = Object.freeze({ outgoing: 10_000, incoming: 10_000 });

interface ServerSubscription {
  readonly id: string;
  readonly destination: string;
  readonly ack: AckMode;
  /** Whether the client provided the id (always true from 1.1 onwards). */
  readonly explicitId: boolean;
}

interface PendingAck {
  readonly key: string;
  readonly subscriptionId: string;
  readonly messageId: string;
}

interface Transaction {
  /** Buffered operations, when the broker does not support transactions. */
  readonly operations: (() => void | Promise<void>)[];
}

/**
 * The server side of a single STOMP session: handles the handshake,
 * validates and processes client frames one at a time, and hands them to a
 * {@link Broker}. Implements {@link BrokerSession}, the broker's view of it.
 */
export class ServerSession implements BrokerSession {
  readonly id: string;
  readonly context: unknown;

  readonly #options: ServerSessionOptions;
  readonly #broker: Broker;
  readonly #connection: Connection<'server'>;
  readonly #versions: readonly StompVersion[];
  readonly #nativeTransactions: boolean;
  readonly #subscriptions = new Map<string, ServerSubscription>();
  readonly #pendingAcks = new Map<string, PendingAck>();
  readonly #transactions = new Map<string, Transaction>();
  readonly #closed = Promise.withResolvers<ConnectionCloseInfo>();

  #state: ServerSessionState = 'connecting';
  #version: StompVersion = '1.0';
  #principal: unknown;
  #host: string | undefined;
  #brokerConnected = false;
  #ackCounter = 0;
  #connectTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: ServerSessionOptions) {
    this.id = options.id ?? globalThis.crypto.randomUUID();
    this.context = options.context;
    this.#options = options;
    this.#broker = options.broker;
    this.#versions = options.versions ?? STOMP_VERSIONS;
    this.#nativeTransactions =
      typeof options.broker.begin === 'function' &&
      typeof options.broker.commit === 'function' &&
      typeof options.broker.abort === 'function';
    this.#connection = new Connection({
      role: 'server',
      transport: options.transport,
      ...(options.validation === undefined ? {} : { validation: options.validation }),
      ...(options.messageType === undefined ? {} : { messageType: options.messageType }),
      ...(options.heartbeatTolerance === undefined ? {} : { heartbeatTolerance: options.heartbeatTolerance }),
      ...(options.maxHeaderSize === undefined ? {} : { maxHeaderSize: options.maxHeaderSize }),
      ...(options.maxHeaders === undefined ? {} : { maxHeaders: options.maxHeaders }),
      ...(options.maxBodySize === undefined ? {} : { maxBodySize: options.maxBodySize }),
      onFrame: (frame) => this.#receive(frame),
      onClose: (info) => this.#onClose(info),
    });
    const connectTimeout = options.connectTimeout ?? 10_000;
    if (connectTimeout > 0 && this.#state === 'connecting') {
      this.#connectTimer = setTimeout(() => {
        this.#reject(new StompProtocolError(`No CONNECT frame received within ${connectTimeout} ms`));
      }, connectTimeout);
    }
  }

  get state(): ServerSessionState {
    return this.#state;
  }

  get version(): StompVersion {
    return this.#version;
  }

  get principal(): unknown {
    return this.#principal;
  }

  get host(): string | undefined {
    return this.#host;
  }

  get active(): boolean {
    return this.#state === 'connected';
  }

  /** Ids of the session's current subscriptions. */
  get subscriptions(): readonly string[] {
    return [...this.#subscriptions.keys()];
  }

  /** Resolves (never rejects) once the session is closed. */
  get closed(): Promise<ConnectionCloseInfo> {
    return this.#closed.promise;
  }

  deliver(subscriptionId: string, message: OutboundMessage): boolean {
    if (this.#state !== 'connected') {
      return false;
    }
    const subscription = this.#subscriptions.get(subscriptionId);
    if (subscription === undefined) {
      return false;
    }
    let ackId: string | undefined;
    let pending: PendingAck | undefined;
    if (subscription.ack !== 'auto') {
      ackId = this.#version === '1.2' ? `${this.#ackCounter++}` : undefined;
      const key = this.#ackKey(ackId, subscription.id, message.messageId);
      pending = { key, subscriptionId: subscription.id, messageId: message.messageId };
    }
    try {
      this.#connection.send({
        command: 'MESSAGE',
        headers: {
          ...message.headers,
          'content-length': undefined,
          destination: message.destination,
          'message-id': message.messageId,
          subscription: subscription.explicitId ? subscription.id : undefined,
          ack: ackId,
        },
        body: message.body,
      });
    } catch {
      return false;
    }
    if (pending !== undefined) {
      this.#pendingAcks.set(pending.key, pending);
    }
    return true;
  }

  close(error?: SessionError): void {
    if (error === undefined) {
      this.#connection.close();
      return;
    }
    this.#connection.fail(new StompError(error.message), {
      command: 'ERROR',
      headers: { ...error.headers, message: sanitize(error.message) },
      ...(error.body === undefined ? {} : { body: error.body }),
    });
  }

  /**
   * Processes frames one at a time: incoming frames are held back while a
   * frame is being processed asynchronously.
   */
  #receive(frame: ClientFrame): void {
    let result: Promise<void>;
    try {
      result = this.#state === 'connecting' ? this.#handshake(frame) : this.#process(frame);
    } catch (err) {
      this.#reject(err, frame);
      return;
    }
    this.#connection.pause();
    result.then(
      () => this.#connection.resume(),
      (err: unknown) => this.#reject(err, frame),
    );
  }

  async #handshake(frame: ClientFrame): Promise<void> {
    if (frame.command !== 'CONNECT' && frame.command !== 'STOMP') {
      throw new StompProtocolError(`Expected a CONNECT frame, got ${frame.command}`);
    }
    const version = negotiateVersion(frame.headers['accept-version'], this.#versions);
    if (version === undefined) {
      this.#reject(new StompProtocolError('No supported protocol version'), frame, {
        version: this.#versions.join(','),
      });
      return;
    }
    const { login, passcode, host } = frame.headers;
    const authenticate = this.#options.authenticate;
    const principal =
      authenticate === undefined
        ? undefined
        : await authenticate({ frame, version, login, passcode, host, context: this.context });
    if (this.#connection.state !== 'open') {
      return;
    }
    this.#principal = principal;
    this.#host = host;
    this.#version = version;
    this.#connection.version = version;
    await this.#broker.connect?.(this);
    this.#brokerConnected = true;
    if (this.#connection.state !== 'open') {
      return;
    }
    const local = this.#options.heartbeat ?? DEFAULT_SERVER_HEARTBEAT;
    const server = this.#options.server ?? 'typed-stomp';
    this.#connection.send({
      command: 'CONNECTED',
      headers: {
        version,
        session: this.id,
        server: server === false ? undefined : server,
        'heart-beat': formatHeartBeat([local.outgoing, local.incoming]),
      },
    });
    this.#connection.startHeartbeat(
      negotiateHeartBeat([local.outgoing, local.incoming], parseHeartBeat(frame.headers['heart-beat'])),
    );
    this.#state = 'connected';
    clearTimeout(this.#connectTimer);
    this.#options.onConnect?.(this);
  }

  async #process(frame: ClientFrame): Promise<void> {
    if (this.#state !== 'connected') {
      return;
    }
    if (frame.command === 'DISCONNECT') {
      this.#sendReceipt(frame.headers.receipt);
      this.#connection.close();
      return;
    }
    await this.#options.authorize?.(this, frame);
    switch (frame.command) {
      case 'CONNECT':
      case 'STOMP':
        throw new StompProtocolError('Already connected');
      case 'SEND':
        await this.#send(frame);
        break;
      case 'SUBSCRIBE':
        await this.#subscribe(frame);
        break;
      case 'UNSUBSCRIBE':
        await this.#unsubscribe(frame);
        break;
      case 'ACK':
      case 'NACK':
        await this.#acknowledge(frame);
        break;
      case 'BEGIN':
        await this.#begin(frame.headers.transaction);
        break;
      case 'COMMIT':
        await this.#commit(frame.headers.transaction);
        break;
      case 'ABORT':
        await this.#abort(frame.headers.transaction);
        break;
    }
    this.#sendReceipt(frame.headers.receipt);
  }

  async #send(frame: SendFrame): Promise<void> {
    const {
      destination,
      transaction,
      receipt: _receipt,
      'content-length': _contentLength,
      ...headers
    } = frame.headers;
    const message: InboundMessage = { destination, headers, body: frame.body };
    if (transaction === undefined) {
      await this.#broker.publish(this, message);
    } else if (this.#nativeTransactions) {
      this.#transaction(transaction);
      await this.#broker.publish(this, message, transaction);
    } else {
      this.#transaction(transaction).operations.push(() => this.#broker.publish(this, message));
    }
  }

  async #subscribe(frame: SubscribeFrame): Promise<void> {
    const { destination } = frame.headers;
    const explicitId = frame.headers.id !== undefined;
    // STOMP 1.0 subscriptions may omit the id; the destination identifies them.
    const id = frame.headers.id ?? destination;
    if (this.#subscriptions.has(id)) {
      throw new StompProtocolError(`Subscription id "${id}" is already in use`);
    }
    const ack = frame.headers.ack ?? 'auto';
    this.#subscriptions.set(id, { id, destination, ack, explicitId });
    try {
      await this.#broker.subscribe(this, { id, destination, ack, headers: frame.headers });
    } catch (err) {
      this.#subscriptions.delete(id);
      throw err;
    }
  }

  async #unsubscribe(frame: UnsubscribeFrame): Promise<void> {
    let id = frame.headers.id;
    if (id === undefined) {
      const destination = frame.headers['destination'];
      id = [...this.#subscriptions.values()].find((sub) => sub.destination === destination)?.id;
    }
    if (id === undefined || !this.#subscriptions.delete(id)) {
      // Unknown subscriptions are ignored.
      return;
    }
    for (const [key, pending] of this.#pendingAcks) {
      if (pending.subscriptionId === id) {
        this.#pendingAcks.delete(key);
      }
    }
    await this.#broker.unsubscribe(this, id);
  }

  async #acknowledge(frame: AckFrame | NackFrame): Promise<void> {
    const headers = frame.headers as FrameHeaders;
    const key = this.#ackKey(headers['id'], headers['subscription'], headers['message-id']);
    const pending = this.#pendingAcks.get(key);
    if (pending === undefined) {
      throw new StompProtocolError(`${frame.command} for an unknown or already acknowledged message`);
    }
    this.#settle(pending);
    const ref: AckRef = { subscriptionId: pending.subscriptionId, messageId: pending.messageId };
    const transaction = headers['transaction'];
    const method = frame.command === 'ACK' ? 'ack' : 'nack';
    if (transaction === undefined) {
      await this.#broker[method](this, ref);
    } else if (this.#nativeTransactions) {
      this.#transaction(transaction);
      await this.#broker[method](this, ref, transaction);
    } else {
      this.#transaction(transaction).operations.push(() => this.#broker[method](this, ref));
    }
  }

  /**
   * Forgets pending acknowledgements settled by an ACK/NACK: just the one
   * message, or every earlier message of the subscription in `client` mode.
   */
  #settle(pending: PendingAck): void {
    const subscription = this.#subscriptions.get(pending.subscriptionId);
    if (subscription?.ack !== 'client') {
      this.#pendingAcks.delete(pending.key);
      return;
    }
    for (const [key, other] of this.#pendingAcks) {
      if (other.subscriptionId === pending.subscriptionId) {
        this.#pendingAcks.delete(key);
      }
      if (key === pending.key) {
        break;
      }
    }
  }

  #ackKey(ackId: string | undefined, subscriptionId: string | undefined, messageId: string | undefined): string {
    switch (this.#version) {
      case '1.2':
        return `id:${ackId}`;
      case '1.1':
        return `sub:${subscriptionId}\0${messageId}`;
      case '1.0':
        return `msg:${messageId}`;
    }
  }

  #transaction(id: string): Transaction {
    const transaction = this.#transactions.get(id);
    if (transaction === undefined) {
      throw new StompProtocolError(`Unknown transaction "${id}"`);
    }
    return transaction;
  }

  async #begin(id: string): Promise<void> {
    if (this.#transactions.has(id)) {
      throw new StompProtocolError(`Transaction "${id}" has already begun`);
    }
    this.#transactions.set(id, { operations: [] });
    if (this.#nativeTransactions) {
      await this.#broker.begin?.(this, id);
    }
  }

  async #commit(id: string): Promise<void> {
    const transaction = this.#transaction(id);
    this.#transactions.delete(id);
    if (this.#nativeTransactions) {
      await this.#broker.commit?.(this, id);
      return;
    }
    for (const operation of transaction.operations) {
      await operation();
    }
  }

  async #abort(id: string): Promise<void> {
    this.#transaction(id);
    this.#transactions.delete(id);
    if (this.#nativeTransactions) {
      await this.#broker.abort?.(this, id);
    }
  }

  #sendReceipt(receipt: string | undefined): void {
    if (receipt !== undefined && this.#connection.state === 'open') {
      this.#connection.send({ command: 'RECEIPT', headers: { 'receipt-id': receipt } });
    }
  }

  /** Answers a failed frame with an `ERROR` frame and closes the session. */
  #reject(error: unknown, frame?: ClientFrame, headers: FrameHeaders = {}): void {
    if (this.#connection.state === 'closed') {
      return;
    }
    const message = error instanceof Error ? error.message : String(error);
    const errorFrame: FrameInit = {
      command: 'ERROR',
      headers: {
        ...headers,
        message: sanitize(message),
        'receipt-id': frame?.headers.receipt,
      },
    };
    this.#connection.fail(error, errorFrame);
  }

  #onClose(info: ConnectionCloseInfo): void {
    this.#state = 'closed';
    clearTimeout(this.#connectTimer);
    this.#subscriptions.clear();
    this.#pendingAcks.clear();
    const transactions = [...this.#transactions.keys()];
    this.#transactions.clear();
    this.#options.onClose?.(this, info);
    if (this.#brokerConnected) {
      void this.#cleanUp(transactions).finally(() => this.#closed.resolve(info));
    } else {
      this.#closed.resolve(info);
    }
  }

  async #cleanUp(transactions: readonly string[]): Promise<void> {
    if (this.#nativeTransactions) {
      for (const transaction of transactions) {
        try {
          await this.#broker.abort?.(this, transaction);
        } catch (err) {
          this.#reportError(err);
        }
      }
    }
    try {
      await this.#broker.disconnect(this);
    } catch (err) {
      this.#reportError(err);
    }
  }

  #reportError(error: unknown): void {
    this.#options.onError?.(error, this);
  }
}

/** Header values cannot contain line breaks in STOMP 1.0. */
function sanitize(message: string): string {
  return message.replace(/[\r\n]+/g, ' ');
}
