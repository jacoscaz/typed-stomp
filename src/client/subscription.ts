import type { FrameHeaders } from '../codec/frame.ts';
import type { AckMode } from '../frames/types.ts';
import { Dispatcher, type OverflowPolicy } from './dispatcher.ts';
import { StompBufferOverflowError } from './errors.ts';
import type { StompMessage } from './message.ts';
import type { ClientSession, ReceiptOptions, SessionSubscription } from './session.ts';

export type MessageHandler = (message: StompMessage) => void | Promise<void>;

export interface SubscribeOptions {
  /** Subscription id, stable across reconnections; generated if omitted. */
  readonly id?: string;
  /** Defaults to `auto`. */
  readonly ack?: AckMode;
  /** Additional `SUBSCRIBE` headers (e.g. broker-specific ones). */
  readonly headers?: FrameHeaders;
  /** Wait for a `RECEIPT` each time the subscription is (re)established. */
  readonly receipt?: boolean;
  /** Maximum number of handler calls in flight. Defaults to 1 (in order). */
  readonly concurrency?: number;
  readonly buffer?: {
    /** Maximum number of buffered messages. Defaults to 10000. */
    readonly limit?: number;
    /** Defaults to `error`, which ends the subscription. */
    readonly overflow?: OverflowPolicy;
  };
  /**
   * Called when the handler throws or rejects, and when the subscription
   * fails. Defaults to emitting the client's `error` event.
   */
  readonly onError?: (error: unknown, message?: StompMessage) => void;
}

/** @internal Hooks the client provides to its subscriptions. */
export interface SubscriptionHost {
  readonly remove: (subscription: Subscription) => void;
  readonly reportError: (error: unknown, subscription: Subscription, message?: StompMessage) => void;
}

/** @internal */
export const ATTACH = Symbol('attach');
/** @internal */
export const DETACH = Symbol('detach');
/** @internal */
export const CLOSE = Symbol('close');

/**
 * A subscription that survives reconnections: the client re-subscribes it,
 * with the same id, every time a new session is established.
 *
 * Messages are fed to the handler through a dispatcher that keeps at most
 * `concurrency` handler calls in flight, buffers messages that cannot be
 * handled yet, and can be paused.
 *
 * When a session ends, buffered messages received with a `client` ack mode
 * are discarded: they can no longer be acknowledged, and the broker will
 * redeliver them. Messages received in `auto` mode are still handled.
 */
export class Subscription {
  readonly id: string;
  readonly destination: string;
  readonly ack: AckMode;

  readonly #host: SubscriptionHost;
  readonly #headers: FrameHeaders | undefined;
  readonly #receipt: boolean;
  readonly #onError: SubscribeOptions['onError'];
  readonly #dispatcher: Dispatcher<StompMessage>;
  readonly #ready = Promise.withResolvers<void>();
  #sessionSubscription: SessionSubscription | undefined;
  #active = true;
  #readySettled = false;

  /** @internal Created by `Client.subscribe`. */
  constructor(
    host: SubscriptionHost,
    id: string,
    destination: string,
    handler: MessageHandler,
    options: SubscribeOptions,
  ) {
    this.#host = host;
    this.id = id;
    this.destination = destination;
    this.ack = options.ack ?? 'auto';
    this.#headers = options.headers;
    this.#receipt = options.receipt ?? false;
    this.#onError = options.onError;
    this.#ready.promise.catch(() => {});
    const limit = options.buffer?.limit ?? 10_000;
    this.#dispatcher = new Dispatcher<StompMessage>({
      handler,
      ...(options.concurrency === undefined ? {} : { concurrency: options.concurrency }),
      limit,
      overflow: options.buffer?.overflow ?? 'error',
      onError: (error, message) => this.#report(error, message),
      onOverflow: () => this.#fail(new StompBufferOverflowError(limit)),
    });
  }

  /** False once unsubscribed, failed, or the client closed. */
  get active(): boolean {
    return this.#active;
  }

  /** Whether the subscription is currently established on a live session. */
  get established(): boolean {
    return this.#sessionSubscription !== undefined;
  }

  /**
   * Resolves the first time the subscription is established (confirmed by
   * a receipt if requested); rejects if it ends before that.
   */
  get ready(): Promise<void> {
    return this.#ready.promise;
  }

  get paused(): boolean {
    return this.#dispatcher.paused;
  }

  /** Number of messages waiting to be handled. */
  get buffered(): number {
    return this.#dispatcher.buffered;
  }

  /** Number of handler calls in progress. */
  get inFlight(): number {
    return this.#dispatcher.inFlight;
  }

  /**
   * Stops calling the handler. Incoming messages are buffered, up to the
   * buffer limit. Handler calls in progress complete normally.
   */
  pause(): void {
    this.#dispatcher.pause();
  }

  /** Resumes calling the handler, starting with buffered messages. */
  resume(): void {
    this.#dispatcher.resume();
  }

  /** Resolves once no messages are buffered or being handled. */
  idle(): Promise<void> {
    return this.#dispatcher.idle();
  }

  /**
   * Ends the subscription: sends `UNSUBSCRIBE` if a session is live, and
   * discards buffered messages. Handler calls in progress complete normally.
   */
  async unsubscribe(options: ReceiptOptions = {}): Promise<void> {
    if (!this.#active) {
      return;
    }
    const sessionSubscription = this.#sessionSubscription;
    this[CLOSE]();
    await sessionSubscription?.unsubscribe(options);
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.unsubscribe();
  }

  /** @internal Establishes the subscription on a new session. */
  [ATTACH](session: ClientSession): void {
    if (!this.#active) {
      return;
    }
    try {
      const sessionSubscription = session.subscribe(
        this.destination,
        (message) => {
          this.#dispatcher.push(message);
        },
        {
          id: this.id,
          ack: this.ack,
          receipt: this.#receipt,
          ...(this.#headers === undefined ? {} : { headers: this.#headers }),
        },
      );
      this.#sessionSubscription = sessionSubscription;
      sessionSubscription.ready.then(
        () => this.#settleReady(),
        (error: unknown) => {
          // A session that closed in the meantime is not this subscription's failure.
          if (session.state === 'connected') {
            this.#fail(error);
          }
        },
      );
    } catch (error) {
      this.#fail(error);
    }
  }

  /** @internal Called when the session the subscription lives on has ended. */
  [DETACH](): void {
    this.#sessionSubscription = undefined;
    if (this.ack !== 'auto') {
      this.#dispatcher.clear();
    }
  }

  /** @internal Ends the subscription without sending anything. */
  [CLOSE](reason?: unknown): void {
    if (!this.#active) {
      return;
    }
    this.#active = false;
    this.#sessionSubscription = undefined;
    this.#dispatcher.close();
    this.#host.remove(this);
    if (!this.#readySettled) {
      this.#readySettled = true;
      this.#ready.reject(reason ?? new Error('Subscription ended before it was established'));
    }
  }

  #settleReady(): void {
    if (!this.#readySettled) {
      this.#readySettled = true;
      this.#ready.resolve();
    }
  }

  /** Ends the subscription because of an error, and reports it. */
  #fail(error: unknown): void {
    if (!this.#active) {
      return;
    }
    const sessionSubscription = this.#sessionSubscription;
    this[CLOSE](error);
    sessionSubscription?.unsubscribe().catch(() => {});
    this.#report(error);
  }

  #report(error: unknown, message?: StompMessage): void {
    if (this.#onError !== undefined) {
      try {
        this.#onError(error, message);
        return;
      } catch (handlerError) {
        error = handlerError;
      }
    }
    this.#host.reportError(error, this, message);
  }
}
