import type { AckMode, MessageFrame, MessageHeaders } from '../frames/types.ts';

export interface AckOptions {
  /** Acknowledge as part of a transaction. */
  readonly transaction?: string;
  /** Wait for the server to confirm with a `RECEIPT`. */
  readonly receipt?: boolean;
}

/** Sends the `ACK`/`NACK` frame for a message. */
export type Acknowledger = (kind: 'ACK' | 'NACK', message: StompMessage, options: AckOptions) => Promise<void>;

const decoder = new TextDecoder();

/**
 * A message received through a subscription.
 */
export class StompMessage {
  readonly frame: MessageFrame;
  /** Ack mode of the subscription the message was received through. */
  readonly ackMode: AckMode;
  readonly #acknowledge: Acknowledger;
  #settled: Promise<void> | undefined;

  constructor(frame: MessageFrame, ackMode: AckMode, acknowledge: Acknowledger) {
    this.frame = frame;
    this.ackMode = ackMode;
    this.#acknowledge = acknowledge;
  }

  get headers(): MessageHeaders {
    return this.frame.headers;
  }

  get body(): Uint8Array {
    return this.frame.body;
  }

  get destination(): string {
    return this.frame.headers.destination;
  }

  get messageId(): string {
    return this.frame.headers['message-id'];
  }

  /** The subscription id; always present from STOMP 1.1 onwards. */
  get subscription(): string | undefined {
    return this.frame.headers.subscription;
  }

  get contentType(): string | undefined {
    return this.frame.headers['content-type'];
  }

  /** Whether the message must be acknowledged (ack mode is not `auto`). */
  get ackRequired(): boolean {
    return this.ackMode !== 'auto';
  }

  /** Whether `ack()` or `nack()` has been called. */
  get acknowledged(): boolean {
    return this.#settled !== undefined;
  }

  /** The body decoded as UTF-8 text. */
  text(): string {
    return decoder.decode(this.frame.body);
  }

  /**
   * The body parsed as JSON. `T` is not validated: validating payloads is
   * the application's responsibility.
   */
  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }

  /**
   * Acknowledges the message. A no-op in `auto` ack mode; only the first
   * call to `ack()` or `nack()` has effect.
   */
  ack(options: AckOptions = {}): Promise<void> {
    return this.#settle('ACK', options);
  }

  /**
   * Rejects the message (STOMP 1.1+). A no-op in `auto` ack mode; only the
   * first call to `ack()` or `nack()` has effect.
   */
  nack(options: AckOptions = {}): Promise<void> {
    return this.#settle('NACK', options);
  }

  #settle(kind: 'ACK' | 'NACK', options: AckOptions): Promise<void> {
    if (!this.ackRequired) {
      return Promise.resolve();
    }
    this.#settled ??= this.#acknowledge(kind, this, options);
    return this.#settled;
  }
}
