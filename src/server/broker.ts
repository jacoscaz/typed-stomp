import type { FrameHeaders, StompVersion } from '../codec/frame.ts';
import type { AckMode } from '../frames/types.ts';

/**
 * The broker's view of a client session.
 */
export interface BrokerSession {
  /** Unique session id, also sent to the client in `CONNECTED`. */
  readonly id: string;
  /** Negotiated STOMP version. */
  readonly version: StompVersion;
  /** Whatever the `authenticate` hook returned. */
  readonly principal: unknown;
  /** The `host` header sent by the client, if any. */
  readonly host: string | undefined;
  /** Application-provided context, e.g. the HTTP upgrade request. */
  readonly context: unknown;
  /** Whether the session can still receive messages. */
  readonly active: boolean;
  /**
   * Sends a `MESSAGE` frame for one of the session's subscriptions. The
   * session fills in the `subscription`, `message-id` and `ack` headers as
   * required by the negotiated version. Returns false if the session is no
   * longer active or the subscription does not exist.
   */
  deliver(subscriptionId: string, message: OutboundMessage): boolean;
  /**
   * Closes the session. With an error, sends an `ERROR` frame first.
   */
  close(error?: SessionError): void;
}

export interface SessionError {
  readonly message: string;
  readonly headers?: FrameHeaders;
  readonly body?: string | Uint8Array;
}

export interface SubscriptionInfo {
  /** Subscription id, unique within the session. */
  readonly id: string;
  readonly destination: string;
  readonly ack: AckMode;
  /** All `SUBSCRIBE` headers, for broker-specific options (e.g. `prefetch-count`). */
  readonly headers: FrameHeaders;
}

/** A message sent by a client. */
export interface InboundMessage {
  readonly destination: string;
  /**
   * `SEND` headers, minus `destination`, `receipt`, `transaction` and
   * `content-length`.
   */
  readonly headers: FrameHeaders;
  readonly body: Uint8Array;
}

/** A message to deliver to a subscription. */
export interface OutboundMessage {
  /** Unique within the subscription; used to acknowledge the message. */
  readonly messageId: string;
  readonly destination: string;
  /** Additional headers (e.g. `content-type`). */
  readonly headers?: FrameHeaders;
  readonly body: string | Uint8Array;
}

/**
 * Identifies a delivered message in `ACK` and `NACK`, whatever the version.
 */
export interface AckRef {
  readonly subscriptionId: string;
  readonly messageId: string;
}

type MaybePromise<T> = T | Promise<T>;

/**
 * Routes messages between sessions. Implement it to plug the STOMP server
 * into any backend; see `InMemoryBroker` for a reference implementation.
 *
 * Throwing (or rejecting) from any method rejects the frame being processed:
 * the client receives an `ERROR` frame with the error's message, and the
 * session is closed. Frames of a session are processed one at a time, in
 * order: the next frame is not handed to the broker until the promise
 * returned for the previous one settles.
 *
 * Transactions: if the broker implements `begin`, `commit` and `abort`,
 * transactional `SEND`, `ACK` and `NACK` frames are passed to it with their
 * transaction id. Otherwise the session buffers them and replays them,
 * without transaction ids, on `COMMIT`.
 */
export interface Broker {
  /** A session has been authenticated, before `CONNECTED` is sent. */
  connect?(session: BrokerSession): MaybePromise<void>;
  /**
   * A session has ended, for whatever reason. Its subscriptions are gone;
   * unacknowledged messages should be made available again.
   */
  disconnect(session: BrokerSession): MaybePromise<void>;
  subscribe(session: BrokerSession, subscription: SubscriptionInfo): MaybePromise<void>;
  unsubscribe(session: BrokerSession, subscriptionId: string): MaybePromise<void>;
  publish(session: BrokerSession, message: InboundMessage, transaction?: string): MaybePromise<void>;
  ack(session: BrokerSession, ref: AckRef, transaction?: string): MaybePromise<void>;
  nack(session: BrokerSession, ref: AckRef, transaction?: string): MaybePromise<void>;
  begin?(session: BrokerSession, transaction: string): MaybePromise<void>;
  commit?(session: BrokerSession, transaction: string): MaybePromise<void>;
  abort?(session: BrokerSession, transaction: string): MaybePromise<void>;
}
