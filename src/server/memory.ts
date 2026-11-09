import type { FrameHeaders } from '../codec/frame.ts';
import type { AckMode } from '../frames/types.ts';
import type { AckRef, Broker, BrokerSession, InboundMessage, SubscriptionInfo } from './broker.ts';

export interface InMemoryBrokerOptions {
  /**
   * Destinations starting with one of these prefixes are queues; all others
   * are topics. Defaults to `['/queue/']`.
   */
  readonly queuePrefixes?: readonly string[];
  /**
   * Default maximum number of unacknowledged messages per subscription in
   * `client` and `client-individual` ack modes. Defaults to `Infinity`.
   * Subscriptions can override it with a `prefetch-count` header.
   */
  readonly prefetch?: number;
}

interface StoredMessage {
  readonly id: string;
  readonly destination: string;
  readonly headers: FrameHeaders;
  readonly body: Uint8Array;
  redelivered: boolean;
}

interface Queue {
  readonly destination: string;
  readonly pending: StoredMessage[];
  readonly consumers: Subscription[];
  next: number;
}

interface Subscription {
  readonly session: BrokerSession;
  readonly id: string;
  readonly destination: string;
  readonly ack: AckMode;
  readonly prefetch: number;
  /** Delivered, unacknowledged messages, in delivery order. */
  readonly unacked: Map<string, StoredMessage>;
  /** Topic messages waiting for prefetch capacity. */
  readonly backlog: StoredMessage[];
  readonly queue: Queue | undefined;
}

type Operation = () => void;

const encoder = new TextEncoder();

/**
 * A minimal, single-process broker, for development, tests and small
 * deployments. Nothing is persisted.
 *
 * - **Topics** (the default) fan out each message to all current
 *   subscribers. Messages published while nobody is subscribed are lost.
 * - **Queues** (destinations starting with a queue prefix, `/queue/` by
 *   default) hand each message to one consumer, round-robin. Messages are
 *   kept until a consumer is available.
 * - All three ack modes are supported. In `client` mode an `ACK` or `NACK`
 *   covers all earlier unacknowledged messages of the subscription.
 * - Unacknowledged queue messages are redelivered (with a `redelivered:
 *   true` header) when nacked, or when their consumer unsubscribes or
 *   disconnects. Unacknowledged topic messages are dropped in those cases.
 * - Prefetch limits the number of unacknowledged messages per subscription.
 * - Transactions are applied atomically on commit.
 */
export class InMemoryBroker implements Broker {
  readonly #queuePrefixes: readonly string[];
  readonly #defaultPrefetch: number;
  readonly #sessions = new Map<BrokerSession, Map<string, Subscription>>();
  readonly #topics = new Map<string, Set<Subscription>>();
  readonly #queues = new Map<string, Queue>();
  readonly #transactions = new Map<BrokerSession, Map<string, Operation[]>>();
  #counter = 0;

  constructor(options: InMemoryBrokerOptions = {}) {
    this.#queuePrefixes = options.queuePrefixes ?? ['/queue/'];
    this.#defaultPrefetch = options.prefetch ?? Number.POSITIVE_INFINITY;
  }

  isQueue(destination: string): boolean {
    return this.#queuePrefixes.some((prefix) => destination.startsWith(prefix));
  }

  /** Number of messages waiting in a queue for a consumer. */
  pending(destination: string): number {
    return this.#queues.get(destination)?.pending.length ?? 0;
  }

  /** Number of subscriptions to a destination. */
  subscribers(destination: string): number {
    return this.isQueue(destination)
      ? (this.#queues.get(destination)?.consumers.length ?? 0)
      : (this.#topics.get(destination)?.size ?? 0);
  }

  /** Publishes a message from the application, outside of any session. */
  send(destination: string, body: string | Uint8Array = '', headers: FrameHeaders = {}): void {
    this.#route({ destination, headers, body: typeof body === 'string' ? encoder.encode(body) : body });
  }

  connect(session: BrokerSession): void {
    this.#sessions.set(session, new Map());
  }

  disconnect(session: BrokerSession): void {
    const subscriptions = this.#sessions.get(session);
    this.#sessions.delete(session);
    this.#transactions.delete(session);
    for (const subscription of subscriptions?.values() ?? []) {
      this.#remove(subscription);
    }
  }

  subscribe(session: BrokerSession, info: SubscriptionInfo): void {
    let subscriptions = this.#sessions.get(session);
    if (subscriptions === undefined) {
      subscriptions = new Map();
      this.#sessions.set(session, subscriptions);
    }
    const queue = this.isQueue(info.destination) ? this.#queue(info.destination) : undefined;
    const subscription: Subscription = {
      session,
      id: info.id,
      destination: info.destination,
      ack: info.ack,
      prefetch: parsePrefetch(info.headers['prefetch-count']) ?? this.#defaultPrefetch,
      unacked: new Map(),
      backlog: [],
      queue,
    };
    subscriptions.set(info.id, subscription);
    if (queue === undefined) {
      let topic = this.#topics.get(info.destination);
      if (topic === undefined) {
        topic = new Set();
        this.#topics.set(info.destination, topic);
      }
      topic.add(subscription);
    } else {
      queue.consumers.push(subscription);
      this.#dispatch(queue);
    }
  }

  unsubscribe(session: BrokerSession, subscriptionId: string): void {
    const subscriptions = this.#sessions.get(session);
    const subscription = subscriptions?.get(subscriptionId);
    if (subscription !== undefined) {
      subscriptions?.delete(subscriptionId);
      this.#remove(subscription);
    }
  }

  publish(session: BrokerSession, message: InboundMessage, transaction?: string): void {
    this.#run(session, transaction, () => this.#route(message));
  }

  ack(session: BrokerSession, ref: AckRef, transaction?: string): void {
    this.#run(session, transaction, () => {
      const subscription = this.#sessions.get(session)?.get(ref.subscriptionId);
      if (subscription !== undefined) {
        this.#settle(subscription, ref.messageId);
        this.#refill(subscription);
      }
    });
  }

  nack(session: BrokerSession, ref: AckRef, transaction?: string): void {
    this.#run(session, transaction, () => {
      const subscription = this.#sessions.get(session)?.get(ref.subscriptionId);
      if (subscription === undefined) {
        return;
      }
      const settled = this.#settle(subscription, ref.messageId);
      if (subscription.queue !== undefined) {
        this.#requeue(subscription.queue, settled);
      }
      this.#refill(subscription);
    });
  }

  begin(session: BrokerSession, transaction: string): void {
    let transactions = this.#transactions.get(session);
    if (transactions === undefined) {
      transactions = new Map();
      this.#transactions.set(session, transactions);
    }
    if (transactions.has(transaction)) {
      throw new Error(`Transaction "${transaction}" has already begun`);
    }
    transactions.set(transaction, []);
  }

  commit(session: BrokerSession, transaction: string): void {
    const operations = this.#takeTransaction(session, transaction);
    for (const operation of operations) {
      operation();
    }
  }

  abort(session: BrokerSession, transaction: string): void {
    this.#takeTransaction(session, transaction);
  }

  #run(session: BrokerSession, transaction: string | undefined, operation: Operation): void {
    if (transaction === undefined) {
      operation();
      return;
    }
    const operations = this.#transactions.get(session)?.get(transaction);
    if (operations === undefined) {
      throw new Error(`Unknown transaction "${transaction}"`);
    }
    operations.push(operation);
  }

  #takeTransaction(session: BrokerSession, transaction: string): Operation[] {
    const transactions = this.#transactions.get(session);
    const operations = transactions?.get(transaction);
    if (operations === undefined) {
      throw new Error(`Unknown transaction "${transaction}"`);
    }
    transactions?.delete(transaction);
    return operations;
  }

  #queue(destination: string): Queue {
    let queue = this.#queues.get(destination);
    if (queue === undefined) {
      queue = { destination, pending: [], consumers: [], next: 0 };
      this.#queues.set(destination, queue);
    }
    return queue;
  }

  #route(message: InboundMessage): void {
    const stored: StoredMessage = {
      id: `msg-${this.#counter++}`,
      destination: message.destination,
      headers: message.headers,
      body: message.body,
      redelivered: false,
    };
    if (this.isQueue(message.destination)) {
      const queue = this.#queue(message.destination);
      queue.pending.push(stored);
      this.#dispatch(queue);
      return;
    }
    for (const subscription of this.#topics.get(message.destination) ?? []) {
      if (this.#hasCapacity(subscription)) {
        this.#deliver(subscription, stored);
      } else {
        subscription.backlog.push(stored);
      }
    }
  }

  /** Hands pending queue messages to consumers with capacity, round-robin. */
  #dispatch(queue: Queue): void {
    while (queue.pending.length > 0) {
      const consumer = this.#nextConsumer(queue);
      if (consumer === undefined) {
        return;
      }
      const message = queue.pending.shift() as StoredMessage;
      if (!this.#deliver(consumer, message)) {
        queue.pending.unshift(message);
        return;
      }
    }
  }

  #nextConsumer(queue: Queue): Subscription | undefined {
    const count = queue.consumers.length;
    for (let i = 0; i < count; i += 1) {
      const index = (queue.next + i) % count;
      const consumer = queue.consumers[index] as Subscription;
      if (consumer.session.active && this.#hasCapacity(consumer)) {
        queue.next = (index + 1) % count;
        return consumer;
      }
    }
    return undefined;
  }

  #hasCapacity(subscription: Subscription): boolean {
    return subscription.ack === 'auto' || subscription.unacked.size < subscription.prefetch;
  }

  #deliver(subscription: Subscription, message: StoredMessage): boolean {
    const delivered = subscription.session.deliver(subscription.id, {
      messageId: message.id,
      destination: message.destination,
      headers: message.redelivered ? { ...message.headers, redelivered: 'true' } : message.headers,
      body: message.body,
    });
    if (delivered && subscription.ack !== 'auto') {
      subscription.unacked.set(message.id, message);
    }
    return delivered;
  }

  /**
   * Removes acknowledged messages from a subscription's unacknowledged set:
   * just `messageId`, or every earlier one too in `client` mode.
   */
  #settle(subscription: Subscription, messageId: string): StoredMessage[] {
    if (!subscription.unacked.has(messageId)) {
      return [];
    }
    if (subscription.ack !== 'client') {
      const message = subscription.unacked.get(messageId) as StoredMessage;
      subscription.unacked.delete(messageId);
      return [message];
    }
    const settled: StoredMessage[] = [];
    for (const [id, message] of subscription.unacked) {
      subscription.unacked.delete(id);
      settled.push(message);
      if (id === messageId) {
        break;
      }
    }
    return settled;
  }

  /** Delivers backlog or pending messages after capacity was freed. */
  #refill(subscription: Subscription): void {
    if (subscription.queue !== undefined) {
      this.#dispatch(subscription.queue);
      return;
    }
    while (subscription.backlog.length > 0 && this.#hasCapacity(subscription)) {
      if (!this.#deliver(subscription, subscription.backlog.shift() as StoredMessage)) {
        return;
      }
    }
  }

  /** Puts messages back at the front of a queue, marked as redelivered. */
  #requeue(queue: Queue, messages: readonly StoredMessage[]): void {
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const message = messages[i] as StoredMessage;
      message.redelivered = true;
      queue.pending.unshift(message);
    }
  }

  #remove(subscription: Subscription): void {
    const { queue } = subscription;
    if (queue === undefined) {
      const topic = this.#topics.get(subscription.destination);
      topic?.delete(subscription);
      if (topic?.size === 0) {
        this.#topics.delete(subscription.destination);
      }
      return;
    }
    const index = queue.consumers.indexOf(subscription);
    if (index >= 0) {
      queue.consumers.splice(index, 1);
      if (queue.next > index) {
        queue.next -= 1;
      }
      if (queue.next >= queue.consumers.length) {
        queue.next = 0;
      }
    }
    this.#requeue(queue, [...subscription.unacked.values()]);
    subscription.unacked.clear();
    this.#dispatch(queue);
  }
}

function parsePrefetch(value: string | undefined): number | undefined {
  if (value === undefined || !/^\d+$/.test(value)) {
    return undefined;
  }
  const prefetch = Number(value);
  return prefetch > 0 ? prefetch : undefined;
}
