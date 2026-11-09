import { Client, type ClientOptions } from '../../src/client/client.ts';
import { ClientSession, type ClientSessionOptions } from '../../src/client/session.ts';
import type {
  AckRef,
  Broker,
  BrokerSession,
  InboundMessage,
  SubscriptionInfo,
} from '../../src/server/broker.ts';
import { StompServer, type StompServerOptions } from '../../src/server/server.ts';
import { MemoryTransport } from '../../src/transport/memory.ts';

export type BrokerCall =
  | { method: 'connect' | 'disconnect'; session: BrokerSession }
  | { method: 'subscribe'; session: BrokerSession; subscription: SubscriptionInfo }
  | { method: 'unsubscribe'; session: BrokerSession; subscriptionId: string }
  | { method: 'publish'; session: BrokerSession; message: InboundMessage; transaction: string | undefined }
  | { method: 'ack' | 'nack'; session: BrokerSession; ref: AckRef; transaction: string | undefined }
  | { method: 'begin' | 'commit' | 'abort'; session: BrokerSession; transaction: string };

/**
 * A broker that records calls. `fail` makes a method throw; transactions
 * are only supported when `transactions` is true.
 */
export function recordingBroker(options: { transactions?: boolean; fail?: Partial<Record<string, Error>> } = {}) {
  const calls: BrokerCall[] = [];
  const check = (method: string) => {
    const error = options.fail?.[method];
    if (error !== undefined) {
      throw error;
    }
  };
  const broker: Broker = {
    connect: (session) => {
      check('connect');
      calls.push({ method: 'connect', session });
    },
    disconnect: (session) => {
      calls.push({ method: 'disconnect', session });
    },
    subscribe: (session, subscription) => {
      check('subscribe');
      calls.push({ method: 'subscribe', session, subscription });
    },
    unsubscribe: (session, subscriptionId) => {
      calls.push({ method: 'unsubscribe', session, subscriptionId });
    },
    publish: async (session, message, transaction) => {
      // Asynchronous on purpose, to exercise in-order processing.
      await Promise.resolve();
      check('publish');
      calls.push({ method: 'publish', session, message, transaction });
    },
    ack: (session, ref, transaction) => {
      calls.push({ method: 'ack', session, ref, transaction });
    },
    nack: (session, ref, transaction) => {
      calls.push({ method: 'nack', session, ref, transaction });
    },
    ...(options.transactions
      ? {
          begin: (session: BrokerSession, transaction: string) => {
            calls.push({ method: 'begin', session, transaction });
          },
          commit: (session: BrokerSession, transaction: string) => {
            calls.push({ method: 'commit', session, transaction });
          },
          abort: (session: BrokerSession, transaction: string) => {
            calls.push({ method: 'abort', session, transaction });
          },
        }
      : {}),
  };
  return { broker, calls, methods: () => calls.map((call) => call.method) };
}

/** Creates a server and returns a factory of in-memory client transports. */
export function serve(options: StompServerOptions) {
  const server = new StompServer(options);
  const transport = (context?: unknown) => {
    const [clientEnd, serverEnd] = MemoryTransport.pair();
    server.accept(serverEnd, context);
    return clientEnd;
  };
  return { server, transport };
}

/** Connects a ClientSession to a server. */
export function session(
  transport: () => MemoryTransport,
  options: Partial<ClientSessionOptions> = {},
): Promise<ClientSession> {
  return ClientSession.connect({ transport: transport(), heartbeat: { outgoing: 0, incoming: 0 }, ...options });
}

/** Creates a Client for a server. */
export function client(transport: () => MemoryTransport, options: Partial<ClientOptions> = {}): Client {
  return new Client({
    transport: () => transport(),
    heartbeat: { outgoing: 0, incoming: 0 },
    reconnect: false,
    ...options,
  });
}
