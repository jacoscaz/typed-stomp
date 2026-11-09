import type { FrameHeaders, StompVersion } from '../../src/codec/frame.ts';
import { Connection, type ConnectionCloseInfo } from '../../src/connection/connection.ts';
import { negotiateVersion } from '../../src/connection/negotiate.ts';
import type { ClientFrame } from '../../src/frames/types.ts';
import { MemoryTransport } from '../../src/transport/memory.ts';
import type { Transport } from '../../src/transport/types.ts';
import { flush } from '../helpers.ts';

export interface FakeServerOptions {
  /** Versions the server supports. Defaults to all. */
  versions?: StompVersion[];
  /** `heart-beat` header of CONNECTED. Defaults to `0,0`. */
  heartBeat?: string;
  /** Return an error message to answer CONNECT with ERROR. */
  rejectConnect?: (frame: ClientFrame, connectionIndex: number) => string | undefined;
  /** Do not answer CONNECT at all. */
  silent?: boolean;
  /** Override the version returned in CONNECTED. */
  forceVersion?: string;
  /** Answer frames carrying a receipt header with RECEIPT. Defaults to true. */
  autoReceipt?: boolean;
}

/**
 * A scripted STOMP server for client tests, built on the server-side
 * `Connection` over in-memory transports.
 */
export class FakeServer {
  readonly options: FakeServerOptions;
  readonly connections: FakeServerConnection[] = [];

  constructor(options: FakeServerOptions = {}) {
    this.options = options;
  }

  /** Creates a connected transport pair; returns the client end. */
  transport(): MemoryTransport {
    const [clientEnd, serverEnd] = MemoryTransport.pair();
    this.accept(serverEnd);
    return clientEnd;
  }

  /** Serves a connection over an existing server-side transport. */
  accept(transport: Transport): FakeServerConnection {
    const connection = new FakeServerConnection(this, transport, this.connections.length);
    this.connections.push(connection);
    return connection;
  }

  get last(): FakeServerConnection {
    const connection = this.connections.at(-1);
    if (connection === undefined) {
      throw new Error('No connections yet');
    }
    return connection;
  }

  /** Waits until `count` connections have been made. */
  async waitForConnections(count: number): Promise<void> {
    await until(() => this.connections.length >= count);
  }
}

export class FakeServerConnection {
  readonly server: FakeServer;
  readonly transport: Transport;
  readonly connection: Connection<'server'>;
  readonly frames: ClientFrame[] = [];
  readonly index: number;
  closeInfo: ConnectionCloseInfo | undefined;
  #messageCounter = 0;

  constructor(server: FakeServer, transport: Transport, index: number) {
    this.server = server;
    this.transport = transport;
    this.index = index;
    this.connection = new Connection({
      role: 'server',
      transport,
      onFrame: (frame) => this.#receive(frame),
      onClose: (info) => {
        this.closeInfo = info;
      },
    });
  }

  get closed(): boolean {
    return this.closeInfo !== undefined;
  }

  /** Commands received so far. */
  get commands(): string[] {
    return this.frames.map((frame) => frame.command);
  }

  /** Waits for a frame matching `command` (and `predicate`), returns it. */
  async waitFor(command: string, predicate: (frame: ClientFrame) => boolean = () => true): Promise<ClientFrame> {
    let found: ClientFrame | undefined;
    await until(() => {
      found = this.frames.find((frame) => frame.command === command && predicate(frame));
      return found !== undefined;
    });
    return found!;
  }

  /** Sends a MESSAGE for the given subscription. */
  message(subscription: string, body: string, headers: FrameHeaders = {}): string {
    const messageId = `m-${this.index}-${this.#messageCounter++}`;
    const subscribe = this.frames.find(
      (frame) => frame.command === 'SUBSCRIBE' && frame.headers['id'] === subscription,
    );
    this.connection.send({
      command: 'MESSAGE',
      headers: {
        destination: subscribe?.headers['destination'] ?? '/unknown',
        'message-id': messageId,
        subscription,
        ack: this.connection.version === '1.2' ? `ack-${messageId}` : undefined,
        ...headers,
      },
      body,
    });
    return messageId;
  }

  /** Sends an ERROR frame and closes. */
  error(message: string, headers: FrameHeaders = {}): void {
    this.connection.fail(new Error(message), { command: 'ERROR', headers: { message, ...headers } });
  }

  /** Simulates an abrupt connection loss. */
  kill(): void {
    if (!(this.transport instanceof MemoryTransport)) {
      throw new TypeError('Only in-memory connections can be killed');
    }
    this.transport.terminate();
  }

  #receive(frame: ClientFrame): void {
    this.frames.push(frame);
    const { options } = this.server;
    if (frame.command === 'CONNECT' || frame.command === 'STOMP') {
      if (options.silent) {
        return;
      }
      const rejection = options.rejectConnect?.(frame, this.index);
      if (rejection !== undefined) {
        this.error(rejection);
        return;
      }
      const version = negotiateVersion(frame.headers['accept-version'], options.versions ?? ['1.0', '1.1', '1.2']);
      if (version === undefined) {
        this.error('No common version');
        return;
      }
      this.connection.send({
        command: 'CONNECTED',
        headers: {
          version: options.forceVersion ?? version,
          'heart-beat': options.heartBeat ?? '0,0',
          server: 'fake/1.0',
          session: `session-${this.index}`,
        },
      });
      this.connection.version = version;
      return;
    }
    const receipt = frame.headers['receipt'];
    if (receipt !== undefined && options.autoReceipt !== false) {
      this.connection.send({ command: 'RECEIPT', headers: { 'receipt-id': receipt } });
    }
    if (frame.command === 'DISCONNECT') {
      this.connection.close();
    }
  }
}

/** Polls `condition` across event-loop turns. Works with mocked timers. */
export async function until(condition: () => boolean, maxTurns = 2000): Promise<void> {
  for (let turn = 0; turn < maxTurns; turn += 1) {
    if (condition()) {
      return;
    }
    await flush();
  }
  throw new Error('Condition not met');
}

/** Waits for a real-time delay. */
export const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
