import { TransportBase } from './base.ts';
import { TransportClosedError } from './errors.ts';
import type { TransportData } from './types.ts';

/**
 * WebSocket subprotocols for STOMP, in order of preference.
 */
export const STOMP_SUBPROTOCOLS: readonly string[] = Object.freeze(['v12.stomp', 'v11.stomp', 'v10.stomp']);

const CONNECTING = 0;
const OPEN = 1;

/**
 * The subset of the standard `WebSocket` API used by this library. Browser,
 * Node.js, Deno and Bun `WebSocket`s match it, and so does the `WebSocket`
 * class of the `ws` package (both client and server-side sockets).
 */
export interface WebSocketLike {
  readonly readyState: number;
  readonly protocol?: string;
  binaryType: string;
  send(data: string | Uint8Array<ArrayBuffer>): void;
  close(code?: number, reason?: string): void;
  // Listener types are loose on purpose, so that implementations with
  // stricter, overloaded signatures remain assignable.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  addEventListener(type: string, listener: (event: any) => void): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  removeEventListener(type: string, listener: (event: any) => void): void;
}

/**
 * A `WebSocket` constructor, such as the global `WebSocket` or the
 * `WebSocket` class of the `ws` package. `O` is the type of the optional,
 * implementation-specific third constructor argument (e.g. `ws` client
 * options such as custom headers); browsers ignore it.
 */
export type WebSocketConstructor<O = never> = new (
  url: string | URL,
  protocols?: string | string[],
  options?: O,
) => WebSocketLike;

export interface WebSocketTransportOptions<O = never> {
  /**
   * The `WebSocket` implementation to instantiate. Defaults to
   * `globalThis.WebSocket`.
   */
  readonly WebSocket?: WebSocketConstructor<O>;
  /** Third constructor argument, passed as-is to the implementation. */
  readonly webSocketOptions?: O;
  /** Defaults to {@link STOMP_SUBPROTOCOLS}. */
  readonly protocols?: string | readonly string[];
  /** Aborts the connection attempt. */
  readonly signal?: AbortSignal;
}

interface CloseEventLike {
  readonly code?: number;
  readonly reason?: string;
  readonly wasClean?: boolean;
}

/**
 * A {@link Transport} backed by an open `WebSocket`.
 */
export class WebSocketTransport extends TransportBase {
  readonly socket: WebSocketLike;
  #error: unknown;

  /**
   * Wraps an **open** WebSocket. Use {@link WebSocketTransport.open} to wrap
   * a socket that may still be connecting.
   */
  constructor(socket: WebSocketLike) {
    super();
    if (socket.readyState !== OPEN) {
      throw new TransportClosedError('WebSocket is not open; use WebSocketTransport.open() to wait for it');
    }
    this.socket = socket;
    socket.binaryType = 'arraybuffer';
    socket.addEventListener('message', (event: { data?: unknown }) => {
      this.#onMessage(event.data);
    });
    socket.addEventListener('error', (event: unknown) => {
      this.#error ??= toError(event);
    });
    socket.addEventListener('close', (event: CloseEventLike) => {
      this.finish({
        clean: event.wasClean ?? false,
        ...(event.code === undefined ? {} : { code: event.code }),
        ...(event.reason === undefined ? {} : { reason: event.reason }),
        ...(this.#error === undefined ? {} : { error: this.#error }),
      });
    });
  }

  /**
   * Opens a WebSocket to `url` using the given (or the global) `WebSocket`
   * implementation, and resolves once it is open.
   *
   * ```ts
   * await WebSocketTransport.connect('wss://broker/ws');                  // global WebSocket
   * await WebSocketTransport.connect('wss://broker/ws', { WebSocket: ws }); // `ws` package
   * ```
   */
  static async connect<O = never>(
    url: string | URL,
    options: WebSocketTransportOptions<O> = {},
  ): Promise<WebSocketTransport> {
    options.signal?.throwIfAborted();
    const WebSocketImpl: WebSocketConstructor<O> | undefined =
      options.WebSocket ?? (globalThis as { WebSocket?: WebSocketConstructor<O> }).WebSocket;
    if (WebSocketImpl === undefined) {
      throw new TypeError('No WebSocket implementation available; pass one via the WebSocket option');
    }
    const raw = options.protocols ?? STOMP_SUBPROTOCOLS;
    const protocols = typeof raw === 'string' ? raw : [...raw];
    const socket =
      options.webSocketOptions === undefined
        ? new WebSocketImpl(url, protocols)
        : new WebSocketImpl(url, protocols, options.webSocketOptions);
    return WebSocketTransport.open(socket, options.signal);
  }

  /**
   * Waits for an existing WebSocket to open, then wraps it. Useful on the
   * server side, e.g. with Deno's `upgradeWebSocket`.
   */
  static async open(socket: WebSocketLike, signal?: AbortSignal): Promise<WebSocketTransport> {
    await waitForOpen(socket, signal);
    return new WebSocketTransport(socket);
  }

  /** The negotiated WebSocket subprotocol, or an empty string. */
  get protocol(): string {
    return this.socket.protocol ?? '';
  }

  protected sendData(data: TransportData): void {
    if (typeof data === 'string' || data.buffer instanceof ArrayBuffer) {
      this.socket.send(data as string | Uint8Array<ArrayBuffer>);
    } else {
      // Views over SharedArrayBuffers cannot be sent as-is.
      this.socket.send(data.slice());
    }
  }

  protected closeTransport(code?: number, reason?: string): void {
    if (code === undefined) {
      this.socket.close();
      return;
    }
    try {
      this.socket.close(code, reason);
    } catch {
      // Browsers only accept 1000 and 3000-4999 from applications.
      this.socket.close();
    }
  }

  #onMessage(data: unknown): void {
    if (typeof data === 'string') {
      this.receive(data);
    } else if (data instanceof ArrayBuffer) {
      this.receive(new Uint8Array(data));
    } else if (ArrayBuffer.isView(data)) {
      this.receive(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    } else {
      this.#error ??= new TypeError('Unsupported WebSocket message data type');
      this.close(1003, 'Unsupported data');
    }
  }
}

function waitForOpen(socket: WebSocketLike, signal: AbortSignal | undefined): Promise<void> {
  if (socket.readyState === OPEN) {
    return Promise.resolve();
  }
  if (socket.readyState !== CONNECTING) {
    return Promise.reject(new TransportClosedError('WebSocket is closing or closed'));
  }
  return new Promise<void>((resolve, reject) => {
    const cleanup = () => {
      socket.removeEventListener('open', onOpen);
      socket.removeEventListener('error', onError);
      socket.removeEventListener('close', onClose);
      signal?.removeEventListener('abort', onAbort);
    };
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = (event: unknown) => {
      cleanup();
      reject(new TransportClosedError('WebSocket failed to open', { cause: toError(event) }));
    };
    const onClose = (event: CloseEventLike) => {
      cleanup();
      reject(new TransportClosedError(`WebSocket closed before opening (code ${event.code ?? 'unknown'})`));
    };
    const onAbort = () => {
      cleanup();
      // Closing a connecting socket makes some implementations (e.g. `ws`)
      // emit an error, which must not go unhandled.
      socket.addEventListener('error', ignore);
      socket.close();
      reject(signal?.reason);
    };
    if (signal?.aborted) {
      onAbort();
      return;
    }
    socket.addEventListener('open', onOpen);
    socket.addEventListener('error', onError);
    socket.addEventListener('close', onClose);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

const ignore = (): void => {};

function toError(event: unknown): Error {
  if (typeof event === 'object' && event !== null) {
    if ('error' in event && event.error instanceof Error) {
      return event.error;
    }
    if ('message' in event && typeof event.message === 'string' && event.message !== '') {
      return new Error(event.message);
    }
  }
  return new Error('WebSocket error');
}
