/**
 * Data carried by a single transport message: text or binary.
 */
export type TransportData = string | Uint8Array;

export interface TransportCloseInfo {
  /** WebSocket close code, if any. */
  readonly code?: number;
  /** WebSocket close reason, if any. */
  readonly reason?: string;
  /** Whether the transport closed via an orderly closing handshake. */
  readonly clean: boolean;
  /** The error that caused the transport to close, if any. */
  readonly error?: unknown;
}

export interface TransportHandlers {
  onMessage(data: TransportData): void;
  onClose(info: TransportCloseInfo): void;
}

/**
 * A message-oriented, bidirectional channel between a STOMP client and a
 * STOMP server, usually a WebSocket.
 *
 * Contract:
 * - A transport is **open when constructed**. Establishing the underlying
 *   connection (e.g. waiting for a WebSocket's `open` event) happens before
 *   a transport is created.
 * - {@link Transport.attach} is called exactly once. Events that occur
 *   before that are buffered and delivered on attach, in order.
 * - `onClose` is called exactly once; no `onMessage` calls follow it.
 * - {@link Transport.send} throws once the transport is closed or closing.
 * - {@link Transport.close} is idempotent.
 */
export interface Transport {
  /** Whether the transport is closing or closed. */
  readonly closed: boolean;
  attach(handlers: TransportHandlers): void;
  send(data: TransportData): void;
  close(code?: number, reason?: string): void;
}
