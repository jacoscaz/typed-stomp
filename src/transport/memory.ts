import { TransportBase } from './base.ts';
import type { TransportCloseInfo, TransportData } from './types.ts';

/**
 * One end of an in-memory transport pair. Messages are delivered to the peer
 * asynchronously (on a microtask), in order, and binary data is copied, so
 * that the pair behaves like a real network connection.
 */
export class MemoryTransport extends TransportBase {
  #peer!: MemoryTransport;

  /** Creates two connected transports. */
  static pair(): [MemoryTransport, MemoryTransport] {
    const a = new MemoryTransport();
    const b = new MemoryTransport();
    a.#peer = b;
    b.#peer = a;
    return [a, b];
  }

  /**
   * Simulates an abrupt connection loss: both ends close without a closing
   * handshake (code 1006), optionally with an error.
   */
  terminate(error?: unknown): void {
    const info: TransportCloseInfo = { code: 1006, reason: '', clean: false, ...(error === undefined ? {} : { error }) };
    const peer = this.#peer;
    queueMicrotask(() => {
      this.finish(info);
      peer.finish(info);
    });
  }

  protected sendData(data: TransportData): void {
    const copy = typeof data === 'string' ? data : data.slice();
    const peer = this.#peer;
    queueMicrotask(() => peer.receive(copy));
  }

  protected closeTransport(code = 1000, reason = ''): void {
    const info: TransportCloseInfo = { code, reason, clean: true };
    const peer = this.#peer;
    queueMicrotask(() => {
      this.finish(info);
      peer.finish(info);
    });
  }
}

/** Creates two connected in-memory transports. */
export function createMemoryTransportPair(): [MemoryTransport, MemoryTransport] {
  return MemoryTransport.pair();
}
