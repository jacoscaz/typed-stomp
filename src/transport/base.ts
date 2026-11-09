import { TransportClosedError } from './errors.ts';
import type { Transport, TransportCloseInfo, TransportData, TransportHandlers } from './types.ts';

type PendingEvent =
  | { readonly type: 'message'; readonly data: TransportData }
  | { readonly type: 'close'; readonly info: TransportCloseInfo };

/**
 * Implements the bookkeeping part of the {@link Transport} contract:
 * buffering events until handlers are attached, delivering `onClose` once,
 * dropping messages after close, and refusing to send once closed.
 *
 * Subclasses implement `sendData` and `closeTransport`, and report incoming
 * events via `receive` and `finish`.
 */
export abstract class TransportBase implements Transport {
  #handlers: TransportHandlers | undefined;
  #pending: PendingEvent[] = [];
  #closeRequested = false;
  #finished = false;

  get closed(): boolean {
    return this.#closeRequested || this.#finished;
  }

  attach(handlers: TransportHandlers): void {
    if (this.#handlers !== undefined) {
      throw new Error('Transport handlers are already attached');
    }
    this.#handlers = handlers;
    const pending = this.#pending;
    this.#pending = [];
    for (const event of pending) {
      if (event.type === 'message') {
        handlers.onMessage(event.data);
      } else {
        handlers.onClose(event.info);
      }
    }
  }

  send(data: TransportData): void {
    if (this.closed) {
      throw new TransportClosedError();
    }
    this.sendData(data);
  }

  close(code?: number, reason?: string): void {
    if (this.closed) {
      return;
    }
    this.#closeRequested = true;
    this.closeTransport(code, reason);
  }

  protected abstract sendData(data: TransportData): void;

  protected abstract closeTransport(code?: number, reason?: string): void;

  /** Reports an incoming message. Ignored once the transport has finished. */
  protected receive(data: TransportData): void {
    if (this.#finished) {
      return;
    }
    if (this.#handlers === undefined) {
      this.#pending.push({ type: 'message', data });
    } else {
      this.#handlers.onMessage(data);
    }
  }

  /** Reports that the transport has closed. Only the first call has effect. */
  protected finish(info: TransportCloseInfo): void {
    if (this.#finished) {
      return;
    }
    this.#finished = true;
    if (this.#handlers === undefined) {
      this.#pending.push({ type: 'close', info });
    } else {
      this.#handlers.onClose(info);
    }
  }
}
