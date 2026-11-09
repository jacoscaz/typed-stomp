import type { Frame } from '../codec/frame.ts';

interface PendingReceipt {
  readonly resolve: (frame: Frame) => void;
  readonly reject: (error: unknown) => void;
}

export interface ReceiptRequest {
  /** Value for the frame's `receipt` header. */
  readonly id: string;
  /** Settles when the matching `RECEIPT` (or `ERROR`) arrives. */
  readonly promise: Promise<Frame>;
}

/**
 * Tracks receipts requested from the peer.
 */
export class ReceiptTracker {
  readonly #prefix: string;
  readonly #pending = new Map<string, PendingReceipt>();
  #counter = 0;

  constructor(prefix = 'receipt-') {
    this.#prefix = prefix;
  }

  /** Number of receipts still awaited. */
  get size(): number {
    return this.#pending.size;
  }

  has(id: string): boolean {
    return this.#pending.has(id);
  }

  /** Allocates a receipt id and a promise for its arrival. */
  create(): ReceiptRequest {
    const id = `${this.#prefix}${this.#counter++}`;
    const { promise, resolve, reject } = Promise.withResolvers<Frame>();
    // Rejections are reported to whoever awaits the promise; nobody awaiting
    // it must not crash the process with an unhandled rejection.
    promise.catch(() => {});
    this.#pending.set(id, { resolve, reject });
    return { id, promise };
  }

  /** Resolves the receipt with the given id. Returns false if unknown. */
  resolve(id: string, frame: Frame): boolean {
    const pending = this.#pending.get(id);
    if (pending === undefined) {
      return false;
    }
    this.#pending.delete(id);
    pending.resolve(frame);
    return true;
  }

  /** Rejects the receipt with the given id. Returns false if unknown. */
  reject(id: string, error: unknown): boolean {
    const pending = this.#pending.get(id);
    if (pending === undefined) {
      return false;
    }
    this.#pending.delete(id);
    pending.reject(error);
    return true;
  }

  /** Rejects all pending receipts, e.g. when the connection is lost. */
  rejectAll(error: unknown): void {
    const pending = [...this.#pending.values()];
    this.#pending.clear();
    for (const { reject } of pending) {
      reject(error);
    }
  }
}
