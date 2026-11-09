/**
 * What to do when a message arrives and the buffer is full:
 * - `error`: reject the new message and report an overflow;
 * - `drop-oldest`: discard the oldest buffered message;
 * - `drop-newest`: discard the new message.
 */
export type OverflowPolicy = 'error' | 'drop-oldest' | 'drop-newest';

export interface DispatcherOptions<T> {
  /** Processes one item. May return a promise, which is awaited. */
  readonly handler: (item: T) => unknown;
  /** Maximum number of handler calls in flight. Defaults to 1. */
  readonly concurrency?: number;
  /** Maximum number of buffered (not yet started) items. Defaults to `Infinity`. */
  readonly limit?: number;
  /** Defaults to `error`. */
  readonly overflow?: OverflowPolicy;
  /** Called when the handler throws or rejects. */
  readonly onError: (error: unknown, item: T) => void;
  /** Called with the rejected item when the buffer overflows with policy `error`. */
  readonly onOverflow?: (item: T) => void;
  /** Called with items discarded by the `drop-oldest` and `drop-newest` policies. */
  readonly onDrop?: (item: T) => void;
}

/**
 * Feeds items to a handler, in order, with bounded concurrency, a bounded
 * buffer, and the ability to pause.
 *
 * With the default concurrency of 1, items are processed strictly one after
 * the other: an asynchronous handler's promise settles before the next item
 * is handled. Synchronous handlers are called synchronously from `push`.
 */
export class Dispatcher<T> {
  readonly #handler: (item: T) => unknown;
  readonly #concurrency: number;
  readonly #limit: number;
  readonly #overflow: OverflowPolicy;
  readonly #onError: (error: unknown, item: T) => void;
  readonly #onOverflow: ((item: T) => void) | undefined;
  readonly #onDrop: ((item: T) => void) | undefined;

  #queue: T[] = [];
  #head = 0;
  #inFlight = 0;
  #paused = false;
  #closed = false;
  #pumping = false;
  #idleWaiters: (() => void)[] = [];

  constructor(options: DispatcherOptions<T>) {
    this.#handler = options.handler;
    this.#concurrency = Math.max(1, options.concurrency ?? 1);
    this.#limit = Math.max(0, options.limit ?? Number.POSITIVE_INFINITY);
    this.#overflow = options.overflow ?? 'error';
    this.#onError = options.onError;
    this.#onOverflow = options.onOverflow;
    this.#onDrop = options.onDrop;
  }

  /** Number of items waiting to be handled. */
  get buffered(): number {
    return this.#queue.length - this.#head;
  }

  /** Number of handler calls in progress. */
  get inFlight(): number {
    return this.#inFlight;
  }

  get paused(): boolean {
    return this.#paused;
  }

  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Enqueues an item and starts handling it if possible. Returns false if
   * the item was not accepted (dispatcher closed, or buffer overflow).
   */
  push(item: T): boolean {
    if (this.#closed) {
      return false;
    }
    this.#queue.push(item);
    this.#pump();
    if (this.buffered <= this.#limit) {
      return true;
    }
    switch (this.#overflow) {
      case 'drop-oldest':
        this.#safely(this.#onDrop, this.#take());
        return true;
      case 'drop-newest':
        this.#queue.pop();
        this.#safely(this.#onDrop, item);
        return false;
      case 'error':
        this.#queue.pop();
        this.#safely(this.#onOverflow, item);
        return false;
    }
  }

  /** Stops starting new handler calls; calls in flight run to completion. */
  pause(): void {
    this.#paused = true;
  }

  resume(): void {
    if (!this.#paused) {
      return;
    }
    this.#paused = false;
    this.#pump();
  }

  /** Discards and returns all buffered items. */
  clear(): T[] {
    const items = this.#queue.slice(this.#head);
    this.#queue = [];
    this.#head = 0;
    this.#checkIdle();
    return items;
  }

  /**
   * Stops accepting and handling items; discards and returns buffered ones.
   * Calls in flight run to completion.
   */
  close(): T[] {
    this.#closed = true;
    return this.clear();
  }

  /**
   * Resolves once nothing is buffered or in flight. While paused with
   * buffered items, waits until resumed and drained (or cleared).
   */
  idle(): Promise<void> {
    if (this.buffered === 0 && this.#inFlight === 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => this.#idleWaiters.push(resolve));
  }

  #take(): T {
    const item = this.#queue[this.#head] as T;
    this.#queue[this.#head] = undefined as T;
    this.#head += 1;
    if (this.#head === this.#queue.length) {
      this.#queue = [];
      this.#head = 0;
    } else if (this.#head >= 1024 && this.#head * 2 >= this.#queue.length) {
      this.#queue = this.#queue.slice(this.#head);
      this.#head = 0;
    }
    return item;
  }

  #pump(): void {
    if (this.#pumping) {
      // Re-entrant call from a synchronous handler: the outer loop continues.
      return;
    }
    this.#pumping = true;
    try {
      while (!this.#paused && !this.#closed && this.#inFlight < this.#concurrency && this.buffered > 0) {
        this.#start(this.#take());
      }
    } finally {
      this.#pumping = false;
    }
    this.#checkIdle();
  }

  #start(item: T): void {
    this.#inFlight += 1;
    let result: unknown;
    try {
      result = this.#handler(item);
    } catch (err) {
      this.#done(item, true, err);
      return;
    }
    if (isPromiseLike(result)) {
      result.then(
        () => this.#done(item, false, undefined),
        (err: unknown) => this.#done(item, true, err),
      );
    } else {
      this.#done(item, false, undefined);
    }
  }

  #done(item: T, failed: boolean, error: unknown): void {
    this.#inFlight -= 1;
    if (failed) {
      try {
        this.#onError(error, item);
      } catch {
        // Error handlers must not break dispatching.
      }
    }
    this.#pump();
  }

  #checkIdle(): void {
    if (this.buffered === 0 && this.#inFlight === 0 && this.#idleWaiters.length > 0) {
      const waiters = this.#idleWaiters;
      this.#idleWaiters = [];
      for (const resolve of waiters) {
        resolve();
      }
    }
  }

  #safely(callback: ((item: T) => void) | undefined, item: T): void {
    try {
      callback?.(item);
    } catch {
      // Callbacks must not break dispatching.
    }
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}
