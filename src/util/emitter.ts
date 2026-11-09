export type Listener<T> = (event: T) => void;

/**
 * Reports an error that has no caller to propagate to, the same way the
 * runtime reports errors thrown by event listeners: as an uncaught
 * exception, without disrupting the code that raised it.
 */
export function reportError(error: unknown): void {
  queueMicrotask(() => {
    throw error;
  });
}

/**
 * A minimal, typed event emitter. Errors thrown by listeners are reported
 * via {@link reportError} and do not affect other listeners or the emitter.
 */
export class Emitter<Events extends { [K in keyof Events]: unknown }> {
  readonly #listeners = new Map<keyof Events, Set<Listener<never>>>();

  /** Adds a listener; returns a function that removes it. */
  on<K extends keyof Events>(type: K, listener: Listener<Events[K]>): () => void {
    let set = this.#listeners.get(type);
    if (set === undefined) {
      set = new Set();
      this.#listeners.set(type, set);
    }
    set.add(listener as Listener<never>);
    return () => this.off(type, listener);
  }

  /** Adds a listener that is removed after its first call. */
  once<K extends keyof Events>(type: K, listener: Listener<Events[K]>): () => void {
    const off = this.on(type, (event) => {
      off();
      listener(event);
    });
    return off;
  }

  off<K extends keyof Events>(type: K, listener: Listener<Events[K]>): void {
    this.#listeners.get(type)?.delete(listener as Listener<never>);
  }

  listenerCount<K extends keyof Events>(type: K): number {
    return this.#listeners.get(type)?.size ?? 0;
  }

  /** Calls all listeners of `type`; returns false if there were none. */
  emit<K extends keyof Events>(type: K, event: Events[K]): boolean {
    const set = this.#listeners.get(type);
    if (set === undefined || set.size === 0) {
      return false;
    }
    for (const listener of [...set] as Listener<Events[K]>[]) {
      try {
        listener(event);
      } catch (err) {
        reportError(err);
      }
    }
    return true;
  }
}
