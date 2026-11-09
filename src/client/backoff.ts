export interface ReconnectOptions {
  /** Delay before the first reconnection attempt, in ms. Defaults to 1000. */
  readonly initialDelay?: number;
  /** Upper bound for the delay, in ms. Defaults to 30000. */
  readonly maxDelay?: number;
  /** Multiplier applied to the delay after each failed attempt. Defaults to 2. */
  readonly factor?: number;
  /**
   * Fraction of the delay that is randomized, between 0 and 1, to avoid
   * reconnection storms. Defaults to 0.5: delays vary between 50% and 100%
   * of their nominal value.
   */
  readonly jitter?: number;
  /**
   * Maximum number of consecutive failed attempts before giving up.
   * Defaults to `Infinity`.
   */
  readonly maxAttempts?: number;
  /**
   * Decides whether to try again after an error; `attempt` counts
   * consecutive failures, starting at 1. Defaults to always retrying.
   */
  readonly shouldReconnect?: (error: unknown, attempt: number) => boolean;
}

/**
 * Computes the delay before reconnection attempt number `attempt`
 * (starting at 1), using exponential backoff with jitter.
 */
export function backoffDelay(attempt: number, options: ReconnectOptions = {}, random: () => number = Math.random): number {
  const initialDelay = options.initialDelay ?? 1000;
  const maxDelay = options.maxDelay ?? 30_000;
  const factor = options.factor ?? 2;
  const jitter = Math.min(1, Math.max(0, options.jitter ?? 0.5));
  const nominal = Math.min(maxDelay, initialDelay * factor ** Math.max(0, attempt - 1));
  return Math.round(nominal * (1 - jitter * random()));
}
