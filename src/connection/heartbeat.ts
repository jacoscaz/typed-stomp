export interface HeartbeatMonitorOptions {
  /** Interval at which to send heart-beats, in ms; 0 disables sending. */
  readonly outgoing: number;
  /** Interval at which heart-beats are expected, in ms; 0 disables checks. */
  readonly incoming: number;
  /**
   * Multiplier applied to `incoming` before declaring the peer dead, to
   * absorb network jitter and timer throttling.
   */
  readonly tolerance: number;
  /** Timestamp (`Date.now()`) of the last data sent. */
  readonly lastSent: () => number;
  /** Timestamp (`Date.now()`) of the last data received. */
  readonly lastReceived: () => number;
  /** Sends a heart-beat. */
  readonly send: () => void;
  /** Called once if the peer is considered dead. */
  readonly timeout: (elapsed: number) => void;
}

/**
 * Drives heart-beat timers. Uses one timer per direction, re-armed from the
 * last activity timestamps, so that sending and receiving frames costs
 * nothing beyond updating a timestamp.
 */
export class HeartbeatMonitor {
  readonly #options: HeartbeatMonitorOptions;
  #outgoingTimer: ReturnType<typeof setTimeout> | undefined;
  #incomingTimer: ReturnType<typeof setTimeout> | undefined;
  #running = false;

  constructor(options: HeartbeatMonitorOptions) {
    this.#options = options;
  }

  get running(): boolean {
    return this.#running;
  }

  start(): void {
    if (this.#running) {
      return;
    }
    this.#running = true;
    const { outgoing, incoming } = this.#options;
    if (outgoing > 0) {
      this.#scheduleOutgoing();
    }
    if (incoming > 0) {
      this.#scheduleIncoming();
    }
  }

  stop(): void {
    this.#running = false;
    clearTimeout(this.#outgoingTimer);
    clearTimeout(this.#incomingTimer);
    this.#outgoingTimer = undefined;
    this.#incomingTimer = undefined;
  }

  #scheduleOutgoing(): void {
    const { outgoing, lastSent } = this.#options;
    const delay = Math.max(0, outgoing - (Date.now() - lastSent()));
    this.#outgoingTimer = setTimeout(this.#onOutgoing, delay);
  }

  #onOutgoing = (): void => {
    if (!this.#running) {
      return;
    }
    const { outgoing, lastSent, send } = this.#options;
    if (Date.now() - lastSent() >= outgoing) {
      send();
    }
    if (this.#running) {
      this.#scheduleOutgoing();
    }
  };

  #scheduleIncoming(): void {
    const { incoming, tolerance, lastReceived } = this.#options;
    const limit = incoming * tolerance;
    const delay = Math.max(1, limit - (Date.now() - lastReceived()));
    this.#incomingTimer = setTimeout(this.#onIncoming, delay);
  }

  #onIncoming = (): void => {
    if (!this.#running) {
      return;
    }
    const { incoming, tolerance, lastReceived, timeout } = this.#options;
    const elapsed = Date.now() - lastReceived();
    if (elapsed >= incoming * tolerance) {
      this.stop();
      timeout(elapsed);
      return;
    }
    this.#scheduleIncoming();
  };
}
