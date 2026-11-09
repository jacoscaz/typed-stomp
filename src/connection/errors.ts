import { StompError } from '../codec/errors.ts';
import type { Frame } from '../codec/frame.ts';
import type { ValidationIssue } from '../frames/validate.ts';

export interface StompProtocolErrorOptions extends ErrorOptions {
  /** Validation issues, when the error comes from frame validation. */
  readonly issues?: readonly ValidationIssue[];
  /** The offending frame, if it could be parsed. */
  readonly frame?: Frame;
}

/**
 * The peer violated the STOMP protocol: malformed data, an invalid frame, or
 * a frame that is not acceptable in the current state.
 */
export class StompProtocolError extends StompError {
  override name = 'StompProtocolError';
  readonly issues: readonly ValidationIssue[];
  readonly frame: Frame | undefined;

  constructor(message: string, options: StompProtocolErrorOptions = {}) {
    super(message, options);
    this.issues = options.issues ?? [];
    this.frame = options.frame;
  }
}

/**
 * Nothing was received from the peer within the negotiated heart-beat
 * interval (times the configured tolerance).
 */
export class StompHeartbeatTimeoutError extends StompError {
  override name = 'StompHeartbeatTimeoutError';
  /** Milliseconds elapsed since data was last received. */
  readonly elapsed: number;

  constructor(elapsed: number) {
    super(`No data received from peer for ${elapsed} ms`);
    this.elapsed = elapsed;
  }
}

/**
 * Raised when using a connection that is closing or closed.
 */
export class StompConnectionClosedError extends StompError {
  override name = 'StompConnectionClosedError';

  constructor(message = 'Connection is closed', options?: ErrorOptions) {
    super(message, options);
  }
}
