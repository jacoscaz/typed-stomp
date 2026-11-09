import { StompError } from '../codec/errors.ts';
import type { ErrorFrame } from '../frames/types.ts';

const decoder = new TextDecoder();

/**
 * The server sent an `ERROR` frame. The server closes the connection right
 * after, as required by the specification.
 */
export class StompServerError extends StompError {
  override name = 'StompServerError';
  readonly frame: ErrorFrame;

  constructor(frame: ErrorFrame) {
    super(frame.headers.message ?? 'The server reported an error');
    this.frame = frame;
  }

  /** The body of the `ERROR` frame, decoded as text. */
  get details(): string {
    return decoder.decode(this.frame.body);
  }
}

/**
 * The server did not answer `CONNECT` with `CONNECTED` in time.
 */
export class StompConnectTimeoutError extends StompError {
  override name = 'StompConnectTimeoutError';
  readonly timeout: number;

  constructor(timeout: number) {
    super(`No CONNECTED frame received within ${timeout} ms`);
    this.timeout = timeout;
  }
}

/**
 * An operation needs a connection, but the client is not connected.
 */
export class StompNotConnectedError extends StompError {
  override name = 'StompNotConnectedError';

  constructor(message = 'Client is not connected', options?: ErrorOptions) {
    super(message, options);
  }
}

/**
 * A subscription's buffer overflowed and its overflow policy is `error`.
 */
export class StompBufferOverflowError extends StompError {
  override name = 'StompBufferOverflowError';
  readonly limit: number;

  constructor(limit: number) {
    super(`Subscription buffer limit of ${limit} messages exceeded`);
    this.limit = limit;
  }
}
