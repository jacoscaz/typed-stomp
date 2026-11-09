import { StompError } from '../codec/errors.ts';

/**
 * Raised when sending on, or waiting for, a transport that is closed.
 */
export class TransportClosedError extends StompError {
  override name = 'TransportClosedError';

  constructor(message = 'Transport is closed', options?: ErrorOptions) {
    super(message, options);
  }
}
