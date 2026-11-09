import type { StompVersion } from '../codec/frame.ts';
import type { Command } from './types.ts';

export interface CommandSpec {
  /** Which side sends this command. */
  readonly from: 'client' | 'server';
  /** First version in which the command exists. */
  readonly since: StompVersion;
  /** Whether the command may carry a body. */
  readonly body: boolean;
  /**
   * Whether this is a negotiation frame (`CONNECT`, `STOMP`, `CONNECTED`),
   * which is exchanged before a version is agreed upon and validated by
   * dedicated rules.
   */
  readonly negotiation: boolean;
  /** Headers that must be present, per version. */
  readonly required: Readonly<Record<StompVersion, readonly string[]>>;
}

const none = { '1.0': [], '1.1': [], '1.2': [] } as const;
const all = (...headers: string[]) => ({ '1.0': headers, '1.1': headers, '1.2': headers });

const ackHeaders = { '1.0': ['message-id'], '1.1': ['message-id', 'subscription'], '1.2': ['id'] };

/**
 * The STOMP specification, as a table of commands.
 *
 * Rules that cannot be expressed as a table (header value formats, the
 * `CONNECT` host requirement, the 1.0 `UNSUBSCRIBE` id-or-destination rule)
 * live in the validator.
 */
export const COMMANDS: Readonly<Record<Command, CommandSpec>> = {
  CONNECT: { from: 'client', since: '1.0', body: false, negotiation: true, required: none },
  STOMP: { from: 'client', since: '1.1', body: false, negotiation: true, required: all('accept-version') },
  SEND: { from: 'client', since: '1.0', body: true, negotiation: false, required: all('destination') },
  SUBSCRIBE: {
    from: 'client',
    since: '1.0',
    body: false,
    negotiation: false,
    required: { '1.0': ['destination'], '1.1': ['destination', 'id'], '1.2': ['destination', 'id'] },
  },
  UNSUBSCRIBE: {
    from: 'client',
    since: '1.0',
    body: false,
    negotiation: false,
    required: { '1.0': [], '1.1': ['id'], '1.2': ['id'] },
  },
  ACK: { from: 'client', since: '1.0', body: false, negotiation: false, required: ackHeaders },
  NACK: { from: 'client', since: '1.1', body: false, negotiation: false, required: ackHeaders },
  BEGIN: { from: 'client', since: '1.0', body: false, negotiation: false, required: all('transaction') },
  COMMIT: { from: 'client', since: '1.0', body: false, negotiation: false, required: all('transaction') },
  ABORT: { from: 'client', since: '1.0', body: false, negotiation: false, required: all('transaction') },
  DISCONNECT: { from: 'client', since: '1.0', body: false, negotiation: false, required: none },
  CONNECTED: { from: 'server', since: '1.0', body: false, negotiation: true, required: none },
  MESSAGE: {
    from: 'server',
    since: '1.0',
    body: true,
    negotiation: false,
    required: {
      '1.0': ['destination', 'message-id'],
      '1.1': ['destination', 'message-id', 'subscription'],
      '1.2': ['destination', 'message-id', 'subscription'],
    },
  },
  RECEIPT: { from: 'server', since: '1.0', body: false, negotiation: false, required: all('receipt-id') },
  ERROR: { from: 'server', since: '1.0', body: true, negotiation: false, required: none },
};

export function isCommand(value: string): value is Command {
  return Object.hasOwn(COMMANDS, value);
}
