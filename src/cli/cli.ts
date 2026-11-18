/// <reference types="node" />
/**
 * A basic STOMP command-line client built on {@link Client}, using the
 * runtime's standard `WebSocket` implementation.
 */
import { parseArgs, type ParseArgsConfig } from 'node:util';
import { Client } from '../client/client.ts';
import { StompServerError } from '../client/errors.ts';
import type { StompMessage } from '../client/message.ts';
import type { FrameHeaders } from '../codec/frame.ts';
import type { AckMode } from '../frames/types.ts';

export interface CliIO {
  readonly stdout: { write(chunk: string): unknown };
  readonly stderr: { write(chunk: string): unknown };
  /** Read when `send` is given no body. */
  readonly stdin?: AsyncIterable<string | Uint8Array> & { readonly isTTY?: boolean };
  /** Aborting stops a running `subscribe` and closes the client. */
  readonly signal?: AbortSignal;
  readonly env?: Readonly<Record<string, string | undefined>>;
}

export const USAGE = `Usage:
  typed-stomp send <destination> [body] [options]
  typed-stomp subscribe <destination>... [options]

Commands:
  send          Send a message. Without a body argument, the body is read
                from standard input.
  subscribe     Print messages received from one or more destinations until
                interrupted (or until --count messages have been received).

Connection options:
  -u, --url <url>          WebSocket URL of the broker (or STOMP_URL)
  -l, --login <login>      Login (or STOMP_LOGIN)
  -p, --passcode <pass>    Passcode (or STOMP_PASSCODE)
      --host <host>        Value of the CONNECT host header (default: /)
      --reconnect          Reconnect with backoff when the connection drops
      --heartbeat <ms>     Heart-beat interval in both directions (default: 10000, 0 disables)

Send options:
  -H, --header <k:v>       Add a header (repeatable)
  -t, --content-type <t>   Content type of the body
  -r, --receipt            Wait for the broker to confirm with a RECEIPT

Subscribe options:
  -H, --header <k:v>       Add a SUBSCRIBE header (repeatable)
  -a, --ack <mode>         auto (default), client or client-individual
  -n, --count <n>          Exit after receiving n messages
      --json               Print one JSON object per message (with headers)

  -h, --help               Show this help
`;

const options = {
  url: { type: 'string', short: 'u' },
  login: { type: 'string', short: 'l' },
  passcode: { type: 'string', short: 'p' },
  host: { type: 'string' },
  reconnect: { type: 'boolean' },
  heartbeat: { type: 'string' },
  header: { type: 'string', short: 'H', multiple: true },
  'content-type': { type: 'string', short: 't' },
  receipt: { type: 'boolean', short: 'r' },
  ack: { type: 'string', short: 'a' },
  count: { type: 'string', short: 'n' },
  json: { type: 'boolean' },
  help: { type: 'boolean', short: 'h' },
} as const satisfies NonNullable<ParseArgsConfig['options']>;

type Values = ReturnType<typeof parseArgs<{ options: typeof options; allowPositionals: true }>>['values'];

class UsageError extends Error {
  override name = 'UsageError';
}

/**
 * Runs the CLI with the given arguments (without the `node` and script
 * paths). Resolves with the process exit code.
 */
export async function run(argv: readonly string[], io: CliIO): Promise<number> {
  try {
    const { values, positionals } = parseArgs({ args: [...argv], options, allowPositionals: true });
    const [command, ...rest] = positionals;
    if (values.help || command === undefined || command === 'help') {
      io.stdout.write(USAGE);
      return 0;
    }
    switch (command) {
      case 'send':
        return await send(rest, values, io);
      case 'subscribe':
        return await subscribe(rest, values, io);
      default:
        throw new UsageError(`Unknown command "${command}"`);
    }
  } catch (error) {
    if (error instanceof UsageError || isParseArgsError(error)) {
      io.stderr.write(`typed-stomp: ${(error as Error).message}\n\n${USAGE}`);
      return 2;
    }
    io.stderr.write(`typed-stomp: ${describe(error)}\n`);
    return 1;
  }
}

async function send(args: readonly string[], values: Values, io: CliIO): Promise<number> {
  const [destination, body, ...extra] = args;
  if (destination === undefined) {
    throw new UsageError('send requires a destination');
  }
  if (extra.length > 0) {
    throw new UsageError('send takes a single body argument; quote it if it contains spaces');
  }
  const payload = body ?? (await readStdin(io));
  const client = createClient(values, io);
  try {
    await client.connect();
    await client.publish(destination, payload, {
      headers: parseHeaders(values.header),
      receipt: values.receipt ?? false,
      ...(values['content-type'] === undefined ? {} : { contentType: values['content-type'] }),
    });
  } finally {
    await client.close();
  }
  return 0;
}

async function subscribe(destinations: readonly string[], values: Values, io: CliIO): Promise<number> {
  if (destinations.length === 0) {
    throw new UsageError('subscribe requires at least one destination');
  }
  const ack = parseAck(values.ack);
  const count = values.count === undefined ? Infinity : parsePositiveInteger(values.count, '--count');
  const headers = parseHeaders(values.header);
  const client = createClient(values, io);
  const done = Promise.withResolvers<number>();
  let received = 0;

  const onAbort = () => done.resolve(130);
  io.signal?.addEventListener('abort', onAbort, { once: true });
  client.on('close', ({ error }) => {
    if (error !== undefined) {
      done.reject(error);
    } else {
      done.resolve(0);
    }
  });
  client.on('error', ({ error }) => done.reject(error));

  const handler = async (message: StompMessage) => {
    if (received >= count) {
      return;
    }
    received += 1;
    io.stdout.write(values.json ? `${formatJson(message)}\n` : `${message.text()}\n`);
    await message.ack();
    if (received >= count) {
      done.resolve(0);
    }
  };
  try {
    for (const destination of destinations) {
      client.subscribe(destination, handler, { ack, headers, onError: (error) => done.reject(error) });
    }
    if (io.signal?.aborted) {
      return 130;
    }
    await Promise.race([client.connect(), done.promise]);
    return await done.promise;
  } finally {
    io.signal?.removeEventListener('abort', onAbort);
    await client.close();
  }
}

function createClient(values: Values, io: CliIO): Client {
  const env = io.env ?? {};
  const url = values.url ?? env['STOMP_URL'];
  if (url === undefined || url === '') {
    throw new UsageError('a broker URL is required (--url or STOMP_URL)');
  }
  const login = values.login ?? env['STOMP_LOGIN'];
  const passcode = values.passcode ?? env['STOMP_PASSCODE'];
  const connectHeaders: Record<string, string> = {};
  if (login !== undefined) connectHeaders['login'] = login;
  if (passcode !== undefined) connectHeaders['passcode'] = passcode;
  const heartbeat =
    values.heartbeat === undefined ? undefined : parseNonNegativeInteger(values.heartbeat, '--heartbeat');
  const client = new Client({
    url,
    connectHeaders,
    reconnect: values.reconnect ? {} : false,
    ...(values.host === undefined ? {} : { host: values.host }),
    ...(heartbeat === undefined ? {} : { heartbeat: { outgoing: heartbeat, incoming: heartbeat } }),
  });
  client.on('reconnecting', ({ attempt, delay, error }) => {
    io.stderr.write(`typed-stomp: ${describe(error)}; reconnecting in ${delay} ms (attempt ${attempt})\n`);
  });
  client.on('warning', ({ issues }) => {
    for (const issue of issues) {
      io.stderr.write(`typed-stomp: warning: ${issue.message}\n`);
    }
  });
  return client;
}

function parseHeaders(raw: readonly string[] | undefined): FrameHeaders {
  const headers: Record<string, string> = {};
  for (const entry of raw ?? []) {
    const index = entry.indexOf(':');
    if (index <= 0) {
      throw new UsageError(`invalid header "${entry}", expected name:value`);
    }
    headers[entry.slice(0, index).trim()] = entry.slice(index + 1).trim();
  }
  return headers;
}

function parseAck(raw: string | undefined): AckMode {
  if (raw === undefined || raw === 'auto' || raw === 'client' || raw === 'client-individual') {
    return raw ?? 'auto';
  }
  throw new UsageError(`invalid ack mode "${raw}"`);
}

function parsePositiveInteger(raw: string, name: string): number {
  const value = parseNonNegativeInteger(raw, name);
  if (value === 0) {
    throw new UsageError(`${name} must be a positive integer`);
  }
  return value;
}

function parseNonNegativeInteger(raw: string, name: string): number {
  if (!/^\d+$/.test(raw)) {
    throw new UsageError(`${name} must be a non-negative integer`);
  }
  return Number(raw);
}

async function readStdin(io: CliIO): Promise<Uint8Array> {
  const stdin = io.stdin;
  if (stdin === undefined || stdin.isTTY) {
    throw new UsageError('send requires a body argument or a body on standard input');
  }
  const encoder = new TextEncoder();
  const chunks: Uint8Array[] = [];
  for await (const chunk of stdin) {
    chunks.push(typeof chunk === 'string' ? encoder.encode(chunk) : chunk);
  }
  const body = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.length;
  }
  return body;
}

function formatJson(message: StompMessage): string {
  return JSON.stringify({ destination: message.destination, headers: message.headers, body: message.text() });
}

function describe(error: unknown): string {
  if (error instanceof StompServerError) {
    const details = error.details.trim();
    return details === '' ? error.message : `${error.message}: ${details}`;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function isParseArgsError(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === 'string' && code.startsWith('ERR_PARSE_ARGS_');
}
