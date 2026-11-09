import { StompSerializeError } from './errors.ts';
import { escapeHeader, usesHeaderEscaping } from './escape.ts';
import type { FrameHeaders, FrameInit, StompVersion } from './frame.ts';

export interface SerializeOptions {
  /** Version used to escape headers. */
  readonly version: StompVersion;
  /**
   * Whether to add a `content-length` header to frames with a non-empty
   * body. Defaults to `true`. An explicit `content-length` header is always
   * written, and must match the actual body length.
   */
  readonly contentLength?: boolean;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });
const EMPTY = new Uint8Array(0);
const LINE_BREAK = /[\r\n]/;

/**
 * Serializes a frame to bytes, suitable for a binary WebSocket message.
 */
export function serializeFrame(frame: FrameInit, options: SerializeOptions): Uint8Array {
  const body = typeof frame.body === 'string' ? encoder.encode(frame.body) : (frame.body ?? EMPTY);
  if (options.contentLength === false && frame.headers?.['content-length'] === undefined && body.includes(0)) {
    throw nullInBody();
  }
  const head = encoder.encode(serializeHead(frame.command, frame.headers, body.byteLength, options));
  const out = new Uint8Array(head.byteLength + body.byteLength + 1);
  out.set(head, 0);
  out.set(body, head.byteLength);
  // The trailing NUL is already there: typed arrays are zero-filled.
  return out;
}

/**
 * Serializes a frame to a string, suitable for a text WebSocket message.
 * Throws if the body is binary data that is not valid UTF-8.
 */
export function serializeFrameToString(frame: FrameInit, options: SerializeOptions): string {
  let text: string;
  let byteLength: number;
  if (typeof frame.body === 'string') {
    text = frame.body;
    byteLength = text === '' ? 0 : encoder.encode(text).byteLength;
  } else if (frame.body !== undefined && frame.body.byteLength > 0) {
    try {
      text = decoder.decode(frame.body);
    } catch {
      throw new StompSerializeError('binary-body', 'Frame body is not valid UTF-8 and cannot be sent as text');
    }
    byteLength = frame.body.byteLength;
  } else {
    text = '';
    byteLength = 0;
  }
  if (options.contentLength === false && frame.headers?.['content-length'] === undefined && text.includes('\0')) {
    throw nullInBody();
  }
  return serializeHead(frame.command, frame.headers, byteLength, options) + text + '\0';
}

/**
 * Serializes the command line, headers and blank line of a frame.
 */
function serializeHead(
  command: string,
  headers: FrameHeaders | undefined,
  bodyLength: number,
  options: SerializeOptions,
): string {
  if (command === '' || LINE_BREAK.test(command)) {
    throw new StompSerializeError('invalid-command', `Invalid frame command: ${JSON.stringify(command)}`);
  }
  const { version } = options;
  const escape = usesHeaderEscaping(command, version);
  let out = `${command}\n`;
  let explicitLength: string | undefined;
  if (headers !== undefined) {
    for (const name of Object.keys(headers)) {
      const value = headers[name];
      if (value === undefined) {
        continue;
      }
      if (name === 'content-length') {
        explicitLength = value;
        continue;
      }
      out += escape
        ? `${encodeEscaped(name, version, true)}:${encodeEscaped(value, version, false)}\n`
        : `${encodeRaw(name, true)}:${encodeRaw(value, false)}\n`;
    }
  }
  const actualLength = String(bodyLength);
  if (explicitLength !== undefined && explicitLength !== actualLength) {
    throw new StompSerializeError(
      'content-length-mismatch',
      `content-length header is "${explicitLength}" but the body is ${actualLength} bytes long`,
    );
  }
  if (explicitLength !== undefined || (bodyLength > 0 && options.contentLength !== false)) {
    out += `content-length:${actualLength}\n`;
  }
  return `${out}\n`;
}

function encodeEscaped(value: string, version: StompVersion, isName: boolean): string {
  if (isName && value === '') {
    throw new StompSerializeError('unencodable-header', 'Header names cannot be empty');
  }
  if (version === '1.1' && value.includes('\r')) {
    throw new StompSerializeError('unencodable-header', 'STOMP 1.1 cannot encode carriage returns in headers');
  }
  return escapeHeader(value, version);
}

function encodeRaw(value: string, isName: boolean): string {
  if (isName && (value === '' || value.includes(':'))) {
    throw new StompSerializeError(
      'unencodable-header',
      `Header name ${JSON.stringify(value)} cannot be sent without escaping`,
    );
  }
  if (LINE_BREAK.test(value)) {
    throw new StompSerializeError(
      'unencodable-header',
      `Header ${isName ? 'name' : 'value'} ${JSON.stringify(value)} contains a line break and cannot be sent without escaping`,
    );
  }
  return value;
}

function nullInBody(): StompSerializeError {
  return new StompSerializeError('null-in-body', 'Frame body contains NUL bytes and requires a content-length header');
}
