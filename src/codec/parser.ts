import { StompParseError } from './errors.ts';
import { unescapeHeader, usesHeaderEscaping } from './escape.ts';
import type { Frame, StompVersion } from './frame.ts';

const LF = 0x0a;
const CR = 0x0d;
const NUL = 0x00;
const COLON = 0x3a;

const INITIAL_CAPACITY = 4096;
const SHRINK_THRESHOLD = INITIAL_CAPACITY * 16;
const CONTENT_LENGTH = /^\d+$/;

export interface ParserLimits {
  /** Maximum size in bytes of the command line plus all header lines. */
  readonly maxHeaderSize?: number;
  /** Maximum number of header lines, including repeated ones. */
  readonly maxHeaders?: number;
  /** Maximum size in bytes of a frame body. */
  readonly maxBodySize?: number;
}

export const DEFAULT_PARSER_LIMITS: Required<ParserLimits> = Object.freeze({
  maxHeaderSize: 64 * 1024,
  maxHeaders: 128,
  maxBodySize: 16 * 1024 * 1024,
});

export interface FrameParserOptions extends ParserLimits {
  /**
   * Version used to decode header escapes. Defaults to `'1.0'`, which is
   * correct before negotiation since `CONNECT` and `CONNECTED` are never
   * escaped. Can be changed at any time via {@link FrameParser.version}.
   */
  readonly version?: StompVersion;
  /**
   * Called synchronously for every complete frame. The parser's state is
   * fully updated before the call, so it is safe to change
   * {@link FrameParser.version} from within this callback.
   */
  readonly onFrame: (frame: Frame) => void;
  /** Called for every EOL received between frames. */
  readonly onHeartbeat?: () => void;
}

export type ParserInput = string | ArrayBuffer | ArrayBufferView;

type State = 'idle' | 'command' | 'headers' | 'body';

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });

/**
 * Incremental, byte-oriented STOMP frame parser.
 *
 * Feed it chunks of data as they arrive with {@link FrameParser.push}; it
 * emits complete frames through `onFrame` and EOL heart-beats through
 * `onHeartbeat`. Chunks may contain several frames, or parts of a frame.
 */
export class FrameParser {
  /** Version used to decode header escapes of subsequent frames. */
  version: StompVersion;

  readonly #onFrame: (frame: Frame) => void;
  readonly #onHeartbeat: (() => void) | undefined;
  readonly #maxHeaderSize: number;
  readonly #maxHeaders: number;
  readonly #maxBodySize: number;

  #buf = new Uint8Array(INITIAL_CAPACITY);
  /** Start of unconsumed data. */
  #start = 0;
  /** End of buffered data. */
  #end = 0;
  /** Position from which to resume scanning for a delimiter. */
  #scan = 0;

  #state: State = 'idle';
  #failed = false;
  #paused = false;
  #draining = false;
  #command = '';
  #headers: Record<string, string> = Object.create(null);
  #headerCount = 0;
  #headerBytes = 0;
  #contentLength = -1;

  constructor(options: FrameParserOptions) {
    this.version = options.version ?? '1.0';
    this.#onFrame = options.onFrame;
    this.#onHeartbeat = options.onHeartbeat;
    this.#maxHeaderSize = options.maxHeaderSize ?? DEFAULT_PARSER_LIMITS.maxHeaderSize;
    this.#maxHeaders = options.maxHeaders ?? DEFAULT_PARSER_LIMITS.maxHeaders;
    this.#maxBodySize = options.maxBodySize ?? DEFAULT_PARSER_LIMITS.maxBodySize;
  }

  /** Number of bytes received but not yet emitted as part of a frame. */
  get buffered(): number {
    return this.#end - this.#start;
  }

  /** Whether the parser is in the middle of a frame. */
  get partial(): boolean {
    return this.#state !== 'idle' || this.#end > this.#start;
  }

  /**
   * Feeds a chunk of data to the parser.
   *
   * Throws {@link StompParseError} on malformed input, after which the parser
   * refuses further input until {@link FrameParser.reset} is called. Errors
   * thrown by the callbacks propagate unchanged and leave the parser usable:
   * frames left in the buffer are emitted on the next call to `push` or
   * `resume`.
   */
  push(input: ParserInput): void {
    if (this.#failed) {
      throw new StompParseError('parser-failed', 'Parser is in a failed state and must be reset');
    }
    this.#append(toBytes(input));
    this.#run();
  }

  /** Whether frame emission is paused. */
  get paused(): boolean {
    return this.#paused;
  }

  /**
   * Stops emitting frames. Data passed to `push` is buffered until
   * {@link FrameParser.resume} is called. When called from within `onFrame`,
   * the remaining data of the current chunk is held back too, which lets the
   * consumer finish an asynchronous step (e.g. authentication) and update
   * {@link FrameParser.version} before further frames are decoded.
   */
  pause(): void {
    this.#paused = true;
  }

  /**
   * Resumes emitting frames, synchronously processing any buffered data.
   * May throw, like {@link FrameParser.push}.
   */
  resume(): void {
    if (!this.#paused) {
      return;
    }
    this.#paused = false;
    if (!this.#failed) {
      this.#run();
    }
  }

  #run(): void {
    if (this.#draining) {
      // Re-entrant call from a callback: the outer loop picks up new data.
      return;
    }
    this.#draining = true;
    try {
      this.#drain();
    } catch (err) {
      if (err instanceof StompParseError) {
        this.#failed = true;
      }
      throw err;
    } finally {
      this.#draining = false;
      this.#release();
    }
  }

  /** Discards all buffered data and clears the failed state. */
  reset(): void {
    this.#buf = new Uint8Array(INITIAL_CAPACITY);
    this.#start = 0;
    this.#end = 0;
    this.#scan = 0;
    this.#state = 'idle';
    this.#failed = false;
    this.#paused = false;
    this.#command = '';
    this.#headers = Object.create(null);
    this.#headerCount = 0;
    this.#headerBytes = 0;
    this.#contentLength = -1;
  }

  #drain(): void {
    while (!this.#paused) {
      switch (this.#state) {
        case 'idle':
          if (!this.#skipEol()) {
            return;
          }
          break;
        case 'command':
          if (!this.#readCommand()) {
            return;
          }
          break;
        case 'headers':
          if (!this.#readHeader()) {
            return;
          }
          break;
        case 'body':
          if (!this.#readBody()) {
            return;
          }
          break;
      }
    }
  }

  /** Consumes heart-beat EOLs; switches to `command` at the first other byte. */
  #skipEol(): boolean {
    if (this.#start >= this.#end) {
      return false;
    }
    const byte = this.#buf[this.#start];
    if (byte === LF) {
      this.#start += 1;
      this.#scan = this.#start;
      this.#onHeartbeat?.();
      return true;
    }
    if (byte === CR) {
      if (this.#start + 1 >= this.#end) {
        return false;
      }
      if (this.#buf[this.#start + 1] !== LF) {
        throw new StompParseError('invalid-eol', 'Carriage return not followed by line feed');
      }
      this.#start += 2;
      this.#scan = this.#start;
      this.#onHeartbeat?.();
      return true;
    }
    this.#state = 'command';
    this.#scan = this.#start;
    this.#headerBytes = 0;
    return true;
  }

  #readCommand(): boolean {
    const lf = this.#findLine();
    if (lf < 0) {
      return false;
    }
    this.#command = this.#decode(this.#start, this.#lineEnd(lf));
    this.#headers = Object.create(null);
    this.#headerCount = 0;
    this.#start = lf + 1;
    this.#scan = this.#start;
    this.#state = 'headers';
    return true;
  }

  #readHeader(): boolean {
    const lf = this.#findLine();
    if (lf < 0) {
      return false;
    }
    const lineStart = this.#start;
    const lineEnd = this.#lineEnd(lf);
    this.#start = lf + 1;
    this.#scan = this.#start;
    if (lineEnd === lineStart) {
      this.#beginBody();
      return true;
    }
    this.#headerCount += 1;
    if (this.#headerCount > this.#maxHeaders) {
      throw new StompParseError('too-many-headers', `Frame has more than ${this.#maxHeaders} headers`);
    }
    const colon = this.#buf.subarray(lineStart, lineEnd).indexOf(COLON);
    if (colon <= 0) {
      throw new StompParseError(
        'invalid-header',
        colon === 0 ? 'Header with an empty name' : 'Header line without a colon',
      );
    }
    let name = this.#decode(lineStart, lineStart + colon);
    let value = this.#decode(lineStart + colon + 1, lineEnd);
    if (usesHeaderEscaping(this.#command, this.version)) {
      name = unescapeHeader(name, this.version);
      value = unescapeHeader(value, this.version);
    }
    // Repeated headers: only the first occurrence counts.
    if (!Object.hasOwn(this.#headers, name)) {
      this.#headers[name] = value;
    }
    return true;
  }

  #beginBody(): void {
    const raw = this.#headers['content-length'];
    if (raw === undefined) {
      this.#contentLength = -1;
    } else {
      const length = CONTENT_LENGTH.test(raw) ? Number(raw) : Number.NaN;
      if (!Number.isSafeInteger(length)) {
        throw new StompParseError('invalid-content-length', `Invalid content-length header: "${raw}"`);
      }
      if (length > this.#maxBodySize) {
        throw this.#bodyTooLarge();
      }
      this.#contentLength = length;
    }
    this.#state = 'body';
  }

  #readBody(): boolean {
    let body: Uint8Array;
    const length = this.#contentLength;
    if (length >= 0) {
      if (this.#end - this.#start < length + 1) {
        return false;
      }
      if (this.#buf[this.#start + length] !== NUL) {
        throw new StompParseError('missing-null', 'Frame body not terminated by NUL after content-length bytes');
      }
      body = this.#buf.slice(this.#start, this.#start + length);
      this.#start += length + 1;
    } else {
      const offset = this.#buf.subarray(this.#scan, this.#end).indexOf(NUL);
      if (offset < 0) {
        this.#scan = this.#end;
        if (this.#end - this.#start > this.#maxBodySize) {
          throw this.#bodyTooLarge();
        }
        return false;
      }
      const nul = this.#scan + offset;
      if (nul - this.#start > this.#maxBodySize) {
        throw this.#bodyTooLarge();
      }
      body = this.#buf.slice(this.#start, nul);
      this.#start = nul + 1;
    }
    const frame: Frame = { command: this.#command, headers: this.#headers, body };
    this.#scan = this.#start;
    this.#state = 'idle';
    this.#command = '';
    this.#headers = Object.create(null);
    this.#onFrame(frame);
    return true;
  }

  /**
   * Finds the next LF within the command/header section, enforcing the
   * header size limit. Returns -1 if more data is needed.
   */
  #findLine(): number {
    const offset = this.#buf.subarray(this.#scan, this.#end).indexOf(LF);
    if (offset < 0) {
      this.#scan = this.#end;
      if (this.#headerBytes + (this.#end - this.#start) > this.#maxHeaderSize) {
        throw this.#headerTooLarge();
      }
      return -1;
    }
    const lf = this.#scan + offset;
    this.#headerBytes += lf + 1 - this.#start;
    if (this.#headerBytes > this.#maxHeaderSize) {
      throw this.#headerTooLarge();
    }
    return lf;
  }

  /** End of the line ending at `lf`, excluding an optional trailing CR. */
  #lineEnd(lf: number): number {
    return lf > this.#start && this.#buf[lf - 1] === CR ? lf - 1 : lf;
  }

  #decode(from: number, to: number): string {
    try {
      return decoder.decode(this.#buf.subarray(from, to));
    } catch {
      throw new StompParseError('invalid-utf8', 'Command or header is not valid UTF-8');
    }
  }

  #append(bytes: Uint8Array): void {
    const length = bytes.byteLength;
    if (length === 0) {
      return;
    }
    if (this.#end + length > this.#buf.byteLength) {
      const live = this.#end - this.#start;
      const needed = live + length;
      if (needed <= this.#buf.byteLength) {
        this.#buf.copyWithin(0, this.#start, this.#end);
      } else {
        let capacity = this.#buf.byteLength * 2;
        while (capacity < needed) {
          capacity *= 2;
        }
        const next = new Uint8Array(capacity);
        next.set(this.#buf.subarray(this.#start, this.#end));
        this.#buf = next;
      }
      this.#scan -= this.#start;
      this.#start = 0;
      this.#end = live;
    }
    this.#buf.set(bytes, this.#end);
    this.#end += length;
  }

  /** Resets offsets once the buffer is drained, shrinking it if it grew large. */
  #release(): void {
    if (this.#start < this.#end) {
      return;
    }
    this.#start = 0;
    this.#end = 0;
    this.#scan = 0;
    if (this.#buf.byteLength > SHRINK_THRESHOLD) {
      this.#buf = new Uint8Array(INITIAL_CAPACITY);
    }
  }

  #headerTooLarge(): StompParseError {
    return new StompParseError('header-too-large', `Frame headers exceed ${this.#maxHeaderSize} bytes`);
  }

  #bodyTooLarge(): StompParseError {
    return new StompParseError('body-too-large', `Frame body exceeds ${this.#maxBodySize} bytes`);
  }
}

function toBytes(input: ParserInput): Uint8Array {
  if (typeof input === 'string') {
    return encoder.encode(input);
  }
  if (input instanceof ArrayBuffer) {
    return new Uint8Array(input);
  }
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  throw new TypeError('Parser input must be a string, an ArrayBuffer or an ArrayBufferView');
}
