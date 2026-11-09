import { StompParseError, StompSerializeError } from '../codec/errors.ts';
import type { Frame, FrameInit, StompVersion } from '../codec/frame.ts';
import { FrameParser, type ParserLimits } from '../codec/parser.ts';
import { serializeFrame, serializeFrameToString } from '../codec/serializer.ts';
import type { ClientFrame, ServerFrame } from '../frames/types.ts';
import {
  validateClientFrame,
  validateServerFrame,
  type ValidationIssue,
  type ValidationMode,
} from '../frames/validate.ts';
import type { Transport, TransportCloseInfo, TransportData } from '../transport/types.ts';
import { StompConnectionClosedError, StompHeartbeatTimeoutError, StompProtocolError } from './errors.ts';
import { HeartbeatMonitor } from './heartbeat.ts';
import type { HeartBeatIntervals } from './negotiate.ts';

export type ConnectionRole = 'client' | 'server';

/** Frames a connection receives, given its role. */
export type IncomingFrame<R extends ConnectionRole> = R extends 'client' ? ServerFrame : ClientFrame;

/**
 * How frames are sent over the transport:
 * - `text`: always as text (fails for bodies that are not valid UTF-8);
 * - `binary`: always as binary;
 * - `auto`: as text, unless the body is not valid UTF-8.
 */
export type MessageType = 'auto' | 'text' | 'binary';

export type ConnectionState = 'open' | 'closing' | 'closed';

export interface ConnectionCloseInfo {
  /** Why the connection closed, if not via an orderly {@link Connection.close}. */
  readonly error?: unknown;
  /** Transport close code, if known. */
  readonly code?: number;
  /** Transport close reason, if known. */
  readonly reason?: string;
}

export interface ConnectionOptions<R extends ConnectionRole> extends ParserLimits {
  readonly role: R;
  readonly transport: Transport;
  /** Defaults to `lenient` for clients and `strict` for servers. */
  readonly validation?: ValidationMode;
  /** Defaults to `auto`. */
  readonly messageType?: MessageType;
  /** Multiplier for the incoming heart-beat interval. Defaults to 2. */
  readonly heartbeatTolerance?: number;
  /**
   * Called for every valid incoming frame, with any validation warnings.
   * Must not throw: errors thrown here close the connection.
   */
  readonly onFrame: (frame: IncomingFrame<R>, warnings: readonly ValidationIssue[]) => void;
  /** Called exactly once, when the connection is closed. */
  readonly onClose: (info: ConnectionCloseInfo) => void;
}

/**
 * The protocol core shared by clients and servers: it wires a frame parser
 * and serializer to a transport, validates incoming frames, drives
 * heart-beats and handles protocol errors.
 *
 * It does not implement any session logic (connection negotiation,
 * subscriptions, receipts...): that is the job of client and server sessions
 * built on top of it.
 *
 * Errors:
 * - Malformed or invalid incoming frames are protocol errors. A server
 *   connection replies with an `ERROR` frame describing the problem; then the
 *   connection fails.
 * - A connection *fails* via {@link Connection.fail}: the transport is closed
 *   and `onClose` is called immediately with the error, without waiting for
 *   the transport's closing handshake, which may never complete if the peer
 *   is unresponsive.
 */
export class Connection<R extends ConnectionRole = ConnectionRole> {
  readonly role: R;

  readonly #transport: Transport;
  readonly #parser: FrameParser;
  readonly #validation: ValidationMode;
  readonly #messageType: MessageType;
  readonly #heartbeatTolerance: number;
  readonly #onFrame: ConnectionOptions<R>['onFrame'];
  readonly #onClose: ConnectionOptions<R>['onClose'];

  #version: StompVersion = '1.0';
  #state: ConnectionState = 'open';
  #error: unknown;
  #heartbeat: HeartbeatMonitor | undefined;
  #lastSent = Date.now();
  #lastReceived = Date.now();

  constructor(options: ConnectionOptions<R>) {
    this.role = options.role;
    this.#transport = options.transport;
    this.#validation = options.validation ?? (options.role === 'client' ? 'lenient' : 'strict');
    this.#messageType = options.messageType ?? 'auto';
    this.#heartbeatTolerance = options.heartbeatTolerance ?? 2;
    this.#onFrame = options.onFrame;
    this.#onClose = options.onClose;
    this.#parser = new FrameParser({
      ...(options.maxHeaderSize === undefined ? {} : { maxHeaderSize: options.maxHeaderSize }),
      ...(options.maxHeaders === undefined ? {} : { maxHeaders: options.maxHeaders }),
      ...(options.maxBodySize === undefined ? {} : { maxBodySize: options.maxBodySize }),
      onFrame: (frame) => this.#receiveFrame(frame),
    });
    this.#transport.attach({
      onMessage: (data) => this.#receiveData(data),
      onClose: (info) => this.#transportClosed(info),
    });
  }

  get state(): ConnectionState {
    return this.#state;
  }

  /**
   * The STOMP version in use, which drives header escaping and validation.
   * Starts at `1.0` and is set by the session once negotiated.
   */
  get version(): StompVersion {
    return this.#version;
  }

  set version(version: StompVersion) {
    this.#version = version;
    this.#parser.version = version;
  }

  /** Timestamp (`Date.now()`) of the last data sent. */
  get lastSent(): number {
    return this.#lastSent;
  }

  /** Timestamp (`Date.now()`) of the last data received. */
  get lastReceived(): number {
    return this.#lastReceived;
  }

  /**
   * Sends a frame. Throws {@link StompConnectionClosedError} if the
   * connection is not open, and serialization errors as-is.
   */
  send(frame: FrameInit): void {
    if (this.#state !== 'open') {
      throw new StompConnectionClosedError();
    }
    this.#write(this.#encode(frame));
  }

  /**
   * Stops processing incoming frames; data keeps being buffered (and keeps
   * the connection alive as far as heart-beats are concerned). When called
   * from `onFrame`, frames already received in the same transport message
   * are held back too.
   */
  pause(): void {
    this.#parser.pause();
  }

  /** Resumes processing incoming frames, including buffered ones. */
  resume(): void {
    if (this.#state !== 'open') {
      return;
    }
    try {
      this.#parser.resume();
    } catch (err) {
      this.#receiveError(err);
    }
  }

  get paused(): boolean {
    return this.#parser.paused;
  }

  /**
   * Starts heart-beating with the given intervals, as computed by
   * `negotiateHeartBeat`. Replaces any previous heart-beat schedule.
   */
  startHeartbeat(intervals: HeartBeatIntervals): void {
    this.stopHeartbeat();
    if (this.#state !== 'open' || (intervals.outgoing <= 0 && intervals.incoming <= 0)) {
      return;
    }
    this.#heartbeat = new HeartbeatMonitor({
      outgoing: intervals.outgoing,
      incoming: intervals.incoming,
      tolerance: this.#heartbeatTolerance,
      lastSent: () => this.#lastSent,
      lastReceived: () => this.#lastReceived,
      send: () => this.#sendHeartbeat(),
      timeout: (elapsed) => this.fail(new StompHeartbeatTimeoutError(elapsed)),
    });
    this.#heartbeat.start();
  }

  stopHeartbeat(): void {
    this.#heartbeat?.stop();
    this.#heartbeat = undefined;
  }

  /**
   * Closes the connection in an orderly fashion: `onClose` is called once the
   * transport has closed. Does nothing if already closing or closed.
   */
  close(code?: number, reason?: string): void {
    if (this.#state !== 'open') {
      return;
    }
    this.#state = 'closing';
    this.stopHeartbeat();
    this.#transport.close(code, reason);
  }

  /**
   * Fails the connection: optionally sends a final frame (typically an
   * `ERROR` frame), closes the transport and calls `onClose` with `error`
   * right away. Does nothing if already closed.
   */
  fail(error: unknown, finalFrame?: FrameInit): void {
    if (this.#state === 'closed') {
      return;
    }
    this.#error ??= error;
    if (finalFrame !== undefined && this.#state === 'open') {
      try {
        this.#write(this.#encode(finalFrame));
      } catch {
        // The connection is failing anyway.
      }
    }
    this.#state = 'closing';
    this.stopHeartbeat();
    this.#transport.close();
    this.#finish({});
  }

  #receiveData(data: TransportData): void {
    this.#lastReceived = Date.now();
    if (this.#state !== 'open') {
      return;
    }
    try {
      this.#parser.push(data);
    } catch (err) {
      this.#receiveError(err);
    }
  }

  #receiveFrame(frame: Frame): void {
    if (this.#state !== 'open') {
      return;
    }
    const options = { version: this.#version, mode: this.#validation };
    const result = this.role === 'client' ? validateServerFrame(frame, options) : validateClientFrame(frame, options);
    if (!result.valid) {
      const errors = result.issues.filter((issue) => issue.severity === 'error');
      throw new StompProtocolError(errors.map((issue) => issue.message).join('; '), {
        issues: result.issues,
        frame,
      });
    }
    const warnings = result.issues;
    this.#onFrame(result.frame as IncomingFrame<R>, warnings);
  }

  /** Handles errors raised while processing incoming data. */
  #receiveError(err: unknown): void {
    const error =
      err instanceof StompParseError ? new StompProtocolError(`Malformed frame: ${err.message}`, { cause: err }) : err;
    const finalFrame = this.role === 'server' && error instanceof StompProtocolError ? errorFrame(error) : undefined;
    this.fail(error, finalFrame);
  }

  #sendHeartbeat(): void {
    if (this.#state !== 'open') {
      return;
    }
    try {
      this.#write(this.#messageType === 'binary' ? EOL_BYTES : '\n');
    } catch (err) {
      this.fail(err);
    }
  }

  #write(data: TransportData): void {
    this.#transport.send(data);
    this.#lastSent = Date.now();
  }

  #encode(frame: FrameInit): TransportData {
    const options = { version: this.#version };
    switch (this.#messageType) {
      case 'text':
        return serializeFrameToString(frame, options);
      case 'binary':
        return serializeFrame(frame, options);
      case 'auto':
        if (frame.body instanceof Uint8Array && frame.body.byteLength > 0) {
          try {
            return serializeFrameToString(frame, options);
          } catch (err) {
            if (err instanceof StompSerializeError && err.code === 'binary-body') {
              return serializeFrame(frame, options);
            }
            throw err;
          }
        }
        return serializeFrameToString(frame, options);
    }
  }

  #transportClosed(info: TransportCloseInfo): void {
    if (info.error !== undefined) {
      this.#error ??= info.error;
    } else if (this.#state === 'open' && !info.clean) {
      this.#error ??= new StompConnectionClosedError('Transport closed unexpectedly');
    }
    this.#finish(info);
  }

  #finish(info: { readonly code?: number; readonly reason?: string }): void {
    if (this.#state === 'closed') {
      return;
    }
    this.#state = 'closed';
    this.stopHeartbeat();
    this.#onClose({
      ...(this.#error === undefined ? {} : { error: this.#error }),
      ...(info.code === undefined ? {} : { code: info.code }),
      ...(info.reason === undefined ? {} : { reason: info.reason }),
    });
  }
}

const EOL_BYTES = Uint8Array.of(0x0a);

/**
 * Builds the `ERROR` frame a server sends in response to a protocol error.
 */
export function errorFrame(error: StompProtocolError): FrameInit {
  const receipt = error.frame?.headers['receipt'];
  const details = error.issues.map((issue) => `${issue.severity}: ${issue.message}`).join('\n');
  return {
    command: 'ERROR',
    headers: {
      // Header values cannot contain line breaks in STOMP 1.0.
      message: error.message.replace(/[\r\n]+/g, ' '),
      'receipt-id': receipt,
      'content-type': details === '' ? undefined : 'text/plain',
    },
    body: details,
  };
}
