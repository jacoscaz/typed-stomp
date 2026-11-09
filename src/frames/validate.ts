import { StompError } from '../codec/errors.ts';
import { compareVersions, isStompVersion, type Frame, type StompVersion } from '../codec/frame.ts';
import { COMMANDS, isCommand } from './spec.ts';
import type { ClientFrame, ServerFrame } from './types.ts';

/**
 * In `strict` mode every violation of the specification is an error. In
 * `lenient` mode, violations that do not prevent processing the frame
 * (e.g. a body on a `SUBSCRIBE` frame, or a malformed `heart-beat` header)
 * are reported as warnings instead.
 */
export type ValidationMode = 'strict' | 'lenient';

export interface ValidateOptions<V extends StompVersion = StompVersion> {
  /**
   * The negotiated version. Ignored for negotiation frames (`CONNECT`,
   * `STOMP`, `CONNECTED`), which have their own rules.
   */
  readonly version: V;
  /** Defaults to `'strict'`. */
  readonly mode?: ValidationMode;
}

export type IssueCode =
  | 'unknown-command'
  | 'unexpected-command'
  | 'unsupported-command'
  | 'missing-header'
  | 'empty-header'
  | 'invalid-header'
  | 'unexpected-body'
  | 'content-length-mismatch';

export interface ValidationIssue {
  readonly severity: 'error' | 'warning';
  readonly code: IssueCode;
  readonly message: string;
  /** The offending header, if any. */
  readonly header?: string;
}

export type ValidationResult<F extends Frame> =
  | { readonly valid: true; readonly frame: F; readonly issues: readonly ValidationIssue[] }
  | { readonly valid: false; readonly issues: readonly ValidationIssue[] };

/**
 * Raised by {@link assertValid}.
 */
export class StompValidationError extends StompError {
  override name = 'StompValidationError';
  readonly issues: readonly ValidationIssue[];

  constructor(issues: readonly ValidationIssue[]) {
    const errors = issues.filter((issue) => issue.severity === 'error');
    super(errors.map((issue) => issue.message).join('; ') || 'Invalid frame');
    this.issues = issues;
  }
}

/**
 * Validates a frame sent by a client. Use on the server side for incoming
 * frames, or on the client side before sending.
 */
export function validateClientFrame<V extends StompVersion>(
  frame: Frame,
  options: ValidateOptions<V>,
): ValidationResult<ClientFrame<V>> {
  return validate(frame, 'client', options) as ValidationResult<Frame> as ValidationResult<ClientFrame<V>>;
}

/**
 * Validates a frame sent by a server. Use on the client side for incoming
 * frames, or on the server side before sending.
 */
export function validateServerFrame<V extends StompVersion>(
  frame: Frame,
  options: ValidateOptions<V>,
): ValidationResult<ServerFrame<V>> {
  return validate(frame, 'server', options) as ValidationResult<Frame> as ValidationResult<ServerFrame<V>>;
}

/**
 * Returns the validated frame, or throws a {@link StompValidationError}.
 */
export function assertValid<F extends Frame>(result: ValidationResult<F>): F {
  if (result.valid) {
    return result.frame;
  }
  throw new StompValidationError(result.issues);
}

const ACK_MODES = new Set(['auto', 'client', 'client-individual']);
const HEART_BEAT = /^\d+,\d+$/;
const VERSION_TOKEN = /^\d+\.\d+$/;
const CONTENT_LENGTH = /^\d+$/;

class Issues {
  readonly list: ValidationIssue[] = [];
  readonly #strict: boolean;
  #failed = false;

  constructor(strict: boolean) {
    this.#strict = strict;
  }

  get failed(): boolean {
    return this.#failed;
  }

  /** A violation that always makes the frame invalid. */
  error(code: IssueCode, message: string, header?: string): void {
    this.#add('error', code, message, header);
  }

  /** A violation that makes the frame invalid only in strict mode. */
  soft(code: IssueCode, message: string, header?: string): void {
    this.#add(this.#strict ? 'error' : 'warning', code, message, header);
  }

  /**
   * A violation that is common enough among widely used implementations
   * that it is only ever reported as a warning.
   */
  warn(code: IssueCode, message: string, header?: string): void {
    this.#add('warning', code, message, header);
  }

  #add(severity: 'error' | 'warning', code: IssueCode, message: string, header: string | undefined): void {
    if (severity === 'error') {
      this.#failed = true;
    }
    this.list.push(header === undefined ? { severity, code, message } : { severity, code, message, header });
  }
}

function validate(frame: Frame, from: 'client' | 'server', options: ValidateOptions): ValidationResult<Frame> {
  const { version } = options;
  const issues = new Issues((options.mode ?? 'strict') === 'strict');
  const { command, headers, body } = frame;

  if (!isCommand(command)) {
    issues.error('unknown-command', `Unknown command ${JSON.stringify(command)}`);
    return result(frame, issues);
  }
  const spec = COMMANDS[command];
  if (spec.from !== from) {
    issues.error('unexpected-command', `${command} frames are sent by ${spec.from}s, not ${from}s`);
    return result(frame, issues);
  }
  if (!spec.negotiation && compareVersions(version, spec.since) < 0) {
    issues.error('unsupported-command', `${command} frames do not exist in STOMP ${version}`);
    return result(frame, issues);
  }

  for (const name of spec.required[version]) {
    const value = headers[name];
    if (value === undefined) {
      issues.error('missing-header', `${command} frames require a "${name}" header`, name);
    } else if (value === '') {
      issues.soft('empty-header', `"${name}" header of ${command} frame is empty`, name);
    }
  }

  if (!spec.body && body.byteLength > 0) {
    issues.soft('unexpected-body', `${command} frames cannot have a body`);
  }

  const contentLength = headers['content-length'];
  if (contentLength !== undefined) {
    if (!CONTENT_LENGTH.test(contentLength)) {
      issues.error('invalid-header', `Invalid content-length header: "${contentLength}"`, 'content-length');
    } else if (Number(contentLength) !== body.byteLength) {
      issues.error(
        'content-length-mismatch',
        `content-length header is ${contentLength} but the body is ${body.byteLength} bytes long`,
        'content-length',
      );
    }
  }

  switch (command) {
    case 'CONNECT':
    case 'STOMP':
      checkConnect(frame, issues);
      break;
    case 'CONNECTED':
      checkConnected(frame, issues);
      break;
    case 'SUBSCRIBE':
      checkAckMode(frame, version, issues);
      break;
    case 'UNSUBSCRIBE':
      if (version === '1.0' && headers['id'] === undefined && headers['destination'] === undefined) {
        issues.error('missing-header', 'UNSUBSCRIBE frames require an "id" or a "destination" header');
      }
      break;
    default:
      break;
  }

  return result(frame, issues);
}

function checkConnect({ command, headers }: Frame, issues: Issues): void {
  const acceptVersion = headers['accept-version'];
  if (acceptVersion !== undefined) {
    const tokens = acceptVersion.split(',').map((token) => token.trim());
    if (tokens.some((token) => !VERSION_TOKEN.test(token))) {
      issues.soft('invalid-header', `Invalid accept-version header: "${acceptVersion}"`, 'accept-version');
    }
    // From 1.1 onwards clients must also send a host header. Popular clients
    // (Spring's WebSocketStompClient, stomp.js) omit it, so this is only a
    // warning, even in strict mode.
    const modern = tokens.some((token) => isStompVersion(token) && token !== '1.0');
    if (modern && headers['host'] === undefined) {
      issues.warn('missing-header', `${command} frames for STOMP 1.1+ require a "host" header`, 'host');
    }
  }
  checkHeartBeat(headers['heart-beat'], issues);
}

function checkConnected({ headers }: Frame, issues: Issues): void {
  const version = headers['version'];
  if (version !== undefined && !isStompVersion(version)) {
    issues.error('invalid-header', `Unsupported version in CONNECTED frame: "${version}"`, 'version');
  }
  checkHeartBeat(headers['heart-beat'], issues);
}

function checkHeartBeat(value: string | undefined, issues: Issues): void {
  if (value !== undefined && !HEART_BEAT.test(value)) {
    issues.soft('invalid-header', `Invalid heart-beat header: "${value}"`, 'heart-beat');
  }
}

function checkAckMode({ headers }: Frame, version: StompVersion, issues: Issues): void {
  const ack = headers['ack'];
  if (ack === undefined) {
    return;
  }
  if (!ACK_MODES.has(ack)) {
    issues.error('invalid-header', `Invalid ack mode: "${ack}"`, 'ack');
  } else if (ack === 'client-individual' && version === '1.0') {
    issues.error('invalid-header', 'The client-individual ack mode does not exist in STOMP 1.0', 'ack');
  }
}

function result(frame: Frame, issues: Issues): ValidationResult<Frame> {
  return issues.failed ? { valid: false, issues: issues.list } : { valid: true, frame, issues: issues.list };
}
