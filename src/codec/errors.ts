/**
 * Base class for all errors raised by this library.
 */
export class StompError extends Error {
  override name = 'StompError';
}

export type ParseErrorCode =
  | 'invalid-eol'
  | 'invalid-header'
  | 'invalid-escape'
  | 'invalid-content-length'
  | 'invalid-utf8'
  | 'missing-null'
  | 'header-too-large'
  | 'too-many-headers'
  | 'body-too-large'
  | 'parser-failed';

/**
 * Raised by the parser when incoming data is not a well-formed STOMP frame.
 * Parse errors are fatal: the parser refuses further input until reset.
 */
export class StompParseError extends StompError {
  override name = 'StompParseError';
  readonly code: ParseErrorCode;

  constructor(code: ParseErrorCode, message: string) {
    super(message);
    this.code = code;
  }
}

export type SerializeErrorCode =
  | 'invalid-command'
  | 'unencodable-header'
  | 'content-length-mismatch'
  | 'null-in-body'
  | 'binary-body';

/**
 * Raised by the serializer when a frame cannot be represented on the wire.
 */
export class StompSerializeError extends StompError {
  override name = 'StompSerializeError';
  readonly code: SerializeErrorCode;

  constructor(code: SerializeErrorCode, message: string) {
    super(message);
    this.code = code;
  }
}
