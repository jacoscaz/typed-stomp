import { StompParseError } from './errors.ts';
import type { StompVersion } from './frame.ts';

/**
 * Whether header names and values of a frame are escaped on the wire.
 * STOMP 1.0 has no escaping at all; from 1.1 onwards every frame is escaped
 * except `CONNECT` and `CONNECTED`.
 */
export function usesHeaderEscaping(command: string, version: StompVersion): boolean {
  return version !== '1.0' && command !== 'CONNECT' && command !== 'CONNECTED';
}

const ESCAPE_1_1 = /[\\\n:]/g;
const ESCAPE_1_2 = /[\\\r\n:]/g;
const ESCAPES: Readonly<Record<string, string>> = {
  '\\': '\\\\',
  '\r': '\\r',
  '\n': '\\n',
  ':': '\\c',
};
const replaceEscape = (char: string): string => ESCAPES[char] ?? char;

/**
 * Escapes a header name or value for STOMP 1.1 or 1.2. Callers must check
 * {@link usesHeaderEscaping} first.
 */
export function escapeHeader(value: string, version: StompVersion): string {
  return value.replace(version === '1.2' ? ESCAPE_1_2 : ESCAPE_1_1, replaceEscape);
}

/**
 * Reverses {@link escapeHeader}. Undefined escape sequences are fatal, as
 * required by the specification.
 */
export function unescapeHeader(value: string, version: StompVersion): string {
  if (!value.includes('\\')) {
    return value;
  }
  let out = '';
  for (let i = 0; i < value.length; i += 1) {
    const char = value.charAt(i);
    if (char !== '\\') {
      out += char;
      continue;
    }
    i += 1;
    const next = value.charAt(i);
    switch (next) {
      case 'n':
        out += '\n';
        break;
      case 'c':
        out += ':';
        break;
      case '\\':
        out += '\\';
        break;
      case 'r':
        if (version === '1.2') {
          out += '\r';
          break;
        }
        throw invalidEscape(next, version);
      default:
        throw invalidEscape(next, version);
    }
  }
  return out;
}

function invalidEscape(char: string, version: StompVersion): StompParseError {
  const sequence = char === '' ? 'trailing backslash' : `"\\${char}"`;
  return new StompParseError('invalid-escape', `Undefined escape sequence in STOMP ${version} header: ${sequence}`);
}
