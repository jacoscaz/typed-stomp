/**
 * STOMP protocol versions supported by this library.
 */
export type StompVersion = '1.0' | '1.1' | '1.2';

/**
 * All supported versions, from oldest to newest.
 */
export const STOMP_VERSIONS: readonly StompVersion[] = Object.freeze(['1.0', '1.1', '1.2']);

const VERSION_ORDER: Readonly<Record<StompVersion, number>> = { '1.0': 0, '1.1': 1, '1.2': 2 };

export function isStompVersion(value: string): value is StompVersion {
  return value === '1.0' || value === '1.1' || value === '1.2';
}

/**
 * Compares two versions, returning a negative number, zero or a positive
 * number if `a` is older than, equal to or newer than `b`.
 */
export function compareVersions(a: StompVersion, b: StompVersion): number {
  return VERSION_ORDER[a] - VERSION_ORDER[b];
}

/**
 * Frame headers. Values set to `undefined` are skipped when serializing,
 * which makes it convenient to build frames with optional headers. Parsed
 * frames never contain `undefined` values.
 */
export type FrameHeaders = { readonly [name: string]: string | undefined };

/**
 * A STOMP frame. Bodies are always raw bytes.
 */
export interface Frame<C extends string = string, H extends FrameHeaders = FrameHeaders> {
  readonly command: C;
  readonly headers: H;
  readonly body: Uint8Array;
}

/**
 * Input accepted by the serializer: like {@link Frame}, but headers and body
 * are optional and the body may also be a string.
 */
export interface FrameInit {
  readonly command: string;
  readonly headers?: FrameHeaders;
  readonly body?: string | Uint8Array;
}
