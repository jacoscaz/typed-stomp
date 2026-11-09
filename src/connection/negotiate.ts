import { STOMP_VERSIONS, compareVersions, isStompVersion, type StompVersion } from '../codec/frame.ts';

/**
 * Parses an `accept-version` header. A missing header means the peer only
 * speaks STOMP 1.0. Unknown versions are ignored.
 */
export function parseAcceptVersion(value: string | undefined): StompVersion[] {
  if (value === undefined) {
    return ['1.0'];
  }
  const versions = new Set<StompVersion>();
  for (const token of value.split(',')) {
    const version = token.trim();
    if (isStompVersion(version)) {
      versions.add(version);
    }
  }
  return [...versions];
}

/**
 * Picks the highest version supported by both sides, given the client's
 * `accept-version` header. Returns `undefined` if there is none.
 */
export function negotiateVersion(
  acceptVersion: string | undefined,
  supported: readonly StompVersion[] = STOMP_VERSIONS,
): StompVersion | undefined {
  let best: StompVersion | undefined;
  for (const version of parseAcceptVersion(acceptVersion)) {
    if (supported.includes(version) && (best === undefined || compareVersions(version, best) > 0)) {
      best = version;
    }
  }
  return best;
}

/**
 * A `heart-beat` header value: the smallest interval (in ms) at which the
 * sender can send heart-beats, and the desired interval at which it wants to
 * receive them. Zero means "cannot send" and "does not want", respectively.
 */
export type HeartBeat = readonly [canSend: number, wantsToReceive: number];

const HEART_BEAT = /^(\d+),(\d+)$/;

/**
 * Parses a `heart-beat` header. Missing or malformed values (which frame
 * validation reports separately) mean no heart-beating: `[0, 0]`.
 */
export function parseHeartBeat(value: string | undefined): HeartBeat {
  const match = value === undefined ? null : HEART_BEAT.exec(value);
  if (match === null) {
    return [0, 0];
  }
  const canSend = Number(match[1]);
  const wantsToReceive = Number(match[2]);
  return Number.isSafeInteger(canSend) && Number.isSafeInteger(wantsToReceive) ? [canSend, wantsToReceive] : [0, 0];
}

export function formatHeartBeat([canSend, wantsToReceive]: HeartBeat): string {
  return `${Math.max(0, Math.trunc(canSend))},${Math.max(0, Math.trunc(wantsToReceive))}`;
}

export interface HeartBeatIntervals {
  /** Interval at which we must send heart-beats, or 0 for none. */
  readonly outgoing: number;
  /** Interval at which we expect to receive heart-beats, or 0 for none. */
  readonly incoming: number;
}

/**
 * Computes the heart-beat intervals for one side of the connection, from its
 * own `heart-beat` header and the peer's. The rule is symmetric, so both
 * clients and servers use it.
 */
export function negotiateHeartBeat(local: HeartBeat, remote: HeartBeat): HeartBeatIntervals {
  const [localSend, localReceive] = local;
  const [remoteSend, remoteReceive] = remote;
  return {
    outgoing: localSend > 0 && remoteReceive > 0 ? Math.max(localSend, remoteReceive) : 0,
    incoming: remoteSend > 0 && localReceive > 0 ? Math.max(remoteSend, localReceive) : 0,
  };
}
